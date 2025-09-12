package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/user"
	"regexp"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

type BECCodecWrapper struct {
	BecCodec BecCodecData `msgpack:"__bec_codec__" json:"__bec_codec__"`
}

type BecCodecData struct {
	EncoderName string                 `msgpack:"encoder_name" json:"encoder_name"`
	TypeName    string                 `msgpack:"type_name" json:"type_name"`
	Data        VariableMessagePayload `msgpack:"data" json:"data"`
}

type VariableMessagePayload struct {
	MsgType  string            `msgpack:"msg_type" json:"msg_type"`
	Value    interface{}       `msgpack:"value" json:"value"`
	Metadata map[string]string `msgpack:"metadata" json:"metadata"`
}

func handleExistingData(data []byte, force bool) bool {
	var decoded BECCodecWrapper
	if err := msgpack.Unmarshal(data, &decoded); err != nil {
		fmt.Printf("Warning: Failed to decode existing message: %v\n", err)
		return true
	}

	// Show current account
	fmt.Printf("Current active account: %v\n", decoded.BecCodec.Data.Value)
	for k, v := range decoded.BecCodec.Data.Metadata {
		fmt.Printf("%s: %s\n", k, v)
	}

	if force {
		return true
	}

	// Ask for confirmation
	var input string
	fmt.Print("Are you sure you want to overwrite it? [y/N]: ")
	fmt.Scanln(&input)
	if input != "y" && input != "Y" {
		fmt.Println("Aborted, old account", decoded.BecCodec.Data.Value, "remains active.")
		return false
	}

	return true
}

func checkExistingAccount(rdb *redis.Client, ctx context.Context, key string, force bool) bool {
	// Check for existing stream data
	existing, err := rdb.XRange(ctx, key, "-", "+").Result()

	// Handle actual errors (not just "key not found")
	if err != nil && err != redis.Nil {
		fmt.Printf("Failed to check existing account: %v\n", err)
		panic(fmt.Sprintf("Redis access failed: %v", err))
	}

	// No existing stream data, proceed
	if err == redis.Nil || len(existing) == 0 {
		return true
	}

	// Extract and handle stream data - XRange returns []redis.XMessage directly
	msgData := existing[0].Values["data"]
	msgBytes, ok := msgData.(string)
	if !ok {
		fmt.Println("Warning: Unexpected data format in existing stream message")
		return true
	}

	return handleExistingData([]byte(msgBytes), force)
}

func main() {
	// CLI flags
	redisHost := flag.String("redis-host", "", "Redis host (e.g. awi-bec-001)")
	pgroup := flag.String("pgroup", "", "Process group (e.g. p16602 )")
	force := flag.Bool("force", false, "Force overwrite existing account without confirmation")
	flag.Parse()

	if *redisHost == "" {
		fmt.Println("Missing required argument: --redis-host")
		os.Exit(1)
	}
	if matched, _ := regexp.MatchString(`^p\d{5}$`, *pgroup); !matched {
		fmt.Println("Invalid --pgroup format. It must start with 'p' followed by exactly 5 digits (e.g. p12345).")
		os.Exit(1)
	}

	// Connect to Redis (default port)
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: *redisHost + ":6379",
	})

	// Test the connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		fmt.Printf("Failed to connect to Redis: %v\n", err)
		os.Exit(1)
	}

	key := "info/account"

	// Check existing account and get user confirmation if needed
	if !checkExistingAccount(rdb, ctx, key, *force) {
		os.Exit(0)
	}

	// Prepare message
	currentUser, _ := user.Current()
	now := time.Now().Format(time.RFC3339)

	msg := BECCodecWrapper{
		BecCodec: BecCodecData{
			EncoderName: "BECMessage",
			TypeName:    "VariableMessage",
			Data: VariableMessagePayload{
				MsgType: "var_message",
				Value:   *pgroup,
				Metadata: map[string]string{
					"timestamp": now,
					"user":      currentUser.Username,
				},
			},
		},
	}
	// Encode as msgpack
	packed, err := msgpack.Marshal(msg)
	if err != nil {
		fmt.Println("Failed to set account")
		panic(err)
	}

	// Set key
	if err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: key,
		Values: map[string]interface{}{"data": packed},
		MaxLen: 1,     // Keep only the latest entry
		Approx: false, // Exact trimming
	}).Err(); err != nil {
		fmt.Println("Failed to set account")
		panic(err)
	}

	fmt.Println("Account", *pgroup, "has been set successfully.")
}
