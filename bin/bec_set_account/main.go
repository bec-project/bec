package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/user"
	"regexp"
	"time"

	"bec_lib_go/endpoints"
	"bec_lib_go/messages"
	"bec_lib_go/redisconnector"

	"github.com/redis/go-redis/v9"
)

func handleExistingData(data []byte, force bool) bool {
	decoded, err := messages.DecodeVariableMessage(data)
	if err != nil {
		fmt.Printf("Warning: Failed to decode existing message: %v\n", err)
		return true
	}

	// Show current account
	fmt.Printf("Current active account: %v\n", decoded.Value)
	for k, v := range decoded.Metadata {
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
		fmt.Println("Aborted, old account", decoded.Value, "remains active.")
		return false
	}

	return true
}

func checkExistingAccount(rdb *redisconnector.Client, ctx context.Context, key string, force bool) bool {
	// Check for existing stream data
	existing, err := rdb.XRange(ctx, key)

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
	rdb := redisconnector.New(*redisHost, 6379)
	defer rdb.Close()

	// Test the connection
	if err := rdb.Ping(ctx); err != nil {
		fmt.Printf("Failed to connect to Redis: %v\n", err)
		os.Exit(1)
	}

	key := endpoints.Account

	// Check existing account and get user confirmation if needed
	if !checkExistingAccount(rdb, ctx, key, *force) {
		os.Exit(0)
	}

	// Prepare message
	currentUser, _ := user.Current()
	now := time.Now().Format(time.RFC3339)

	packed, err := messages.Encode(messages.NewVariableMessage(*pgroup, map[string]string{
		"timestamp": now,
		"user":      currentUser.Username,
	}))
	if err != nil {
		fmt.Println("Failed to set account")
		panic(err)
	}

	// Set key
	if err := rdb.AddStreamMessage(ctx, key, packed, 1, false); err != nil {
		fmt.Println("Failed to set account")
		panic(err)
	}

	fmt.Println("Account", *pgroup, "has been set successfully.")
}
