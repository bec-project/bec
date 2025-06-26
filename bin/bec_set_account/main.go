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

func main() {
	// CLI flags
	redisHost := flag.String("redis-host", "", "Redis host (e.g. awi-bec-001)")
	pgroup := flag.String("pgroup", "", "Process group (e.g. p16602 )")
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
	key := "info/account"

	// Check existing account
	existing, err := rdb.Get(ctx, key).Bytes()
	if err == nil {
		var decoded BECCodecWrapper
		if err := msgpack.Unmarshal(existing, &decoded); err != nil {
			fmt.Println("Warning: Failed to decode existing message:", err)
		} else {
			fmt.Println("Current active account", decoded.BecCodec.Data.Value)
			for k, v := range decoded.BecCodec.Data.Metadata {
				fmt.Printf("%s: %s\n", k, v)
		}

		var input string
		fmt.Print("Are you sure you want to overwrite it? [y/N]: ")
		fmt.Scanln(&input)
		if input != "y" && input != "Y" {
			fmt.Println("Aborted, old account", decoded.BecCodec.Data.Value, "remains active.")
			os.Exit(0)
		}
	}
	} else if err != redis.Nil {
		// An actual error (not just "key not found")
		fmt.Println("Failed to set account")
		panic(err)
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
	if err := rdb.Set(ctx, key, packed, 0).Err(); err != nil {
		fmt.Println("Failed to set account")
		panic(err)
	}

	fmt.Println("Account", *pgroup, "has been set successfully.")
}
