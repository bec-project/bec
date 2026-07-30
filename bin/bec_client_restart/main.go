package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"bec_lib_go/endpoints"
	"bec_lib_go/messages"
	"bec_lib_go/redisconnector"
)

func main() {
	redisHost := flag.String("redis-host", "", "Redis host (e.g. awi-bec-001)")
	redisPort := flag.Int("redis-port", 6379, "Redis port")
	aclFile := flag.String("acl-file", "", "Path to ACL file (optional)")
	reason := flag.String("reason", "", "Reason to include in the client restart message")
	flag.Parse()

	if *redisHost == "" {
		fmt.Println("Missing required argument: --redis-host")
		os.Exit(1)
	}

	ctx := context.Background()
	rdb, err := redisconnector.ConnectWithOptionalACL(ctx, *redisHost, *redisPort, *aclFile)
	if err != nil {
		fmt.Printf("Failed to connect to Redis: %v\n", err)
		os.Exit(1)
	}
	defer rdb.Close()

	if err := rdb.Publish(ctx, endpoints.ClientRestart, messages.ClientRestartMessage{
		Reason:   *reason,
		Metadata: map[string]string{},
	}); err != nil {
		fmt.Printf("Failed to publish restart message: %v\n", err)
		os.Exit(1)
	}

	if *reason != "" {
		fmt.Printf(
			"Published ClientRestartMessage to %s with reason: %s\n",
			endpoints.ClientRestart.Topic,
			*reason,
		)
		return
	}
	fmt.Printf("Published ClientRestartMessage to %s.\n", endpoints.ClientRestart.Topic)
}
