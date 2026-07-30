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
)

func handleExistingData(data []byte, force bool) bool {
	decodedAny, err := messages.Decode(data)
	if err != nil {
		fmt.Printf("Warning: Failed to decode existing message: %v\n", err)
		return true
	}
	decoded, ok := decodedAny.(messages.VariableMessage)
	if !ok {
		fmt.Printf("Warning: Unexpected decoded message type: %T\n", decodedAny)
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

func checkExistingAccount(
	ctx context.Context, rdb *redisconnector.Client, endpoint endpoints.EndpointInfo, force bool,
) bool {
	// Check for existing stream data
	existing, err := rdb.XRange(ctx, endpoint)

	// Handle actual errors.
	if err != nil {
		fmt.Printf("Failed to check existing account: %v\n", err)
		panic(fmt.Sprintf("Redis access failed: %v", err))
	}

	// No existing stream data, proceed.
	if len(existing) == 0 {
		return true
	}

	return handleExistingData(existing[0].Data, force)
}

func main() {
	// CLI flags
	redisHost := flag.String("redis-host", "", "Redis host (e.g. awi-bec-001)")
	pgroup := flag.String("pgroup", "", "Process group (e.g. p16602 )")
	force := flag.Bool("force", false, "Force overwrite existing account without confirmation")
	aclFile := flag.String("acl-file", "", "Path to ACL file (optional)")
	flag.Parse()

	if *redisHost == "" {
		fmt.Println("Missing required argument: --redis-host")
		os.Exit(1)
	}
	if matched, _ := regexp.MatchString(`^p\d{5}$`, *pgroup); !matched {
		fmt.Println("Invalid --pgroup format. It must start with 'p' followed by exactly 5 digits (e.g. p12345).")
		os.Exit(1)
	}

	mainCtx := context.Background()

	// We set the timeout to 30 seconds. Should be plenty of time for this
	ctx, cancel := context.WithTimeout(mainCtx, 30*time.Second)
	defer cancel()

	// Connect to Redis (default port)
	rdb, err := redisconnector.ConnectWithOptionalACL(ctx, *redisHost, 6379, *aclFile)
	if err != nil {
		fmt.Printf("Failed to connect to Redis: %v\n", err)
		os.Exit(1)
	}
	defer rdb.Close()

	// Check existing account and get user confirmation if needed
	if !checkExistingAccount(ctx, rdb, endpoints.Account, *force) {
		os.Exit(0)
	}

	// Prepare message
	currentUser, _ := user.Current()
	now := time.Now().Format(time.RFC3339)

	message := messages.VariableMessage{
		Value: *pgroup,
		Metadata: map[string]string{
			"timestamp": now,
			"user":      currentUser.Username,
		},
	}

	// Set key
	if err := rdb.AddStreamMessage(ctx, endpoints.Account, message, 1, false); err != nil {
		fmt.Println("Failed to set account")
		panic(err)
	}

	fmt.Println("Account", *pgroup, "has been set successfully.")
}
