package redisconnector

import (
	"context"
	"fmt"
	"os"
)

// ReadACLFile reads a .env-style ACL file with REDIS_USER and REDIS_PASSWORD.
func ReadACLFile(filePath string) (string, string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", "", err
	}
	defer file.Close()

	var userName, aclToken string
	_, err = fmt.Fscanf(file, "REDIS_USER=%s\nREDIS_PASSWORD=%s", &userName, &aclToken)
	if err != nil {
		return "", "", err
	}

	return userName, aclToken, nil
}

// ConnectWithOptionalACL creates a client and verifies it with ping.
// If ACL credentials are provided but fail, it retries with the default user.
func ConnectWithOptionalACL(
	ctx context.Context, host string, port int, aclFile string,
) (*Client, error) {
	if aclFile == "" {
		client := New(host, port)
		if err := client.Ping(ctx); err != nil {
			_ = client.Close()
			return nil, err
		}
		return client, nil
	}

	userName, aclToken, err := ReadACLFile(aclFile)
	if err != nil {
		return nil, err
	}

	client := NewWithAuth(host, port, userName, aclToken)
	if err := client.Ping(ctx); err == nil {
		return client, nil
	}

	_ = client.Close()
	client = New(host, port)
	if err := client.Ping(ctx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf(
			"failed to connect with provided ACL credentials and fallback default user: %w",
			err,
		)
	}

	return client, nil
}
