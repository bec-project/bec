package redisconnector

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadACLFile(t *testing.T) {
	t.Parallel()

	aclFile := filepath.Join(t.TempDir(), ".bec_acl.env")
	content := "REDIS_USER=test-user\nREDIS_PASSWORD=super-secret-token"

	if err := os.WriteFile(aclFile, []byte(content), 0o600); err != nil {
		t.Fatalf("write ACL file: %v", err)
	}

	userName, aclToken, err := ReadACLFile(aclFile)
	if err != nil {
		t.Fatalf("ReadACLFile returned error: %v", err)
	}

	if userName != "test-user" {
		t.Fatalf("userName = %q, want %q", userName, "test-user")
	}

	if aclToken != "super-secret-token" {
		t.Fatalf("aclToken = %q, want %q", aclToken, "super-secret-token")
	}
}
