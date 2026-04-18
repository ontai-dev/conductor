package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// WS1 — Domain subcommand unit tests.
// The domain subcommand calls os.Exit(1) inline in main(), so it cannot be
// tested directly. The standard Go subprocess pattern is used: a child process
// is spawned that runs just the domain path, and the parent asserts the exit code.

// TestDomain_ExitCode1WithReservedMessage verifies that invoking the domain
// subcommand produces:
//   - exit code 1
//   - stderr output containing "reserved"
//
// conductor-schema.md §9: domain is a reserved subcommand, not yet implemented.
func TestDomain_ExitCode1WithReservedMessage(t *testing.T) {
	if os.Getenv("COMPILER_TEST_SUBPROCESS_DOMAIN") == "1" {
		// Child process: invoke main() with domain args.
		os.Args = []string{"compiler", "domain"}
		main()
		return
	}

	// Parent process: run this test as a subprocess.
	cmd := exec.Command(os.Args[0], "-test.run=TestDomain_ExitCode1WithReservedMessage")
	cmd.Env = append(os.Environ(), "COMPILER_TEST_SUBPROCESS_DOMAIN=1")

	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected exit code 1; process exited 0")
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("unexpected error type: %T: %v", err, err)
	}
	if exitErr.ExitCode() != 1 {
		t.Errorf("exit code: got %d; want 1", exitErr.ExitCode())
	}

	output := string(out)
	if !strings.Contains(output, "reserved") {
		t.Errorf("stderr does not contain 'reserved': %q", output)
	}
}
