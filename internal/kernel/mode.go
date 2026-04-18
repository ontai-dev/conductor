// Package kernel implements the Core Kernel Layer: process lifecycle, mode
// dispatch, fatal invariant enforcement, and structured exit control.
// conductor-design.md §2.1.
package kernel

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// invariantViolationExit is the structured JSON document written to stderr before
// exiting with code 2 on an InvariantViolation. conductor-design.md §6.1.
type invariantViolationExit struct {
	Category  string    `json:"category"`
	Reason    string    `json:"reason"`
	Timestamp time.Time `json:"timestamp"`
}

// ExitInvariantViolation writes a structured InvariantViolation to stderr and
// terminates the process with exit code 2. This is called when a mode boundary
// is crossed — specifically when the Conductor binary is invoked with compile
// mode. INV-023, conductor-design.md §2.1.
//
// This function never returns.
func ExitInvariantViolation(reason string) {
	doc := invariantViolationExit{
		Category:  string(runnerlib.InvariantViolation),
		Reason:    reason,
		Timestamp: time.Now().UTC(),
	}
	data, err := json.Marshal(doc)
	if err != nil {
		fmt.Fprintf(os.Stderr, `{"category":"InvariantViolation","reason":"%s"}`, reason)
	} else {
		fmt.Fprintf(os.Stderr, "%s\n", data)
	}
	os.Exit(2)
}

// GuardNotCompileMode calls ExitInvariantViolation if mode is ModeCompile.
// Called at Conductor binary startup before any other initialization.
// INV-023: compile mode attempted on Conductor causes immediate InvariantViolation.
func GuardNotCompileMode(mode config.Mode) {
	if mode == config.ModeCompile {
		ExitInvariantViolation(
			"INV-023: Conductor binary does not support compile mode; " +
				"use the Compiler binary for compile operations",
		)
	}
}
