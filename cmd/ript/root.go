package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

const (
	outputTable = "table"
	outputJSON  = "json"
)

var rootCmd = &cobra.Command{
	Use:   "ript",
	Short: "RIPT - Reclaimer of Inactive Partitions and Topics",
	Long:  "RIPT runs either as a long-lived daemon or as a CLI utility for listing and scanning topic usage state.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return runDaemon(cmd.Context())
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.SilenceUsage = true
	rootCmd.AddCommand(newDaemonCmd())
	rootCmd.AddCommand(newListCmd())
	rootCmd.AddCommand(newScanCmd())
}

func validateOutputMode(value string) error {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case outputTable, outputJSON:
		return nil
	default:
		return fmt.Errorf("invalid output format %q: expected %q or %q", value, outputTable, outputJSON)
	}
}
