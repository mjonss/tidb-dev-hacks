package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	subcmd := os.Args[1]
	switch subcmd {
	case "setup":
		cfg := &Config{}
		fs := flag.NewFlagSet("setup", flag.ExitOnError)
		RegisterFlags(fs, cfg)
		fs.Parse(os.Args[2:])
		if err := cfg.Validate(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if err := runSetup(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "profile":
		cfg := &Config{}
		fs := flag.NewFlagSet("profile", flag.ExitOnError)
		RegisterFlags(fs, cfg)
		fs.Parse(os.Args[2:])
		if err := cfg.Validate(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if err := runProfile(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "help", "-h", "--help":
		printUsage()

	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", subcmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage: analyze-profile <subcommand> [flags]

Subcommands:
  setup    Create a partitioned table and insert test data
  profile  Run ANALYZE TABLE and collect performance data

Use "analyze-profile <subcommand> -h" for subcommand flags.
`)
}
