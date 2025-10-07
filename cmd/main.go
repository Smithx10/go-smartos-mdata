package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"smartos-mdata/mdata"
)

const (
	ERROR_NOTFOUND = "request failed with code: NOTFOUND"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "mdata",
		Short: "SmartOS metadata client",
	}

	var getCmd = &cobra.Command{
		Use:   "get [key]",
		Short: "Get a metadata key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCommand(func(client mdata.MetadataClient) (string, error) {
				return client.Get(args[0])
			})
		},
	}

	var keysCmd = &cobra.Command{
		Use:   "keys",
		Short: "List metadata keys with optional prefix",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCommand(func(client mdata.MetadataClient) (string, error) {
				return client.Keys()
			})
		},
	}

	var putCmd = &cobra.Command{
		Use:   "put [key] [value]",
		Short: "Put a metadata key-value pair",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCommand(func(client mdata.MetadataClient) (string, error) {
				if err := client.Put(args[0], args[1]); err != nil {
					return "", err
				}
				return "", nil
			})
		},
	}

	var deleteCmd = &cobra.Command{
		Use:   "delete [key]",
		Short: "Delete a metadata key",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCommand(func(client mdata.MetadataClient) (string, error) {
				if err := client.Delete(args[0]); err != nil {
					return "", err
				}
				return "", nil
			})
		},
	}

	rootCmd.AddCommand(getCmd, keysCmd, putCmd, deleteCmd)
	rootCmd.SilenceUsage = true
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// runCommand executes a metadata operation with the given key and optional value
func runCommand(op func(mdata.MetadataClient) (string, error)) error {
	cfg := mdata.DefaultClientConfig()
	client, err := mdata.NewMetadataClient(cfg)
	if err != nil {
		return fmt.Errorf("failed to create client (%s): %w", cfg.Transport, err)
	}
	defer client.Close()

	result, err := op(client)
	if err != nil {
		switch err.Error() {
		case ERROR_NOTFOUND:
			return err
		}
		return err
	}
	if result != "" {
		fmt.Println(result)
	}
	return nil
}
