/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package logs implements the `logs` command
package logs

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/cmd"
	"sigs.k8s.io/kind/pkg/fs"
	"sigs.k8s.io/kind/pkg/log"

	"sigs.k8s.io/kind/pkg/internal/cli"
	"sigs.k8s.io/kind/pkg/internal/runtime"
)

type flagpole struct {
	Name        string
	Timeout     time.Duration
	Concurrency int
	MaxLogSize  string
}

// NewCommand returns a new cobra.Command for getting the cluster logs
func NewCommand(logger log.Logger, streams cmd.IOStreams) *cobra.Command {
	flags := &flagpole{}
	cmd := &cobra.Command{
		Args:  cobra.MaximumNArgs(1),
		Use:   "logs [output-dir]",
		Short: "Exports logs to a tempdir or [output-dir] if specified",
		Long: `Exports logs to a tempdir or [output-dir] if specified.

Collection is best effort: a node that is unresponsive does not prevent the
other nodes from being exported. Every export writes summary.json, which
records each node and step as "success", "failure" or "not-applicable".

If output-dir already exists and is not empty the command refuses to run and
leaves the existing contents untouched. A partially collected step is written
as <name>.partial and is marked as incomplete in summary.json.

Standard output and standard error of each step are merged into a single file
in arrival order. Large logs keep only their last --max-log-size bytes (0
disables truncation); truncated files are marked in summary.json.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cli.OverrideDefaultName(cmd.Flags())
			return runE(logger, streams, flags, args)
		},
	}
	cmd.Flags().StringVarP(
		&flags.Name,
		"name",
		"n",
		cluster.DefaultName,
		"the cluster context name",
	)
	cmd.Flags().DurationVar(
		&flags.Timeout,
		"timeout",
		2*time.Minute,
		"per-step timeout for log collection; a timed out step is marked incomplete and does not stop other nodes",
	)
	cmd.Flags().IntVar(
		&flags.Concurrency,
		"concurrency",
		4,
		"maximum number of nodes collected in parallel",
	)
	cmd.Flags().StringVar(
		&flags.MaxLogSize,
		"max-log-size",
		"10MiB",
		"maximum bytes retained per log file (only the tail is kept); supports B, KiB, MiB, GiB or 0 for unlimited",
	)
	return cmd
}

func runE(logger log.Logger, streams cmd.IOStreams, flags *flagpole, args []string) error {
	provider := cluster.NewProvider(
		cluster.ProviderWithLogger(logger),
		runtime.GetDefault(logger),
	)

	// Check if the cluster has any running nodes
	nodes, err := provider.ListNodes(flags.Name)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return fmt.Errorf("unknown cluster %q", flags.Name)
	}

	if flags.Concurrency < 1 {
		return fmt.Errorf("--concurrency must be at least 1, got %d", flags.Concurrency)
	}
	if flags.Timeout <= 0 {
		return fmt.Errorf("--timeout must be positive, got %s", flags.Timeout)
	}
	maxLogSize, err := ParseMaxLogSize(flags.MaxLogSize)
	if err != nil {
		return err
	}

	// get the optional directory argument, or create a tempdir
	var dir string
	if len(args) == 0 {
		t, err := fs.TempDir("", "")
		if err != nil {
			return err
		}
		dir = t
	} else {
		dir = args[0]
	}

	// Refuse a non-empty existing directory before printing the output path so
	// a refused export never claims a directory it did not write to.
	if err := cluster.CheckLogsDir(dir); err != nil {
		return err
	}

	// NOTE: the path is the output of this command to be captured by calling tools
	// whereas "Exporting logs..." is info / debug (stderr)
	logger.V(0).Infof("Exporting logs for cluster %q to:", flags.Name)
	fmt.Fprintln(streams.Out, dir)

	// collect the logs
	return provider.CollectLogsWithOptions(flags.Name, dir, cluster.LogExportOptions{
		Concurrency: flags.Concurrency,
		StepTimeout: flags.Timeout,
		MaxLogSize:  &maxLogSize,
	})
}
