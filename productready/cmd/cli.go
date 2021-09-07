package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"mraft/productready"
	"mraft/productready/config"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
)

var (
	clusterList     = []string{}
	dataDir         string
	discoverAddress = "auto"
	native          bool
	pidFile                = "raft.pid"
	raftPort        uint16 = 0

	tryRun  = false
	version = false
	verbose = 0
	silent  = false
)

var cli = &cobra.Command{
	ValidArgs: []string{"init", "join"},
	Args:      cobra.OnlyValidArgs,
	Use:       os.Args[0],
	PreRunE: func(cmd *cobra.Command, args []string) error {
		ip, err := getIP(discoverAddress)
		if err != nil {
			return err
		}
		discoverAddress = ip
		if args[0] == "init" {
			ipList := make([]string, 0, len(clusterList))
			for _, s := range clusterList {
				if strings.TrimSpace(s) == "" {
					continue
				}
				if strings.ContainsRune(s, ',') {
					ipList = append(ipList, strings.Split(s, ",")...)
				} else {
					ipList = append(ipList, s)
				}
			}
			if len(ipList) == 0 {
				return errors.New("must set cluster-address")
			}
			clusterList = ipList
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		switch args[0] {
		case "init":
			native = true
		case "join":
			native = false
		}
		cfg := &config.DynamicConfig{
			RaftDir:   dataDir,
			RaftPeers: clusterList,
			Native:    native,
			IP:        discoverAddress,
			RaftPort:  raftPort,
		}
		Run(cfg)
	},
}

func init() {
	cli.PersistentFlags().BoolVarP(&version, "version", "V", false, "show version and build info.")
	cli.PersistentFlags().CountVarP(&verbose, "verbose", "v", "set log level [ 0=warn | 1=info | 2=debug ].")
	cli.PersistentFlags().BoolVarP(&silent, "silent", "s", false, "set log level to error.")
	cli.PersistentFlags().BoolVarP(&tryRun, "try-run", "t", false, "dump config but not serve.")

	cli.PersistentFlags().StringVarP(&dataDir, "data-dir", "d", "./data", "set raft data dir")

	cli.PersistentFlags().StringVarP(&discoverAddress, "discover-address", "i", discoverAddress, "address exported to others")
	cli.PersistentFlags().Uint16VarP(&raftPort, "port-cluster", "p", 13890, "raft port exported to cluster, port-grpc=raft-grpc + 1")

	cli.Flags().StringArrayVarP(&clusterList, "cluster", "c", nil, "add cluster node <host:port> | <host:port,host:port...>")

	_ = cli.MarkPersistentFlagDirname("data-dir")

	cli.SetUsageTemplate(`Usage:{{if .Runnable}}
  {{.UseLine}} <init|join>{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

Aliases:
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}

Available Commands:{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasHelpSubCommands}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`)
}

func Exec() {
	err := cli.Execute()
	if err != nil {
		log.Fatalln(err)
	}
}

func Run(cfg *config.DynamicConfig) {
	bs, _ := json.MarshalIndent(cfg, "", "    ")
	fmt.Printf("config: %s\n", bs)
	if tryRun {
		os.Exit(0)
	}
	eg := productready.NewEngine(cfg)
	SavePidToFile()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	log.Println(<-signals)

	eg.Stop()
}
