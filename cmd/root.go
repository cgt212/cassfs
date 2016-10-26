package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// These options are for the global flags
var (
	server      string
	keyspace    string
	environment string
	statedir    string
	owner       int
)

// This is the root command that all other commands will be added to
var RootCommand = &cobra.Command{
	Use:   "cassfs",
	Short: "CassFS is a user space file system for multi-client mounts",
	Long:  `A filesystem that uses Cassandra as the datastore
		and is able to be mounted by multiple clients.`,
}

func init() {
	//Begin cobra configuration
	RootCommand.PersistentFlags().StringVarP(&server, "server", "s", "localhost", "Server to connect to, separate multiple servers with a \",\"")
	RootCommand.PersistentFlags().StringVarP(&keyspace, "keyspace", "k", "cassfs", "Keyspace to use for cassandra")
	RootCommand.PersistentFlags().StringVar(&statedir, "statedir", "/var/run/cassfs", "Directory to use for state")
	RootCommand.PersistentFlags().IntVarP(&owner, "owner", "o", 1, "Owner ID")
	RootCommand.PersistentFlags().StringVarP(&environment, "environment", "e", "production", "Environment to mount")
	RootCommand.PersistentFlags().Bool("debug", false, "Enable debugging")
	//Begin viper configuration
	viper.SetEnvPrefix("CASSFS")
	viper.AutomaticEnv()
	//End viper configuration
	//Read from a config file
	viper.SetConfigName("cassfs")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("/etc/cassfs")
	viper.AddConfigPath("$HOME/.cassfs")
	viper.AddConfigPath(".")
	//Begin viper/cobra integration
	viper.BindPFlag("server", RootCommand.PersistentFlags().Lookup("server"))
	viper.BindPFlag("statedir", RootCommand.PersistentFlags().Lookup("statedir"))
	viper.BindPFlag("keyspace", RootCommand.PersistentFlags().Lookup("keyspace"))
	viper.BindPFlag("owner", RootCommand.PersistentFlags().Lookup("owner"))
	viper.BindPFlag("environment", RootCommand.PersistentFlags().Lookup("environment"))
	viper.BindPFlag("debug", RootCommand.PersistentFlags().Lookup("debug"))
}
