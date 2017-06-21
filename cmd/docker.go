package cmd

import (
	"fmt"
	"github.com/cgt212/cassfs/driver"
	"github.com/docker/go-plugins-helpers/volume"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// These variables are for the flags
var (
	voldir string
)

// This is the root command that all other commands will be added to
var DockerCommand = &cobra.Command{
	Use:   "docker",
	Short: "Provide docker volume server for containers",
	Long:  `Run a docker volume daemon that can mount volumes
		specified from the docker command line`,
	Run:   docker,
}

func init() {
	//Begin cobra configuration
	DockerCommand.Flags().StringVarP(&voldir, "voldir", "v", "/var/lib/cassfs", "Root directory to mount volumes under")
	viper.BindPFlag("voldir", DockerCommand.Flags().Lookup("voldir"))
	RootCommand.AddCommand(DockerCommand)
}

func docker(cmd *cobra.Command, args []string) {
	config := driver.DriverConfig{
		Consistency: viper.GetString("consistency"),
		Keyspace:    viper.GetString("keyspace"),
		Server:      viper.GetString("server"),
		StateDir:    viper.GetString("statedir"),
		VolumeDir:   viper.GetString("voldir"),
	}
	driver := driver.NewCassFsDriver(&config)
	if driver == nil {
		panic("Got nil back for driver")
	}
	handler := volume.NewHandler(driver)
	//fmt.Println(handler.ServeUnix("root", "cassfs"))
	fmt.Println(handler.ServeUnix("root", 0))
}
