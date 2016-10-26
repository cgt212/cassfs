/*
 *CassFs is a filesystem that uses Cassandra as the data store.  It is
 *meant for docker like systems that require a lightweight filesystem
 *that can be distributed across many systems.
 *Copyright (C) 2016  Chris Tsonis (cgt212@whatbroke.com)
 *
 *This program is free software: you can redistribute it and/or modify
 *it under the terms of the GNU General Public License as published by
 *the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 *This program is distributed in the hope that it will be useful,
 *but WITHOUT ANY WARRANTY; without even the implied warranty of
 *MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the 
 *GNU General Public License for more details.
 *
 *You should have received a copy of the GNU General Public License
 *along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cmd

import (
	"log"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/gocql/gocql"

	"github.com/spf13/cobra"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/spf13/viper"

	"github.com/cgt212/cassfs/cass"
)

var MountCommand = &cobra.Command{
	Use:   "mount",
	Short: "Print the configuration",
	Long:  "I don't know anymore",
	Run:   mount,
}

var (
	entry_ttl    float64
	negative_ttl float64
	fcache_ttl   int64
	consistency  string
)

func init() {
	MountCommand.Flags().Float64VarP(&entry_ttl, "entry_ttl", "t", 1.0, "fuse entry cache TTL.")
	MountCommand.Flags().Float64VarP(&negative_ttl, "negative_ttl", "n", 1.0, "fuse negative cache TTL.")
	MountCommand.Flags().Int64VarP(&fcache_ttl, "fcache_ttl", "f", 1, "File cache TTL.")
	MountCommand.Flags().StringVarP(&consistency, "consistency", "c", "ONE", "Consistency level to use (ANY,ONE,TWO,THREE,QUORUM,ALL,...)")
	viper.BindPFlag("entry_ttl", MountCommand.Flags().Lookup("entry_ttl"))
	viper.BindPFlag("negative_ttl", MountCommand.Flags().Lookup("negative_ttl"))
	viper.BindPFlag("fcache_ttl", MountCommand.Flags().Lookup("fcache_ttl"))
	viper.BindPFlag("consistency", MountCommand.Flags().Lookup("consistency"))

	RootCommand.AddCommand(MountCommand)
}

func mount(cmd *cobra.Command, args []string) {

	if len(args) != 1 {
		cmd.Usage()
		panic("Mount point required")
	}
	mount := args[0]

	//Set cstore options relating to the Database
	c := cass.NewDefaultCass()
	c.Host = strings.Split(viper.GetString("server"), ",")
	c.Keyspace = viper.GetString("keyspace")
	c.OwnerId = viper.GetInt64("owner")
	c.Consistency = gocql.ParseConsistency(viper.GetString("consistency"))
	c.Environment = viper.GetString("environment")
	c.FcacheDuration = fcache_ttl
	err := c.Init()
	if err != nil {
		log.Println("Could not initialize cluster connection:", err)
		os.Exit(1)
	}

        //The stat of the directory on the file system is being used to create the Owner and Permissions of the directory
        dinfo, err := os.Stat(mount)
        if err != nil {
                log.Println("Error opening:", err)
                os.Exit(1)
        }
	owner := fuse.Owner{
		Uid:      dinfo.Sys().(*syscall.Stat_t).Uid,
		Gid:      dinfo.Sys().(*syscall.Stat_t).Gid,
	}
	mode := uint32(dinfo.Mode())

	opts := &cass.CassFsOptions{
		Owner: owner,
		Mode:  mode,
	}

	fs := cass.NewCassFs(c, opts)
	//This section is taken directly from the examples - not fully understood
	nodeFs := pathfs.NewPathNodeFs(fs, &pathfs.PathNodeFsOptions{ClientInodes: true})
	mOpts := nodefs.Options{
		EntryTimeout:    time.Duration(entry_ttl * float64(time.Second)),
		AttrTimeout:     time.Duration(entry_ttl * float64(time.Second)),
		NegativeTimeout: time.Duration(negative_ttl * float64(time.Second)),
		PortableInodes:  false,
	}
	mountState, _, err := nodefs.MountRoot(mount, nodeFs.Root(), &mOpts)
	if err != nil {
		log.Fatal("Mount fail:", err)
	}

	mountState.SetDebug(viper.GetBool("debug"))
	mountState.Serve()
}
