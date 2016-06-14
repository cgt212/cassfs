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

package main

import "flag"
import "fmt"
import "log"
import "os"
import "syscall"
import "time"

import (
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

func main() {

	entry_ttl := flag.Float64("entry_ttl", 1.0, "fuse entry cache TTL.")
	negative_ttl := flag.Float64("negative_ttl", 1.0, "fuse negative entry cache TTL.")
	server := flag.String("server", "localhost", "Cassandra server to connect to")
	keyspace := flag.String("keyspace", "test", "Keyspace to use for the filesystem")
	ownerId := flag.Int64("owner", 1, "ID of the FS owner")
	env := flag.String("environment", "prod", "Environment to mount")
	debug := flag.Bool("debug", false, "Turn on debugging")

	//delcache_ttl := flag.Float64("deletion_cache_ttl", 5.0, "Deletion cache TTL in seconds.")
	//branchcache_ttl := flag.Float64("branchcache_ttl", 5.0, "Branch cache TTL in seconds.")

	flag.Parse()

	if len(os.Args) < 2 {
		fmt.Printf("%s requires one argument", os.Args[0])
		os.Exit(1)
	}
	mountDir := os.Args[1]

	//Set cstore options relating to the Database
	c := NewDefaultCass()
	c.Host = *server
	c.Keyspace = *keyspace
	c.OwnerId = *ownerId
	c.Environment = *env
	err := c.Init()
	if err != nil {
		fmt.Printf("Could not initialize cluster connection: %s\n", err)
		os.Exit(1)
	}

	//The stat of the directory on the file system is being used to create the Owner and Permissions of the directory
	dinfo, err := os.Stat(mountDir)
	if err != nil {
		fmt.Printf("Error opening %s: %s\n", mountDir, err)
		os.Exit(1)
	}
	owner := fuse.Owner{
		Uid:      dinfo.Sys().(*syscall.Stat_t).Uid,
		Gid:      dinfo.Sys().(*syscall.Stat_t).Gid,
	}
	mode := uint32(dinfo.Mode())

	opts := &CassFsOptions{
		Owner: owner,
		Mode:  mode,
	}

	fs := NewCassFs(c, opts)
	//This section is taken directly from the examples - not fully understood
	nodeFs := pathfs.NewPathNodeFs(fs, &pathfs.PathNodeFsOptions{ClientInodes: true})
	mOpts := nodefs.Options{
		EntryTimeout:    time.Duration(*entry_ttl * float64(time.Second)),
		AttrTimeout:     time.Duration(*entry_ttl * float64(time.Second)),
		NegativeTimeout: time.Duration(*negative_ttl * float64(time.Second)),
		PortableInodes:  false,
	}
	mountState, _, err := nodefs.MountRoot(flag.Arg(0), nodeFs.Root(), &mOpts)
	if err != nil {
		log.Fatal("Mount fail:", err)
	}

	mountState.SetDebug(*debug)
	mountState.Serve()
}
