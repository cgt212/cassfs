package main

import "flag"
import "fmt"
import "log"
import "os"
import "time"

import (
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

func main() {

	entry_ttl := flag.Float64("entry_ttl", 1.0, "fuse entry cache TTL.")
	negative_ttl := flag.Float64("negative_ttl", 1.0, "fuse negative entry cache TTL.")
	server := flag.String("server", "localhost", "Cassandra server to connect to")
	keyspace := flag.String("keyspace", "test", "Keyspace to use for the filesystem")

	//delcache_ttl := flag.Float64("deletion_cache_ttl", 5.0, "Deletion cache TTL in seconds.")
	//branchcache_ttl := flag.Float64("branchcache_ttl", 5.0, "Branch cache TTL in seconds.")

	flag.Parse()

	if len(os.Args) < 2 {
		fmt.Printf("%s requires one argument", os.Args[0])
		os.Exit(1)
	}
	mountDir := os.Args[1]
	fmt.Printf("Should be mounting to: %s (%s)\n", mountDir, flag.Arg(0))

	c := NewDefaultCass()
	c.Host = *server
	c.Keyspace = *keyspace
	err := c.Init()
	if err != nil {
		fmt.Printf("Could not initialize cluster connection: %s\n", err)
		os.Exit(1)
	}
	fs := NewCassFs(c, nil)
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

	mountState.SetDebug(true)
	mountState.Serve()
}
