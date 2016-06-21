#CassFS

CassFS is a fuse filesystem that uses [Cassandra](http://cassandra.apache.org/) as the storage engine.  It has a concept of owners and environments to support multi-tenency.  The idea is based on the concept of the [Valhalla filesystem](https://pantheon.io/blog/inside-pantheon-valhalla-filesystem) created by Pantheon.  Except that they use a webdav mounted filesystem, and a lot on the server side.  CassFS interacts directly with Cassandra.

All of the normal rules apply when talking about any new filesystem.

* Do not keep important data on it, you will likely lose any data at this point
* The schema and data structures are not stable, so breaking changes will likely occur
* There is no guarantee or warranty for stability or safety in using this software

####Why?
To be able to run something like drupal or wordpress in a container without having to set up a distributed file system.  If you need to support running a database or some other persistent datastore with large files, this is not intended for that use.  The blog from Pantheon does a very good job describing the type of behaviour that this is suited for.  It is best for smaller files, more reads than writes, and when there are writes they are of the entire file.

####Use

This is how I have been testing it, running an apache/php container and drupal from the file system.  

Start it with `cassfs [options] <mountpoint>`

To stop it run `fusermout -u <moutnpoint>`
The options are:
```
-debug=false: Turn on debugging
-entry_ttl=1: fuse entry cache TTL.
-environment="prod": Environment to mount
-keyspace="test": Keyspace to use for the filesystem
-negative_ttl=1: fuse negative entry cache TTL.
-owner=1: ID of the FS owner
-server="localhost": Cassandra server to connect to
```

####Example Usage with Local Caching
[go-fuse](https://github.com/hanwen/go-fuse), one of the required modules includes a unionfs example.  This will locally cache files, using both should provide a significant performance boost for sites with enough traffic where the cache would be able to serve files.  

In my testing on CentOS, you need to be sure to run this to allow docker to be able to read from a fuse mount
`setsebool virt_use_fusefs true`

####Todo
This is still very early on, so there are surely many thing that I have not even thought of.  This list is really too long to get right on the first pass, but here are a few of the things.
1. Testing
2. Caching?
3. docker/kubernetes integration
4. Mount script
5. More robust options handling (Environment variables)
