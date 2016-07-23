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

package cass


import (
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/gocql/gocql"
	"github.com/golang/groupcache"
)


//Setting the blocksize to 1M for now
const BLOBSIZE = 1024 * 1024

type CassMetadata struct {
	Attr  *fuse.Attr
	XAttr map[string]string
}

type CassFsMetadata struct {
	Metadata  CassMetadata
	Timestamp int64
	Hash      []byte
}

type Cass struct {
	Host           string
	Port           int
	ProtoVersion   int
	Keyspace       string
	OwnerId        int64
	Environment    string
	CacheEnabled   bool
	CacheSize      int64
	FcacheDuration int64
	cache          *groupcache.Group
	cluster        *gocql.ClusterConfig
	cacheLock      sync.RWMutex
	fileCache      map[string]*CassFsMetadata
	uuidLock       sync.RWMutex
	uuidCache      map[string]string
	session        *gocql.Session
}

func NewDefaultCass() *Cass {
	return &Cass{
		Host:           "localhost",
		Port:           1234,
		ProtoVersion:   4,
		Keyspace:       "cstore",
		OwnerId:        1,
		Environment:    "prod",
		FcacheDuration: 60,
	}
}

//ShaSum calculates the SHA512 of a byte array
func ShaSum(data []byte) []byte {
	hash512 := sha512.New()
	hash512.Write(data)
	return hash512.Sum(nil)
}

//splitPath accepts a string argument that it will split into a directory and filename
func (c *Cass) splitPath(path string) (string, string) {
	_path := path
	if strings.HasSuffix(path, "/") {
		_path = path[:len(path)-1]
	}
	idx := strings.LastIndex(_path, "/")
	if idx > 0 {
		parentDir := _path[:idx]
		parent, err := c.FindDir(parentDir)
		if err != nil {
			log.Printf("Unable to split and find parent: %s\n", err)
		}
		child := _path[idx+1:len(_path)]
		return parent, child
	}
	if strings.HasPrefix(_path, "/") {
		return "", _path[1:]
	}
	return "", _path
}

//SplitPath is a globally accessible version of splitPath
//func SplitPath(path string) (*gocql.UUID, string) {
//	return splitPath(path)
//}

//Init initializes the connection to the Cassandra server
func (c *Cass) Init() error {
	c.cluster = gocql.NewCluster(c.Host)
	c.cluster.ProtoVersion = 4
	c.cluster.Keyspace = c.Keyspace
	session, err := c.cluster.CreateSession()
	if err != nil {
		return err
	}
	c.fileCache = make(map[string]*CassFsMetadata)
	c.uuidCache = make(map[string]string)
	if c.CacheEnabled {
		var getterFunc = func(ctx groupcache.Context, key string, dest groupcache.Sink) error {
			cass := ctx.(*Cass)
			data, err := cass.ReadData([]byte(key))
			if err != nil {
				return err
			}
			dest.SetBytes(data)
			return nil
		}
		groupName := fmt.Sprintf("%d:%s", c.OwnerId, c.Environment)
		c.cache = groupcache.NewGroup(groupName, c.CacheSize, groupcache.GetterFunc(getterFunc))
	}
	c.session = session
	return nil
}

//FindDir will find the UUID of the directory
func (c *Cass) FindDir(dir string) (string, error) {
	var parentBytes []byte
	log.Printf("Finding %s ...\n", dir)
	c.uuidLock.RLock()
	entry, ok := c.uuidCache[dir]
	c.uuidLock.RUnlock()
	if ok {
		return entry, nil
	}
	paths := strings.Split(dir, "/")
	log.Printf("Searching for %s:%s", "", paths[0])
	//We have to bootstrap the lookup process by finding the first-level directory
	err := c.session.Query("SELECT hash FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ?", c.OwnerId, c.Environment, "", paths[0]).Consistency(gocql.One).Scan(&parentBytes)
	if err != nil {
		log.Printf("There was an error finding the root dir:%s\n", err)
		return "", err
	}
	parent, err := gocql.UUIDFromBytes(parentBytes)
	if err != nil {
		log.Printf("Unable to parse UUID from bytes: %s\n", err)
		return "", err
	}
	for _, d := range paths[1:] {
		log.Printf("Searching for %s:%s", parent.String(), d)
		err = c.session.Query("SELECT hash FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ?", c.OwnerId, c.Environment, parent.String(), d).Consistency(gocql.One).Scan(&parentBytes)
		if err != nil {
			log.Printf("There was an error finding the root dir:%s\n", err)
			return "", err
		}
		parent, err = gocql.UUIDFromBytes(parentBytes)
		if err != nil {
			log.Printf("Unable to parse UUID from bytes: %s\n", err)
			return "", err
		}
	}
	c.uuidLock.Lock()
	c.uuidCache[dir] = parent.String()
	c.uuidLock.Unlock()
	return parent.String(), nil
}

//These are the new rounds of functions on the storage

//incrementDataRef updates the reference count on a data row when new files reference it
func (c *Cass) incrementDataRef(hash []byte) error {
	return c.session.Query("UPDATE fileref SET refs = refs + 1 WHERE hash = ?", hash).Exec()
}

//decrementDataRef updates the reference count on a data row when files that reference it are deleted or modified
func (c *Cass) decrementDataRef(hash []byte) error {
	return c.session.Query("UPDATE fileref SET refs = refs - 1 WHERE hash = ?", hash).Exec()
}

//GetFiledata looks up the file path in name and returns the Metadata or an error
func (c *Cass) GetFiledata(name string) (*CassFsMetadata, error) {
	var meta CassMetadata
	var metajson, hash []byte
	parent, file := c.splitPath(name)
	c.cacheLock.RLock()
	entry, ok := c.fileCache[name]
	c.cacheLock.RUnlock()
	if ok {
		now := time.Now()
		if now.Unix() - entry.Timestamp < c.FcacheDuration {
			return entry, nil
		} else {
			c.cacheLock.Lock()
			delete(c.fileCache, name)
			c.cacheLock.Unlock()
		}
	}
	err := c.session.Query("SELECT hash, metadata FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ?", c.OwnerId, c.Environment, parent, file).Consistency(gocql.One).Scan(&hash, &metajson)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(metajson, &meta)
	ret := &CassFsMetadata{
		Metadata: meta,
		Hash: hash,
		Timestamp: time.Now().Unix(),
	}
	c.cacheLock.Lock()
	c.fileCache[name] = ret
	c.cacheLock.Unlock()
	return ret, nil
}

//CreateFile creates the file that will be a reference to a data row it will store the path, attributes and the hash
func (c *Cass) CreateFile(name string, attr *fuse.Attr, hash []byte) error {
	meta, err := json.Marshal(CassMetadata{
		Attr:  attr,
		XAttr: nil,
	})
	if err != nil {
		log.Printf("Encoding error on metadata: %s\n", err)
		return err
	}
	dir, file := c.splitPath(name)
	err = c.session.Query("INSERT INTO filesystem (cust_id, environment, directory, name, hash, metadata) VALUES(?, ?, ?, ?, ?, ?)", c.OwnerId, c.Environment, dir, file, hash, meta).Consistency(gocql.One).Exec()
	if err != nil {
		return err
	}
	if len(hash) > 0 {
		err = c.incrementDataRef(hash)
	}
	return err
}

//Rename changes the filename in cassandra
func (c *Cass) Rename(oldName string, newName string) error {
	var hash []byte
	var meta []byte
	oldDir, oldFile := c.splitPath(oldName)
	newDir, newFile := c.splitPath(newName)

	err := c.session.Query("SELECT hash, metadata FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ?", c.OwnerId, c.Environment, oldDir, oldFile).Consistency(gocql.One).Scan(&hash, &meta)
	if err != nil {
		log.Printf("Error finding file to move from: %s\n", err)
		return err
	}
	err = c.session.Query("INSERT INTO filesystem (cust_id, environment, directory, name, hash, metadata) VALUES(?, ?, ?, ?, ?, ?)", c.OwnerId, c.Environment, newDir, newFile, hash, meta).Consistency(gocql.One).Exec()
	if err != nil {
		log.Printf("Error inserting new file: %s\n", err)
		return err
	}
	err = c.session.Query("DELETE FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ?", c.OwnerId, c.Environment, oldDir, oldFile).Consistency(gocql.One).Exec()
	//Skipping an error, because at this point the rename was completed.

	return nil
}

//Handling a directory rename will not work in the current setup.
//This function will need to be updated to handle directories differently
//func (c *Cass) RenameDir(oldName string, newName string) error {
//	var name string
//	var hash, meta []byte
//
//	oldParent, oldDir := c.splitPath(oldName)
//	newParent, newDir := c.splitPath(newName)
//
//	batch := gocql.NewBatch(gocql.LoggedBatch)
//
//	//Get the list of directory entries to rename
//	iter := c.session.Query("SELECT name, hash, metadata FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ?", c.OwnerId, c.Environment, oldParent).Iter()
//	for iter.Scan(&name, &hash, &meta) {
//		batch.Query("INSERT INTO filesystem (cust_id, environment, directory, name, hash, metadata) VALUES(?, ?, ?, ?, ?, ?)", c.OwnerId, c.Environment, newParent, name, hash, meta)
//	}
//	if err := iter.Close(); err != nil {
//		log.Printf("Error iterating over bulk insert: %s\n", err)
//		return err
//	}
//	err := c.session.ExecuteBatch(batch)
//	if err != nil {
//		log.Printf("Error inserting batch: %s\n", err)
//		return err
//	}
//	return nil
//}

func (c *Cass) WriteMetadata(path string, meta CassMetadata) error {
	dir, file := c.splitPath(path)

	metab, err := json.Marshal(meta)
	if err != nil {
		log.Printf("Error encoding metadata: %s\n", err)
		return err
	}

	c.cacheLock.RLock()
	_, ok := c.fileCache[path]
	c.cacheLock.RUnlock()
	if ok {
		c.cacheLock.Lock()
		delete(c.fileCache, path)
		c.cacheLock.Unlock()
	}

	err = c.session.Query("UPDATE filesystem SET metadata = ? WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ?", metab, c.OwnerId, c.Environment, dir, file).Consistency(gocql.One).Exec()
	return err
}

//UpdateFile Updates the attributes and data hash when a file changes
func (c *Cass) UpdateFile(f *CassFileData) error {
	parent, file := c.splitPath(f.Name)
	hash, err := c.WriteFileData(f.Data)
	if err != nil {
		log.Printf("Error writing Data: %s\n", err)
		return err
	}
	old_hash := f.Hash
	f.Hash = hash
	meta, err := json.Marshal(CassMetadata{
		Attr: f.Attr,
	})
	if err != nil {
		log.Printf("Encoding error: %s\n", err)
		return err
	}
	err = c.session.Query("UPDATE filesystem SET hash=?, metadata=? WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ?", f.Hash, meta, c.OwnerId, c.Environment, parent, file).Consistency(gocql.One).Exec()
	if err != nil {
		return err
	}
	err = c.incrementDataRef(hash)
	if len(old_hash) > 0 {
		c.decrementDataRef(old_hash)
	}
	if err != nil {
		return err
	}
	c.cacheLock.RLock()
	_, ok := c.fileCache[f.Name]
	c.cacheLock.RUnlock()
	if ok {
		c.cacheLock.Lock()
		delete(c.fileCache, f.Name)
		c.cacheLock.Unlock()
	}
	return nil
}

//read reads in the data for the hash blob and returns it as a byte array
func (c *Cass) ReadData(hash []byte) ([]byte, error) {
	var buffer, data []byte
	var loc int
	iter := c.session.Query("SELECT location, data FROM filedata WHERE hash = ?", hash).Iter()
	for iter.Scan(&loc, &data) {
		buffer = append(buffer, data...)
	}
	return buffer, nil
}

//Read is the wrapper for read that will check the cache before reading from cassandra
func (c *Cass) Read(hash []byte) ([]byte, error) {
	var data []byte
	var err error
	if c.CacheEnabled {
		err = c.cache.Get(c, string(hash), groupcache.AllocatingByteSliceSink(&data))
	} else {
		data, err = c.ReadData(hash)
	}
	if err != nil {
		log.Printf("%s\n", err)
		return nil, err
	}
	return data, err
}

//DeleteFile removes a file from the filesystem and updates the reference count
func (c *Cass) DeleteFile(name string) error {
	var hash []byte
	dir, file := c.splitPath(name)
	err := c.session.Query("SELECT hash FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? and name = ?", c.OwnerId, c.Environment, dir, file).Scan(&hash)
	if err != nil {
		return err
	}
	err = c.session.Query("DELETE FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? and name = ?", c.OwnerId, c.Environment, dir, file).Exec()
	if err != nil {
		return err
	}
	if len(hash) > 0 {
		err = c.decrementDataRef(hash)
	}
	//Check if there is an entry in the cache
	if _, ok := c.fileCache[name]; ok {
		delete(c.fileCache, name)
	}
	return err
}

//OpenDir returns the files stored in dir
func (c *Cass) OpenDir(dir string) ([]fuse.DirEntry, error) {
	var file_list []fuse.DirEntry
	var meta, hash []byte
	var file string

	now := time.Now()

	dirId, err := c.FindDir(dir)
	if err != nil {
		log.Printf("When looking up %s\n", dir)
		log.Printf("Something bad happened about the lookup: %s\n", err)
	}
	iter := c.session.Query("SELECT name, metadata, hash FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ?", c.OwnerId, c.Environment, dirId).Iter()
	for iter.Scan(&file, &meta, &hash) {
		finfo := &CassMetadata{}
		err := json.Unmarshal(meta, finfo)
		if err != nil {
			log.Printf("Error decoding metadata for (%s): %s\n", file, err)
			continue
		}
		key := fmt.Sprintf("%s/%s", dir, file)
		c.cacheLock.Lock()
		c.fileCache[key] = &CassFsMetadata{
			Metadata: *finfo,
			Timestamp: now.Unix(),
			Hash: hash,
		}
		c.cacheLock.Unlock()
		file_list = append(file_list, fuse.DirEntry{Mode: finfo.Attr.Mode, Name: file})
	}
	err = iter.Close();
	if err != nil {
		return nil, err
	}
	return file_list, nil
}

//CopyFile copies the file orig to newFile
func (c *Cass) CopyFile(orig string, newFile string) error {
	var hash, metadata []byte
	dir, file := c.splitPath(orig)
	newDir, newFile := c.splitPath(newFile)
	err := c.session.Query("SELECT hash, metadata FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ?", c.OwnerId, c.Environment, dir, file).Consistency(gocql.One).Scan(&hash, &metadata)
	if err != nil {
		return err
	}
	err = c.session.Query("INSERT INTO filesystem (cust_id, environment, directory, name, hash, metadata) VALUES(?, ?, ?, ?, ?, ?)", c.OwnerId, c.Environment, newDir, newFile, hash, metadata).Consistency(gocql.One).Exec()
	if err != nil {
		return err
	}
	err = c.incrementDataRef(hash)
	if err != nil {
		//We need to remove the new file entry to prevent an unallocated reference from being kept
		c.session.Query("DELETE FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ?", c.OwnerId, c.Environment, newDir, newFile).Consistency(gocql.One).Exec()
		return err
	}
	return nil
}

//WriteFileData writes the data passed in into the file data table in chunks of BLOBSIZE
func (c *Cass) WriteFileData(data []byte) ([]byte, error) {
	var h []byte
	start := 0
	end := BLOBSIZE
	if end > len(data) {
		end = len(data)
	}
	hash := ShaSum(data)
	err := c.session.Query("SELECT hash FROM filedata WHERE hash = ?", hash).Consistency(gocql.One).Scan(&h)
	if err == nil {
		//The data is already in the DB
		return hash, nil
	}
	if err != gocql.ErrNotFound {
		//The error was not a not found error, so there's a problem
		return nil, err
	}
	for {
		err := c.session.Query("INSERT INTO filedata (hash, location, data) VALUES(?, ?, ?)", hash, start, data[start:end]).Exec()
		if err != nil {
			log.Printf("Error writing data: %s\n", err)
			return nil, err
		}
		start += BLOBSIZE + 1
		if start > len(data) {
			break
		}
		if (end + BLOBSIZE + 1) > len(data) {
			end = len(data)
		} else {
			end += BLOBSIZE + 1
		}
	}
	return hash, nil
}

//MakeDirectory creates a directory at path directory with attributes attr
func (c *Cass) MakeDirectory(directory string, attr *fuse.Attr) error {
	parent, child := c.splitPath(directory)

	meta, err := json.Marshal(CassMetadata{Attr: attr})
	if err != nil {
		log.Printf("Encoding err: %s\n", err)
		return err
	}

	uuid := gocql.TimeUUID()

	return c.session.Query("INSERT INTO filesystem (cust_id, environment, directory, name, hash, metadata) VALUES(?, ?, ?, ?, ?, ?)", c.OwnerId, c.Environment, parent, child, uuid.Bytes(), meta).Consistency(gocql.One).Exec()
}

//GetFileCount returns the number of files in the environment
func (c *Cass) GetFileCount() (uint64, error) {
	var fcount uint64
	err := c.session.Query("SELECT count(1) FROM filesystem WHERE cust_id = ? AND environment = ?", c.OwnerId, c.Environment).Consistency(gocql.One).Scan(&fcount)
	if err != nil {
		return 0, err
	}
	return fcount, nil
}
