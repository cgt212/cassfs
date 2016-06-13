package main

import "github.com/hanwen/go-fuse/fuse"

import (
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

import "github.com/gocql/gocql"

const BLOBSIZE = 1024 * 1024

type RemoteFile struct {
	Parent   string
	Name     string
	Metadata string
	Hash     []byte
	Dirty    bool
}

type CassMetadata struct {
	Attr  *fuse.Attr
	XAttr map[string]string
}

type CassFsMetadata struct {
	Metadata CassMetadata
	Hash     []byte
}

type Cass struct {
	Host         string
	Port         int
	ProtoVersion int
	Keyspace     string
	OwnerId      int64
	Environment  string
	cluster      *gocql.ClusterConfig
	session      *gocql.Session
}

func NewDefaultCass() *Cass {
	return &Cass{
		Host:         "localhost",
		Port:         1234,
		ProtoVersion: 4,
		Keyspace:     "cstore",
		OwnerId:      1,
		Environment:  "prod",
	}
}

func ShaSum(data []byte) []byte {
	hash512 := sha512.New()
	hash512.Write(data)
	return hash512.Sum(nil)
}

func splitPath(path string) (string, string) {
	_path := path
	if strings.HasSuffix(path, "/") {
		_path = path[:len(path)-1]
	}
	idx := strings.LastIndex(_path, "/")
	if idx > 0 {
		parent := _path[:idx]
		child := _path[idx+1:len(_path)]
		return parent, child
	}
	if strings.HasPrefix(_path, "/") {
		return "/", _path[1:]
	}
	return "/", _path
}

func SplitPath(path string) (string, string) {
	return splitPath(path)
}

func (c *Cass) Init() error {
	c.cluster = gocql.NewCluster(c.Host)
	c.cluster.ProtoVersion = 4
	c.cluster.Keyspace = c.Keyspace
	session, err := c.cluster.CreateSession()
	if err != nil {
		return err
	}
	c.session = session
	return nil
}

//These are the new rounds of functions on the storage

func (c *Cass) incrementDataRef(hash []byte) error {
	return c.session.Query("UPDATE fileref SET refs = refs + 1 WHERE hash = ?", hash).Exec()
}

func (c *Cass) decrementDataRef(hash []byte) error {
	return c.session.Query("UPDATE fileref SET refs = refs - 1 WHERE hash = ?", hash).Exec()
}

func (c *Cass) GetFiledata(name string) (*CassFsMetadata, error) {
	var meta CassMetadata
	var metajson, hash []byte
	parent, file := splitPath(name)
	err := c.session.Query("SELECT hash, metadata FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ?", c.OwnerId, c.Environment, parent, file).Consistency(gocql.One).Scan(&hash, &metajson)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(metajson, &meta)
	ret := &CassFsMetadata{
		Metadata: meta,
		Hash: hash,
	}
	return ret, nil
}

func (c *Cass) CreateFile(name string, attr *fuse.Attr, hash []byte) error {
	meta, err := json.Marshal(CassMetadata{
		Attr:  attr,
		XAttr: nil,
	})
	if err != nil {
		fmt.Printf("Encoding error on metadata: %s\n", err)
		return err
	}
	dir, file := splitPath(name)
	return c.session.Query("INSERT INTO filesystem (cust_id, environment, directory, name, hash, metadata) VALUES(?, ?, ?, ?, ?, ?)", c.OwnerId, c.Environment, dir, file, hash, meta).Consistency(gocql.One).Exec()
}

func (c *Cass) UpdateFile(f *CassFileData) error {
	parent, file := splitPath(f.Name)
	hash, err := c.WriteFileData(f.Data)
	if err != nil {
		fmt.Printf("Error writing Data: %s\n", err)
		return err
	}
	old_hash := f.Hash
	f.Hash = hash
	meta, err := json.Marshal(CassMetadata{
		Attr: f.Attr,
	})
	if err != nil {
		fmt.Printf("Encoding error: %s\n", err)
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
	return nil
}

func (c *Cass) GetFileHash(name string) ([]byte, error) {
	var hash []byte
	parent, file := splitPath(name)
	err := c.session.Query("SELECT hash FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ?", c.OwnerId, c.Environment, parent, file).Consistency(gocql.One).Scan(&hash)
	if err != nil {
		return nil, err
	}
	return hash, nil
}

func (c *Cass) WriteBlock(hash []byte, location int, data []byte) error {
	return c.session.Query("INSERT INTO filedata (hash, location, data) VALUES(?, ?, ?)", hash, location, data).Consistency(gocql.One).Exec()
}

func (c *Cass) Read(hash []byte) ([]byte, error) {
	var buffer, data []byte
	var loc int
	iter := c.session.Query("SELECT location, data FROM filedata WHERE hash = ?", hash).Iter()
	for iter.Scan(&loc, &data) {
		buffer = append(buffer, data...)
	}
	return buffer, nil
}

func (c *Cass) insertNewFileInformation(file *RemoteFile) error {
	return c.session.Query("INSERT INTO filesystem (cust_id, environment, directory, name, hash, metadata) VALUES(?, ?, ?, ?, ?)", c.OwnerId, c.Environment, file.Parent, file.Name, file.Hash, file.Metadata).Consistency(gocql.One).Exec()
}

func (c *Cass) updateFileInformation(file *RemoteFile) error {
	return c.session.Query("UPDATE filesystem SET hash=?, metadata=? WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ?", file.Hash, file.Metadata, c.OwnerId, c.Environment, file.Parent, file.Name).Consistency(gocql.One).Exec()
}

func (c *Cass) Copy(orig string, newFile string) error {
	data, err := c.GetFiledata(orig)
	if err != nil {
		return err
	}
	err = c.CreateFile(newFile, data.Metadata.Attr, data.Hash)
	if err != nil {
		return err
	}
	return c.incrementDataRef(data.Hash)
}

func (c *Cass) DeleteFile(name string) error {
	var hash []byte
	dir, file := splitPath(name)
	err := c.session.Query("SELECT hash FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? and name = ?", c.OwnerId, c.Environment, dir, file).Scan(&hash)
	err = c.session.Query("DELETE FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? and name = ?", c.OwnerId, c.Environment, dir, file).Exec()
	if err != nil {
		return err
	}
	return c.decrementDataRef(hash)
}

func (c *Cass) OpenDir(dir string) ([]fuse.DirEntry, error) {
	var file_list []fuse.DirEntry
	var meta []byte
	var file string
	if dir == "" {
		dir = "/"
	}
	fmt.Printf("STORE: Opening Dir (%s)\n", dir)
	iter := c.session.Query("SELECT name, metadata FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ?", c.OwnerId, c.Environment, dir).Iter()
	for iter.Scan(&file, &meta) {
		finfo := &CassMetadata{}
		err := json.Unmarshal(meta, finfo)
		if err != nil {
			fmt.Printf("Error decoding metadata for (%s): %s\n", file, err)
			continue
		}
		fmt.Printf("Appending %s to the directory list\n", file)
		file_list = append(file_list, fuse.DirEntry{Mode: finfo.Attr.Mode | 0777, Name: file})
	}
	err := iter.Close();
	if err != nil {
		return nil, err
	}
	return file_list, nil
}

func (c *Cass) CopyFile(orig string, newFile string) error {
	var hash []byte
	var metadata CassMetadata
	dir, file := splitPath(orig)
	newDir, newFile := splitPath(newFile)
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

func (c *Cass) DeleteDirectory(path string) fuse.Status {
	parent, dir := splitPath(path)
	err := c.session.Query("DELETE FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ?", c.OwnerId, c.Environment, parent, dir).Consistency(gocql.One).Exec()
	if err != nil {
		return fuse.EIO
	}
	return fuse.OK
}

func (c *Cass) WriteFileData(data []byte) ([]byte, error) {
	var h []byte
	start := 0
	end := BLOBSIZE
	if end > len(data) {
		end = len(data)
	}
	hash := ShaSum(data)
	fmt.Printf("Writing %d bytes for file\n", len(data))
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
		fmt.Printf("Writing blocks from: %d to %d\n", start, end)
		err := c.session.Query("INSERT INTO filedata (hash, location, data) VALUES(?, ?, ?)", hash, start, data[start:end]).Exec()
		if err != nil {
			fmt.Printf("Error writing data: %s\n", err)
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

func (c *Cass) insertFileData(hash []byte, filename string) error {
	buffer := make([]byte, BLOBSIZE)
	location := 0
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	for {
		cnt, err := file.Read(buffer)
		if err != nil && err == io.EOF && cnt == 0 {
			//EOF
			return nil
		}
		err = c.session.Query("INSERT INTO filedata (hash, location, data) VALUES(?, ?, ?)", hash, location, buffer).Exec()
		if err != nil {
			fmt.Printf("Error inserting file blob: %s\n", err)
			return err
		}
		location = location + cnt
	}
	return nil
}

func (c *Cass) AddFileData(filename string) ([]byte, error) {
	var hash []byte
	localHash := ShaSum([]byte(filename))
	err := c.session.Query("SELECT hash FROM filedata WHERE hash = ?", localHash).Consistency(gocql.One).Scan(&hash)
	if err != nil {
		if err != gocql.ErrNotFound {
			//Ther error was not a not found error, so there's a problem
			return nil, err
		}
		err = c.insertFileData(localHash, filename)
	}
	//Getting here means that the data is in the DB
	return localHash, err
}

func (c *Cass) GetFileInformation(filename string) (*RemoteFile, error) {
	var metadata string
	var hash []byte

	directory, file := splitPath(filename)

	err := c.session.Query("SELECT hash, metadata FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ? AND directory = ?", c.OwnerId, c.Environment, directory, file, directory).Consistency(gocql.One).Scan(&hash, &metadata)
	if err != nil {
		if err == gocql.ErrNotFound {
			fmt.Printf("Query found nothing for file %s\n", filename)
			ret := &RemoteFile{
				Name:     filename,
				Metadata: "",
				Hash:     nil,
				Dirty:    true,
			}
			return ret, nil
		}
		return nil, err
	}

	fmt.Printf("Found %s with hash %s and metadata \"%s\"\n", filename, hash, metadata)
	ret := &RemoteFile{
		Name:     filename,
		Metadata: metadata,
		Hash:     hash,
		Dirty:    false,
	}
	return ret, nil
}

//func (c *Cass) GetFileData(filename string, hash []byte) error {
//	var loc int64
//	var data []byte
//	if hash == nil {
//		//No hash makes this a no-op
//		return nil
//	}
//	iter := c.session.Query("SELECT location, data FROM filedata WHERE hash = ?", hash).Iter()
//
//	file = os.Create(filename)
//	defer file.Close()
//	for iter.Scan(&loc, &data) {
//		cnt, err := os.WriteAt(data, loc)
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}

func (c *Cass) MakeDirectory(directory string, attr *fuse.Attr) error {
	parent, child := splitPath(directory)

	meta, err := json.Marshal(CassMetadata{Attr: attr})
	if err != nil {
		fmt.Printf("Encoding err: %s\n", err)
		return err
	}

	return c.session.Query("INSERT INTO filesystem (cust_id, environment, directory, name, hash, metadata) VALUES(?, ?, ?, ?, ?, ?)", c.OwnerId, c.Environment, parent, child, nil, meta).Consistency(gocql.One).Exec()
}

func (c *Cass) GetFileCount() (uint64, error) {
	var fcount uint64
	err := c.session.Query("SELECT count(1) FROM filesystem WHERE cust_id = ? AND environment = ?", c.OwnerId, c.Environment).Consistency(gocql.One).Scan(&fcount)
	if err != nil {
		return 0, err
	}
	return fcount, nil
}
