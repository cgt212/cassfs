package main

import (
	"crypto/sha512"
	"fmt"
	"hash"
	"io"
	"os"
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

type Cass struct {
	Host         string
	Port         string
	ProtoVersion int
	Keyspace     string
	cluster      gocql.Cluster
	session      gocql.Conn
}

func newDefaultCass() *Cass {
	return &Cass{}
}

func splitPath(path string) (string, string) {
	_path := path
	if strings.HasSuffix(path, "/"[0]) {
		_path = path[:len(path)-1]]
	}
	idx := strings.LastIndexByte(_path, "/"[0])
	if idx > 0 {
		parent := _path[:idx]
		child := _path[idx:len(_path)]
		return (parent, child)
	}
	if strings.HasPrefix(_path, "/"[0]) {
		return ("/", _path[1:])
	}
	return ("/", _path)
}

func ShaSum(filename string) (hash.Hash, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	hash := sha512.New()
	_, err = io.Copy(hash, file)
	if err != nil {
		return nil, err
	}
	return hash, nil
}

func (c *Cass) Init() error {
	c.cluster = gocql.NewCluster(c.host)
	c.cluster.ProtoVersion = 4
	c.cluster.Keyspace = c.Keyspace
	session, err := c.cluster.CreateSession()
	if err != nil {
		return err
	}
	c.session = session
	return nil
}

func (c *Cass) incrementDataRef(hash []byte) error {
	return c.session.Query("UPDATE fileref SET refs = refs + 1 WHERE hash = ?", hash).Exec()
}

func (c *Cass) insertNewFileInformation(file *RemoteFile) error {
	return c.session.Query("INSERT INTO filesystem (cust_id, environment, directory, name, hash, metadata) VALUES(?, ?, ?, ?, ?)", 1, "prod", file.Parent, File.Name, file.Hash, file.Metadata).Consistency(gocql.One).Exec()
}

func (c *Cass) updateFileInformation(file *RemoteFile) error {
	return c.session.Query("UPDATE filesystem SET (hash, metadata) VALUES(?, ?) WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ?", file.Hash, file.Metadata, 1, "prod", file.Parent, file.Name).Consistency(gocql.One).Exec()
}

func (c *Cass) setFileInformation(file *RemoteFile) error {
	return insertNewFileInformation(file, session)
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
	localHash, err := ShaSum(filename)
	if err != nil {
		fmt.Printf("Could not get SHA sum: %s\n", err)
		return nil, err
	}
	err = session.Query("SELECT hash FROM filedata WHERE hash = ?", localHash.Sum(nil)).Consistency(gocql.One).Scan(&hash)
	if err != nil {
		if err != gocql.ErrNotFound {
			//Ther error was not a not found error, so there's a problem
			return nil, err
		}
		err = c.insertFileData(localHash.Sum(nil), filename)
	}
	//Getting here means that the data is in the DB
	return localHash.Sum(nil), err
}

func (c *Cass) GetFileInformation(filename string) (*RemoteFile, error) {
	var metadata string
	var hash []byte

	directory, file := splitPath(filename)

	err := session.Query("SELECT hash, metadata FROM filesystem WHERE cust_id = ? AND environment = ? AND directory = ? AND name = ? AND directory = ?", 1, "prod", directory, file, directory).Consistency(gocql.One).Scan(&hash, &metadata)
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

func (c *Cass) GetFileData(filename string, hash []byte) error {
	var loc int64
	var data []byte
	if hash == nil {
		//No hash makes this a no-op
		return nil
	}
	iter, err := c.session.Query("SELECT location, data FROM filedata WHERE hash = ?", hash)
	if err != nil {
		return err
	}
	file = os.Create(filename)
	defer file.Close()
	for iter.Scan(&loc, &data) {
		cnt, err := os.WriteAt(data, loc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cass) MakeDirectory(directory string) error {
	parent, child := splitPath(directory)

	return c.session.Query("INSERT INTO filesystem (cust_id, environment, directory, name, hash, metadata) VALUES(?, ?, ?, ?, ?)", 1, "prod", parent, child, nil, "{ \"Dir\": true }").Consistency(gocql.One).Exec()
	if err != nil {
		fmt.Printf("Error inserting file blob: %s\n", err)
		return err
	}
}
