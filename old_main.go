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
	Name     string
	Metadata string
	Hash     []byte
	Dirty    bool
}

func incrementDataRef(hash []byte, session *gocql.Session) error {
	err := session.Query("UPDATE fileref SET refs = refs + 1 WHERE hash = ?", hash).Exec()
	return err
}

func insertNewFileInformation(file *RemoteFile, session *gocql.Session) error {
	err := session.Query("INSERT INTO filesystem (cust_id, environment, name, hash, metadata) VALUES(?, ?, ?, ?, ?)", 1, "prod", file.Name, file.Hash, file.Metadata).Consistency(gocql.One).Exec()
	if err != nil {
		return err
	}
	return nil
}

func updateFileInformation(file RemoteFile, session *gocql.Session) error {
	err := session.Query("UPDATE filesystem SET (hash, metadata) VALUES(?, ?) WHERE cust_id = ? AND environment = ? AND name = ?", file.Hash, file.Metadata, 1, "prod", file.Name).Consistency(gocql.One).Exec()
	if err != nil {
		return err
	}
	return nil
}

func setFileInformation(file *RemoteFile, session *gocql.Session) error {
	return insertNewFileInformation(file, session)
}

func insertFileData(hash []byte, filename string, session *gocql.Session) error {
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
		err = session.Query("INSERT INTO filedata (hash, location, data) VALUES(?, ?, ?)", hash, location, buffer).Exec()
		if err != nil {
			fmt.Printf("Error on insert: %s\n", err)
			return err
		}
		location = location + cnt
	}
	return nil
}

func addFileData(filename string, session *gocql.Session) ([]byte, error) {
	var hash []byte
	localHash, err := ShaSum(filename)
	if err != nil {
		fmt.Printf("Could not get SHA512: %s\n", err)
		return nil, err
	}
	err = session.Query("SELECT hash FROM filedata WHERE hash = ?", localHash.Sum(nil)).Consistency(gocql.One).Scan(&hash)
	if err != nil {
		if err != gocql.ErrNotFound {
			//The error was not a not found error, so there's a problem
			return nil, err
		}
		//The error was that the hash was not found, so we should add it
		err = insertFileData(localHash.Sum(nil), filename, session)
		return localHash.Sum(nil), err
	}
	//If we are here, then the data is in the DB
	return hash, err
}

func getFileInformation(filename string, session *gocql.Session) (*RemoteFile, error) {
	var metadata string
	var hash []byte

	err := session.Query("SELECT hash, metadata FROM filesystem WHERE cust_id = ? AND environment = ? AND name = ?", 1, "prod", filename).Consistency(gocql.One).Scan(&hash, &metadata)
	if err != nil {
		if err == gocql.ErrNotFound {
			fmt.Printf("Query found nothing\n")
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

	fmt.Printf("Found %s with hash %s and metadata %s\n", filename, hash, metadata)
	ret := &RemoteFile{
		Name:     filename,
		Metadata: metadata,
		Hash:     hash,
		Dirty:    false,
	}
	return ret, nil
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

func main() {

	if len(os.Args) != 2 {
		fmt.Printf("%s requires one argument", os.Args[0])
		os.Exit(1)
	}

	filename := os.Args[1]

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "test"
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	file, err := getFileInformation(filename, session)
	if err != nil {
		panic(err)
	}
	if file.Hash == nil {
		fmt.Printf("File not found")
	} else {
		fmt.Printf("Got file: %v\n", file)
	}

	hash, err := addFileData(filename, session)
	if err != nil {
		fmt.Printf("Failed to insert data: %s\n", err)
	}
	fmt.Printf("Filedata with hash %x stored\n", hash)
	file.Hash = hash
	err = setFileInformation(file, session)
	if err != nil {
		fmt.Printf("Error adding file info: %s", err)
		panic(err)
	}
	err = incrementDataRef(file.Hash, session)
	if err != nil {
		fmt.Printf("Unable to increment ref count for %x\n", file.Hash)
		panic(err)
	}
}
