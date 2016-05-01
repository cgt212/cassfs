package main

import "os"

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("%s requires two arguments", os.Args[0])
		os.Exit(1)
	}
	fromdir := os.Args[1]
	todir := os.Args[0]

	c := NewDefaultCass()
	c.Host = "localhost"
	c.Keyspace = "test"
	err := c.Init()
	if err != nil {
		fmt.Printf("Could not initialize cluster connection: %s\n", err)
		os.Exit(1)
	}
	source, err := ioutil.ReadDir(fromdir)
	if err != nil {
		fmt.Printf("Failed to open directory: %s\n", fromdir)
		panic(err)
	}
	for file := range source {
		
	}
	file, err := c.getFileInformation(filename)
	if err != nil {
		panic(err)
	}
	if file.Hash == nil {
		fmt.Printf("File not found")
	} else {
		fmt.Printf("Got file: %v\n", file)
	}
}
