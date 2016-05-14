package main

import "bytes"
import "sync"
import "github.com/hanwen/go-fuse/fuse"

type CassFileHandle struct {
	at       int64
	closed   bool
	fileData *CassFileData
}

type CassFileData struct {
	sync.Mutex
	fs *CassFs
	name string
	data []byte
	attr *fuse.Attr
}

func NewFileHandle(f *CassFileData) *CassFileHandle {
	return &CassFileHandle{
		at:       0,
		closed:   false,
		fileData: f,
	}
}

func (c *CassFileHandle) String() string {
	return c.fileData.name
}

func (c *CassFileHandle) Read(buf []byte, off int64) (fuse.ReadResult, fuse.Status) {
	end := int(off) + int(len(buf))
	if end > len(c.fileData.data) {
		end = len(c.fileData.data)
	}
	return fuse.ReadResultData(c.fileData.data[off:end]), fuse.OK
}

func (c *CassFileHandle) Write(data []byte, offset int64) (uint32, fuse.Status) {
	if int(offset) > len(c.fileData.data) {
		c.fileData.data = append(c.fileData.data, bytes.Repeat([]byte{0}, int(offset) - len(c.fileData.data))...)
		c.fileData.data = append(c.fileData.data, data...)
		return uint32(len(data)), fuse.OK
	}
	c.fileData.data = append(c.fileData.data[0:offset], data...)
	return uint32(len(data)), fuse.OK
}
