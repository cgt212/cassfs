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
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
)

type CassFileHandle struct {
	at       int64
	closed   bool
	fileData *CassFileData
}

type CassFileData struct {
	sync.Mutex
	Fs *CassFs
	Refs int32
	Name string
	Data []byte
	Hash []byte
	Dirty bool
	Attr *fuse.Attr
}

func NewFileHandle(f *CassFileData) *CassFileHandle {
	atomic.AddInt32(&f.Refs, 1)
	return &CassFileHandle{
		at:       0,
		closed:   false,
		fileData: f,
	}
}

func NewEmptyFileData(path string) *CassFileData {
	return &CassFileData{
		Refs: 0,
		Dirty: true,
	}
}

func NewFileData(path string, hash []byte, data []byte) *CassFileData {
	return &CassFileData{
		Refs: 0,
		Name: path,
		Data: data,
		Hash: hash,
		Dirty: false,
		Attr: nil,
	}
}

func (c *CassFileHandle) String() string {
	return c.fileData.Name
}

func (c *CassFileHandle) Chmod(mod uint32) fuse.Status {
	attr := c.fileData.Attr
	old_mode := attr.Mode
	attr.Mode = (attr.Mode & 07000) | mod
	if attr.Mode != old_mode {
		c.fileData.Dirty = true
	}
	return fuse.OK
}

func (c *CassFileHandle) Chown(uid uint32, gid uint32) fuse.Status {
	if c.fileData.Attr.Uid != uid {
		c.fileData.Attr.Uid = uid
		c.fileData.Dirty = true
	}
	if c.fileData.Attr.Gid != gid {
		c.fileData.Attr.Gid = gid
		c.fileData.Dirty = true
	}
	return fuse.OK
}

func (c *CassFileHandle) Read(buf []byte, off int64) (fuse.ReadResult, fuse.Status) {
	end := int(off) + int(len(buf))
	if end > len(c.fileData.Data) {
		end = len(c.fileData.Data)
	}
	log.Printf("FILE: Read getting %d bytes\n", end)
	return fuse.ReadResultData(c.fileData.Data[off:end]), fuse.OK
}

func (c *CassFileHandle) Write(data []byte, offset int64) (uint32, fuse.Status) {
	if int(offset) > len(c.fileData.Data) {
		c.fileData.Data = append(c.fileData.Data, bytes.Repeat([]byte{0}, int(offset) - len(c.fileData.Data))...)
		c.fileData.Data = append(c.fileData.Data, data...)
		return uint32(len(data)), fuse.OK
	}
	c.fileData.Dirty = true
	c.fileData.Data = append(c.fileData.Data[0:offset], data...)
	c.fileData.Attr.Size = uint64(len(c.fileData.Data))
	return uint32(len(data)), fuse.OK
}

func (c *CassFileHandle) Flush() fuse.Status {
	//This function should write everything back
	if ! c.fileData.Dirty {
		return fuse.OK
	}
	err := c.fileData.Fs.FlushFile(c.fileData)
	if err != nil {
		log.Printf("Error updating file: %s\n", err)
		return fuse.EIO
	}
	return fuse.OK
}

func (c *CassFileHandle) Allocate(off uint64, size uint64, mode uint32) fuse.Status {
	return fuse.OK
}

func (c *CassFileHandle) Release() {
	return
}

func (c *CassFileHandle) Fsync(flags int) fuse.Status {
	return fuse.OK
}

func (c *CassFileHandle) GetAttr(out *fuse.Attr) fuse.Status {
	attr := c.fileData.Attr
	out.Ino = attr.Ino
	out.Size = attr.Size
	out.Blocks = attr.Blocks
	out.Atime = attr.Atime
	out.Mtime = attr.Mtime
	out.Ctime = attr.Ctime
	out.Atimensec = attr.Atimensec
	out.Mtimensec = attr.Mtimensec
	out.Ctimensec = attr.Ctimensec
	out.Mode = attr.Mode
	out.Nlink = attr.Nlink
	out.Uid = attr.Uid
	out.Gid = attr.Gid
	out.Rdev = attr.Rdev
	out.Blksize = attr.Blksize
	out.Padding = attr.Padding
	return fuse.OK
}

func (c *CassFileHandle) InnerFile() nodefs.File {
	return c
}

func (c *CassFileHandle) SetInode(i *nodefs.Inode) {
}

func (c *CassFileHandle) Truncate(size uint64) fuse.Status {
	c.fileData.Data = c.fileData.Data[:size]
	return fuse.OK
}

func (c *CassFileHandle) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	c.fileData.Attr.Atimensec = uint32(atime.Nanosecond())
	c.fileData.Attr.Mtimensec = uint32(mtime.Nanosecond())
	return fuse.OK

}
