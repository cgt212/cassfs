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
 *u
 *along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cass

import (
	"log"
	"sync"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

type CassFsOptions struct {
	Owner fuse.Owner
	Mode  uint32
	mount bool
}

type CassFs struct {
	pathfs.FileSystem
	Mount     *string
	cacheLock sync.RWMutex
	fileCache map[string]*CassFileData
	store     *Cass
	options   *CassFsOptions
}

func NewCassFs(s *Cass, opts *CassFsOptions) *CassFs {
	return &CassFs{
		store:     s,
		options:   opts,
		fileCache: make(map[string]*CassFileData),
	}
}

func (c *CassFs) OnMount(nodefs *pathfs.PathNodeFs) {
}

func (c *CassFs) OnUnmount() {
}

func (c *CassFs) StatFs(name string) *fuse.StatfsOut {
	fcount, err := c.store.GetFileCount()
	if err != nil {
		return nil
	}
	return &fuse.StatfsOut{
		Files: fcount,
		Ffree: fcount * 2,
	}
}

func (c *CassFs) Access(name string, mode uint32, context *fuse.Context) fuse.Status {
	//For now we are just going to allow all access
	return fuse.OK
}

func (c *CassFs) Rename(oldName string, newName string, context *fuse.Context) fuse.Status {
	_, status := c.GetAttr(oldName, context)
	if status != fuse.OK {
		return status
	}
	err := c.store.Rename(oldName, newName)
	if err != nil {
		return fuse.EIO
	}
	return fuse.OK
}

func (c *CassFs) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	res, err := c.store.OpenDir(name)
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil, fuse.ENOENT
		}
		log.Println("There was some kind of other error")
		return nil, fuse.EIO
	}
	return res, fuse.OK
}

func (c *CassFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	if name == "" {
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | c.options.Mode,
			Owner: fuse.Owner{
				Uid: c.options.Owner.Uid,
				Gid: c.options.Owner.Gid,
			},
		}, fuse.OK
	}
	meta, err := c.store.GetFiledata(name)
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil, fuse.ENOENT
		}
		log.Println("I/O Error:", err)
		return nil, fuse.EIO
	}
	return meta.Metadata.Attr, fuse.OK
}

// This is the start of the FS Interface implementation
func (c *CassFs) Link(orig string, newName string, context *fuse.Context) fuse.Status {
	err := c.store.CopyFile(orig, newName)
	if err != nil {
		return -1
	}
	return 0
}

func (c *CassFs) Rmdir(path string, context *fuse.Context) fuse.Status {
	data, err := c.store.GetFiledata(path)
	if err != nil {
		log.Println("Unable to get information for %s: %s", path, err)
		return fuse.EIO
	}
	if !data.Metadata.Attr.IsDir() {
		return fuse.Status(syscall.ENOTDIR)
	}

	dirlist, err := c.store.OpenDir(path)
	if len(dirlist) > 0 {
		return fuse.Status(syscall.ENOTEMPTY)
	}
	err = c.store.DeleteFile(path)
	if err != nil {
		if err == gocql.ErrNotFound {
			return fuse.ENOENT
		}
		return fuse.EIO
	}
	return 0
}

func (c *CassFs) Mkdir(path string, mode uint32, context *fuse.Context) fuse.Status {
	_, err := c.store.GetFiledata(path)
	if err == nil {
		return fuse.Status(syscall.EEXIST)
	}
	err = c.store.MakeDirectory(path, &fuse.Attr{Mode: fuse.S_IFDIR | mode})
	if err != nil {
		log.Println("There was an error making directory (%s): %s", path, err)
		return fuse.EIO
	}
	return fuse.OK
}

func (c *CassFs) Symlink(pointedTo string, linkName string, context *fuse.Context) fuse.Status {
	ctime := time.Now()
	attr := fuse.Attr{
		Mode:      fuse.S_IFLNK | 0777,
		Ctime:     uint64(ctime.Unix()),
		Ctimensec: uint32(ctime.Nanosecond()),
	}
	err := c.store.CreateFile(linkName, &attr, []byte(pointedTo))
	if err != nil {
		log.Println("Error creating symlink (%s): %s", linkName, err)
		return fuse.EIO
	}
	return fuse.OK
}

func (c *CassFs) Truncate(path string, size uint64, context *fuse.Context) fuse.Status {
	return fuse.EINVAL
}

func (c *CassFs) Utimens(name string, atime *time.Time, mtime *time.Time, context *fuse.Context) fuse.Status {
	meta, err := c.store.GetFiledata(name)
	if err != nil {
		log.Println("Error getting (%s) metadata: %s", name, err)
		return fuse.EIO
	}
	meta.Metadata.Attr.Atime = uint64(atime.Unix())
	meta.Metadata.Attr.Atimensec = uint32(atime.Nanosecond())
	meta.Metadata.Attr.Mtime = uint64(mtime.Unix())
	meta.Metadata.Attr.Mtimensec = uint32(mtime.Nanosecond())
	err = c.store.WriteMetadata(name, meta.Metadata)
	if err != nil {
		log.Println("Error updating file:", err)
		return fuse.EIO
	}
	return fuse.OK
}

func (c *CassFs) Chown(name string, uid uint32, gid uint32, context *fuse.Context) fuse.Status {
	log.Println("Changing ownership of \"" + name + "\"")
	if name == "" {
		log.Println("Changing ownership of root mountpoint")
		c.options.Owner.Uid = uid
		c.options.Owner.Gid = gid
		return fuse.OK
	}
	meta, err := c.store.GetFiledata(name)
	if err != nil {
		log.Println("Error getting (%s) metadata: %s", name, err)
		return fuse.EIO
	}
	if int32(uid) > 0 {
		meta.Metadata.Attr.Owner.Uid = uid
	}
	if int32(gid) > 0 {
		meta.Metadata.Attr.Owner.Gid = gid
	}
	err = c.store.WriteMetadata(name, meta.Metadata)
	if err != nil {
		log.Println("Error writing (%s) metadata: %s", name, err)
		return fuse.EIO
	}
	return fuse.OK
}

func (c *CassFs) Chmod(name string, mode uint32, context *fuse.Context) fuse.Status {
	permMask := uint32(07777)

	if name == "" {
		c.options.Mode = (c.options.Mode &^ permMask) | mode
		return fuse.OK
	}

	meta, err := c.store.GetFiledata(name)
	if err != nil {
		log.Println("Could not get metadata for file:", name)
		return fuse.EIO
	}
	meta.Metadata.Attr.Mode = (meta.Metadata.Attr.Mode &^ permMask) | mode
	//There needs to be a set filedata function in the store, which there is not
	err = c.store.WriteMetadata(name, meta.Metadata)
	if err != nil {
		log.Println("Error writing (%s) metadata: %s", name, err)
		return fuse.EIO
	}
	return fuse.OK
}

func (c *CassFs) Unlink(name string, context *fuse.Context) fuse.Status {
	err := c.store.DeleteFile(name)
	if err != nil {
		return fuse.EIO
	}
	return fuse.OK
}

func (c *CassFs) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	meta, err := c.store.GetFiledata(name)
	if err != nil {
		log.Println("could not get metadata for:", name)
		return "", fuse.EIO
	}
	return string(meta.Hash), fuse.OK
}

func (c *CassFs) FlushFile(fd *CassFileData) error {
	return c.store.UpdateFile(fd)
}

func (c *CassFs) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	c.cacheLock.RLock()
	if entry, ok := c.fileCache[name]; ok {
		fh := NewFileHandle(entry)
		c.cacheLock.RUnlock()
		return fh, fuse.OK
	}
	c.cacheLock.RUnlock()
	mdata, err := c.store.GetFiledata(name)
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil, fuse.ENOENT
		}
		return nil, fuse.EIO
	}
	data, err := c.store.Read(mdata.Hash)
	if err != nil {
		return nil, fuse.EIO
	}
	fd := NewFileData(&name, c, mdata.Hash, data, mdata.Metadata.Attr)
	c.cacheLock.Lock()
	c.fileCache[name] = fd
	c.cacheLock.Unlock()
	fh := NewFileHandle(fd)
	return fh, fuse.OK
}

func (c *CassFs) Release(name string) {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()
	if _, ok := c.fileCache[name]; ok {
		delete(c.fileCache, name)
	}
}

//This needs to be fixed
func (c *CassFs) Create(name string, flags uint32, mode uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	_, err := c.store.GetFiledata(name)
	if err != nil {
		if err == gocql.ErrNotFound {
			attr := fuse.Attr{
				Mode: fuse.S_IFREG | mode,
			}
			err = c.store.CreateFile(name, &attr, []byte{})
			if err != nil {
				log.Println("Error creating file:", err)
				return nil, fuse.EIO
			}
			fd := NewFileData(&name, c, []byte{}, []byte{}, &attr)
			c.cacheLock.Lock()
			c.fileCache[name] = fd
			c.cacheLock.Unlock()
			fh := NewFileHandle(fd)
			return fh, fuse.OK
		} else {
			log.Println("could not get file information for:", name)
			return nil, fuse.EIO
		}
	}
	return nil, fuse.Status(syscall.EEXIST)
}

func (c *CassFs) GetXAttr(name string, attribute string, context *fuse.Context) ([]byte, fuse.Status) {
	return []byte{}, fuse.OK
}

func (c *CassFs) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	return fuse.OK
}

func (c *CassFs) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	return fuse.OK
}

func (c *CassFs) ListXAttr(name string, context *fuse.Context) ([]string, fuse.Status) {
	return []string{}, fuse.OK
}
