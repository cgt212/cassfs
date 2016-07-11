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
	store *Cass
	options *CassFsOptions
}

func NewCassFs(s *Cass, opts *CassFsOptions) *CassFs {
	return &CassFs{
		store:   s,
		options: opts,
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
	log.Printf("Opening Dir (%s)...\n", name)
	res, err := c.store.OpenDir(name)
	if err != nil {
		if err == gocql.ErrNotFound {
			log.Printf("The dir wasn't found, returning NOENT")
			return nil, fuse.ENOENT
		}
		log.Printf("There was some kind of other error")
		return nil, fuse.EIO
	}
	log.Printf("All good, returning what I've got\n")
	return res, fuse.OK
}

func (c *CassFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	log.Printf("Trying to get attribute of %s...\n", name)
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
		log.Printf("There was a lookup error...")
		if err == gocql.ErrNotFound {
			log.Printf("File not found\n")
			return nil, fuse.ENOENT
		}
		log.Printf("I/O Error: %s\n", err)
		return nil, fuse.EIO
	}
	log.Printf("Should be no error\n")
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
		log.Printf("Unable to get information for %s: %s\n", path, err)
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
		log.Printf("There was an error making directory (%s): %s\n", path, err)
		return fuse.EIO
	}
	return fuse.OK
}

func (c *CassFs) Symlink(pointedTo string, linkName string, context *fuse.Context) fuse.Status {
	ctime := time.Now()
	attr := fuse.Attr{
		Mode: fuse.S_IFLNK | 0777,
		Ctime: uint64(ctime.Unix()),
		Ctimensec: uint32(ctime.Nanosecond()),
	}
	err := c.store.CreateFile(linkName, &attr, []byte(pointedTo))
	if err != nil {
		log.Printf("Error creating symlink (%s): %s\n", linkName, err)
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
		log.Printf("Error getting (%s) metadata: %s\n", name, err)
		return fuse.EIO
	}
	meta.Metadata.Attr.Atime = uint64(atime.Unix())
        meta.Metadata.Attr.Atimensec = uint32(atime.Nanosecond())
        meta.Metadata.Attr.Mtime = uint64(mtime.Unix())
        meta.Metadata.Attr.Mtimensec = uint32(mtime.Nanosecond())
        err = c.store.WriteMetadata(name, meta.Metadata)
        if err != nil {
                log.Printf("Error updating file: %s\n", err)
                return fuse.EIO
        }
	return fuse.OK
}

func (c *CassFs) Chown(name string, uid uint32, gid uint32, context *fuse.Context) fuse.Status {
	meta, err := c.store.GetFiledata(name)
	if err != nil {
		log.Printf("Error getting (%s) metadata: %s\n", name, err)
		return fuse.EIO
	}
	log.Printf("Changing file (%s) owner from %d:%d to %d:%d\n", name, meta.Metadata.Attr.Owner.Uid, meta.Metadata.Attr.Owner.Gid, uid, gid)
	if int32(uid) > 0 {
		meta.Metadata.Attr.Owner.Uid = uid
	}
	if int32(gid) > 0 {
		meta.Metadata.Attr.Owner.Gid = gid
	}
	err = c.store.WriteMetadata(name, meta.Metadata)
	if err != nil {
		log.Printf("Error writing (%s) metadata: %s\n", name, err)
		return fuse.EIO
	}
	return fuse.OK
}

func (c *CassFs) Chmod(name string, mode uint32, context *fuse.Context) fuse.Status {
	permMask := uint32(07777)
	meta, err := c.store.GetFiledata(name)
	if err != nil {
		log.Printf("Could not get metadata for file: %s\n", name)
		return fuse.EIO
	}
	log.Printf("Changing file (%s) mode from %d to %d\n", name, meta.Metadata.Attr.Mode, mode)
	meta.Metadata.Attr.Mode = (meta.Metadata.Attr.Mode &^ permMask) | mode
	//There needs to be a set filedata function in the store, which there is not
	err = c.store.WriteMetadata(name, meta.Metadata)
	if err != nil {
		log.Printf("Error writing (%s) metadata: %s\n", name, err)
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
		log.Printf("could not get metadata for: %s\n", name)
		return "", fuse.EIO
	}
	return string(meta.Hash), fuse.OK
}

func (c *CassFs) FlushFile(fd *CassFileData) error {
	return c.store.UpdateFile(fd)
}

func (c *CassFs) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
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
	fd := NewFileData(name, mdata.Hash, data)
	fd.Attr = mdata.Metadata.Attr
	fh := NewFileHandle(fd)
	fh.fileData.Fs = c
	return fh, fuse.OK
}

//This needs to be fixed
func (c *CassFs) Create(name string, flags uint32, mode uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	log.Printf("Should be creating file (%s)\n", name)
	_, err := c.store.GetFiledata(name)
	if err != nil {
		if err == gocql.ErrNotFound {
			attr := fuse.Attr{
				Mode: fuse.S_IFREG | mode,
			}
			err = c.store.CreateFile(name, &attr, []byte{})
			if err != nil {
				log.Printf("Error creating file: %s\n", err)
				return nil, fuse.EIO
			}
			fd := NewFileData(name, []byte{}, []byte{})
			fd.Attr = &attr
			fh := NewFileHandle(fd)
			fh.fileData.Fs = c
			return fh, fuse.OK
		} else {
			log.Printf("could not get file information for: %s\n", name)
			return nil, fuse.EIO
		}
	}
	log.Printf("The file exists\n")
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
