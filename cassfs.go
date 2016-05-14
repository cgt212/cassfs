package main

import "fmt"
import "syscall"
import "time"
import "github.com/hanwen/go-fuse/fuse"
//import "github.com/hanwen/go-fuse/fuse/nodefs"
import "github.com/hanwen/go-fuse/fuse/pathfs"

type CassFsOptions struct {
	mount bool
}

type CassFs struct {
	pathfs.FileSystem
	store Cass
	options *CassFsOptions
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
	f := c.getEntry(path)
	if !f.attr.IsDir() {
		return fuse.Status(syscall.ENOTDIR)
	}

	dirlist, err := c.store.OpenDir(path)
	if len(dirlist) > 0 {
		return fuse.Status(syscall.ENOTEMPTY)
	}
	err = c.store.DeleteEntry(path)
	if err != nil {
		return -1
	}
	return 0
}

func (c *CassFs) Mkdir(path string, mode uint32, context *fuse.Context) fuse.Status {
	metadata, err := c.store.GetFiledata(path)
	if err == nil {
		return fuse.Status(syscall.EEXIST)
	}
	err = c.store.MakeDirectory(path, &fuse.Attr{Mode: fuse.S_IFDIR | mode})
	if err != nil {
		fmt.Printf("There was an error making directory (%s): %s\n", path, err)
		return fuse.EIO
	}
	return fuse.OK
}

func (c *CassFs) Symlink(pointedTo string, linkName string, context *fuse.Context) fuse.Status {
	attr := fuse.Attr{
		Mode: fuse.S_IFLNK | 0777,
	}
	err := c.store.CreateFile(linkName, &attr, []byte(pointedTo))
	if err != nil {
		fmt.Printf("Error creating symlink (%s): %s\n", linkName, err)
		return fuse.EIO
	}
	return fuse.OK
}

func (c *CassFs) Truncate(path string, size uint64, context *fuse.Context) fuse.Status {
	return fuse.EINVAL
}

func (c *CassFs) Utimens(name string, atime *time.Time, mtime *time.Time, context *fuse.Context) fuse.Status {
	return fuse.EINVAL
}

func (c *CassFs) Chown(name string, uid uint32, gid uint32, context *fuse.Context) fuse.Status {
	return fuse.EINVAL
}

func (c *CassFs) Chmod(name string, mode uint32, context *fuse.Context) fuse.Status {
	filemode := 0777
	meta, err := c.store.GetFiledata(name)
	if err != nil {
		fmt.Printf("Could not get metadata for file: %s\n", name)
		return fuse.EIO
	}
	meta.Metadata.Attr.Mode = meta.Metadata.Attr.Mode | (mode & uint32(filemode))
	//There needs to be a set filedata function in the store, which there is not
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
		fmt.Printf("could not get metadata for: %s\n", name)
		return "", fuse.EIO
	}
	return string(meta.Hash), fuse.OK
}

//This needs to be fixed
func (c *CassFs) Create(name string, flags uint32, mode uint32, context *fuse.Context) fuse.Status {
	meta, err := c.store.GetFiledata(name)
	if err != nil {
		fmt.Printf("could not get file information for: %s\n", name)
		return fuse.EIO
	}
	return fuse.OK
}
