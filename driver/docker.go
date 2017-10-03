package driver

import (
	"errors"
	"fmt"
	"github.com/coreos/go-systemd/dbus"
	"github.com/docker/go-plugins-helpers/volume"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"text/template"
)

var unit_template = `[Unit]
Description=Mount point for drupal
Wants=docker.service

[Service]
EnvironmentFile={{.StateDir}}/environments/{{.Hash}}.env
ExecStart=/usr/local/bin/cassfs mount ${MOUNT}
ExecStop=/bin/fusermount -u ${MOUNT}`

var unit_env_tmpl = `
CASSFS_SERVER={{.Server}}
CASSFS_CONSISTENCY={{.Consistency}}
CASSFS_KEYSPACE={{.Keyspace}}
CASSFS_ENVIRONMENT={{.Environment}}
CASSFS_OWNER={{.Owner}}
MOUNT={{.Mount}}
`

type DriverConfig struct {
	Consistency string
	Keyspace    string
	Server      string
	StateDir    string
	VolumeDir   string
}

type CassFsDriver struct {
	db      *VolumeDb
	lock    *sync.Mutex
	systemd *dbus.Conn
	config  *DriverConfig
}

func NewCassFsDriver(config *DriverConfig) *CassFsDriver {
	db, err := NewVolumeDb(config)
	if err != nil {
		fmt.Printf("Unable to open DB: %s\n", err)
		return nil
	}
	systemd, err := dbus.New()
	if err != nil {
		fmt.Printf("Unable to connect to DBus: %s\n", err)
		panic(err)
	}

	// Make sure some needed directories exist
	err = makeDirs(filepath.Join(config.StateDir, "systemd"))
	if err != nil {
		fmt.Printf("Unable to make directory: %s\n", err)
		return nil
	}
	err = makeDirs(filepath.Join(config.StateDir, "environments"))
	if err != nil {
		fmt.Printf("Unable to make directory: %s\n", err)
		return nil
	}

	driver := &CassFsDriver{
		config:  config,
		db:      db,
		lock:    &sync.Mutex{},
		systemd: systemd,
	}
	// Create the template systemd file
//	err = writeUnitFile(filepath.Join(config.StateDir, "systemd", "cassfs@.service"), config.StateDir)
//	err = driver.systemd.Reload()
//	if err != nil {
//		fmt.Printf("Error on reload: %s\n", err)
//		return nil
//	}
//	err = driver.enableSystemdUnit(filepath.Join(config.StateDir, "systemd", "cassfs@.service"))
//	if err != nil {
//		fmt.Printf("Unable to enable unit file: %s\n", err)
//		return nil
//	}
	return driver
}

func (c *CassFsDriver) Create(r *volume.CreateRequest) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	return nil
}

func (c *CassFsDriver) create(r volume.CreateRequest) error {

	// Try to find the mount to see if it already exists
	// CreateVolume is idempotent, so it will return an
	// existing volue if it is already present
	// if it is a new volume, then there will only be
	// 1 client, we will check that next
	// Given this, we have to ensure that the volume matches
	// an owner.environment pattern otherwise there may be
	// undetectable naming collisions

	args := strings.Split(r.Name, ".")
	if len(args) != 2 {
		return errors.New("Volume name must be in the form of <owner>.<environment>")
	}

	owner, err := strconv.Atoi(args[0])
	if err != nil {
		return errors.New("Owner must be an integer value")
	}

	// Put name format verification here
	// instead of in the writeEnvFile function
	mount, err := c.db.CreateVolume(r.Name, owner, args[1])
	if err != nil {
		fmt.Printf("Error attaching volume: %s\n", err)
		return err
	}

	if mount.Clients == 1 {
		// This is the first mount for this name
		// we have to write the environment path
		location := filepath.Join(c.config.StateDir, "environments", mount.Hash + ".env")
		writeEnvFile(location, c.config, mount)
		// Create the template systemd file
		err = writeUnitFile(filepath.Join(c.config.StateDir, "systemd", "cassfs-" + mount.Hash + ".service"), c.config.StateDir, mount.Hash)
		if err != nil {
			fmt.Printf("Error writing unit file: %s\n", err)
			return err
		}
		err = c.enableSystemdUnit(filepath.Join(c.config.StateDir, "systemd", "cassfs-" + mount.Hash + ".service"))
		if err != nil {
			fmt.Printf("Unable to enable unit file: %s\n", err)
			return err
		}
		err = c.systemd.Reload()
		if err != nil {
			fmt.Printf("Error on reload: %s\n", err)
			return errors.New("Unable to reload systemd: " + err.Error())
		}
	}

	return nil
}

func (c *CassFsDriver) Remove(r *volume.RemoveRequest) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	mount, err := c.db.DeleteVolume(r.Name)
	if err != nil {
		return err
	}
	if mount.Clients == 0 {
		// There are no more containers using the mount, remove it
		location := filepath.Join(c.config.StateDir, "environment", mount.Hash + ".env")
		deleteEnvFile(location)
	}

	return nil
}

func (c *CassFsDriver) Mount(r *volume.MountRequest) (*volume.MountResponse, error ) {
	c.lock.Lock()
	defer c.lock.Unlock()

	fmt.Println("[Mount] Request for " + r.Name)

	mount, err := c.db.FindVolume(r.Name)
	if err != nil {
		fmt.Println("[Mount] Error finding volume " + r.Name)
		return &volume.MountResponse{}, err
	}
	if mount == nil {
		err := c.create(volume.CreateRequest{ Name: r.Name, Options: nil })
		if err != nil {
			return &volume.MountResponse{}, err
		}
	}

	mount, err = c.db.MountVolume(r.Name)
	if err != nil {
		return &volume.MountResponse{}, errors.New("DB Error: " + err.Error())
	}

	err = os.MkdirAll(mount.Location, 0755)
	if err != nil {
		return &volume.MountResponse{}, errors.New("Mkdir Error: " + err.Error())
	}
	err = c.startService(mount.Hash)
	if err != nil {
		return &volume.MountResponse{}, errors.New("Service Error: " + err.Error())
	}
	return &volume.MountResponse{ Mountpoint: mount.Location }, nil
}

func (c *CassFsDriver) Unmount(r *volume.UnmountRequest) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	mount, err := c.db.FindVolume(r.Name)
	if err != nil {
		return err
	}
	err = c.stopService(mount.Hash)
	if err != nil {
		return err
	}
	_, err = c.db.UnmountVolume(r.Name)
	if err != nil {
		return err
	}
	err = os.Remove(mount.Location)
	if err != nil {
		return err
	}
	err = c.stopService(mount.Hash)
	if err != nil {
		return err
	}
	return nil
}


func (c *CassFsDriver) Path(r *volume.PathRequest) (*volume.PathResponse, error) {
	mount, err := c.db.FindVolume(r.Name)
	if err != nil {
		fmt.Printf("Returning that there was an error finding the volume: %s\n", err)
		return &volume.PathResponse{}, err
	}

	if mount == nil {
		fmt.Println("Returning that this is an unknown volume")
		return &volume.PathResponse{}, errors.New("Unknown volume")
	}

	return &volume.PathResponse{ Mountpoint: mount.Location }, nil
}

func (c *CassFsDriver) Get(r *volume.GetRequest) (*volume.GetResponse, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	mount, err := c.db.FindVolume(r.Name)
	if err != nil {
		return &volume.GetResponse{}, err
	}

	if mount == nil {
		return &volume.GetResponse{}, errors.New("Unknown volume")
	}

	return &volume.GetResponse{ Volume: &volume.Volume{ Name: mount.Name, Mountpoint: mount.Location } }, nil
}

func (c *CassFsDriver) List() (*volume.ListResponse, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var volumes []*volume.Volume

	mounts, err := c.db.GetAll()
	if err != nil {
		return &volume.ListResponse{}, err
	}

	for _, mount := range mounts {
		volumes = append(volumes, &volume.Volume{ Name: mount.Name, Mountpoint: mount.Location })
	}
	return &volume.ListResponse{ Volumes: volumes }, nil
}

func (c *CassFsDriver) Capabilities() *volume.CapabilitiesResponse {
	var resp volume.CapabilitiesResponse
	resp.Capabilities = volume.Capability{ Scope: "local" }
	return &resp
}

func (c *CassFsDriver) enableSystemdUnit(path string) error {
	_, _, err := c.systemd.EnableUnitFiles([]string{ path }, true, false)
	//_, err := os.Lstat("/var/run/systemd/system/cassfs@.service")
	//if err == nil {
		// The symlink exists already, but instead of going into it - let's just remove it
		//os.Remove("/var/run/systemd/system/cassfs@.service")
	//}
	//err = os.Symlink(path, filepath.Join("/var/run/systemd/system/cassfs@.service"))
	return err
}

func (c *CassFsDriver) startService(id string) error {
	pid, err := c.systemd.StartUnit("cassfs-" + id + ".service", "fail", nil)
	if pid == 0 && err != nil {
		return err
	}
	return nil
}

func (c *CassFsDriver) stopService(id string) error {
	_, err := c.systemd.StopUnit("cassfs-" + id + ".service", "fail", nil)
	return err
}

func deleteEnvFile(location string) error {
	return os.Remove(location)
}

func writeUnitFile(location string, statedir string, hash string) error {
	//First see if the file exists - we will still write ours in
	//in case something has changed

	if _, err := os.Stat(location); err == nil {
		os.Remove(location)
	}
	tmpl, err := template.New("unit").Parse(unit_template)
	if err != nil {
		return err
	}
	unit_data := struct {
		StateDir string
		Hash     string
	}{
		statedir,
		hash,
	}

	f, err := os.OpenFile(location, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	err = tmpl.Execute(f, unit_data)
	if err != nil {
		return err
	}
	return nil
}

func writeEnvFile(location string, config *DriverConfig, mount *Mount) error {
	// Check to see if the file exists, we will delete it if it does
	// just in case things have changes
	if _, err := os.Stat(location); err == nil {
		os.Remove(location)
	}
	tmpl, err := template.New("env").Parse(unit_env_tmpl)
	if err != nil {
		return err
	}

	env_data := struct {
		Server      string
		Consistency string
		Keyspace    string
		Environment string
		Owner       int
		Mount       string
	}{
		config.Server,
		config.Consistency,
		config.Keyspace,
		mount.Environment,
		mount.Owner,
		mount.Location,
	}

	f, err := os.OpenFile(location, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	err = tmpl.Execute(f, env_data)
	if err != nil {
		return err
	}

	return nil
}

func makeDirs(path string) error {
	err := os.MkdirAll(path, os.FileMode(0755))
	return err
}
