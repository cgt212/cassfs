package driver

import (
	"database/sql"
	"path/filepath"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"crypto/sha1"
)

type VolumeDb struct {
	config *DriverConfig
	db *sql.DB
}

type Mount struct {
	Name        string
	Hash        string
	Environment string
	Owner       int
	Clients     int
	Location    string
}

func NewVolumeDb(config *DriverConfig) (*VolumeDb, error) {
	create := false
	dbLocation := filepath.Join(config.StateDir, "volumes.db")

	if _, err := os.Stat(dbLocation); os.IsNotExist(err) {
		create = true
	}

	db, err := sql.Open("sqlite3", dbLocation + "?cache=shared&mode=wcr")
	if err != nil {
		return nil, err
	}
	if create {
		stmt, err := db.Prepare(`CREATE TABLE IF NOT EXISTS 'mount' (
					'name' VARCHAR(256) PRIMARY KEY,
					'hash' CHARACTER(40),
					'owner' INTEGER,
					'environment' VARCHAR(256),
					'clients' INTEGER,
					'location' VARCHAR(256) )`)
		if err != nil {
			return nil, err
		}
		_, err = stmt.Exec()
		if err != nil {
			return nil, err
		}
	}
	return &VolumeDb{
		config: config,
		db:     db,
	}, nil
}

func (v *VolumeDb) FindVolume(name string) (*Mount, error) {
	stmt, err := v.db.Prepare("SELECT name, hash, clients, owner, environment, location FROM mount WHERE name=?")
	if err != nil {
		fmt.Printf("SQL Prepare Error: %s\n", err)
		return nil, err
	}
	fmt.Printf("[FindVolume] Searching for %s\n", name)
	rows, err := stmt.Query(name)
	if err != nil {
		fmt.Printf("SQL Query Error: %s\n", err)
		return nil, err
	}

	mount := &Mount{}

	if rows.Next() {
		err = rows.Scan(&mount.Name, &mount.Hash, &mount.Clients, &mount.Owner, &mount.Environment, &mount.Location)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			} else {
				return nil, err
			}
		}
	} else {
		return nil, nil
	}
	rows.Close()
	return mount, nil
}

func (v *VolumeDb) CreateVolume(name string, owner int, env string) (*Mount, error) {
	mount, err := v.FindVolume(name)
	if err != nil {
		fmt.Printf("Error finding volume: %s\n", err)
		return nil, err
	}

	if mount == nil {
		mp, sum := MountPoint(v.config.VolumeDir, name)
		//Create a mount point to insert
		mount := &Mount{
			Name:        name,
			Hash:        sum,
			Clients:     1,
			Owner:       owner,
			Environment: env,
			Location:    mp,
		}
		return mount, v.insertVolume(mount)
	}
	return mount, nil
}

func (v *VolumeDb) DeleteVolume(name string) (*Mount, error) {
	mount, err := v.FindVolume(name)
	if err != nil {
		return nil, err
	}
	if mount.Clients == 0 {
		stmt, err := v.db.Prepare("DELETE FROM mount WHERE name=?")
		if err != nil {
			return nil, err
		}
		_, err = stmt.Exec(name)
		if err != nil {
			return nil, err
		}
		mount.Clients = 0
		return mount, nil
	}
	return mount, nil
}

func (v *VolumeDb) MountVolume(name string) (*Mount, error) {
	mount, err := v.FindVolume(name)
	if err != nil {
		return nil, err
	}
	mount.Clients = mount.Clients + 1
	return mount, v.incrementClients(name)
}

func (v *VolumeDb) UnmountVolume(name string) (*Mount, error) {
	mount, err := v.FindVolume(name)
	if err != nil {
		return nil, err
	}
	mount.Clients = mount.Clients - 1
	return mount, v.decrementClients (name)
}

func (v *VolumeDb) insertVolume(m *Mount) error {
	stmt, err := v.db.Prepare("INSERT INTO mount (name, hash, clients, owner, environment, location) VALUES(?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	_, err = stmt.Exec(m.Name, m.Hash, m.Clients, m.Owner, m.Environment, m.Location)
	if err != nil {
		return err
	}
	return nil
}

func (v *VolumeDb) incrementClients(name string) error {
	stmt, err := v.db.Prepare("UPDATE mount SET clients=clients+1 WHERE name=?")
	if err != nil {
		fmt.Printf("[incrementClients] SQL Prepare Error: %s\n", err)
		return err
	}
	_, err = stmt.Exec(name)
	if err != nil {
		fmt.Printf("[incrementClients] SQL Exec Error: %s\n", err)
		return err
	}
	return nil
}

func (v *VolumeDb) decrementClients(name string) error {
	stmt, err := v.db.Prepare("UPDATE mount SET clients=clients-1 WHERE name=?")
	if err != nil {
		return err
	}
	_, err = stmt.Exec(name)
	if err != nil {
		return err
	}
	return nil
}

func (v *VolumeDb) incrementMount(name string) error {
	stmt, err := v.db.Prepare("UPDATE mount SET mounted=mounted+1 WHERE name=?")
	if err != nil {
		return err
	}
	_, err = stmt.Exec(name)
	if err != nil {
		return err
	}
	return nil
}

func (v *VolumeDb) decrementMount(name string) error {
	stmt, err := v.db.Prepare("UPDATE mount SET mounted=mounted-1 WHERE name=?")
	if err != nil {
		return err
	}
	_, err = stmt.Exec(name)
	if err != nil {
		return err
	}
	return nil
}

func (v *VolumeDb) GetAll() ([]*Mount, error) {
	var ret []*Mount
	stmt, err := v.db.Prepare("SELECT name, hash, clients, owner, environment, location  FROM mount")
	if err != nil {
		return nil, err
	}
	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var mnt Mount
		rows.Scan(&mnt.Name, &mnt.Hash, &mnt.Clients, &mnt.Owner, &mnt.Environment, &mnt.Location)
		ret = append(ret, &mnt)
	}
	rows.Close()
	return ret, nil
}

func MountPoint(voldir string, name string) (string, string) {
	sum := sha1.Sum([]byte(name))
	dir := fmt.Sprintf("%x", sum)
	mp := filepath.Join(voldir, dir)
	return mp, dir
}
