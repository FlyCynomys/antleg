package store

import (
	"errors"

	"github.com/FlyCynomys/antleg/config"
	"github.com/FlyCynomys/antleg/store/leveldb"
)

type StoreKeeper interface {
	Name() string
	Open() error
	Close() error
	Get([]byte) ([]byte, error)
	Put([]byte, []byte) error
}

var leveldbstore StoreKeeper

func Init(name string) error {
	leveldbstore = leveldb.NewLevelDB(name, config.GetConfig().LevelDbPath)
	if leveldbstore == nil {
		return errors.New("create leveldb failed")
	}
	return nil
}
