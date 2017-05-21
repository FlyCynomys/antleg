package leveldb

import (
	"encoding/binary"
	"path/filepath"

	"errors"

	"fmt"

	"github.com/FlyCynomys/tools/format"
	"github.com/hashicorp/raft"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDB struct {
	Topic            string      `json:"topic,omitempty"`
	Prefix           string      `json:"prefix,omitempty"`
	Path             string      `json:"path,omitempty"`
	Config           *leveldb.DB `json:"config,omitempty"` // not support yet
	DB               *leveldb.DB `json:"db,omitempty"`
	DataCurrentIndex uint64      `json:"data_current_index,omitempty"`
}

func NewLevelDB(topic, prefix string) *LevelDB {
	return &LevelDB{
		Topic:  topic,
		Path:   filepath.Join(prefix, topic),
		Prefix: prefix,
	}
}

//-----------------------with raft operation-----------------------------//

func (l *LevelDB) FirstIndex() (uint64, []byte, error) {
	var index uint64
	var key, value []byte
	iter := l.DB.NewIterator(nil, nil)
	if !iter.First() {
		return index, nil, errors.New("first index find failed")
	}
	key, value = iter.Key(), iter.Value()
	index = format.BytesToUint64(key)
	if index <= 0 {
		return index, nil, errors.New("first index find failed")
	}
	return index, value, nil
}

func (l *LevelDB) LastIndex() (uint64, []byte, error) {
	var index uint64
	var key, value []byte
	iter := l.DB.NewIterator(nil, nil)

	if !iter.Last() {
		return index, nil, errors.New("last index find failed")
	}
	key, value = iter.Key(), iter.Value()

	index = format.BytesToUint64(key)
	if index <= 0 {
		return index, nil, errors.New("last index find failed")
	}
	return index, value, nil
}

func (l *LevelDB) CurrentIndex() (uint64, []byte, error) {
	if l.DataCurrentIndex == 0 {
		return l.FirstIndex()
	}
	value, err := l.Get(format.Uint64ToBytes(l.DataCurrentIndex))
	return l.DataCurrentIndex, value, err
}

func (l *LevelDB) GetWithRaft(index uint64) (*raft.Log, error) {
	key := format.Uint64ToBytes(index)
	if key == nil {
		return nil, fmt.Errorf("key is error %d ", index)
	}
	value, err := l.Get(key)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	datalog := new(raft.Log)
	err = decodeLog(value, datalog)
	if err != nil {
		return nil, err
	}
	return datalog, nil
}

func (l *LevelDB) WriteWithRaft(datalog *raft.Log) error {
	if datalog == nil {
		return errors.New("data is empty")
	}
	key := format.Uint64ToBytes(datalog.Index)
	value := datalog.Data
	err := l.Put(key, value)
	return err
}

func (l *LevelDB) WiteWithRaftBatch(dataloglist []*raft.Log) error {
	if dataloglist == nil || len(dataloglist) <= 0 {
		return errors.New("data is empty")
	}
	var batchlog = make([][]byte, 16, 32)
	for _, value := range dataloglist {
		batchlog = append(batchlog, format.Uint64ToBytes(value.Index), encodeLog(value))
	}
	err := l.BatchWrite(batchlog)
	return err
}

//-----------------------with raft operation-----------------------------//

func (l *LevelDB) Topics() string {

	return l.Topic
}

func (l *LevelDB) Open() error {
	//leveldb.OpenFile(l.Path, opt.Options{})
	var err error
	l.DB, err = leveldb.OpenFile(l.Path, nil)
	if err != nil {

		return err
	}
	return nil
}

func (l *LevelDB) Close() error {
	if l.DB == nil {
		return nil
	}
	return l.DB.Close()
}

func (l *LevelDB) Get(key []byte) (value []byte, err error) {
	value, err = l.DB.Get(key, &opt.ReadOptions{
		DontFillCache: false,
		Strict:        opt.StrictReader,
	})
	return
}

func (l *LevelDB) Put(key []byte, value []byte) (err error) {
	err = l.DB.Put(key, value, &opt.WriteOptions{
		NoWriteMerge: false,
		Sync:         false,
	})
	return
}

func (l *LevelDB) Delete(key []byte) (err error) {
	err = l.DB.Delete(key, &opt.WriteOptions{
		NoWriteMerge: false,
		Sync:         false,
	})
	return
}

func (l *LevelDB) Exist(key []byte) (ret bool, err error) {
	ret, err = l.DB.Has(key, &opt.ReadOptions{
		DontFillCache: false,
		Strict:        opt.StrictReader,
	})
	return
}

func (l *LevelDB) IterateOperation(start, end []byte, fn func(key, value []byte)) (err error) {
	iter := l.DB.NewIterator(&util.Range{
		Start: start,
		Limit: end,
	}, &opt.ReadOptions{
		DontFillCache: false,
		Strict:        opt.StrictReader,
	})
	for iter.Next() {
		key, value := iter.Key(), iter.Value()
		fn(key, value)
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		return err
	}
	return nil
}

func (l *LevelDB) IterateOperationWithPrefix(prefix []byte, fn func(key, value []byte)) (err error) {
	iter := l.DB.NewIterator(util.BytesPrefix(prefix),
		&opt.ReadOptions{
			DontFillCache: false,
			Strict:        opt.StrictReader,
		})
	for iter.Next() {
		key, value := iter.Key(), iter.Value()
		fn(key, value)
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		return err
	}
	return nil
}

func (l *LevelDB) BatchWrite(keyvaluepairs [][]byte) (err error) {
	batch := new(leveldb.Batch)
	for index := 0; index < len(keyvaluepairs); index = index + 2 {
		batch.Put(keyvaluepairs[index], keyvaluepairs[index+1])
	}
	err = l.DB.Write(batch, &opt.WriteOptions{
		NoWriteMerge: false,
		Sync:         false,
	})
	return
}

func (l *LevelDB) BatchDelete(keylist [][]byte) (err error) {
	batch := new(leveldb.Batch)
	for _, key := range keylist {
		batch.Delete(key)
	}
	err = l.DB.Write(batch, &opt.WriteOptions{
		NoWriteMerge: false,
		Sync:         false,
	})
	return
}

func decodeLog(buf []byte, log *raft.Log) error {
	if len(buf) < 25 {
		return errors.New("log info error")
	}
	log.Index = binary.LittleEndian.Uint64(buf[0:8])
	log.Term = binary.LittleEndian.Uint64(buf[8:16])
	log.Type = raft.LogType(buf[16])
	log.Data = make([]byte, binary.LittleEndian.Uint64(buf[17:25]))
	if len(buf[25:]) < len(log.Data) {
		return errors.New("log info error")
	}
	copy(log.Data, buf[25:])
	return nil
}

func encodeLog(log *raft.Log) []byte {
	var buf []byte
	var num = make([]byte, 8)
	binary.LittleEndian.PutUint64(num, log.Index)
	buf = append(buf, num...)
	binary.LittleEndian.PutUint64(num, log.Term)
	buf = append(buf, num...)
	buf = append(buf, byte(log.Type))
	binary.LittleEndian.PutUint64(num, uint64(len(log.Data)))
	buf = append(buf, num...)
	buf = append(buf, log.Data...)
	return buf
}
