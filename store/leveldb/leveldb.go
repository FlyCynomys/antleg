package leveldb

import "github.com/syndtr/goleveldb/leveldb"

import "github.com/syndtr/goleveldb/leveldb/opt"
import "github.com/syndtr/goleveldb/leveldb/util"

type LevelDB struct {
	Topic string
	Path  string
	DB    *leveldb.DB
}

func NewLevelDB(name, path string) *LevelDB {
	return &LevelDB{
		Topic: name,
		Path:  path,
	}
}
func (l *LevelDB) Name() string {
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
