package leveldb

import "errors"
import "sync"

var (
	dbLogs = []byte("logs")
	dbConf = []byte("conf")
)

type StoreManage struct {
	StoreDataMap map[string]*LevelDB `json:"store_data_map,omitempty"`
	sync.RWMutex `json:",omitempty"`
}

func (s *StoreManage) Initialize(storepath string) {
	if storepath == "" {
		storepath = "."
	}
	s.StoreDataMap = make(map[string]*LevelDB)
	return
}

func (s *StoreManage) AddStore(st *LevelDB) (bool, error) {
	if st != nil {
		return false, errors.New("store is nil")
	}
	s.Lock()
	s.Unlock()
	s.StoreDataMap[st.Topic] = st
	return true, nil
}

func (s *StoreManage) DeleteStore(st *LevelDB) (bool, error) {
	if st != nil {
		return false, errors.New("st is nil")
	}
	s.Lock()
	s.Unlock()
	delete(s.StoreDataMap, st.Topic)
	return true, nil
}

func (s *StoreManage) GetStore(topic string) (*LevelDB, error) {
	if topic == "" {
		return nil, errors.New("topic is empty")
	}
	s.RLock()
	defer s.RUnlock()
	return s.StoreDataMap[topic], nil
}
