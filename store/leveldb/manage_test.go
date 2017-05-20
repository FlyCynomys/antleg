package leveldb

import (
	"sync"
	"testing"
)

func TestStoreManage_AddStore(t *testing.T) {
	type fields struct {
		StoreDataMap map[string]*LevelDB
		RWMutex      sync.RWMutex
	}
	type args struct {
		st *LevelDB
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StoreManage{
				StoreDataMap: tt.fields.StoreDataMap,
				RWMutex:      tt.fields.RWMutex,
			}
			got, err := s.AddStore(tt.args.st)
			if (err != nil) != tt.wantErr {
				t.Errorf("StoreManage.AddStore() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("StoreManage.AddStore() = %v, want %v", got, tt.want)
			}
		})
	}
}
