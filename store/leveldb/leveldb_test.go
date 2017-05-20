package leveldb

import (
	"reflect"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
)

func TestLevelDB_FirstIndex(t *testing.T) {
	type fields struct {
		Topic            string
		Prefix           string
		Path             string
		Config           *leveldb.DB
		DB               *leveldb.DB
		DataCurrentIndex uint64
	}
	tests := []struct {
		name    string
		fields  fields
		want    uint64
		want1   []byte
		wantErr bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LevelDB{
				Topic:            tt.fields.Topic,
				Prefix:           tt.fields.Prefix,
				Path:             tt.fields.Path,
				Config:           tt.fields.Config,
				DB:               tt.fields.DB,
				DataCurrentIndex: tt.fields.DataCurrentIndex,
			}
			got, got1, err := l.FirstIndex()
			if (err != nil) != tt.wantErr {
				t.Errorf("LevelDB.FirstIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LevelDB.FirstIndex() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("LevelDB.FirstIndex() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
