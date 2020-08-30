package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	db := engine_util.CreateDB("kv", conf)
	return &StandAloneStorage{
		db: db,
	}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	return NewReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, modify := range batch {
			var err error
			switch modify.Data.(type) {
			case storage.Put:
				put := modify.Data.(storage.Put)
				key := engine_util.KeyWithCF(put.Cf, put.Key)
				err = txn.Set(key, put.Value)
			case storage.Delete:
				del := modify.Data.(storage.Delete)
				key := engine_util.KeyWithCF(del.Cf, del.Key)
				err = txn.Delete(key)
			default:
				err = errors.New("unknown modify type")
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

type Reader struct {
	txn *badger.Txn
}

func NewReader(txn *badger.Txn) *Reader {
	return &Reader{
		txn: txn,
	}
}

//GetCF(cf string, key []byte) ([]byte, error)
//IterCF(cf string) engine_util.DBIterator
//Close()

func (reader *Reader) GetCF(cf string, key []byte) ([]byte, error) {
	v, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return v, nil
}

func (reader *Reader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}

func (reader *Reader) Close() {
	reader.txn.Discard()
}
