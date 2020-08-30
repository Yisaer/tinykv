package server

import (
	"context"
	"reflect"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	v, err := reader.GetCF(req.Cf, req.Key)

	if err != nil {
		if err == badger.ErrKeyNotFound {
			resp.NotFound = true
			return resp, nil
		}
		return resp, err
	}
	if v == nil {
		resp.NotFound = true
		return resp, nil
	}
	return &kvrpcpb.RawGetResponse{
		Value: v,
	}, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawPutResponse{}
	put := storage.Put{
		Cf:    req.Cf,
		Key:   req.Key,
		Value: req.Value,
	}
	op := storage.Modify{
		Data: put,
	}
	batch := []storage.Modify{
		op,
	}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := &kvrpcpb.RawDeleteResponse{}
	del := storage.Delete{
		Cf:  req.Cf,
		Key: req.Key,
	}
	op := storage.Modify{
		Data: del,
	}
	batch := []storage.Modify{
		op,
	}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	resp := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	iterator := reader.IterCF(req.Cf)
	defer iterator.Close()

	for iterator.Seek(req.StartKey); iterator.Valid() && uint32(len(resp.Kvs)) < req.Limit; iterator.Next() {
		item := iterator.Item()
		kvPair := &kvrpcpb.KvPair{
			Key: item.Key(),
		}
		v, err := item.Value()
		if err != nil {
			return &kvrpcpb.RawScanResponse{}, err
		}
		kvPair.Value = v
		resp.Kvs = append(resp.Kvs, kvPair)
	}
	return resp, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}

// rawRegionError assigns region errors to a RegionError field, and other errors to the Error field,
// of resp. This is only a valid way to handle errors for the raw commands. Returns true if err is
// non-nil, false otherwise.
func rawRegionError(err error, resp interface{}) bool {
	if err == nil {
		return false
	}
	respValue := reflect.ValueOf(resp).Elem()
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		respValue.FieldByName("RegionError").Set(reflect.ValueOf(regionErr.RequestErr))
	} else {
		respValue.FieldByName("Error").Set(reflect.ValueOf(err.Error()))
	}
	return true
}
