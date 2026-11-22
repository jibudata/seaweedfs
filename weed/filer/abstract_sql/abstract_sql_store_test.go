package abstract_sql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// The bucket owner index lives at /buckets/.system/owners/<owner>/<bucket>.
// .system is an internal folder, not an S3 bucket, so getTxOrDB must route it
// to the default table rather than reject it as an invalid bucket name.
func TestGetTxOrDBInternalSystemFolder(t *testing.T) {
	store := &AbstractSqlStore{SupportBucketTable: true}
	ctx := context.Background()

	cases := []struct {
		path          string
		isForChildren bool
	}{
		{"/buckets/.system/owners", false},
		{"/buckets/.system/owners/foo/bucket1", false},
		{"/buckets/.system/owners/foo", true},
	}
	for _, c := range cases {
		_, bucket, shortPath, err := store.getTxOrDB(ctx, util.FullPath(c.path), c.isForChildren)
		if err != nil {
			t.Errorf("getTxOrDB(%s): unexpected error: %v", c.path, err)
		}
		if bucket != DEFAULT_TABLE {
			t.Errorf("getTxOrDB(%s): bucket = %q, want %q", c.path, bucket, DEFAULT_TABLE)
		}
		if string(shortPath) != c.path {
			t.Errorf("getTxOrDB(%s): shortPath = %q, want %q", c.path, shortPath, c.path)
		}
	}
}

func TestGetTxOrDBRealBucket(t *testing.T) {
	store := &AbstractSqlStore{SupportBucketTable: true, dbs: map[string]bool{"mybucket": true}}
	ctx := context.Background()

	_, bucket, shortPath, err := store.getTxOrDB(ctx, util.FullPath("/buckets/mybucket/dir/file.txt"), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bucket != "mybucket" {
		t.Errorf("bucket = %q, want mybucket", bucket)
	}
	if string(shortPath) != "/dir/file.txt" {
		t.Errorf("shortPath = %q, want /dir/file.txt", shortPath)
	}
}

func TestSplitPoolForKv(t *testing.T) {
	cases := []struct {
		maxOpen, mainOpen, kvOpen int
	}{
		{0, 0, 0},   // unbounded: nothing waits, one pool is enough
		{-1, -1, 0}, // same, however the operator spelled it
		{50, 42, 8},
		{16, 12, 4},
		{4, 3, 1},
		{2, 1, 1},
		{1, 1, 1}, // a listing needs two, so a cap of 1 has to become 2
	}
	for _, c := range cases {
		mainOpen, kvOpen := splitPoolForKv(c.maxOpen)
		if mainOpen != c.mainOpen || kvOpen != c.kvOpen {
			t.Errorf("splitPoolForKv(%d) = %d, %d, want %d, %d", c.maxOpen, mainOpen, kvOpen, c.mainOpen, c.kvOpen)
		}
	}
}
type writeGen struct{ SqlGenerator }

func (writeGen) GetSqlInsert(string) string { return "INSERT entry" }
func (writeGen) GetSqlDelete(string) string { return "DELETE entry" }

// What IsolatesTransactions declares: an entry write, an entry delete and a KV
// write under a transaction's context all go through that transaction, the KV
// write too when KV has a pool of its own, and a rollback ends it.
func TestWritesUnderATransactionGoThroughIt(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// nothing is expected of the KV pool: a call to it fails the write
	kvDB, kvMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer kvDB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT entry").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE entry").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT entry").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	store := &AbstractSqlStore{SqlGenerator: writeGen{}, DB: db, KvDB: kvDB}
	if !store.IsolatesTransactions() {
		t.Fatal("IsolatesTransactions = false")
	}
	txCtx, err := store.BeginTransaction(context.Background())
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	// one mock serves the pool and its transactions alike, so the expectations
	// alone would also pass for writes through store.DB
	tx, _ := txCtx.Value("tx").(*sql.Tx)
	if tx == nil {
		t.Fatal("BeginTransaction context carries no *sql.Tx")
	}
	if db, _, _, _ := store.getTxOrDB(txCtx, "/dir/new.txt", false); db != TxOrDB(tx) {
		t.Fatalf("getTxOrDB under the transaction = %T, want its *sql.Tx", db)
	}
	if db := store.kvDB(txCtx); db != TxOrDB(tx) {
		t.Fatalf("kvDB under the transaction = %T, want its *sql.Tx", db)
	}
	if err := store.InsertEntry(txCtx, &filer.Entry{FullPath: "/dir/new.txt"}); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}
	if err := store.DeleteEntry(txCtx, "/dir/old.txt"); err != nil {
		t.Fatalf("DeleteEntry: %v", err)
	}
	if err := store.KvPut(txCtx, []byte("hard-link-id"), []byte("record")); err != nil {
		t.Fatalf("KvPut: %v", err)
	}
	if err := store.RollbackTransaction(txCtx); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
	if err := kvMock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
