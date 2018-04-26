package leveldb

import (
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/tidwall/btree"
	"github.com/tidwall/match"
	"github.com/tidwall/rtree"
)

var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx closed")

	// ErrInvalid is returned when the database file is an invalid format.
	ErrInvalid = errors.New("invalid database")

	// ErrDatabaseClosed is returned when the database is closed.
	ErrDatabaseClosed = errors.New("database closed")

	// ErrIndexExists is returned when an index already exists in the database.
	ErrIndexExists = errors.New("index exists")

	// ErrInvalidOperation is returned when an operation cannot be completed.
	ErrInvalidOperation = errors.New("invalid operation")

	// ErrInvalidSyncPolicy is returned for an invalid SyncPolicy value.
	ErrInvalidSyncPolicy = errors.New("invalid sync policy")

	// ErrShrinkInProcess is returned when a shrink operation is in-process.
	ErrShrinkInProcess = errors.New("shrink is in-process")

	// ErrPersistenceActive is returned when post-loading data from an database
	// not opened with Open(":memory:").
	ErrPersistenceActive = errors.New("persistence active")

	// ErrTxIterating is returned when Set or Delete are called while iterating.
	ErrTxIterating = errors.New("tx is iterating")
)

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy int

const (
	// Never is used to disable syncing data to disk.
	// The faster and less safe method.
	Never SyncPolicy = 0
	// EverySecond is used to sync data to disk every second.
	// It's pretty fast and you can lose 1 second of data if there
	// is a disaster.
	// This is the recommended setting.
	EverySecond = 1
	// Always is used to sync data after every write to disk.
	// Slow. Very safe.
	Always = 2
)

// Config represents database configuration options. These
// options are used to change various behaviors of the database.
type Config struct {
	// SyncPolicy adjusts how often the data is synced to disk.
	// This value can be Never, EverySecond, or Always.
	// The default is EverySecond.
	SyncPolicy SyncPolicy

	// AutoShrinkPercentage is used by the background process to trigger
	// a shrink of the aof file when the size of the file is larger than the
	// percentage of the result of the previous shrunk file.
	// For example, if this value is 100, and the last shrink process
	// resulted in a 100mb file, then the new aof file must be 200mb before
	// a shrink is triggered.
	AutoShrinkPercentage int

	// AutoShrinkMinSize defines the minimum size of the aof file before
	// an automatic shrink can occur.
	AutoShrinkMinSize int

	// AutoShrinkDisabled turns off automatic background shrinking
	AutoShrinkDisabled bool

	// OnExpired is used to custom handle the deletion option when a key
	// has been expired.
	OnExpired func(keys []string)

	// OnExpiredSync will be called inside the same transaction that is performing
	// the deletion of expired items. If OnExpired is present then this callback
	// will not be called. If this callback is present, then the deletion of the
	// timeed-out item is the explicit responsibility of this callback.
	OnExpiredSync func(key, value string, tx *Tx) error
}

//// Open opens a database at the provided path.
//// If the file does not exist then it will be created automatically.
//func Open(path string) (*DB, error) {
//	db := &DB{}
//	// initialize trees and indexes
//	db.keys = btree.New(btreeDegrees, nil)
//	db.exps = btree.New(btreeDegrees, &exctx{db})
//	db.idxs = make(map[string]*index)
//	// initialize default configuration
//	db.config = Config{
//		SyncPolicy:           EverySecond,
//		AutoShrinkPercentage: 100,
//		AutoShrinkMinSize:    32 * 1024 * 1024,
//	}
//	// turn off persistence for pure in-memory
//	db.persist = path != ":memory:"
//	if db.persist {
//		var err error
//		// hardcoding 0666 as the default mode.
//		db.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
//		if err != nil {
//			return nil, err
//		}
//		// load the database from disk
//		if err := db.load(); err != nil {
//			// close on error, ignore close error
//			_ = db.file.Close()
//			return nil, err
//		}
//	}
//	// start the background manager.
//	go db.backgroundManager()
//	return db, nil
//}

//// Close releases all database resources.
//// All transactions must be closed before closing the database.
//func (db *DB) Close() error {
//	db.mu.Lock()
//	defer db.mu.Unlock()
//	if db.closed {
//		return ErrDatabaseClosed
//	}
//	db.closed = true
//	if db.persist {
//		db.file.Sync() // do a sync but ignore the error
//		if err := db.file.Close(); err != nil {
//			return err
//		}
//	}
//	// Let's release all references to nil. This will help both with debugging
//	// late usage panics and it provides a hint to the garbage collector
//	db.keys, db.exps, db.idxs, db.file = nil, nil, nil, nil
//	return nil
//}

//// Save writes a snapshot of the database to a writer. This operation blocks all
//// writes, but not reads. This can be used for snapshots and backups for pure
//// in-memory databases using the ":memory:". Database that persist to disk
//// can be snapshotted by simply copying the database file.
//func (db *DB) Save(wr io.Writer) error {
//	var err error
//	db.mu.RLock()
//	defer db.mu.RUnlock()
//	// use a buffered writer and flush every 4MB
//	var buf []byte
//	// iterated through every item in the database and write to the buffer
//	db.keys.Ascend(func(item btree.Item) bool {
//		dbi := item.(*dbItem)
//		buf = dbi.writeSetTo(buf)
//		if len(buf) > 1024*1024*4 {
//			// flush when buffer is over 4MB
//			_, err = wr.Write(buf)
//			if err != nil {
//				return false
//			}
//			buf = buf[:0]
//		}
//		return true
//	})
//	if err != nil {
//		return err
//	}
//	// one final flush
//	if len(buf) > 0 {
//		_, err = wr.Write(buf)
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}

//// Load loads commands from reader. This operation blocks all reads and writes.
//// Note that this can only work for fully in-memory databases opened with
//// Open(":memory:").
//func (db *DB) Load(rd io.Reader) error {
//	db.mu.Lock()
//	defer db.mu.Unlock()
//	if db.persist {
//		// cannot load into databases that persist to disk
//		return ErrPersistenceActive
//	}
//	return db.readLoad(rd, time.Now())
//}

// CreateIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// An error will occur if an index with the same name already exists.
//
// When a pattern is provided, the index will be populated with
// keys that match the specified pattern. This is a very simple pattern
// match where '*' matches on any number characters and '?' matches on
// any one character.
// The less function compares if string 'a' is less than string 'b'.
// It allows for indexes to create custom ordering. It's possible
// that the strings may be textual or binary. It's up to the provided
// less function to handle the content format and comparison.
// There are some default less function that can be used such as
// IndexString, IndexBinary, etc.
//
// Deprecated: Use Transactions
func (db *DB) CreateIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return db.Update(func(tx *Tx) error {
		return tx.CreateIndex(name, pattern, less...)
	})
}

// ReplaceIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// If a previous index with the same name exists, that index will be deleted.
//
// Deprecated: Use Transactions
func (db *DB) ReplaceIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return db.Update(func(tx *Tx) error {
		err := tx.CreateIndex(name, pattern, less...)
		if err != nil {
			if err == ErrIndexExists {
				err := tx.DropIndex(name)
				if err != nil {
					return err
				}
				return tx.CreateIndex(name, pattern, less...)
			}
			return err
		}
		return nil
	})
}

// CreateSpatialIndex builds a new index and populates it with items.
// The items are organized in an r-tree and can be retrieved using the
// Intersects method.
// An error will occur if an index with the same name already exists.
//
// The rect function converts a string to a rectangle. The rectangle is
// represented by two arrays, min and max. Both arrays may have a length
// between 1 and 20, and both arrays must match in length. A length of 1 is a
// one dimensional rectangle, and a length of 4 is a four dimension rectangle.
// There is support for up to 20 dimensions.
// The values of min must be less than the values of max at the same dimension.
// Thus min[0] must be less-than-or-equal-to max[0].
// The IndexRect is a default function that can be used for the rect
// parameter.
//
// Deprecated: Use Transactions
func (db *DB) CreateSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return db.Update(func(tx *Tx) error {
		return tx.CreateSpatialIndex(name, pattern, rect)
	})
}

// ReplaceSpatialIndex builds a new index and populates it with items.
// The items are organized in an r-tree and can be retrieved using the
// Intersects method.
// If a previous index with the same name exists, that index will be deleted.
//
// Deprecated: Use Transactions
func (db *DB) ReplaceSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return db.Update(func(tx *Tx) error {
		err := tx.CreateSpatialIndex(name, pattern, rect)
		if err != nil {
			if err == ErrIndexExists {
				err := tx.DropIndex(name)
				if err != nil {
					return err
				}
				return tx.CreateSpatialIndex(name, pattern, rect)
			}
			return err
		}
		return nil
	})
}

// DropIndex removes an index.
//
// Deprecated: Use Transactions
func (db *DB) DropIndex(name string) error {
	return db.Update(func(tx *Tx) error {
		return tx.DropIndex(name)
	})
}

// Indexes returns a list of index names.
//
// Deprecated: Use Transactions
func (db *DB) Indexes() ([]string, error) {
	var names []string
	var err = db.View(func(tx *Tx) error {
		var err error
		names, err = tx.Indexes()
		return err
	})
	return names, err
}

//TODO
// ReadConfig returns the database configuration.
//func (db *DB) ReadConfig(config *Config) error {
//	db.mu.RLock()
//	defer db.mu.RUnlock()
//	if db.closed {
//		return ErrDatabaseClosed
//	}
//	*config = db.config
//	return nil
//}

//// SetConfig updates the database configuration.
//func (db *DB) SetConfig(config Config) error {
//	db.mu.Lock()
//	defer db.mu.Unlock()
//	if db.closed {
//		return ErrDatabaseClosed
//	}
//	switch config.SyncPolicy {
//	default:
//		return ErrInvalidSyncPolicy
//	case Never, EverySecond, Always:
//	}
//	db.config = config
//	return nil
//}

// insertIntoDatabase performs inserts an item in to the database and updates
// all indexes. If a previous item with the same key already exists, that item
// will be replaced with the new one, and return the previous item.
func (db *DB) insertIntoDatabase(item *DbItem) *DbItem {
	var pdbi *DbItem
	prev := db.keys.ReplaceOrInsert(item)
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// fully delete this item from all indexes.
		pdbi = prev.(*DbItem)
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the exipres tree.
			db.exps.Delete(pdbi)
		}
		for _, idx := range db.idxs {
			if idx.btr != nil {
				// Remove it from the btree index.
				idx.btr.Delete(pdbi)
			}
			if idx.rtr != nil {
				// Remove it from the rtree index.
				idx.rtr.Remove(pdbi)
			}
		}
	}
	if item.opts != nil && item.opts.ex {
		// The new item has eviction options. Add it to the
		// expires tree
		db.exps.ReplaceOrInsert(item)
	}
	for _, idx := range db.idxs {
		if !idx.match(item.key) {
			continue
		}
		if idx.btr != nil {
			// Add new item to btree index.
			idx.btr.ReplaceOrInsert(item)
		}
		if idx.rtr != nil {
			// Add new item to rtree index.
			idx.rtr.Insert(item)
		}
	}
	// we must return the previous item to the caller.
	return pdbi
}

// deleteFromDatabase removes and item from the database and indexes. The input
// item must only have the key field specified thus "&dbItem{key: key}" is all
// that is needed to fully remove the item with the matching key. If an item
// with the matching key was found in the database, it will be removed and
// returned to the caller. A nil return value means that the item was not
// found in the database
func (db *DB) deleteFromDatabase(item *DbItem) *DbItem {
	var pdbi *DbItem
	prev := db.keys.Delete(item)
	if prev != nil {
		pdbi = prev.(*DbItem)
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the exipres tree.
			db.exps.Delete(pdbi)
		}
		for _, idx := range db.idxs {
			if idx.btr != nil {
				// Remove it from the btree index.
				idx.btr.Delete(pdbi)
			}
			if idx.rtr != nil {
				// Remove it from the rtree index.
				idx.rtr.Remove(pdbi)
			}
		}
	}
	return pdbi
}

//TODO
// backgroundManager runs continuously in the background and performs various
//// operations such as removing expired items and syncing to disk.
//func (db *DB) backgroundManager() {
//	flushes := 0
//	t := time.NewTicker(time.Second)
//	defer t.Stop()
//	for range t.C {
//		var shrink bool
//		// Open a standard view. This will take a full lock of the
//		// database thus allowing for access to anything we need.
//		var onExpired func([]string)
//		var expired []*dbItem
//		var onExpiredSync func(key, value string, tx *Tx) error
//		err := db.Update(func(tx *Tx) error {
//			onExpired = db.config.OnExpired
//			if onExpired == nil {
//				onExpiredSync = db.config.OnExpiredSync
//			}
//			if db.persist && !db.config.AutoShrinkDisabled {
//				pos, err := db.file.Seek(0, 1)
//				if err != nil {
//					return err
//				}
//				aofsz := int(pos)
//				if aofsz > db.config.AutoShrinkMinSize {
//					prc := float64(db.config.AutoShrinkPercentage) / 100.0
//					shrink = aofsz > db.lastaofsz+int(float64(db.lastaofsz)*prc)
//				}
//			}
//			// produce a list of expired items that need removing
//			db.exps.AscendLessThan(&dbItem{
//				opts: &dbItemOpts{ex: true, exat: time.Now()},
//			}, func(item btree.Item) bool {
//				expired = append(expired, item.(*dbItem))
//				return true
//			})
//			if onExpired == nil && onExpiredSync == nil {
//				for _, itm := range expired {
//					if _, err := tx.Delete(itm.key); err != nil {
//						// it's ok to get a "not found" because the
//						// 'Delete' method reports "not found" for
//						// expired items.
//						if err != ErrNotFound {
//							return err
//						}
//					}
//				}
//			} else if onExpiredSync != nil {
//				for _, itm := range expired {
//					if err := onExpiredSync(itm.key, itm.val, tx); err != nil {
//						return err
//					}
//				}
//			}
//			return nil
//		})
//		if err == ErrDatabaseClosed {
//			break
//		}

//		// send expired event, if needed
//		if onExpired != nil && len(expired) > 0 {
//			keys := make([]string, 0, 32)
//			for _, itm := range expired {
//				keys = append(keys, itm.key)
//			}
//			onExpired(keys)
//		}

//		// execute a disk sync, if needed
//		func() {
//			db.mu.Lock()
//			defer db.mu.Unlock()
//			if db.persist && db.config.SyncPolicy == EverySecond &&
//				flushes != db.flushes {
//				_ = db.file.Sync()
//				flushes = db.flushes
//			}
//		}()
//		if shrink {
//			if err = db.Shrink(); err != nil {
//				if err == ErrDatabaseClosed {
//					break
//				}
//			}
//		}
//	}
//}

// managed calls a block of code that is fully contained in a transaction.
// This method is intended to be wrapped by Update and View
func (db *DB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	var tx *Tx
	tx, err = db.Begin(writable)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			// The caller returned an error. We must rollback.
			_ = tx.Rollback()
			return
		}
		if writable {
			// Everything went well. Lets Commit()
			err = tx.Commit()
		} else {
			// read-only transaction can only roll back.
			err = tx.Rollback()
		}
	}()
	tx.funcd = true
	defer func() {
		tx.funcd = false
	}()
	err = fn(tx)
	return
}

// View executes a function within a managed read-only transaction.
// When a non-nil error is returned from the function that error will be return
// to the caller of View().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.managed(false, fn)
}

// Update executes a function within a managed read/write transaction.
// The transaction has been committed when no error is returned.
// In the event that an error is returned, the transaction will be rolled back.
// When a non-nil error is returned from the function, the transaction will be
// rolled back and the that error will be return to the caller of Update().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.managed(true, fn)
}

// get return an item or nil if not found.
func (db *DB) mget(key string) *DbItem {
	item := db.keys.Get(&DbItem{key: key})
	if item != nil {
		return item.(*DbItem)
	}
	return nil
}

// Tx represents a transaction on the database. This transaction can either be
// read-only or read/write. Read-only transactions can be used for retrieving
// values for keys and iterating through keys and values. Read/write
// transactions can set and delete keys.
//
// All transactions must be committed or rolled-back when done.
type Tx struct {
	db       *DB             // the underlying database.
	writable bool            // when false mutable operations fail.
	funcd    bool            // when true Commit and Rollback panic.
	wc       *txWriteContext // context for writable transactions.
}

type txWriteContext struct {
	// rollback when deleteAll is called
	rbkeys *btree.BTree      // a tree of all item ordered by key
	rbexps *btree.BTree      // a tree of items ordered by expiration
	rbidxs map[string]*Index // the index trees.

	rollbackItems   map[string]*DbItem // details for rolling back tx.
	commitItems     map[string]*DbItem // details for committing tx.
	itercount       int                // stack of iterators
	rollbackIndexes map[string]*Index  // details for dropped indexes.
}

// DeleteAll deletes all items from the database.
func (tx *Tx) DeleteAll() error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}

	// check to see if we've already deleted everything
	if tx.wc.rbkeys == nil {
		// we need to backup the live data in case of a rollback.
		tx.wc.rbkeys = tx.db.keys
		tx.wc.rbexps = tx.db.exps
		tx.wc.rbidxs = tx.db.idxs
	}

	// now reset the live database trees
	tx.db.keys = btree.New(btreeDegrees, nil)
	tx.db.exps = btree.New(btreeDegrees, &exctx{tx.db})
	tx.db.idxs = make(map[string]*Index)

	// finally re-create the indexes
	for name, idx := range tx.wc.rbidxs {
		tx.db.idxs[name] = idx.clearCopy()
	}

	// always clear out the commits
	tx.wc.commitItems = make(map[string]*DbItem)

	return nil
}

// Begin opens a new transaction.
// Multiple read-only transactions can be opened at the same time but there can
// only be one read/write transaction at a time. Attempting to open a read/write
// transactions while another one is in progress will result in blocking until
// the current read/write transaction is completed.
//
// All transactions must be closed by calling Commit() or Rollback() when done.
func (db *DB) Begin(writable bool) (*Tx, error) {
	tx := &Tx{
		db:       db,
		writable: writable,
	}
	tx.lock()
	if db.closer == nil {
		tx.unlock()
		return nil, ErrDatabaseClosed
	}
	if writable {
		// writable transactions have a writeContext object that
		// contains information about changes to the database.
		tx.wc = &txWriteContext{}
		tx.wc.rollbackItems = make(map[string]*DbItem)
		tx.wc.rollbackIndexes = make(map[string]*Index)
		if db.persist {
			tx.wc.commitItems = make(map[string]*DbItem)
		}
	}
	return tx, nil
}

// lock locks the database based on the transaction type.
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

// unlock unlocks the database based on the transaction type.
func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}

// rollbackInner handles the underlying rollback logic.
// Intended to be called from Commit() and Rollback().
func (tx *Tx) rollbackInner() {
	// rollback the deleteAll if needed
	if tx.wc.rbkeys != nil {
		tx.db.keys = tx.wc.rbkeys
		tx.db.idxs = tx.wc.rbidxs
		tx.db.exps = tx.wc.rbexps
	}
	for key, item := range tx.wc.rollbackItems {
		tx.db.deleteFromDatabase(&DbItem{key: key})
		if item != nil {
			// When an item is not nil, we will need to reinsert that item
			// into the database overwriting the current one.
			tx.db.insertIntoDatabase(item)
		}
	}
	for name, idx := range tx.wc.rollbackIndexes {
		delete(tx.db.idxs, name)
		if idx != nil {
			// When an index is not nil, we will need to rebuilt that index
			// this could be an expensive process if the database has many
			// items or the index is complex.
			tx.db.idxs[name] = idx
			idx.rebuild()
		}
	}
}

// Commit writes all changes to disk.
// An error is returned when a write error occurs, or when a Commit() is called
// from a read-only transaction.
func (tx *Tx) Commit() error {
	if tx.funcd {
		panic("managed tx commit not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}
	var err error
	if tx.db.persist && (len(tx.wc.commitItems) > 0 || tx.wc.rbkeys != nil) {
		tx.db.buf = tx.db.buf[:0]
		// write a flushdb if a deleteAll was called.
		if tx.wc.rbkeys != nil {
			tx.db.buf = append(tx.db.buf, "*1\r\n$7\r\nflushdb\r\n"...)
		}
		// Each committed record is written to disk
		for key, item := range tx.wc.commitItems {
			if item == nil {
				tx.db.buf = (&DbItem{key: key}).writeDeleteTo(tx.db.buf)
			} else {
				tx.db.buf = item.writeSetTo(tx.db.buf)
			}
		}
		// Flushing the buffer only once per transaction.
		// If this operation fails then the write did failed and we must
		// rollback.
		//	TODO	if _, err = tx.db.file.Write(tx.db.buf); err != nil {
		//			tx.rollbackInner()
		//		}

		//TODO	_ = tx.db.file.Sync()

		// Increment the number of flushes. The background syncing uses this.
		tx.db.flushes++
	}
	// Unlock the database and allow for another writable transaction.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return err
}

// Rollback closes the transaction and reverts all mutable operations that
// were performed on the transaction such as Set() and Delete().
//
// Read-only transactions can only be rolled back, not committed.
func (tx *Tx) Rollback() error {
	if tx.funcd {
		panic("managed tx rollback not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	}
	// The rollback func does the heavy lifting.
	if tx.writable {
		tx.rollbackInner()
	}
	// unlock the database for more transactions.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return nil
}

// GetLess returns the less function for an index. This is handy for
// doing ad-hoc compares inside a transaction.
// Returns ErrNotFound if the index is not found or there is no less
// function bound to the index
func (tx *Tx) GetLess(index string) (func(a, b string) bool, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	idx, ok := tx.db.idxs[index]
	if !ok || idx.less == nil {
		return nil, ErrNotFound
	}
	return idx.less, nil
}

// GetRect returns the rect function for an index. This is handy for
// doing ad-hoc searches inside a transaction.
// Returns ErrNotFound if the index is not found or there is no rect
// function bound to the index
func (tx *Tx) GetRect(index string) (func(s string) (min, max []float64),
	error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	idx, ok := tx.db.idxs[index]
	if !ok || idx.rect == nil {
		return nil, ErrNotFound
	}
	return idx.rect, nil
}

// Set inserts or replaces an item in the database based on the key.
// The opt params may be used for additional functionality such as forcing
// the item to be evicted at a specified time. When the return value
// for err is nil the operation succeeded. When the return value of
// replaced is true, then the operaton replaced an existing item whose
// value will be returned through the previousValue variable.
// The results of this operation will not be available to other
// transactions until the current transaction has successfully committed.
//
// Only a writable transaction can be used with this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
func (tx *Tx) MSet(key, value string, opts *SetOptions) (previousValue string,
	replaced bool, err error) {
	if tx.db == nil {
		return "", false, ErrTxClosed
	} else if !tx.writable {
		return "", false, ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return "", false, ErrTxIterating
	}
	item := &DbItem{key: key, val: value}
	if opts != nil {
		if opts.Expires {
			// The caller is requesting that this item expires. Convert the
			// TTL to an absolute time and bind it to the item.
			item.opts = &DbItemOpts{ex: true, exat: time.Now().Add(opts.TTL)}
		}
	}
	// Insert the item into the keys tree.
	prev := tx.db.insertIntoDatabase(item)

	// insert into the rollback map if there has not been a deleteAll.
	if tx.wc.rbkeys == nil {
		if prev == nil {
			// An item with the same key did not previously exist. Let's
			// create a rollback entry with a nil value. A nil value indicates
			// that the entry should be deleted on rollback. When the value is
			// *not* nil, that means the entry should be reverted.
			tx.wc.rollbackItems[key] = nil
		} else {
			// A previous item already exists in the database. Let's create a
			// rollback entry with the item as the value. We need to check the
			// map to see if there isn't already an item that matches the
			// same key.
			if _, ok := tx.wc.rollbackItems[key]; !ok {
				tx.wc.rollbackItems[key] = prev
			}
			if !prev.expired() {
				previousValue, replaced = prev.val, true
			}
		}
	}
	// For commits we simply assign the item to the map. We use this map to
	// write the entry to disk.
	if tx.db.persist {
		tx.wc.commitItems[key] = item
	}
	return previousValue, replaced, nil
}

// Get returns a value for a key. If the item does not exist or if the item
// has expired then ErrNotFound is returned. If ignoreExpired is true, then
// the found value will be returned even if it is expired.
func (tx *Tx) MGet(key string, ignoreExpired ...bool) (val string, err error) {
	if tx.db == nil {
		return "", ErrTxClosed
	}
	//TODO 	var ignore bool
	//	if len(ignoreExpired) != 0 {
	//		ignore = ignoreExpired[0]
	//	}
	value, err := tx.db.Get([]byte(key), nil)
	//TODO if item == nil || (item.expired() && !ignore) {
	if err != nil {
		// The item does not exists or has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return "", ErrNotFound
	}
	return string(value), nil
}

// Delete removes an item from the database based on the item's key. If the item
// does not exist or if the item has expired then ErrNotFound is returned.
//
// Only a writable transaction can be used for this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
func (tx *Tx) Delete(key string) (val string, err error) {
	if tx.db == nil {
		return "", ErrTxClosed
	} else if !tx.writable {
		return "", ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return "", ErrTxIterating
	}
	item := tx.db.deleteFromDatabase(&DbItem{key: key})
	if item == nil {
		return "", ErrNotFound
	}
	// create a rollback entry if there has not been a deleteAll call.
	if tx.wc.rbkeys == nil {
		if _, ok := tx.wc.rollbackItems[key]; !ok {
			tx.wc.rollbackItems[key] = item
		}
	}
	if tx.db.persist {
		tx.wc.commitItems[key] = nil
	}
	// Even though the item has been deleted, we still want to check
	// if it has expired. An expired item should not be returned.
	if item.expired() {
		// The item exists in the tree, but has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return "", ErrNotFound
	}
	return item.val, nil
}

//TODO
// TTL returns the remaining time-to-live for an item.
//// A negative duration will be returned for items that do not have an
//// expiration.
//func (tx *Tx) TTL(key string) (time.Duration, error) {
//	//TODO	if tx.db == nil {
//	//		return 0, ErrTxClosed
//	//	}
//	//	item := tx.db.Get([]byte(key), nil)
//	//	if item == nil {
//	//		return 0, ErrNotFound
//	//	} else if item.opts == nil || !item.opts.ex {
//	//		return -1, nil
//	//	}
//	//	dur := item.opts.exat.Sub(time.Now())
//	//	if dur < 0 {
//	//		return 0, ErrNotFound
//	//	}
//	dur := time.Duration{}
//	return dur, nil
//}

// scan iterates through a specified index and calls user-defined iterator
// function for each item encountered.
// The desc param indicates that the iterator should descend.
// The gt param indicates that there is a greaterThan limit.
// The lt param indicates that there is a lessThan limit.
// The index param tells the scanner to use the specified index tree. An
// empty string for the index means to scan the keys, not the values.
// The start and stop params are the greaterThan, lessThan limits. For
// descending order, these will be lessThan, greaterThan.
// An error will be returned if the tx is closed or the index is not found.
func (tx *Tx) scan(desc, gt, lt bool, index, start, stop string,
	iterator func(key, value string) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(item btree.Item) bool {
		dbi := item.(*DbItem)
		return iterator(dbi.key, dbi.val)
	}
	var tr *btree.BTree
	if index == "" {
		// empty index means we will use the keys tree.
		tr = tx.db.keys
	} else {
		idx := tx.db.idxs[index]
		if idx == nil {
			// index was not found. return error
			return ErrNotFound
		}
		tr = idx.btr
		if tr == nil {
			return nil
		}
	}
	// create some limit items
	var itemA, itemB *DbItem
	if gt || lt {
		if index == "" {
			itemA = &DbItem{key: start}
			itemB = &DbItem{key: stop}
		} else {
			itemA = &DbItem{val: start}
			itemB = &DbItem{val: stop}
			if desc {
				itemA.keyless = true
				itemB.keyless = true
			}
		}
	}
	// execute the scan on the underlying tree.
	if tx.wc != nil {
		tx.wc.itercount++
		defer func() {
			tx.wc.itercount--
		}()
	}
	if desc {
		if gt {
			if lt {
				tr.DescendRange(itemA, itemB, iter)
			} else {
				tr.DescendGreaterThan(itemA, iter)
			}
		} else if lt {
			tr.DescendLessOrEqual(itemA, iter)
		} else {
			tr.Descend(iter)
		}
	} else {
		if gt {
			if lt {
				tr.AscendRange(itemA, itemB, iter)
			} else {
				tr.AscendGreaterOrEqual(itemA, iter)
			}
		} else if lt {
			tr.AscendLessThan(itemA, iter)
		} else {
			tr.Ascend(iter)
		}
	}
	return nil
}

// Match returns true if the specified key matches the pattern. This is a very
// simple pattern matcher where '*' matches on any number characters and '?'
// matches on any one character.
func Match(key, pattern string) bool {
	return match.Match(key, pattern)
}

// AscendKeys allows for iterating through keys based on the specified pattern.
func (tx *Tx) AscendKeys(pattern string,
	iterator func(key, value string) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if pattern == "*" {
			return tx.Ascend("", iterator)
		}
		return tx.Ascend("", func(key, value string) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return tx.AscendGreaterOrEqual("", min, func(key, value string) bool {
		if key > max {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// DescendKeys allows for iterating through keys based on the specified pattern.
func (tx *Tx) DescendKeys(pattern string,
	iterator func(key, value string) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if pattern == "*" {
			return tx.Descend("", iterator)
		}
		return tx.Descend("", func(key, value string) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return tx.DescendLessOrEqual("", max, func(key, value string) bool {
		if key < min {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// Ascend calls the iterator for every item in the database within the range
// [first, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Ascend(index string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, false, false, index, "", "", iterator)
}

// AscendGreaterOrEqual calls the iterator for every item in the database within
// the range [pivot, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendGreaterOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, true, false, index, pivot, "", iterator)
}

// AscendLessThan calls the iterator for every item in the database within the
// range [first, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendLessThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, false, true, index, pivot, "", iterator)
}

// AscendRange calls the iterator for every item in the database within
// the range [greaterOrEqual, lessThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendRange(index, greaterOrEqual, lessThan string,
	iterator func(key, value string) bool) error {
	return tx.scan(
		false, true, true, index, greaterOrEqual, lessThan, iterator,
	)
}

// Descend calls the iterator for every item in the database within the range
// [last, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Descend(index string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, false, false, index, "", "", iterator)
}

// DescendGreaterThan calls the iterator for every item in the database within
// the range [last, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendGreaterThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, true, false, index, pivot, "", iterator)
}

// DescendLessOrEqual calls the iterator for every item in the database within
// the range [pivot, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendLessOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, false, true, index, pivot, "", iterator)
}

// DescendRange calls the iterator for every item in the database within
// the range [lessOrEqual, greaterThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendRange(index, lessOrEqual, greaterThan string,
	iterator func(key, value string) bool) error {
	return tx.scan(
		true, true, true, index, lessOrEqual, greaterThan, iterator,
	)
}

// AscendEqual calls the iterator for every item in the database that equals
// pivot, until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	var err error
	var less func(a, b string) bool
	if index != "" {
		less, err = tx.GetLess(index)
		if err != nil {
			return err
		}
	}
	return tx.AscendGreaterOrEqual(index, pivot, func(key, value string) bool {
		if less == nil {
			if key != pivot {
				return false
			}
		} else if less(pivot, value) {
			return false
		}
		return iterator(key, value)
	})
}

// DescendEqual calls the iterator for every item in the database that equals
// pivot, until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	var err error
	var less func(a, b string) bool
	if index != "" {
		less, err = tx.GetLess(index)
		if err != nil {
			return err
		}
	}
	return tx.DescendLessOrEqual(index, pivot, func(key, value string) bool {
		if less == nil {
			if key != pivot {
				return false
			}
		} else if less(value, pivot) {
			return false
		}
		return iterator(key, value)
	})
}

// rect is used by Intersects and Nearby
type rect struct {
	min, max []float64
}

func (r *rect) Rect(ctx interface{}) (min, max []float64) {
	return r.min, r.max
}

// Nearby searches for rectangle items that are nearby a target rect.
// All items belonging to the specified index will be returned in order of
// nearest to farthest.
// The specified index must have been created by AddIndex() and the target
// is represented by the rect string. This string will be processed by the
// same bounds function that was passed to the CreateSpatialIndex() function.
// An invalid index will return an error.
func (tx *Tx) Nearby(index, bounds string,
	iterator func(key, value string, dist float64) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	if index == "" {
		// cannot search on keys tree. just return nil.
		return nil
	}
	// // wrap a rtree specific iterator around the user-defined iterator.
	iter := func(item rtree.Item, dist float64) bool {
		dbi := item.(*DbItem)
		return iterator(dbi.key, dbi.val, dist)
	}
	idx := tx.db.idxs[index]
	if idx == nil {
		// index was not found. return error
		return ErrNotFound
	}
	if idx.rtr == nil {
		// not an r-tree index. just return nil
		return nil
	}
	// execute the nearby search
	var min, max []float64
	if idx.rect != nil {
		min, max = idx.rect(bounds)
	}
	// set the center param to false, which uses the box dist calc.
	idx.rtr.KNN(&rect{min, max}, false, iter)
	return nil
}

// Intersects searches for rectangle items that intersect a target rect.
// The specified index must have been created by AddIndex() and the target
// is represented by the rect string. This string will be processed by the
// same bounds function that was passed to the CreateSpatialIndex() function.
// An invalid index will return an error.
func (tx *Tx) Intersects(index, bounds string,
	iterator func(key, value string) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	if index == "" {
		// cannot search on keys tree. just return nil.
		return nil
	}
	// wrap a rtree specific iterator around the user-defined iterator.
	iter := func(item rtree.Item) bool {
		dbi := item.(*DbItem)
		return iterator(dbi.key, dbi.val)
	}
	idx := tx.db.idxs[index]
	if idx == nil {
		// index was not found. return error
		return ErrNotFound
	}
	if idx.rtr == nil {
		// not an r-tree index. just return nil
		return nil
	}
	// execute the search
	var min, max []float64
	if idx.rect != nil {
		min, max = idx.rect(bounds)
	}
	idx.rtr.Search(&rect{min, max}, iter)
	return nil
}

// Len returns the number of items in the database
func (tx *Tx) Len() (int, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	return tx.db.keys.Len(), nil
}

// CreateIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// An error will occur if an index with the same name already exists.
//
// When a pattern is provided, the index will be populated with
// keys that match the specified pattern. This is a very simple pattern
// match where '*' matches on any number characters and '?' matches on
// any one character.
// The less function compares if string 'a' is less than string 'b'.
// It allows for indexes to create custom ordering. It's possible
// that the strings may be textual or binary. It's up to the provided
// less function to handle the content format and comparison.
// There are some default less function that can be used such as
// IndexString, IndexBinary, etc.
func (tx *Tx) CreateIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return tx.createIndex(name, pattern, less, nil, nil)
}

// CreateIndexOptions is the same as CreateIndex except that it allows
// for additional options.
func (tx *Tx) CreateIndexOptions(name, pattern string,
	opts *IndexOptions,
	less ...func(a, b string) bool) error {
	return tx.createIndex(name, pattern, less, nil, opts)
}

// CreateSpatialIndex builds a new index and populates it with items.
// The items are organized in an r-tree and can be retrieved using the
// Intersects method.
// An error will occur if an index with the same name already exists.
//
// The rect function converts a string to a rectangle. The rectangle is
// represented by two arrays, min and max. Both arrays may have a length
// between 1 and 20, and both arrays must match in length. A length of 1 is a
// one dimensional rectangle, and a length of 4 is a four dimension rectangle.
// There is support for up to 20 dimensions.
// The values of min must be less than the values of max at the same dimension.
// Thus min[0] must be less-than-or-equal-to max[0].
// The IndexRect is a default function that can be used for the rect
// parameter.
func (tx *Tx) CreateSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return tx.createIndex(name, pattern, nil, rect, nil)
}

// CreateSpatialIndexOptions is the same as CreateSpatialIndex except that
// it allows for additional options.
func (tx *Tx) CreateSpatialIndexOptions(name, pattern string,
	opts *IndexOptions,
	rect func(item string) (min, max []float64)) error {
	return tx.createIndex(name, pattern, nil, rect, nil)
}

// createIndex is called by CreateIndex() and CreateSpatialIndex()
func (tx *Tx) createIndex(name string, pattern string,
	lessers []func(a, b string) bool,
	rect func(item string) (min, max []float64),
	opts *IndexOptions,
) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}
	if name == "" {
		// cannot create an index without a name.
		// an empty name index is designated for the main "keys" tree.
		return ErrIndexExists
	}
	// check if an index with that name already exists.
	if _, ok := tx.db.idxs[name]; ok {
		// index with name already exists. error.
		return ErrIndexExists
	}
	// genreate a less function
	var less func(a, b string) bool
	switch len(lessers) {
	default:
		// multiple less functions specified.
		// create a compound less function.
		less = func(a, b string) bool {
			for i := 0; i < len(lessers)-1; i++ {
				if lessers[i](a, b) {
					return true
				}
				if lessers[i](b, a) {
					return false
				}
			}
			return lessers[len(lessers)-1](a, b)
		}
	case 0:
		// no less function
	case 1:
		less = lessers[0]
	}
	var sopts IndexOptions
	if opts != nil {
		sopts = *opts
	}
	if sopts.CaseInsensitiveKeyMatching {
		pattern = strings.ToLower(pattern)
	}
	// intialize new index
	idx := &Index{
		name:    name,
		pattern: pattern,
		less:    less,
		rect:    rect,
		db:      tx.db,
		opts:    sopts,
	}
	idx.rebuild()
	// save the index
	tx.db.idxs[name] = idx
	if tx.wc.rbkeys == nil {
		// store the index in the rollback map.
		if _, ok := tx.wc.rollbackIndexes[name]; !ok {
			// we use nil to indicate that the index should be removed upon rollback.
			tx.wc.rollbackIndexes[name] = nil
		}
	}
	return nil
}

// DropIndex removes an index.
func (tx *Tx) DropIndex(name string) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}
	if name == "" {
		// cannot drop the default "keys" index
		return ErrInvalidOperation
	}
	idx, ok := tx.db.idxs[name]
	if !ok {
		return ErrNotFound
	}
	// delete from the map.
	// this is all that is needed to delete an index.
	delete(tx.db.idxs, name)
	if tx.wc.rbkeys == nil {
		// store the index in the rollback map.
		if _, ok := tx.wc.rollbackIndexes[name]; !ok {
			// we use a non-nil copy of the index without the data to indicate that the
			// index should be rebuilt upon rollback.
			tx.wc.rollbackIndexes[name] = idx.clearCopy()
		}
	}
	return nil
}

// Indexes returns a list of index names.
func (tx *Tx) Indexes() ([]string, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	names := make([]string, 0, len(tx.db.idxs))
	for name := range tx.db.idxs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}
