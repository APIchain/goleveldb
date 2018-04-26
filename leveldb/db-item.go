// Copyright 2017~2022 The Bottos Authors
// This file is part of the Bottos Chain library.
// Created by Rocket Core Team of Bottos.

//This program is free software: you can distribute it and/or modify
//it under the terms of the GNU General Public License as published by
//the Free Software Foundation, either version 3 of the License, or
//(at your option) any later version.

//This program is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//GNU General Public License for more details.

//You should have received a copy of the GNU General Public License
// along with bottos.  If not, see <http://www.gnu.org/licenses/>.

/*
 * file description: database for key-value
 * @Author: May Luo
 * @Date:   2017-12-10
 * @Last Modified by:
 * @Last Modified time:
 */

package leveldb

import (
	"strconv"
	"time"

	"github.com/tidwall/btree"
)

type dbItemOpts struct {
	ex   bool      // does this item expire?
	exat time.Time // when does this item expire?
}
type DbItem struct {
	key, val string      // the binary key and value
	opts     *dbItemOpts // optional meta information
	keyless  bool        // keyless item for scanning
}

func appendArray(buf []byte, count int) []byte {
	buf = append(buf, '*')
	buf = append(buf, strconv.FormatInt(int64(count), 10)...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendBulkString(buf []byte, s string) []byte {
	buf = append(buf, '$')
	buf = append(buf, strconv.FormatInt(int64(len(s)), 10)...)
	buf = append(buf, '\r', '\n')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

// writeSetTo writes an item as a single SET record to the a bufio Writer.
func (dbi *DbItem) writeSetTo(buf []byte) []byte {
	if dbi.opts != nil && dbi.opts.ex {
		ex := dbi.opts.exat.Sub(time.Now()) / time.Second
		buf = appendArray(buf, 5)
		buf = appendBulkString(buf, "set")
		buf = appendBulkString(buf, dbi.key)
		buf = appendBulkString(buf, dbi.val)
		buf = appendBulkString(buf, "ex")
		buf = appendBulkString(buf, strconv.FormatUint(uint64(ex), 10))
	} else {
		buf = appendArray(buf, 3)
		buf = appendBulkString(buf, "set")
		buf = appendBulkString(buf, dbi.key)
		buf = appendBulkString(buf, dbi.val)
	}
	return buf
}

// writeSetTo writes an item as a single DEL record to the a bufio Writer.
func (dbi *DbItem) writeDeleteTo(buf []byte) []byte {
	buf = appendArray(buf, 2)
	buf = appendBulkString(buf, "del")
	buf = appendBulkString(buf, dbi.key)
	return buf
}

// expired evaluates id the item has expired. This will always return false when
// the item does not have `opts.ex` set to true.
func (dbi *DbItem) expired() bool {
	return dbi.opts != nil && dbi.opts.ex && time.Now().After(dbi.opts.exat)
}

// MaxTime from http://stackoverflow.com/questions/25065055#32620397
// This is a long time in the future. It's an imaginary number that is
// used for b-tree ordering.
var maxTime = time.Unix(1<<63-62135596801, 999999999)

// expiresAt will return the time when the item will expire. When an item does
// not expire `maxTime` is used.
func (dbi *DbItem) expiresAt() time.Time {
	if dbi.opts == nil || !dbi.opts.ex {
		return maxTime
	}
	return dbi.opts.exat
}

// Less determines if a b-tree item is less than another. This is required
// for ordering, inserting, and deleting items from a b-tree. It's important
// to note that the ctx parameter is used to help with determine which
// formula to use on an item. Each b-tree should use a different ctx when
// sharing the same item.
func (dbi *DbItem) Less(item btree.Item, ctx interface{}) bool {
	dbi2 := item.(*DbItem)
	switch ctx := ctx.(type) {
	case *exctx:
		// The expires b-tree formula
		if dbi2.expiresAt().After(dbi.expiresAt()) {
			return true
		}
		if dbi.expiresAt().After(dbi2.expiresAt()) {
			return false
		}
	case *Index:
		if ctx.less != nil {
			// Using an index
			if ctx.less(dbi.val, dbi2.val) {
				return true
			}
			if ctx.less(dbi2.val, dbi.val) {
				return false
			}
		}
	}
	// Always fall back to the key comparison. This creates absolute uniqueness.
	if dbi.keyless {
		return false
	} else if dbi2.keyless {
		return true
	}
	return dbi.key < dbi2.key
}

// Rect converts a string to a rectangle.
// An invalid rectangle will cause a panic.
func (dbi *DbItem) Rect(ctx interface{}) (min, max []float64) {
	switch ctx := ctx.(type) {
	case *Index:
		return ctx.rect(dbi.val)
	}
	return nil, nil
}

// SetOptions represents options that may be included with the Set() command.
type SetOptions struct {
	// Expires indicates that the Set() key-value will expire
	Expires bool
	// TTL is how much time the key-value will exist in the database
	// before being evicted. The Expires field must also be set to true.
	// TTL stands for Time-To-Live.
	TTL time.Duration
}
