/*The MIT License (MIT)

Copyright (c) 2016 Josh Baker

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
package leveldb

import (
	"strings"

	"github.com/tidwall/btree"
	"github.com/tidwall/match"
	"github.com/tidwall/rtree"
)

// index represents a b-tree or r-tree index and also acts as the
// b-tree/r-tree context for itself.
type Index struct {
	btr     *btree.BTree                           // contains the items
	rtr     *rtree.RTree                           // contains the items
	name    string                                 // name of the index
	pattern string                                 // a required key pattern
	less    func(a, b string) bool                 // less comparison function
	rect    func(item string) (min, max []float64) // rect from string function
	db      *DB                                    // the origin database
	opts    IndexOptions                           // index options
}
type IndexOptions struct {
	// CaseInsensitiveKeyMatching allow for case-insensitive
	// matching on keys when setting key/values.
	CaseInsensitiveKeyMatching bool
}

// match matches the pattern to the key
func (idx *Index) match(key string) bool {
	if idx.pattern == "*" {
		return true
	}
	if idx.opts.CaseInsensitiveKeyMatching {
		for i := 0; i < len(key); i++ {
			if key[i] >= 'A' && key[i] <= 'Z' {
				key = strings.ToLower(key)
				break
			}
		}
	}
	return match.Match(key, idx.pattern)
}

// clearCopy creates a copy of the index, but with an empty dataset.
func (idx *Index) clearCopy() *Index {
	// copy the index meta information
	nidx := &Index{
		name:    idx.name,
		pattern: idx.pattern,
		db:      idx.db,
		less:    idx.less,
		rect:    idx.rect,
		opts:    idx.opts,
	}
	// initialize with empty trees
	if nidx.less != nil {
		nidx.btr = btree.New(btreeDegrees, nidx)
	}
	if nidx.rect != nil {
		nidx.rtr = rtree.New(nidx)
	}
	return nidx
}

// rebuild rebuilds the index
func (idx *Index) rebuild() {
	// initialize trees
	if idx.less != nil {
		idx.btr = btree.New(btreeDegrees, idx)
	}
	if idx.rect != nil {
		idx.rtr = rtree.New(idx)
	}
	// iterate through all keys and fill the index
	idx.db.keys.Ascend(func(item btree.Item) bool {
		dbi := item.(*DbItem)
		if !idx.match(dbi.key) {
			// does not match the pattern, conintue
			return true
		}
		if idx.less != nil {
			idx.btr.ReplaceOrInsert(dbi)
		}
		if idx.rect != nil {
			idx.rtr.Insert(dbi)
		}
		return true
	})
}
