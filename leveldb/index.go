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
	"strconv"
	"strings"

	"github.com/tidwall/btree"
	"github.com/tidwall/gjson"
	"github.com/tidwall/grect"
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

// Rect is helper function that returns a string representation
// of a rect. IndexRect() is the reverse function and can be used
// to generate a rect from a string.
func Rect(min, max []float64) string {
	r := grect.Rect{Min: min, Max: max}
	return r.String()
}

// Point is a helper function that converts a series of float64s
// to a rectangle for a spatial index.
func Point(coords ...float64) string {
	return Rect(coords, coords)
}

// IndexRect is a helper function that converts string to a rect.
// Rect() is the reverse function and can be used to generate a string
// from a rect.
func IndexRect(a string) (min, max []float64) {
	r := grect.Get(a)
	return r.Min, r.Max
}

// IndexString is a helper function that return true if 'a' is less than 'b'.
// This is a case-insensitive comparison. Use the IndexBinary() for comparing
// case-sensitive strings.
func IndexString(a, b string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] >= 'A' && a[i] <= 'Z' {
			if b[i] >= 'A' && b[i] <= 'Z' {
				// both are uppercase, do nothing
				if a[i] < b[i] {
					return true
				} else if a[i] > b[i] {
					return false
				}
			} else {
				// a is uppercase, convert a to lowercase
				if a[i]+32 < b[i] {
					return true
				} else if a[i]+32 > b[i] {
					return false
				}
			}
		} else if b[i] >= 'A' && b[i] <= 'Z' {
			// b is uppercase, convert b to lowercase
			if a[i] < b[i]+32 {
				return true
			} else if a[i] > b[i]+32 {
				return false
			}
		} else {
			// neither are uppercase
			if a[i] < b[i] {
				return true
			} else if a[i] > b[i] {
				return false
			}
		}
	}
	return len(a) < len(b)
}

// IndexBinary is a helper function that returns true if 'a' is less than 'b'.
// This compares the raw binary of the string.
func IndexBinary(a, b string) bool {
	return a < b
}

// IndexInt is a helper function that returns true if 'a' is less than 'b'.
func IndexInt(a, b string) bool {
	ia, _ := strconv.ParseInt(a, 10, 64)
	ib, _ := strconv.ParseInt(b, 10, 64)
	return ia < ib
}

// IndexUint is a helper function that returns true if 'a' is less than 'b'.
// This compares uint64s that are added to the database using the
// Uint() conversion function.
func IndexUint(a, b string) bool {
	ia, _ := strconv.ParseUint(a, 10, 64)
	ib, _ := strconv.ParseUint(b, 10, 64)
	return ia < ib
}

// IndexFloat is a helper function that returns true if 'a' is less than 'b'.
// This compares float64s that are added to the database using the
// Float() conversion function.
func IndexFloat(a, b string) bool {
	ia, _ := strconv.ParseFloat(a, 64)
	ib, _ := strconv.ParseFloat(b, 64)
	return ia < ib
}

// IndexJSON provides for the ability to create an index on any JSON field.
// When the field is a string, the comparison will be case-insensitive.
// It returns a helper function used by CreateIndex.
func IndexJSON(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), false)
	}
}

// IndexJSONCaseSensitive provides for the ability to create an index on
// any JSON field.
// When the field is a string, the comparison will be case-sensitive.
// It returns a helper function used by CreateIndex.
func IndexJSONCaseSensitive(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), true)
	}
}

// Desc is a helper function that changes the order of an index.
func Desc(less func(a, b string) bool) func(a, b string) bool {
	return func(a, b string) bool { return less(b, a) }
}
