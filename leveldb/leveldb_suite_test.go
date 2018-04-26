package leveldb

import (
	"testing"

	"github.com/bottos-project/goleveldb/leveldb/testutil"
)

func TestLevelDB(t *testing.T) {
	testutil.RunSuite(t, "LevelDB Suite")
}
