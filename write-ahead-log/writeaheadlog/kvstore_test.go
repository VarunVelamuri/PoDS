package writeaheadlog

import "testing"

func TestKVStoreBasic(t *testing.T) {
	kvstore, err := Init("./test_0.data")

}
