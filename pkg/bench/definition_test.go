package bench

import (
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/pingcap/errors"
)

func TestDefinition(t *testing.T) {
	var count int
	err := filepath.WalkDir("../../benchmarks", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		t.Log(path)
		def, err := ParseDefinition(path)
		if err != nil {
			return errors.Wrapf(err, "parsing definition %q", path)
		}
		t.Logf("%#v\n", def)
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Assert that we found at least one definition to guard against passing
	// even if we didn't look in the right place.
	if count == 0 {
		t.Fatal("no definitions found; is the relative path still correct?")
	}
}
