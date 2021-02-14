package writeaheadlog

import (
	"bytes"
	"encoding/hex"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/uplo-tech/fastrand"
)

// TestCommon tests the methods of common.go.
func TestCommon(t *testing.T) {
	// Create testing environment.
	folders := []string{t.Name()}
	for i := 0; i < fastrand.Intn(10); i++ {
		folders = append(folders, hex.EncodeToString(fastrand.Bytes(16)))
	}
	testDir := tempDir(folders...)
	if err := os.MkdirAll(testDir, 0777); err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll(testDir)
		if err != nil {
			t.Fatal(err)
		}
	}()
	testpath := filepath.Join(testDir, "test.file")
	txns, wal, err := New(filepath.Join(testDir, "test.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if len(txns) != 0 {
		t.Fatal("wal wasn't empty")
	}
	// Create a file using an update.
	data := fastrand.Bytes(fastrand.Intn(100) + 100)
	offset := int64(fastrand.Intn(10))
	update := WriteAtUpdate(testpath, offset, data)
	err = wal.CreateAndApplyTransaction(ApplyUpdates, update)
	if err != nil {
		t.Fatal(err)
	}
	// If the offset wasn't 0 we need to prepend 'offset' bytes to the expected data.
	data = append(make([]byte, offset), data...)
	// Make sure the file was created.
	readData, err := ioutil.ReadFile(path.Clean(testpath))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, readData) {
		t.Fatal("Data on disk doesn't match written data")
	}
	// Truncate the file.
	newSize := fastrand.Intn(len(data)) + 1
	update = TruncateUpdate(testpath, int64(newSize))
	err = wal.CreateAndApplyTransaction(ApplyUpdates, update)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure the file has the right contents.
	readData, err = ioutil.ReadFile(path.Clean(testpath))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data[:newSize], readData) {
		t.Fatal("Data on disk doesn't match written data")
	}
	// Delete the file.
	update = DeleteUpdate(testpath)
	err = wal.CreateAndApplyTransaction(ApplyUpdates, update)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure the file is gone.
	if _, err := os.Stat(testpath); !os.IsNotExist(err) {
		t.Fatal("file should've been deleted")
	}
	// Create a folder to remove.
	dir := filepath.Join(testDir, "folder")
	if err := os.Mkdir(dir, 0777); err != nil {
		t.Fatal(err)
	}
	update = DeleteUpdate(dir)
	err = wal.CreateAndApplyTransaction(ApplyUpdates, update)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal(err)
	}
}
