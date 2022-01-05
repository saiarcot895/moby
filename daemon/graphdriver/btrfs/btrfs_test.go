//go:build linux
// +build linux

package btrfs // import "github.com/docker/docker/daemon/graphdriver/btrfs"

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"syscall"
	"testing"

	"github.com/docker/docker/daemon/graphdriver/graphtest"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/reexec"
)

func init() {
	reexec.Init()
}

// This avoids creating a new driver for each test if all tests are run
// Make sure to put new tests between TestBtrfsSetup and TestBtrfsTeardown
func TestBtrfsSetup(t *testing.T) {
	graphtest.GetDriver(t, "btrfs")
}

func TestBtrfsCreateEmpty(t *testing.T) {
	graphtest.DriverTestCreateEmpty(t, "btrfs")
}

func TestBtrfsCreateBase(t *testing.T) {
	graphtest.DriverTestCreateBase(t, "btrfs")
}

func TestBtrfsCreateSnap(t *testing.T) {
	graphtest.DriverTestCreateSnap(t, "btrfs")
}

func TestBtrfsSubvolDelete(t *testing.T) {
	d := graphtest.GetDriver(t, "btrfs")
	if err := d.CreateReadWrite("test", "", nil); err != nil {
		t.Fatal(err)
	}
	defer graphtest.PutDriver(t)

	dir, err := d.Get("test", "")
	if err != nil {
		t.Fatal(err)
	}

	if err := subvolSnapshot("", dir, "subvoltest1"); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(path.Join(dir, "subvoltest1")); err != nil {
		t.Fatal(err)
	}

	idir := path.Join(dir, "intermediate")
	if err := os.Mkdir(idir, 0777); err != nil {
		t.Fatalf("Failed to create intermediate dir %s: %v", idir, err)
	}

	if err := subvolSnapshot("", idir, "subvoltest2"); err != nil {
		t.Fatal(err)
	}

	if err := d.Put("test"); err != nil {
		t.Fatal(err)
	}

	if err := d.Remove("test"); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("expected not exist error on nested subvol, got: %v", err)
	}
}

func TestBtrfsSubvolLongPath(t *testing.T) {
	d := graphtest.GetDriver(t, "btrfs")
	defer graphtest.PutDriver(t)

	wdir, _ := os.Getwd()

	if err := d.Create("rootsubvol", "", nil); err != nil {
		t.Fatalf("Failed to create rootsubvol: %v", err)
	}
	subvoldir, err := d.Get("rootsubvol", "")
	if err != nil {
		t.Fatal(err)
	}

	getMaxFilenameFormPattern := func(pattern string) string {
		name := strings.Repeat(pattern, (syscall.NAME_MAX / len(pattern)))
		return (name + pattern[:(syscall.NAME_MAX-len(name))])
	}

	os.Chdir(subvoldir)
	defer os.Chdir(wdir)

	for i, l := 1, len(subvoldir); l <= syscall.PathMax*2; i++ {
		dfile, err := os.OpenFile("dummyFile", os.O_RDONLY|os.O_CREATE, 0666)
		if err != nil {
			t.Fatalf("Failed to create file at %s: %v", subvoldir, err)
		}
		if err := dfile.Close(); err != nil {
			t.Fatal(err)
		}
		name := getMaxFilenameFormPattern(fmt.Sprintf("LongPathToFirstSubvol_LVL%d_", i))
		if err := os.Mkdir(name, 0777); err != nil {
			t.Fatalf("Failed to create dir %s/%s: %v", subvoldir, name, err)
		}
		if err := os.Chdir(name); err != nil {
			t.Fatal(err)
		}
		l += len(name)
		subvoldir = path.Join(subvoldir, name)
	}

	if err := subvolSnapshot("", subvoldir, "subvolLVL1"); err != nil {
		t.Fatal(err)
	}
	if err := subvolSnapshot("", subvoldir+"/..", "subvolLVL1_0"); err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir("subvolLVL1"); err != nil {
		t.Fatal(err)
	}
	subvoldir = path.Join(subvoldir, "subvolLVL1")

	i := 1
	for l := 0; l < syscall.PathMax; i++ {
		name := getMaxFilenameFormPattern(fmt.Sprintf("LongPathToNestedSub_1_LVL%d_", i))
		if err := os.Mkdir(name, 0777); err != nil {
			t.Fatalf("Failed to create dir %s/%s: %v", subvoldir, name, err)
		}
		if err := os.Chdir(name); err != nil {
			t.Fatal(err)
		}
		l += len(name)
		subvoldir = path.Join(subvoldir, name)
	}

	if err := subvolSnapshot("", subvoldir, "subvolLVL2_1"); err != nil {
		t.Fatal(err)
	}

	for ; i > 1; i-- {
		if err := os.Chdir(".."); err != nil {
			t.Fatal(err)
		}
		subvoldir = path.Dir(subvoldir)
	}

	for i, l := 1, 0; l < syscall.PathMax*2; i++ {
		name := getMaxFilenameFormPattern(fmt.Sprintf("LongPathToNestedSub_2_LVL%d_", i))
		if err := os.Mkdir(name, 0777); err != nil {
			t.Fatalf("Failed to create dir %s/%s: %v", subvoldir, name, err)
		}
		if err := os.Chdir(name); err != nil {
			t.Fatal(err)
		}
		l += len(name)
		subvoldir = path.Join(subvoldir, name)
	}

	if err := subvolSnapshot("", subvoldir, "subvolLVL2_3"); err != nil {
		t.Fatal(err)
	}
	if err := subvolSnapshot("", subvoldir, "subvolLVL2_2"); err != nil {
		t.Fatal(err)
	}

	if err := d.Put("rootsubvol"); err != nil {
		t.Fatal(err)
	}

	if err := d.Remove("rootsubvol"); err != nil {
		t.Fatal(err)
	}
}

func TestBtrfsChanges(t *testing.T) {
	graphtest.DriverTestChanges(t, "btrfs")
}

func TestBtrfsDiffApply10Files(t *testing.T) {
	graphtest.DriverTestDiffApply(t, 10, "btrfs")
}

func TestBtrfsChangesWithSymlinksAndHardlinks(t *testing.T) {
	d := graphtest.GetDriver(t, "btrfs")
	defer graphtest.PutDriver(t)

	wdir, _ := os.Getwd()

	if err := d.Create("basesubvol", "", nil); err != nil {
		t.Fatalf("Failed to create basesubvol: %v", err)
	}
	rootFS, err := d.Get("basesubvol", "")
	if err != nil {
		t.Fatal(err)
	}

	os.Chdir(rootFS)
	defer os.Chdir(wdir)

	if err := os.Mkdir("regularFiles", 0777); err != nil {
		t.Fatalf("Failed to create dir %s/%s: %v", rootFS, "regularFiles", err)
	}

	os.Chdir("regularFiles")

	for i := 0; i < 5; i++ {
		if err := os.WriteFile(fmt.Sprintf("file-%d", i), []byte("test"), 0700); err != nil {
			t.Fatal(err)
		}
	}

	if err := d.Create("subvollayer1", "basesubvol", nil); err != nil {
		t.Fatalf("Failed to create subvollayer1: %v", err)
	}
	layer1FS, err := d.Get("subvollayer1", "")
	if err != nil {
		t.Fatal(err)
	}

	os.Chdir(layer1FS)
	defer os.Chdir(wdir)

	if err := os.RemoveAll("regularFiles"); err != nil {
		t.Fatalf("Failed to delete dir %s/%s: %v", layer1FS, "regularFiles", err)
	}

	if err := os.Mkdir("newFiles", 0777); err != nil {
		t.Fatalf("Failed to create dir %s/%s: %v", layer1FS, "newFiles", err)
	}
	if err := os.Mkdir("symlinks", 0777); err != nil {
		t.Fatalf("Failed to create dir %s/%s: %v", layer1FS, "symlinks", err)
	}
	if err := os.Mkdir("hardlinks", 0777); err != nil {
		t.Fatalf("Failed to create dir %s/%s: %v", layer1FS, "hardlinks", err)
	}

	for i := 0; i < 5; i++ {
		if err := os.WriteFile(fmt.Sprintf("newFiles/test-%d", i), []byte("test"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(fmt.Sprintf("newFiles/test-%d", i), fmt.Sprintf("symlinks/test-%d", i)); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(fmt.Sprintf("newFiles/test-%d", i), fmt.Sprintf("hardlinks/test-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	expectedChanges, err := archive.ChangesDirs(layer1FS, rootFS)
	if err != nil {
		t.Fatal(err)
	}

	changes, err := d.Changes("subvollayer1", "basesubvol")
	if err != nil {
		t.Fatal(err)
	}

	checkChanges(expectedChanges, changes, t)

	if err := d.Put("subvollayer1"); err != nil {
		t.Fatal(err)
	}
	if err := d.Put("basesubvol"); err != nil {
		t.Fatal(err)
	}

	if err := d.Remove("subvollayer1"); err != nil {
		t.Fatal(err)
	}
	if err := d.Remove("basesubvol"); err != nil {
		t.Fatal(err)
	}
}

func TestBtrfsTeardown(t *testing.T) {
	graphtest.PutDriver(t)
}

func max(x, y int) int {
	if x >= y {
		return x
	}
	return y
}

// for sort.Sort
type changesByPath []archive.Change

func (c changesByPath) Less(i, j int) bool { return c[i].Path < c[j].Path }
func (c changesByPath) Len() int           { return len(c) }
func (c changesByPath) Swap(i, j int)      { c[j], c[i] = c[i], c[j] }

func checkChanges(expectedChanges []archive.Change, changes []archive.Change, t *testing.T) {
	sort.Sort(changesByPath(expectedChanges))
	sort.Sort(changesByPath(changes))

	for i := 0; i < len(expectedChanges); i++ {
		fmt.Printf("Expected change %d: %s %s\n", i, expectedChanges[i].Kind.String(), expectedChanges[i].Path)
	}

	for i := 0; i < len(changes); i++ {
		fmt.Printf("Returned change %d: %s %s\n", i, changes[i].Kind.String(), changes[i].Path)
	}

	for i := 0; i < max(len(changes), len(expectedChanges)); i++ {
		if i >= len(expectedChanges) {
			t.Fatalf("unexpected change %s\n", changes[i].String())
		}
		if i >= len(changes) {
			t.Fatalf("no change for expected change %s\n", expectedChanges[i].String())
		}
		if changes[i].Path == expectedChanges[i].Path {
			if changes[i] != expectedChanges[i] {
				t.Fatalf("Wrong change for %s, expected %s, got %s\n", changes[i].Path, changes[i].String(), expectedChanges[i].String())
			}
		} else if changes[i].Path < expectedChanges[i].Path {
			t.Fatalf("unexpected change %s\n", changes[i].String())
		} else {
			t.Fatalf("no change for expected change %s != %s\n", expectedChanges[i].String(), changes[i].String())
		}
	}
}
