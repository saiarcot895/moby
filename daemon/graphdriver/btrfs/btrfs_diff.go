//go:build linux
// +build linux

package btrfs // import "github.com/docker/docker/daemon/graphdriver/btrfs"

/*
#include <btrfs/send.h>
*/
import "C"

import (
	"fmt"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/docker/docker/pkg/archive"
)

func readNextBytes(reader io.Reader, number int) ([]byte, error) {
	bytes := make([]byte, number)

	_, err := reader.Read(bytes)
	if err != nil {
		return nil, fmt.Errorf("Error in reading %d bytes", number)
	}

	return bytes, nil
}

type btrfsStreamHeader struct {
	Magic [13]byte
	Version uint32
}

type btrfsTlvHeader struct {
	TlvType uint16
	TlvLen uint16
}

type btrfsCmdHeader struct {
	Len uint32
	Cmd uint16
	Crc uint32
}

func tlvGetBytes(reader io.Reader, tlvType C.enum_btrfs_tlv_type) ([]byte, error) {
	var tlvHeader btrfsTlvHeader

	err := binary.Read(reader, binary.LittleEndian, &tlvHeader)
	if err != nil {
		return nil, fmt.Errorf("Reading TLV header failed")
	}

	if tlvHeader.TlvType != (uint16)(C.__u16(tlvType)) {
		return nil, fmt.Errorf("Unexpected TLV attribute")
	}

	data, err := readNextBytes(reader, (int)(tlvHeader.TlvLen))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func tlvGetString(reader io.Reader, tlvType C.enum_btrfs_tlv_type) (string, error) {
	data, err := tlvGetBytes(reader, tlvType)
	if err != nil {
		return "", err
	}
	return string(data[:]), nil
}

func tlvGetU64(reader io.Reader, tlvType C.enum_btrfs_tlv_type) (uint64, error) {
	data, err := tlvGetBytes(reader, tlvType)
	if err != nil {
		return 0, err
	}

	var value uint64
	buffer := bytes.NewBuffer(data)
	err = binary.Read(buffer, binary.LittleEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func processSendStream(reader io.Reader, changes *[]archive.Change) (bool, error) {
	var cmdHeader btrfsCmdHeader

	err := binary.Read(reader, binary.LittleEndian, &cmdHeader)
	if err != nil {
		return false, fmt.Errorf("Reading command header failed")
	}

	switch cmdHeader.Cmd {
	case C.BTRFS_SEND_C_MKFILE:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		_, err = tlvGetU64(reader, C.BTRFS_SEND_A_INO)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeAdd,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_MKDIR:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		_, err = tlvGetU64(reader, C.BTRFS_SEND_A_INO)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeAdd,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_MKNOD:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		_, err = tlvGetU64(reader, C.BTRFS_SEND_A_INO)
		if err != nil {
			return false, err
		}
		_, err = tlvGetU64(reader, C.BTRFS_SEND_A_MODE)
		if err != nil {
			return false, err
		}
		_, err = tlvGetU64(reader, C.BTRFS_SEND_A_RDEV)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeAdd,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_MKFIFO:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		_, err = tlvGetU64(reader, C.BTRFS_SEND_A_INO)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeAdd,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_MKSOCK:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		_, err = tlvGetU64(reader, C.BTRFS_SEND_A_INO)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeAdd,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_SYMLINK:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		_, err = tlvGetU64(reader, C.BTRFS_SEND_A_INO)
		if err != nil {
			return false, err
		}
		_, err = tlvGetString(reader, C.BTRFS_SEND_A_PATH_LINK)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeAdd,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_RENAME:
		pathFrom, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		pathTo, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH_TO)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + pathFrom,
			Kind: archive.ChangeDelete,
		}
		*changes = append(*changes, change)
		change = archive.Change{
			Path: "/" + pathTo,
			Kind: archive.ChangeAdd,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_LINK:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		_, err = tlvGetString(reader, C.BTRFS_SEND_A_PATH_LINK)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeAdd,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_UNLINK:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeDelete,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_RMDIR:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeDelete,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_SET_XATTR:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		_, err = tlvGetString(reader, C.BTRFS_SEND_A_XATTR_NAME)
		if err != nil {
			return false, err
		}
		_, err = tlvGetBytes(reader, C.BTRFS_SEND_A_XATTR_DATA)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeModify,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_REMOVE_XATTR:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		_, err = tlvGetString(reader, C.BTRFS_SEND_A_XATTR_NAME)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeModify,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_TRUNCATE:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		_, err = tlvGetU64(reader, C.BTRFS_SEND_A_SIZE)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeAdd,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_CHMOD:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		_, err = tlvGetU64(reader, C.BTRFS_SEND_A_MODE)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeModify,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_CHOWN:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		_, err = tlvGetU64(reader, C.BTRFS_SEND_A_UID)
		if err != nil {
			return false, err
		}
		_, err = tlvGetU64(reader, C.BTRFS_SEND_A_GID)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeModify,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_UTIMES:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		_, err = tlvGetBytes(reader, C.BTRFS_SEND_A_ATIME)
		if err != nil {
			return false, err
		}
		_, err = tlvGetBytes(reader, C.BTRFS_SEND_A_MTIME)
		if err != nil {
			return false, err
		}
		_, err = tlvGetBytes(reader, C.BTRFS_SEND_A_CTIME)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeModify,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_UPDATE_EXTENT:
		path, err := tlvGetString(reader, C.BTRFS_SEND_A_PATH)
		if err != nil {
			return false, err
		}
		_, err = tlvGetU64(reader, C.BTRFS_SEND_A_FILE_OFFSET)
		if err != nil {
			return false, err
		}
		_, err = tlvGetU64(reader, C.BTRFS_SEND_A_SIZE)
		if err != nil {
			return false, err
		}
		change := archive.Change{
			Path: "/" + path,
			Kind: archive.ChangeModify,
		}
		*changes = append(*changes, change)
		return true, nil
	case C.BTRFS_SEND_C_END:
		return false, nil
	default:
		_, err := readNextBytes(reader, (int)(cmdHeader.Len))
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return true, nil
}

func readSendStream(reader io.Reader) ([]archive.Change, error) {
	var streamHeader btrfsStreamHeader

	err := binary.Read(reader, binary.LittleEndian, &streamHeader)
	if err != nil {
		return nil, fmt.Errorf("Reading stream header failed")
	}

	streamHeaderMagic := string(streamHeader.Magic[:12])
	if streamHeaderMagic != C.BTRFS_SEND_STREAM_MAGIC {
		return nil, fmt.Errorf("This is not a btrfs send stream, got %s in the magic header instead of %s",
			streamHeaderMagic, C.BTRFS_SEND_STREAM_MAGIC)
	}

	if streamHeader.Version != 1 {
		return nil, fmt.Errorf("Unknown send stream version %d", streamHeader.Version)
	}

	var changes []archive.Change
	for {
		continueReading, err := processSendStream(reader, &changes)
		if err != nil {
			return nil, err
		}
		if !continueReading {
			return changes, nil
		}
	}
}

func remove(slice []archive.Change, s int) []archive.Change {
	return append(slice[:s], slice[s+1:]...)
}

func cleanChanges(changes []archive.Change) ([]archive.Change, error) {
	// Look for add then delete of the same file; this is effectively a no-op
	for i := 0; i < len(changes); i++ {
		if changes[i].Kind != archive.ChangeAdd {
			continue
		}
		pathToCheck := changes[i].Path
		var indicesToBeDeleted []int
		indicesToBeDeleted = append(indicesToBeDeleted, i)
		deleteChanges := false
		for j := i + 1; j < len(changes); j++ {
			if changes[j].Path != pathToCheck {
				continue
			}
			if changes[j].Kind == archive.ChangeModify {
				indicesToBeDeleted = append(indicesToBeDeleted, j)
			}
			if changes[j].Kind == archive.ChangeAdd {
				return nil, fmt.Errorf("Unexpected add encountered here")
			}
			if changes[j].Kind == archive.ChangeDelete {
				indicesToBeDeleted = append(indicesToBeDeleted, j)
				deleteChanges = true
				break
			}
		}
		if deleteChanges {
			for j := len(indicesToBeDeleted) - 1; j >= 0; j-- {
				changes = remove(changes, indicesToBeDeleted[j])
			}
		}
	}

	// Look for delete then add of the same file; this is effectively a modify
	for i := 0; i < len(changes); i++ {
		if changes[i].Kind != archive.ChangeDelete {
			continue
		}
		pathToCheck := changes[i].Path
		var indicesToBeDeleted []int
		indicesToBeDeleted = append(indicesToBeDeleted, i)
		deleteChanges := false
		for j := i + 1; j < len(changes); j++ {
			if changes[j].Path != pathToCheck {
				continue
			}
			if changes[j].Kind == archive.ChangeModify {
				indicesToBeDeleted = append(indicesToBeDeleted, j)
			}
			if changes[j].Kind == archive.ChangeDelete {
				return nil, fmt.Errorf("Unexpected delete encountered here")
			}
			if changes[j].Kind == archive.ChangeAdd {
				indicesToBeDeleted = append(indicesToBeDeleted, j)
				deleteChanges = true
				break
			}
		}
		if deleteChanges {
			change := archive.Change{
				Path: pathToCheck,
				Kind: archive.ChangeModify,
			}
			changes[indicesToBeDeleted[0]] = change
			for j := len(indicesToBeDeleted) - 1; j >= 1; j-- {
				changes = remove(changes, indicesToBeDeleted[j])
			}
		}
	}

	// Look for multiple modify entries, combine them into one
	for i := 0; i < len(changes); i++ {
		if changes[i].Kind != archive.ChangeModify {
			continue
		}
		pathToCheck := changes[i].Path
		var indicesToBeDeleted []int
		indicesToBeDeleted = append(indicesToBeDeleted, i)
		deleteChanges := false
		for j := i + 1; j < len(changes); j++ {
			if changes[j].Path != pathToCheck {
				continue
			}
			if changes[j].Kind != archive.ChangeModify {
				return nil, fmt.Errorf("Unexpected add/delete encountered here")
			}
			indicesToBeDeleted = append(indicesToBeDeleted, j)
			deleteChanges = true
		}
		if deleteChanges {
			// Keep the first modify here
			for j := len(indicesToBeDeleted) - 1; j >= 1; j-- {
				changes = remove(changes, indicesToBeDeleted[j])
			}
		}
	}

	// Look for add then modify, this is just an add
	for i := 0; i < len(changes); i++ {
		if changes[i].Kind != archive.ChangeAdd {
			continue
		}
		pathToCheck := changes[i].Path
		var indicesToBeDeleted []int
		indicesToBeDeleted = append(indicesToBeDeleted, i)
		deleteChanges := false
		for j := i + 1; j < len(changes); j++ {
			if changes[j].Path != pathToCheck {
				continue
			}
			if changes[j].Kind != archive.ChangeModify {
				return nil, fmt.Errorf("Unexpected add/delete encountered here")
			}
			indicesToBeDeleted = append(indicesToBeDeleted, j)
			deleteChanges = true
		}
		if deleteChanges {
			// Keep the first add here
			for j := len(indicesToBeDeleted) - 1; j >= 1; j-- {
				changes = remove(changes, indicesToBeDeleted[j])
			}
		}
	}

	return changes, nil
}
