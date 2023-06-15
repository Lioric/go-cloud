// Copyright 2018 The Go Cloud Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package fileblob provides a bucket implementation that operates on the local
// filesystem. This should not be used for production: it is intended for local
// development.
//
// Blob names must only contain alphanumeric characters, slashes, periods,
// spaces, underscores, and dashes. Repeated slashes, a leading "./" or "../",
// or the sequence "/./" is not permitted. This is to ensure that blob names map
// cleanly onto files underneath a directory.
package fileblob

import (
	"context"
	"fmt"
	"io"
	"os"
	slashpath "path"
	"path/filepath"
	"strings"
	"time"

	"github.com/Lioric/go-cloud/blob"
	"github.com/Lioric/go-cloud/blob/driver"
)

// Character to append to folder name to diferentiate from filenames
// sample:
//  object unit title: someTitle
//  object unit title with folder structure: someTitle/subFile
//
//  folder structure:
//    \-- someTitle./ -> subfolder
//    \---- subFile -> file inside subfolder
//    \-- someTitle -> file
// const sFolderChar = "."

type bucket struct {
	dir string
}

var DataDir = ""
var MetaDir = ""
var RevisionDir = ""
var AttrsExt = ""

// var ObjectExt = ""

// NewBucket creates a new bucket that reads and writes to dir.
// dir must exist.
func NewBucket(dir string) (*blob.Bucket, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("open file bucket: %v", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("open file bucket: %s is not a directory", dir)
	}
	return blob.NewBucket(&bucket{dir}), nil
}

func resolveLocalFSPath(s string) string {
	pathSep := "/"
	newPathSep := pathSep
	// newPathSep := sFolderChar + "/"

	n := 0
	// Compute number of replacements.
	if m := strings.Count(s, pathSep); m < 3 {
		return s // avoid allocation
	} else {
		n = m - 2
	}

	index1 := strings.Index(s, pathSep) + len(pathSep)
	index2 := strings.Index(s[index1:], pathSep) + len(pathSep)
	// start += strings.Index(s[start:], pathSep)
	start := index1 + index2

	// fmt.Println(s[index1:])
	// fmt.Println(s[start:])

	// Apply replacements to buffer.
	t := make([]byte, len(s)+n*(len(newPathSep)-len(pathSep)))
	w := copy(t[0:], s[0:start])
	for i := 0; i < n; i++ {
		j := start
		j += strings.Index(s[start:], pathSep)
		w += copy(t[w:], s[start:j])
		w += copy(t[w:], newPathSep)
		start = j + len(pathSep)
	}
	w += copy(t[w:], s[start:])
	return string(t[0:w])
}

// resolvePath converts a key into a relative filesystem path. It guarantees
// that there will only be one valid key for a given path and that the resulting
// path will not reach outside the directory.
func resolvePath(rawKey string, exactKeyName bool) (string, error) {
	key := blob.GetBlobName(rawKey)

	key = resolveLocalFSPath(key)

	if !exactKeyName {
		key += ".mb"
	}

	// key = resolveLocalFSPath(key) + ".mb"

	// objName = strings.ReplaceAll(objName, "/", sFolderChar+"/")

	// for _, c := range key {
	// 	if c == '<' || c == '>' || c == ':' || c == '"' || c == '|' || c == '?' || c == '*' || c == '^' || c == '~' || c == '\'' {
	// 		// <|>|\:|\"|\||\?|\*|\^|~|\'
	// 		// if !('A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '/' || c == '.' || c == ' ' || c == '_' || c == '-' || c == '$' || c == '@') {
	// 		return "", fmt.Errorf("contains invalid character %q", c)
	// 	}
	// }
	if cleaned := slashpath.Clean(key); key != cleaned {
		return "", fmt.Errorf("not a clean slash-separated path")
	}
	if slashpath.IsAbs(key) {
		return "", fmt.Errorf("starts with a slash")
	}
	if key == "." {
		return "", fmt.Errorf("invalid path \".\"")
	}
	if strings.HasPrefix(key, "../") {
		return "", fmt.Errorf("starts with \"../\"")
	}

	return filepath.FromSlash(key), nil
	// return filepath.FromSlash(key) + ObjectExt, nil
}

func (b *bucket) Attributes(ctx context.Context, key string, isUID bool) (*driver.ObjectAttrs, error) {
	// File blob is used via sqlfileblob only, attributes are not stored in a separated file
	return nil, fmt.Errorf("File blob should be used via sqlfileblob")
}

func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, exactKeyName bool) (driver.Reader, error) {
	relpath, err := resolvePath(key, exactKeyName)
	if err != nil {
		return nil, fmt.Errorf("open file blob %s: %v", key, err)
	}
	path := filepath.Join(b.dir, relpath)
	// if strings.HasSuffix(path, AttrsExt) {
	// 	return nil, fmt.Errorf("open file blob %s: extension %q cannot be directly read", key, AttrsExt)
	// }
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fileError{relpath: relpath, msg: err.Error(), kind: driver.NotFound}
		}
		return nil, fmt.Errorf("open file blob %s: %v", key, err)
	}
	// TASK: should we move getAttr only to getMetadata code path?
	// xa, err := getAttrs(path)
	// if err != nil {
	// 	return nil, fmt.Errorf("open file attributes %s: %v", key, err)
	// }
	// if length == 0 {
	// 	return reader{
	// 		size:    info.Size(),
	// 		modTime: info.ModTime(),
	// 		xa:      xa,
	// 	}, nil
	// }
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file blob %s: %v", key, err)
	}
	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("open file blob %s: %v", key, err)
		}
	}
	r := io.Reader(f)
	if length > 0 {
		r = io.LimitReader(r, length)
	}
	return reader{
		r:       r,
		c:       f,
		size:    info.Size(),
		modTime: info.ModTime(),
		xa:      nil,
	}, nil
}

type reader struct {
	r       io.Reader
	c       io.Closer
	size    int64
	modTime time.Time
	xa      *xattrs
}

func (r reader) Read(p []byte) (int, error) {
	if r.r == nil {
		return 0, io.EOF
	}
	return r.r.Read(p)
}

func (r reader) Close() error {
	if r.c == nil {
		return nil
	}
	return r.c.Close()
}

func (r reader) Attrs() *driver.ObjectAttrs {
	return &driver.ObjectAttrs{
		Size:        r.size,
		ContentType: r.xa.ContentType,
		ModTime:     r.modTime,
		// Tiddler metadata
		Id:       r.xa.Id,
		Name:     r.xa.Name,
		Fields:   r.xa.Meta,
		Revision: r.xa.Revision,
		Extra:    r.xa.Extra["extraFields"],
	}
}

func (b *bucket) CreateArea(ctx context.Context, area string, groups []string) error {
	if area == "." {
		return fmt.Errorf("area invalid path \".\"")
	}
	if strings.Contains(area, "../") {
		return fmt.Errorf("area starts with \"../\"")
	}

	path := filepath.Join(b.dir, area)
	err := os.Mkdir(path, 0777)
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("create area %s: %v", area, err)
	}

	for _, group := range groups {
		meta := filepath.Join(path, group)
		err = os.Mkdir(meta, 0777)
		if err != nil && !os.IsExist(err) {
			return fmt.Errorf("create group in %s: %v", area, err)
		}
	}

	return nil
}

func (b *bucket) NewTypedWriter(ctx context.Context, key string, contentType string, opt *driver.WriterOptions) (driver.Writer, error) {
	val, ok := opt.Extra["AddData"]
	if ok && val == "false" {
		// Marking an object for deletion, don't create a fs file
		return &writer{
			w:     nil,
			path:  "",
			attrs: nil,
		}, nil
	}

	relpath, err := resolvePath(key, false)
	if err != nil {
		return nil, fmt.Errorf("open file blob %s: %v", key, err)
	}
	path := filepath.Join(b.dir, relpath)
	if strings.HasSuffix(path, AttrsExt) {
		return nil, fmt.Errorf("open file blob %s: extension %q is reserved and cannot be used", key, AttrsExt)
	}

	f, err := os.Create(path)
	if err != nil {
		// if sub directory structure needs to be created, create it and test file creation again
		if os.IsNotExist(err) {
			pathErr := os.MkdirAll(filepath.Dir(path), 0777)
			if pathErr == nil {
				// Retry file creation
				f, err = os.Create(path)
				if err != nil {
					return nil, fmt.Errorf("open file blob %s: %v", key, err)
				}
			} else {
				return nil, fmt.Errorf("open file blob %s: %v", key, pathErr)
			}
		} else {
			// Other file creation related error
			return nil, fmt.Errorf("open file blob %s: %v", key, err)
		}
	}

	attrs := &xattrs{
		ContentType: contentType,
		// Tiddler metadata
		Meta:     opt.Metadata,
		Revision: opt.Revision,
		Extra:    opt.Extra,
	}
	return &writer{
		w:     f,
		path:  path,
		attrs: attrs,
	}, nil
}

type writer struct {
	w     io.WriteCloser
	path  string
	attrs *xattrs
}

func (w writer) Write(p []byte) (n int, err error) {
	if w.w != nil && len(p) > 0 {
		return w.w.Write(p)
	}

	return 0, nil
}

func (w writer) Close() error {
	if err := setAttrs(w.path, w.attrs); err != nil {
		return fmt.Errorf("write blob attributes: %v", err)
	}
	if w.w != nil {
		return w.w.Close()
	}

	return nil
}

func (b *bucket) Move(ctx context.Context, keySrc string, keyDst string) error {
	src, err := resolvePath(keySrc, false)
	if err != nil {
		return fmt.Errorf("move file blob src %s: %v", keySrc, err)
	}

	dst, err := resolvePath(keyDst, false)
	if err != nil {
		return fmt.Errorf("move file blob dst %s: %v", keyDst, err)
	}

	curName := filepath.Join(b.dir, src)
	fsName := filepath.Join(b.dir, dst)
	// curName := filepath.Join(b.dir, objName) + "." + TypeObject
	// fsName := filepath.Join(b.dir, newObjName) + "." + TypeObject
	err = os.Rename(curName, fsName)
	if err != nil {
		// if sub directory structure needs to be created, create it and test file creation again
		if os.IsNotExist(err) {
			pathErr := os.MkdirAll(filepath.Dir(fsName), 0777)
			if pathErr == nil {
				// Retry file creation
				err = os.Rename(curName, fsName)
				if err != nil {
					return err
				}
			} else {
				return pathErr
			}
		} else {
			// Other file creation related error
			return err
		}
	}

	return nil
}

func (b *bucket) Delete(ctx context.Context, key string) error {
	relpath, err := resolvePath(key, false)
	if err != nil {
		return fmt.Errorf("delete file blob %s: %v", key, err)
	}
	path := filepath.Join(b.dir, relpath)
	if strings.HasSuffix(path, AttrsExt) {
		return fmt.Errorf("delete file blob %s: extension %q cannot be directly deleted", key, AttrsExt)
	}
	err = os.Remove(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fileError{relpath: relpath, msg: err.Error(), kind: driver.NotFound}
		}
		return fmt.Errorf("delete file blob %s: %v", key, err)
	}
	// Files are moved to the recyclebin first, before thay can be removed from filesystem
	// and there .meta files are placed next to the real file, so delete .meta file in same directory
	metaFile := path + AttrsExt
	// metaFile := strings.Replace(path, DataDir, MetaDir, 1) + AttrsExt
	if err = os.Remove(metaFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete file blob %s: %v", key, err)
	}
	return nil
}

type fileError struct {
	relpath, msg string
	kind         driver.ErrorKind
}

func (e fileError) Error() string {
	return fmt.Sprintf("fileblob: object %s: %v", e.relpath, e.msg)
}

func (e fileError) BlobError() driver.ErrorKind {
	return e.kind
}
