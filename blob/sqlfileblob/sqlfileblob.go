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
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-cloud/blob"
	"github.com/google/go-cloud/blob/driver"
	"github.com/google/go-cloud/blob/fileblob"
	"github.com/rs/zerolog/log"
	"github.com/rsc_/tiddly/auth/userdata"
)

var sBucketLocation string

type bucket struct {
	dir        string
	fileBucket *blob.Bucket
}

var DataDir = ""
var MetaDir = ""
var RevisionDir = ""
var AttrsExt = ""

func getUserCacheFile(user *userdata.Info) string {
	storageArea := sBucketLocation + string(os.PathSeparator) + getArea(user) + string(os.PathSeparator) + sLocalFileSystemMetaLocation
	metaFile := storageArea + string(os.PathSeparator) + sDBName

	return metaFile
}

func openDB(ctx context.Context, user *userdata.Info) *sql.DB {
	metaFile := getUserCacheFile(user)

	_, err := os.Stat(metaFile)

	// if os.IsNotExist(err) {
	// 	// Create cache database
	// 	p.updateUserCache(ctx, user)
	// }

	sqlDB, err := sql.Open("sqlite3", metaFile)
	if err != nil {
		log.Debug().
			Str("module", "DataStore").
			Str("method", "Platform::openDB").
			Err(err).
			Msg("Failed to open database")

		return nil
	}

	return sqlDB
}

// NewBucket creates a new bucket that reads and writes to dir.
// dir must exist.
func NewBucket(dir string) (*blob.Bucket, error) {
	fileBucket, err := fileblob.NewBucket(dir)
	return blob.NewBucket(&bucket{dir, fileBucket}), err
}

// resolvePath converts a key into a relative filesystem path. It guarantees
// that there will only be one valid key for a given path and that the resulting
// path will not reach outside the directory.
// func resolvePath(key string) (string, error) {
// 	for _, c := range key {
// 		if c == '<' || c == '>' || c == ':' || c == '"' || c == '|' || c == '?' || c == '*' || c == '^' || c == '~' || c == '\'' {
// 			// <|>|\:|\"|\||\?|\*|\^|~|\'
// 			// if !('A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '/' || c == '.' || c == ' ' || c == '_' || c == '-' || c == '$' || c == '@') {
// 			return "", fmt.Errorf("contains invalid character %q", c)
// 		}
// 	}
// 	if cleaned := slashpath.Clean(key); key != cleaned {
// 		return "", fmt.Errorf("not a clean slash-separated path")
// 	}
// 	if slashpath.IsAbs(key) {
// 		return "", fmt.Errorf("starts with a slash")
// 	}
// 	if key == "." {
// 		return "", fmt.Errorf("invalid path \".\"")
// 	}
// 	if strings.HasPrefix(key, "../") {
// 		return "", fmt.Errorf("starts with \"../\"")
// 	}
// 	return filepath.FromSlash(key), nil
// }

func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64) (driver.Reader, error) {
	r, err := b.fileBucket.NewRangeReader(ctx, key, offset, length)
	// relpath, err := resolvePath(key)
	// if err != nil {
	// 	return nil, fmt.Errorf("open file blob %s: %v", key, err)
	// }
	// path := filepath.Join(b.dir, relpath)
	// if strings.HasSuffix(path, AttrsExt) {
	// 	return nil, fmt.Errorf("open file blob %s: extension %q cannot be directly read", key, AttrsExt)
	// }
	// info, err := os.Stat(path)
	// if err != nil {
	// 	if os.IsNotExist(err) {
	// 		return nil, fileError{relpath: relpath, msg: err.Error(), kind: driver.NotFound}
	// 	}
	// 	return nil, fmt.Errorf("open file blob %s: %v", key, err)
	// }
	// TASK: should we move getAttr only to getMetadata code path?
	xa, err := getAttrs(path)
	if err != nil {
		return nil, fmt.Errorf("open file attributes %s: %v", key, err)
	}
	if length == 0 {
		return reader{
			size:    info.Size(),
			modTime: info.ModTime(),
			xa:      xa,
		}, nil
	}
	// f, err := os.Open(path)
	// if err != nil {
	// 	return nil, fmt.Errorf("open file blob %s: %v", key, err)
	// }
	// if offset > 0 {
	// 	if _, err := f.Seek(offset, io.SeekStart); err != nil {
	// 		return nil, fmt.Errorf("open file blob %s: %v", key, err)
	// 	}
	// }
	// r := io.Reader(f)
	// if length > 0 {
	// 	r = io.LimitReader(r, length)
	// }
	// return reader{
	// 	r:       r,
	// 	c:       f,
	// 	size:    info.Size(),
	// 	modTime: info.ModTime(),
	// 	xa:      xa,
	// }, nil
	return r, err
}

// type reader struct {
// 	r       io.Reader
// 	c       io.Closer
// 	size    int64
// 	modTime time.Time
// 	xa      *xattrs
// }

// func (r reader) Read(p []byte) (int, error) {
// 	if r.r == nil {
// 		return 0, io.EOF
// 	}
// 	return r.r.Read(p)
// }

// func (r reader) Close() error {
// 	if r.c == nil {
// 		return nil
// 	}
// 	return r.c.Close()
// }

// func (r reader) Attrs() *driver.ObjectAttrs {
// 	return &driver.ObjectAttrs{
// 		Size:        r.size,
// 		ContentType: r.xa.ContentType,
// 		ModTime:     r.modTime,
// 		// Tiddler metadata
// 		Fields:   r.xa.Meta,
// 		Revision: r.xa.Revision,
// 	}
// }

func (b *bucket) CreateArea(ctx context.Context, area string, groups []string) error {
	err := b.fileBucket.CreateArea(ctx, area, groups)
	if err != nil && os.IsExist(err) == false {
		return err
	}
	// if area == "." {
	// 	return fmt.Errorf("area invalid path \".\"")
	// }
	// if strings.Contains(area, "../") {
	// 	return fmt.Errorf("area starts with \"../\"")
	// }

	// path := filepath.Join(b.dir, area)
	// err := os.Mkdir(path, 0777)
	// if err != nil && os.IsExist(err) == false {
	// 	return fmt.Errorf("Create area %s: %v", area, err)
	// }

	// for _, group := range groups {
	// 	meta := filepath.Join(path, group)
	// 	err = os.Mkdir(meta, 0777)
	// 	if err != nil && os.IsExist(err) == false {
	// 		return fmt.Errorf("Create group in %s: %v", area, err)
	// 	}
	// }

	return nil
}

func (b *bucket) NewTypedWriter(ctx context.Context, key string, contentType string, opt *driver.WriterOptions) (driver.Writer, error) {
	w, err := b.fileBucket.NewTypedWriter(ctx, key, contentType, opt)
	// relpath, err := resolvePath(key)
	// if err != nil {
	// 	return nil, fmt.Errorf("open file blob %s: %v", key, err)
	// }
	// path := filepath.Join(b.dir, relpath)
	// if strings.HasSuffix(path, AttrsExt) {
	// 	return nil, fmt.Errorf("open file blob %s: extension %q is reserved and cannot be used", key, AttrsExt)
	// }

	// f, err := os.Create(path)
	// if err != nil {
	// 	// if sub directory structure needs to be created, create it and test file creation again
	// 	if os.IsNotExist(err) {
	// 		pathErr := os.MkdirAll(filepath.Dir(path), 0777)
	// 		if pathErr == nil {
	// 			// Retry file creation
	// 			f, err = os.Create(path)
	// 			if err != nil {
	// 				return nil, fmt.Errorf("open file blob %s: %v", key, err)
	// 			}
	// 		} else {
	// 			return nil, fmt.Errorf("open file blob %s: %v", key, pathErr)
	// 		}
	// 	} else {
	// 		// Other file creation related error
	// 		return nil, fmt.Errorf("open file blob %s: %v", key, err)
	// 	}
	// }

	// attrs := &xattrs{
	// 	ContentType: contentType,
	// 	// Tiddler metadata
	// 	Meta:     opt.Metadata,
	// 	Revision: opt.Revision,
	// 	Extra:    opt.Extra,
	// }
	// return &writer{
	// 	w:     f,
	// 	path:  path,
	// 	attrs: attrs,
	// }, nil

	return w, err
}

type writer struct {
	w     io.WriteCloser
	path  string
	attrs *xattrs
}

func (w writer) Write(p []byte) (n int, err error) {
	return w.w.Write(p)
}

func (w writer) Close() error {
	if err := setAttrs(w.path, w.attrs); err != nil {
		return fmt.Errorf("write blob attributes: %v", err)
	}
	return w.w.Close()
}

func (b *bucket) Delete(ctx context.Context, key string) error {
	relpath, err := resolvePath(key)
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
