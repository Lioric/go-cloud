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

// Package gcsblob provides an implementation of using blob API on GCS.
// It is a wrapper around GCS client library.
package gcsblob

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/Lioric/go-cloud/blob"
	"github.com/Lioric/go-cloud/blob/driver"
	"github.com/Lioric/go-cloud/gcp"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// OpenBucket returns a GCS Bucket that communicates using the given HTTP client.
func OpenBucket(ctx context.Context, bucketName string, client *gcp.HTTPClient) (*blob.Bucket, error) {
	if client == nil {
		return nil, fmt.Errorf("NewBucket requires an HTTP client to communicate using")
	}
	c, err := storage.NewClient(ctx, option.WithHTTPClient(&client.Client))
	if err != nil {
		return nil, err
	}
	return blob.NewBucket(&bucket{name: bucketName, client: c}), nil
}

// bucket represents a GCS bucket, which handles read, write and delete operations
// on objects within it.
type bucket struct {
	name   string
	client *storage.Client
}

var emptyBody = ioutil.NopCloser(strings.NewReader(""))

// reader reads a GCS object. It implements io.ReadCloser.
type reader struct {
	body        io.ReadCloser
	size        int64
	contentType string
	updated     time.Time

	// Tiddler metadata
	revision int64
	metadata map[string]string
}

func (r *reader) Read(p []byte) (int, error) {
	return r.body.Read(p)
}

// Close closes the reader itself. It must be called when done reading.
func (r *reader) Close() error {
	return r.body.Close()
}

func (r *reader) Attrs() *driver.ObjectAttrs {
	return &driver.ObjectAttrs{
		Size:        r.size,
		ContentType: r.contentType,
		ModTime:     r.updated,

		// Tiddler metadata
		Revision: r.revision,
		Fields:   r.metadata,
	}
}

// CreateUserArea setups a new area with the given id
// Currently a no op to satisfy interface
//
// Different providers enforce different set of rules for number of bucket creation
// so for the time being, on object storage platforms, the area is part of the object key,
// and areas are within the same bucket
// (if this restrictions are lifted by object storage providers, a per area bucket might be considered)
func (b *bucket) CreateArea(ctx context.Context, area string, groups []string) error {
	return nil
}

func (b *bucket) Attributes(ctx context.Context, key string, isUID bool) (*driver.ObjectAttrs, error) {
	bkt := b.client.Bucket(b.name)
	obj := bkt.Object(key)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		if isErrNotExist(err) {
			return nil, gcsError{bucket: b.name, key: key, msg: err.Error(), kind: driver.NotFound}
		}
		return nil, err
	}

	rev, _ := strconv.ParseInt(attrs.Metadata["revision"], 10, 0)

	return &driver.ObjectAttrs{
		Size:        attrs.Size,
		ContentType: attrs.ContentType,
		ModTime:     attrs.Updated,
		Name:        attrs.Name,
		Fields:      attrs.Metadata,
		Revision:    rev,
		// Id,
		// Extra,
	}, nil
}

// NewRangeReader returns a Reader that reads part of an object, reading at most
// length bytes starting at the given offset. If length is 0, it will read only
// the metadata. If length is negative, it will read till the end of the object.
func (b *bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, exactKeyName bool) (driver.Reader, error) {
	key = blob.GetBlobName(key)
	if offset < 0 {
		return nil, fmt.Errorf("negative offset %d", offset)
	}
	bkt := b.client.Bucket(b.name)
	obj := bkt.Object(key)
	// if length == 0 {
	// 	attrs, err := obj.Attrs(ctx)
	// 	if err != nil {
	// 		if isErrNotExist(err) {
	// 			return nil, gcsError{bucket: b.name, key: key, msg: err.Error(), kind: driver.NotFound}
	// 		}
	// 		return nil, err
	// 	}

	// 	rev, _ := strconv.ParseInt(attrs.Metadata["revision"], 10, 0)

	// 	return &reader{
	// 		body:        emptyBody,
	// 		size:        attrs.Size,
	// 		contentType: attrs.ContentType,
	// 		updated:     attrs.Updated,

	// 		// Tiddler metadata
	// 		revision: int(rev),
	// 		metadata: attrs.Metadata,
	// 	}, nil
	// }
	r, err := obj.NewRangeReader(ctx, offset, length)
	if err != nil {
		if isErrNotExist(err) {
			return nil, gcsError{bucket: b.name, key: key, msg: err.Error(), kind: driver.NotFound}
		}
		return nil, err
	}
	// updated is set to zero value when non-nil error returned, no need to check or report.
	updated, _ := r.LastModified()
	return &reader{
		body:        r,
		size:        r.Size(),
		contentType: r.ContentType(),
		updated:     updated,
	}, nil
}

// NewTypedWriter returns Writer that writes to an object associated with key.
//
// A new object will be created unless an object with this key already exists.
// Otherwise any previous object with the same name will be replaced.
// The object will not be available (and any previous object will remain)
// until Close has been called.
//
// A WriterOptions can be given to change the default behavior of the Writer.
//
// The caller must call Close on the returned Writer when done writing.
func (b *bucket) NewTypedWriter(ctx context.Context, key string, contentType string, opts *driver.WriterOptions) (driver.Writer, error) {
	key = blob.GetBlobName(key)
	bkt := b.client.Bucket(b.name)
	obj := bkt.Object(key)
	w := obj.NewWriter(ctx)
	w.ContentType = contentType
	if opts != nil {
		w.ChunkSize = bufferSize(opts.BufferSize)
		// Tiddler metadata
		w.Metadata = opts.Metadata
		w.Metadata["revision"] = strconv.FormatInt(opts.Revision, 10)
	}
	return w, nil
}

// Moves the object associated with key to a new location. It is a no-op if that object
// does not exist.
func (b *bucket) Move(ctx context.Context, keySrc string, keyDst string) error {
	reader, err := b.NewRangeReader(ctx, keySrc, 0, -1, false)
	if err != nil {
		return err
	}

	defer reader.Close()

	buffer, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	w, err := b.NewTypedWriter(ctx, keyDst, "application/octet-stream", nil)
	if err != nil {
		return err
	}

	_, err = w.Write(buffer)

	if err != nil {
		return err
	}

	if err = w.Close(); err != nil {
		return err
	}

	return nil
}

// Delete deletes the object associated with key. It is a no-op if that object
// does not exist.
func (b *bucket) Delete(ctx context.Context, key string) error {
	bkt := b.client.Bucket(b.name)
	obj := bkt.Object(key)
	err := obj.Delete(ctx)
	if isErrNotExist(err) {
		return gcsError{bucket: b.name, key: key, msg: err.Error(), kind: driver.NotFound}
	}
	return err
}

func bufferSize(size int) int {
	if size == 0 {
		return googleapi.DefaultUploadChunkSize
	} else if size > 0 {
		return size
	}
	return 0 // disable buffering
}

type gcsError struct {
	bucket, key, msg string
	kind             driver.ErrorKind
}

func (e gcsError) Error() string {
	return fmt.Sprintf("gcs://%s/%s: %s", e.bucket, e.key, e.msg)
}

func (e gcsError) BlobError() driver.ErrorKind {
	return e.kind
}

func isErrNotExist(err error) bool {
	return err == storage.ErrObjectNotExist
}
