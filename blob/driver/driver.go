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

// Package driver defines a set of interfaces that the blob package uses to interact
// with the underlying blob services.
package driver

import (
	"context"
	"io"
	"time"
)

// ErrorKind is a code to indicate the kind of failure.
type ErrorKind int

const (
	GenericError ErrorKind = iota
	NotFound
)

// Error is an interface that may be implemented by an error returned by
// a driver to indicate the kind of failure.  If an error does not have the
// BlobError method, then it is assumed to be GenericError.
type Error interface {
	error
	BlobError() ErrorKind
}

// Reader reads an object from the blob.
type Reader interface {
	io.ReadCloser

	// Attrs returns the object's metadata. It may return a different pointer each
	// time, but it must return the exact same values each time it is called. The
	// caller must not modify any fields in the returned ObjectAttrs.
	Attrs() *ObjectAttrs
}

// Writer writes an object to the blob.
type Writer interface {
	io.WriteCloser
}

// WriterOptions controls behaviors of Writer.
type WriterOptions struct {
	// BufferSize changes the default size in byte of the maximum part Writer can
	// write in a single request, if supported. Larger objects will be split into
	// multiple requests.
	BufferSize int

	// Kibble metadata
	Metadata map[string]string
	Revision int64
	// Extra options for platform specific implementations
	Id    int
	Name  string
	Extra map[string]string
	// Size of the text segment
	ContentSize int
}

// ObjectAttrs contains metadata of an object.
type ObjectAttrs struct {
	// Size is the size of the object in bytes.
	Size int64
	// ContentType is the MIME type of the blob object. It must not be empty.
	ContentType string
	// ModTime is the modified time of the blob object. Will be time.Time zero value if unknown.
	ModTime time.Time

	// Object metadata
	Name     string
	Fields   map[string]string
	Revision int64

	Id    int
	Extra string // platform specific
}

// Bucket provides read, write and delete operations on objects within it on the
// blob service.
type Bucket interface {
	// CreateUserArea setups a new area with the given id
	//
	// only local filesystem need to support this
	// object storage based providers use object whole path as its key, so there is no need to pre create or setup its area
	CreateArea(ctx context.Context, area string, groups []string) error

	// Attributes returns attributes for the blob. If the specified object does
	// not exist, Attributes must return an error for which ErrorCode returns
	// gcerrors.NotFound.
	// The portable type will not modify the returned Attributes.
	Attributes(ctx context.Context, key string, isUID bool) (*ObjectAttrs, error)

	// NewRangeReader returns a Reader that reads part of an object, reading at
	// most length bytes starting at the given offset. If length is 0, it will read
	// only the metadata. If length is negative, it will read till the end of the
	// object. If the specified object does not exist, NewRangeReader must return
	// an error whose BlobError method returns NotFound.
	NewRangeReader(ctx context.Context, key string, offset, length int64, exactName bool) (Reader, error)

	// NewTypedWriter returns Writer that writes to an object associated with key.
	//
	// A new object will be created unless an object with this key already exists.
	// Otherwise any previous object with the same name will be replaced.
	// The object may not be available (and any previous object will remain)
	// until Close has been called.
	//
	// contentType sets the MIME type of the object to be written. It must not be
	// empty.
	//
	// The caller must call Close on the returned Writer when done writing.
	NewTypedWriter(ctx context.Context, key string, contentType string, opt *WriterOptions) (Writer, error)

	// Move moves the object associated with key from keySrc to KeyDst
	Move(ctx context.Context, keySrc string, keyDst string) error

	// Delete deletes the object associated with key.
	Delete(ctx context.Context, key string) error
}
