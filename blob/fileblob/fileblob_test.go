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

package fileblob

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/google/go-cloud/blob"
	"github.com/google/go-cloud/blob/drivertest"
)

type harness struct {
	dir    string
	closer func()
}

func newHarness(ctx context.Context, t *testing.T) (drivertest.Harness, error) {
	dir := path.Join(os.TempDir(), "go-cloud-fileblob")
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
	return &harness{
		dir:    dir,
		closer: func() { _ = os.RemoveAll(dir) },
	}, nil
}

func (h *harness) MakeBucket(ctx context.Context) (*blob.Bucket, error) {
	return NewBucket(h.dir)
}

func (h *harness) Close() {
	h.closer()
}

func TestConformance(t *testing.T) {
	drivertest.RunConformanceTests(t, newHarness, "../testdata")
}

// File-specific unit tests.
func TestNewBucket(t *testing.T) {
	t.Run("BucketDirMissing", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "fileblob")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)
		_, gotErr := NewBucket(filepath.Join(dir, "notfound"))
		if gotErr == nil {
			t.Errorf("want error, got nil")
		}
	})
	t.Run("BucketIsFile", func(t *testing.T) {
		f, err := ioutil.TempFile("", "fileblob")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		_, gotErr := NewBucket(f.Name())
		if gotErr == nil {
			t.Error("want error, got nil")
		}
	})
}

func Test_resolveLocalFSPath(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "resolveLocalFSPath 0 level",
			args: args{s: "vInterface/data/plugin loader should request external module once"},
			want: "vInterface/data/plugin loader should request external module once",
		},
		{
			name: "resolveLocalFSPath 1 level",
			args: args{s: "vInterface/data/tasks/plugin loader/plugin loader should request external module once"},
			want: "vInterface/data/tasks" + sFolderChar + "/plugin loader" + sFolderChar + "/plugin loader should request external module once",
		},
		{
			name: "resolveLocalFSPath 2 level",
			args: args{s: "vInterface/data/tasks/plugin loader/plugin loader should request external module once"},
			want: "vInterface/data/tasks" + sFolderChar + "/plugin loader" + sFolderChar + "/plugin loader should request external module once",
		},
		{
			name: "resolveLocalFSPath 3 level",
			args: args{s: "vInterface/data/tasks/plugin loader/list/plugin loader should request external module once"},
			want: "vInterface/data/tasks" + sFolderChar + "/plugin loader" + sFolderChar + "/list" + sFolderChar + "/plugin loader should request external module once",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveLocalFSPath(tt.args.s); got != tt.want {
				t.Errorf("resolveLocalFSPath() = %v, want %v", got, tt.want)
			}
		})
	}
}
