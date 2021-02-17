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

// Package blob provides an easy way to interact with Blob objects within
// a bucket. It utilizes standard io packages to handle reads and writes.
package blob

import "testing"

func TestGetBlobName(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "getBlobName",
			args: args{name: "$:/someobject /with a name/ for a folder/dir/location"},
			want: "_system" + sFolderChar + "/someobject " + sFolderChar + "/with a name" + sFolderChar + "/ for a folder" + sFolderChar + "/dir" + sFolderChar + "/location",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetBlobName(tt.args.name); got != tt.want {
				t.Errorf("GetBlobName() = %v, want %v", got, tt.want)
			}
		})
	}
}
