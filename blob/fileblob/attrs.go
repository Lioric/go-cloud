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
	"bufio"
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// xattrs stores extended attributes for an object. The format is like
// filesystem extended attributes, see
// https://www.freedesktop.org/wiki/CommonExtendedAttributes.
type xattrs struct {
	ContentType string `json:"user.content_type"`

	// Tiddler specific meta attributes
	Meta     map[string]string `json:"meta"`
	Revision int               `json:"revision"`

	Name string
	// Extra options for platform specific implementations
	Id    int // used by sql platform
	Extra map[string]string
}

// setAttrs creates a "path.attrs" file along with blob to store the attributes,
// it uses line format.
func setAttrs(path string, xa *xattrs) error {
	if xa.Extra != nil {
		val, ok := xa.Extra["AddMeta"]
		if ok && val == "false" {
			// Server codepath doesn't use external .meta file
			// Comments don't use an external .meta file
			return nil
		}
	}
	metaFile := strings.Replace(path, DataDir, MetaDir, 1) + AttrsExt
	f, err := os.Create(metaFile)
	if err != nil {
		// if sub directory structure needs to be created, create it and test file creation again
		if os.IsNotExist(err) {
			pathErr := os.MkdirAll(filepath.Dir(metaFile), 0777)
			if pathErr == nil {
				// Retry file creation
				f, err = os.Create(metaFile)
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

	// Strore object rev
	xa.Meta["revision"] = strconv.Itoa(xa.Revision)
	// Store content type
	xa.Meta["content-type"] = xa.ContentType

	// Store metadata fields
	// var str string
	for key, value := range xa.Meta {
		if key != "text" {
			str := key + ":" + string(value) + "\n"
			_, err = f.WriteString(str)

			if err != nil {
				break
			}
		}
	}

	if err != nil {
		log.Print("Error writing metadata file", err)
	}

	// if err := json.NewEncoder(f).Encode(xa); err != nil {
	// 	f.Close()
	// 	return err
	// }
	return f.Close()
}

// getAttrs looks at the "path.attrs" file to retrieve the attributes and
// decodes them into a xattrs struct. It doesn't return error when there is no
// such .attrs file.
func getAttrs(path string) (*xattrs, error) {
	metaFile := strings.Replace(path, DataDir, MetaDir, 1) + AttrsExt
	f, err := os.Open(metaFile)
	if err != nil {
		if os.IsNotExist(err) {
			// Handle gracefully for non-existing .attr files.
			return &xattrs{
				ContentType: "application/octet-stream",
			}, nil
		}
		return nil, err
	}
	xa := new(xattrs)
	xa.Meta = make(map[string]string)
	parseMetadata(f, xa)

	rev, _ := strconv.ParseInt(xa.Meta["revision"], 10, 0)
	xa.Revision = int(rev)
	xa.ContentType = xa.Meta["content-type"]

	return xa, f.Close()
}

func parseMetadata(f *os.File, xa *xattrs) error {
	reader := bufio.NewReader(f)
	line, _, err := reader.ReadLine()
	// sc := bufio.NewScanner(f)

	for line != nil {
		// line := sc.Text()
		token := bytes.SplitN(line, []byte(":"), 2)

		if len(token) == 2 {
			key := bytes.TrimSpace(token[0])
			value := bytes.TrimSpace(token[1])

			if len(token[0]) > 0 {
				xa.Meta[string(key)] = string(value)
			}

			line, _, err = reader.ReadLine()

		} else {

			break
		}
	}

	return err
}
