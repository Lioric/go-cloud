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
	"encoding/base64"
	"fmt"
	"io"
	"path/filepath"
	"time"

	// "io"
	"os"
	// "path/filepath"

	"strconv"
	"strings"

	"github.com/Lioric/go-cloud/blob"
	"github.com/Lioric/go-cloud/blob/driver"
	// "github.com/Lioric/go-cloud/blob/fileblob"
)

var DBName = ""
var FTSExt = ""

type sqlbucket struct {
	dir        string
	fileBucket *bucket
}

// var FTSExt = ".fts"

var sPathSep = string(os.PathSeparator)

const SCHEMA_VERSION string = "1"
const NAME_INFO_ENTRY = "info"

// var sBucketLocation string

// var DataDir = ""
// var MetaDir = ""
// var RevisionDir = ""
// var AttrsExt = ""

// func getCacheFile(key string) string {
// storageArea := sBucketLocation + sPathSep + getArea(user) + sPathSep + sLocalFileSystemMetaLocation
// metaFile := storageArea + sPathSep + sDBName

// return metaFile
// }

func createDB(ctx context.Context, name string) (*sql.DB, error) {
	// Create metadata database
	sqlDB, err := sql.Open("sqlite3", name)
	if err != nil {
		return nil, fmt.Errorf("create metadata database %s: %v", name, err)
	}

	defer sqlDB.Close()

	query := `
		CREATE TABLE info (
			name text NOT NULL UNIQUE,
			version INTEGER NOT NULL,
			rev INTEGER NOT NULL,
			mod INTEGER,
			extra BLOB
		);

		CREATE TABLE notes (
			id INTEGER PRIMARY KEY,
			title text NOT NULL,
			creator TEXT DEFAULT "",
			created INTEGER NOT NULL,
			modified INTEGER NOT NULL,
			modifier TEXT DEFAULT "",
			revision INTEGER DEFAULT 0
		);

		CREATE TABLE extrafields (
			noteId INTEGER NOT NULL,
			name text NOT NULL,
			value text,
			FOREIGN KEY(noteId) REFERENCES notes(id) ON UPDATE CASCADE ON DELETE CASCADE
		);

		CREATE TABLE IF NOT EXISTS taglist (
			id INTEGER UNIQUE PRIMARY KEY,
			tags text UNIQUE NOT NULL
		 );

		 CREATE TABLE IF NOT EXISTS tagmap (
			noteId INTEGER NOT NULL,
			tagId INTEGER NOT NULL,
			FOREIGN KEY(noteId) REFERENCES notes(id) ON UPDATE CASCADE ON DELETE CASCADE,
			FOREIGN KEY(tagId) REFERENCES tags(id) ON UPDATE CASCADE ON DELETE CASCADE
			PRIMARY KEY (noteId,tagId)
		 );

		CREATE UNIQUE INDEX titleIndex ON notes(title);
		CREATE INDEX modIndex ON notes(modified);
		CREATE INDEX noteIndex ON extrafields(noteId);

		INSERT INTO info(rowid, name, version, rev) VALUES (0,"` + NAME_INFO_ENTRY + `",` + SCHEMA_VERSION + `, 0);
		PARGMA user_version=3;
	`

	// INSERT INTO notes (id, title, created, revision, modifier) VALUES (0,` + TITLE_INFO_ENTRY + `, CURRENT_TIMESTAMP, 0, 0)

	// extraId INTEGER PRIMARY KEY,

	_, err = sqlDB.Exec(query)
	if err != nil {
		return nil, fmt.Errorf("create table %s: %v", name, err)
	}

	// Create full text search database
	ftsDB, err := sql.Open("sqlite3", name+FTSExt)
	if err != nil {
		return nil, fmt.Errorf("create filter database %s: %v", name, err)
	}

	defer ftsDB.Close()

	query = `CREATE table filters (
		noteId INTEGER PRIMARY KEY,
		filter BLOB
	)`

	_, err = ftsDB.Exec(query)
	if err != nil {
		return nil, fmt.Errorf("create filter table %s: %v", name, err)
	}

	return sqlDB, err
}

func openDB(ctx context.Context, name string) (*sql.DB, error) {
	// metaFile := getCacheFile(key)

	_, err := os.Stat(name)

	if os.IsNotExist(err) {
		return nil, sqlFileError{key: name, msg: "no metadata in area", kind: driver.NotFound}
		// // 	Create metadata database
		// _, err := createDB(ctx, name)

		// if err != nil {
		// 	return nil, err
		// }
	}

	sqlDB, err := sql.Open("sqlite3", name)
	if err != nil {
		return nil, fmt.Errorf("open metadata %s: %v", name, err)
		// log.Debug().
		// 	Str("module", "DataStore").
		// 	Str("method", "Platform::openDB").
		// 	Err(err).
		// 	Msg("Failed to open database")

		// return nil
	}

	return sqlDB, nil
}

func (b *sqlbucket) getMetadataElements(key string) (string, string, string) {
	areaName := strings.SplitN(key, "/", 3)
	area := areaName[0]
	sql := b.dir + areaName[0] + sPathSep + MetaDir + sPathSep + DBName
	// sql := b.dir + areaName[0] + sPathSep + MetaDir + sPathSep + DBName + ".mb"
	objName := areaName[2]
	//	fts := b.dir + sPathSep + areaName[0] + sPathSep + MetaDir + sPathSep + "filter" + ".mb"

	return sql, objName, area
}

func (b *sqlbucket) getInfoMetadata(ctx context.Context, sql string, key string) (*xattrs, error) {
	list := strings.SplitN(key, "/", 2)
	if len(list) < 2 {
		return nil, fmt.Errorf("Incorrect key id: %s", key)
	}

	db, err := openDB(ctx, sql)
	if err != nil {
		return nil, err
	}

	if db == nil {
		return nil, fmt.Errorf("Unable to open: %s", sql)
	}

	defer db.Close()

	row := db.QueryRow("SELECT version,rev,extra,mod from info where name = ?", list[1])

	var version string
	var rev string
	var extra string
	var mod string

	err = row.Scan(&version, &rev, &extra, &mod)
	if err != nil {
		return nil, fmt.Errorf("Error getting info metadata: %v", err)
	}

	xa := new(xattrs)
	xa.Meta = make(map[string]string)

	xa.Meta["version"] = version
	xa.Meta["rev"] = rev
	xa.Meta["extra"] = extra
	xa.Meta["mod"] = mod

	return xa, nil
}

func (b *sqlbucket) getMetadata(ctx context.Context, key string) (*xattrs, error) {
	sql, objName, _ := b.getMetadataElements(key)

	if strings.HasPrefix(objName, "$:/") {
		// Key is from INFO table
		return b.getInfoMetadata(ctx, sql, objName)
	}

	db, err := openDB(ctx, sql)
	if err != nil {
		return nil, err
	}

	if db != nil {
		defer db.Close()

		rows, err := db.Query("select id,title,tags,creator,created,modified,modifier,revision from notes where title = ?", objName)
		// rows, err := db.Query("select id,title,tags,creator,created,modified,modifier,revision,extraFields from notes where title = ?", objName)
		if err != nil {
			return nil, fmt.Errorf("get metadata: %v", err)
		}
		defer rows.Close()

		var id int
		var title string
		var tags string
		var creator string
		var created string
		var modifier string
		var modified string
		var revision int
		// var extraFields string

		isRow := rows.Next()
		err = rows.Err()
		if err != nil {
			return nil, fmt.Errorf("get metadata: %v", err)
			// log.Debug().
			// 	Str("module", "DataStore").
			// 	Str("method", "Platform::getMetadataCache").
			// 	Err(err).
			// 	Msg("Failed to iterate rows")
		}

		if isRow == false {
			return nil, sqlFileError{key: objName, msg: "no key in metadata", kind: driver.NotFound}
		}

		xa := new(xattrs)
		xa.Meta = make(map[string]string)

		err = rows.Scan(&id, &title, &tags, &creator, &created, &modified, &modifier, &revision)
		// err = rows.Scan(&id, &title, &tags, &creator, &created, &modified, &modifier, &revision, &extraFields)
		if err != nil {
			return nil, fmt.Errorf("get metadata: %v", err)
		}

		xa.Id = id
		xa.Name = title
		xa.Revision = revision
		// xa.Meta["title"] = title
		xa.Meta["tags"] = tags
		xa.Meta["creator"] = creator
		xa.Meta["created"] = created
		xa.Meta["modified"] = modified
		xa.Meta["modifier"] = modifier
		// xa.Meta["previous"] = previous

		// if len(extraFields) > 0 {
		extraRows, err := db.Query("SELECT name, value from extrafields WHERE noteId=" + strconv.FormatInt(int64(id), 10))
		// extraRows, err := db.Query("SELECT * from extrafields WHERE rowid IN (" + extraFields + ")")
		if err != nil {
			return nil, fmt.Errorf("get metadata: %v", err)
		}

		defer extraRows.Close()

		for extraRows.Next() {
			// var id int
			// var noteId int
			var name string
			var value string

			// err = extraRows.Scan(&noteId, &name, &value)
			err = extraRows.Scan(&name, &value)
			// err = extraRows.Scan(&id, &noteId, &name, &value)
			if err != nil {
				return nil, fmt.Errorf("get metadata: %v", err)
			}

			xa.Meta[name] = value
		}

		// xa.Extra = map[string]string{"extraFields": extraFields}
		// }

		contentType, ok := xa.Meta["type"]
		if ok == false {
			contentType = "text/plain"
		}

		xa.ContentType = contentType

		return xa, nil
	}

	return nil, nil
}

type extraField struct {
	name  string
	value string
}

// Put info metadata
func (b *sqlbucket) putInfoMetadata(ctx context.Context, name string, id int, revision string, mod int, extra string) error {
	sql, key, _ := b.getMetadataElements(name)

	list := strings.SplitN(key, "/", 2)
	if len(list) < 2 {
		return fmt.Errorf("Incorrect key name: %s", key)
	}

	keyName := list[1]

	db, err := openDB(ctx, sql)
	if err != nil {
		return err
	}

	if db != nil {
		defer db.Close()

		// idStr := strconv.FormatInt(int64(id), 10)
		query := `REPLACE INTO info(name, version, rev, mod, extra) values("` + keyName + `", ?, ?, ?, ?)`

		_, err := db.Exec(query, SCHEMA_VERSION, revision, mod, extra)
		if err != nil {
			return fmt.Errorf("Error updating info metadata[%s]: %v", name, err)
		}

	}

	return nil
}

// Put metadata
func (b *sqlbucket) putMetadata(ctx context.Context, name string, id int, meta map[string]string, revision int) error {
	// func (b *sqlbucket) putMetadata(ctx context.Context, name string, id int, meta map[string]string, revision int, extraFieldIds string) error {
	sql, objName, _ := b.getMetadataElements(name)
	// sql, _, _ := b.getMetadataElements(name)

	db, err := openDB(ctx, sql)
	if err != nil {
		return err
	}

	if db != nil {
		defer db.Close()

		// Enable foreing keys constrains and attach FullTextSearch database
		_, err = db.Exec("PRAGMA foreign_keys=ON; ATTACH DATABASE \"" + sql + FTSExt + "\" AS FTS;")
		if err != nil {
			return fmt.Errorf("setup db connection [%s]: %v", name, err)
		}

		tx, err := db.Begin()

		// if len(extraFieldIds) > 0 {
		// 	// Delete previous extra fields
		// 	_, err := tx.Exec("delete from extrafields where rowid in (" + extraFieldIds + ")")
		// 	// _, err := db.Exec("delete from extrafields where rowid in (" + extraFieldIds + ")")
		// 	if err != nil {
		// 		return fmt.Errorf("put metadata [%s]: %v", name, err)
		// 	}
		// }

		// var title string
		title := objName
		revision := revision
		var tags string
		var creator string
		var created string
		var modifier string
		var modified string
		// var revision int
		// var previous string

		extraFields := make([]extraField, 0, 10)

		var filter []byte

		for key, value := range meta {

			switch key {
			case "text":
				// noop (text is stored in the data file)
			case "title":
				// title = value
			case "tags":
				tags = value
			case "creator":
				creator = value
			case "created":
				created = value
			case "modified":
				modified = value
			case "modifier":
				modifier = value
			case "_index":
				// Full text search filter is stored in a separated sql file
				filter, err = base64.StdEncoding.DecodeString(value)
				if err != nil {
					filter = nil
				}
			case "revision":
				// rev, _ := strconv.ParseInt(value, 10, 0)
				// revision = int(rev)
			// case "previous":
			// 	previous = value
			default:
				extraFields = append(extraFields, extraField{key, value})
			}
		}

		var query string
		var rowIdStr = strconv.FormatInt(int64(id), 10)
		// var filterQuery string

		idStr := "null"

		if id >= 0 {
			idStr = strconv.FormatInt(int64(id), 10)
		}

		query = `REPLACE INTO notes(id, title, creator, created, modified, modifier, revision) values(` + idStr + `, ?, ?, ?, ?, ?, ?)`

		rowRes, err := tx.Exec(query, title, tags, creator, created, modified, modifier, revision)
		// rowRes, err := db.Exec(query, title, tags, creator, created, modified, modifier, revision, filterId, filter)
		// rowRes, err := db.Exec(query, title, tags, creator, created, modified, modifier, revision, id)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("put metadata [%s]: %v", name, err)
		}

		noteId, _ := rowRes.LastInsertId()

		// Tags
		if len(tags) > 0 {
			tagList := strings.Split(tags, ",")
			query += (`INSERT OR IGNORE INTO taglist(tags) VALUES('` + strings.Join(tagList, "','") + `');
						INSERT OR IGNORE INTO tagmap(noteId, tagId) SELECT ` + strconv.FormatInt(noteId, 10) + `,taglist.id from taglist WHERE tags IN ('` + tagList + `');`)
		}

		var filterId string
		if id > 0 {
			filterId = rowIdStr
		} else {
			filterId = strconv.FormatInt(noteId, 10)
		}

		if filter != nil {

			// Insert Full text Search filter
			query = `REPLACE into filters(noteId, filter) values(` + filterId + `, ?)`

			_, err := tx.Exec(query, filter)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("put filter [%s]: %v", name, err)
			}
		}

		// filterId := rowIdStr

		// if id == 0 {
		// Query is INSERT, use rowId of the inserted row from the 'notes' table
		// filterId = "last_insert_rowid()"
		// }

		if noteId > 0 {
			id = int(noteId)
		}

		if len(extraFields) > 0 {
			// tx, err := db.Begin()
			// if err != nil {
			// 	tx.Rollback()
			// 	return fmt.Errorf("put metadata [%s]: %v", name, err)
			// }

			// Extra fields
			extraSmtm, err := tx.Prepare(`insert into extrafields(noteId, name, value) values(?, ?, ?)`)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("put metadata [%s]: %v", name, err)
			}
			defer extraSmtm.Close()

			// extraRows := []string{}

			for _, extraField := range extraFields {
				_, err := extraSmtm.Exec(id, extraField.name, extraField.value)
				// extraRow, err := extraSmtm.Exec(id, extraField.name, extraField.value)
				if err != nil {
					tx.Rollback()
					return fmt.Errorf("put metadata [%s]: %v", name, err)
				}

				// rowid, _ := extraRow.LastInsertId()
				// extraRows = append(extraRows, strconv.FormatInt(rowid, 10))
			}

			// extraString := strings.Join(extraRows[:], ",")
			// _, err = tx.Exec(`UPDATE notes SET fields = ? WHERE rowid = ?`, extraString, id)

			// if err != nil {
			// 	tx.Rollback()
			// 	return fmt.Errorf("put metadata [%s]: %v", name, err)
			// }

			extraSmtm.Close()
		}

		// Full text search
		//		if filter != nil {

		//			ftsDB, err := openDB(ctx, sql+FTSExt)
		//			if err != nil {
		//				return err
		//			}

		//			if ftsDB != nil {
		//				defer ftsDB.Close()

		//				_, err := ftsDB.Exec(filterQuery, title, filter, noteId)
		//				if err != nil {
		//					return fmt.Errorf("put filter [%s]: %v", sql+FTSExt, err)
		//				}

		//			}

		//		}

		err = tx.Commit()
		if err != nil {
			tx.Rollback()

			return fmt.Errorf("put metadata [%s]: %v", name, err)
		}

		db.Close()

	}

	return nil
}

// NewBucket creates a new bucket that reads and writes to dir.
// dir must exist.
func OpenSqlBucket(dir string) (*blob.Bucket, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("open file bucket: %v", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("open file bucket: %s is not a directory", dir)
	}
	sqlb := sqlbucket{dir, &bucket{dir}}
	// return blob.NewBucket(&bucket{dir}), nil

	// fileBucket, err := NewBucket(dir)
	return blob.NewBucket(&sqlb), err
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

func (b *sqlbucket) NewRangeReader(ctx context.Context, key string, offset, length int64, exactKeyName bool) (driver.Reader, error) {
	if length == 0 {
		// Get metadata
		meta, err := b.getMetadata(ctx, key)
		if err != nil {
			return nil, err
		}

		var size int64
		var modTime time.Time

		relpath, err := resolvePath(key, exactKeyName)
		if err != nil {
			return nil, fmt.Errorf("open metadata blob %s: %v", key, err)
		}

		path := filepath.Join(b.dir, relpath)

		info, err := os.Stat(path)
		if err != nil {
			// Info only metadata doesn't have a file in the storage area
			if os.IsNotExist(err) == false {
				return nil, fmt.Errorf("open file blob %s: %v", key, err)
			}

		} else {
			size = info.Size()
			modTime = info.ModTime()
		}

		// path := filepath.Join(b.dir, relpath)
		// info, err := os.Stat(path)
		// if err != nil {
		// 	if os.IsNotExist(err) {
		// 		return nil, fileError{relpath: relpath, msg: err.Error(), kind: driver.NotFound}
		// 	}
		// 	return nil, fmt.Errorf("open file blob %s: %v", key, err)
		// }

		return reader{
			size:    size,
			modTime: modTime,
			xa:      meta,
		}, nil
	}

	// Get object data
	r, err := b.fileBucket.NewRangeReader(ctx, key, offset, length, exactKeyName)

	if err != nil {
		return nil, err
	}

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

func (b *sqlbucket) CreateArea(ctx context.Context, area string, groups []string) error {
	err := b.fileBucket.CreateArea(ctx, area, groups)
	if err != nil && os.IsExist(err) == false {
		return err
	}

	sql, _, _ := b.getMetadataElements(area + sPathSep + "data" + sPathSep + "__placeholder__")

	// 	Create metadata database
	_, err = createDB(ctx, sql)

	if err != nil {
		if os.IsNotExist(err) {
			return sqlFileError{key: area, msg: "area don't exists", kind: driver.NotFound}
		} else {
			return err
		}
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

func (b *sqlbucket) NewTypedWriter(ctx context.Context, key string, contentType string, opt *driver.WriterOptions) (driver.Writer, error) {
	if opt == nil {
		opt = &driver.WriterOptions{}
	}
	if opt.Extra == nil {
		opt.Extra = make(map[string]string)
	}

	// Commets, revisions, attachment files or objects moved to the recycle bin don't need to store meta in the database
	addMeta := opt.Extra["AddMeta"]

	// Metadata is stored in sql database
	// don't create .meta file in the fileblob writer
	if addMeta != "false" {
		opt.Extra["AddMeta"] = "false"
	}

	// Object list info store just metadata, don't create an external file
	addData := strings.HasPrefix(opt.Name, "$:/") == false
	// addData := opt.Extra["AddData"] != "false"

	if addData {
		w, err := b.fileBucket.NewTypedWriter(ctx, key, contentType, opt)

		return sqlWriter{
			w:        w,
			key:      key,
			meta:     opt.Metadata,
			revision: opt.Revision,
			b:        b,
			ctx:      ctx,
			id:       opt.Id,
			// extra:    opt.Extra["extraFields"],
			addMeta: addMeta != "false",
			// addData: addData,
		}, err

	} else {

		return InfoDataWriter{
			key:      key,
			meta:     opt.Metadata,
			revision: opt.Revision,
			b:        b,
			ctx:      ctx,
			id:       opt.Id,
		}, nil
	}
}

type InfoDataWriter struct {
	ctx      context.Context
	w        io.WriteCloser
	b        *sqlbucket
	id       int
	key      string
	meta     map[string]string
	revision int
}

func (w InfoDataWriter) Write(p []byte) (n int, err error) {
	return 0, nil
}

func (w InfoDataWriter) Close() error {
	rev, ok := w.meta["rev"]
	if ok == false {
		return fmt.Errorf("No revision provided[%s]", w.key)
	}

	extra, _ := w.meta["extra"]
	mod, _ := w.meta["mod"]
	modTime, _ := strconv.ParseInt(mod, 10, 0)

	err := w.b.putInfoMetadata(w.ctx, w.key, w.id, rev, int(modTime), extra)
	if err != nil {
		return fmt.Errorf("write blob attributes: %v", err)
	}

	return nil
}

type sqlWriter struct {
	w        io.WriteCloser
	key      string
	meta     map[string]string
	revision int
	b        *sqlbucket
	id       int
	// extra    string
	ctx     context.Context
	addMeta bool
	addData bool
}

func (w sqlWriter) Write(p []byte) (n int, err error) {
	return w.w.Write(p)
	// if w.addData {
	// 	return w.w.Write(p)
	// }

	// return 0, nil
}

func (w sqlWriter) Close() error {
	if w.addMeta != false {
		err := w.b.putMetadata(w.ctx, w.key, w.id, w.meta, w.revision)
		// err := w.b.putMetadata(w.ctx, w.key, w.id, w.meta, w.revision, w.extra)

		if err != nil {
			return fmt.Errorf("write blob attributes: %v", err)
		}
	}
	return w.w.Close()
}

// Move is used only by the revision system when creating
// a new revision point before updating object with new contents,
// no need to delete previous object or change metadata
func (b *sqlbucket) Move(ctx context.Context, keySrc string, keyDst string) error {
	return b.fileBucket.Move(ctx, keySrc, keyDst)
}

func (b *sqlbucket) Delete(ctx context.Context, key string) error {
	sql, objName, _ := b.getMetadataElements(key)

	db, err := openDB(ctx, sql)
	if err != nil {
		return err
	}

	if db != nil {
		defer db.Close()

		_, err = db.Exec("PRAGMA foreign_keys=ON;DELETE FROM notes WHERE title = ?", objName)
		if err != nil {
			return fmt.Errorf("delete sql entry %s: %v", objName, err)
		}

		/*
			rows, _ := db.Query("SELECT rowid, fields FROM notes where title = ?", objName)

			if rows.Next() {
				var id int
				var fields string

				err := rows.Scan(&id, &fields)
				rows.Close()
				if err == nil {
					if len(fields) > 0 {
						// Delete previous extra fields
						db.Exec("delete from extrafields where rowid in (" + fields + ")")
						// _, err := db.Exec("delete from extrafields where rowid in (" + fields + ")")
						// if err != nil {
						// 	return fmt.Errorf("delete extra data rows: %v", err)
						// }
					}

					_, err = db.Exec("delete from notes where rowid = ?", id)
					if err != nil {
						return fmt.Errorf("delete sql entry %s: %v", objName, err)
					}
				}
			}
		*/

	}

	return b.fileBucket.Delete(ctx, key)
	// relpath, err := resolvePath(key)
	// if err != nil {
	// 	return fmt.Errorf("delete file blob %s: %v", key, err)
	// }
	// path := filepath.Join(b.dir, relpath)
	// if strings.HasSuffix(path, AttrsExt) {
	// 	return fmt.Errorf("delete file blob %s: extension %q cannot be directly deleted", key, AttrsExt)
	// }
	// err = os.Remove(path)
	// if err != nil {
	// 	if os.IsNotExist(err) {
	// 		return fileError{relpath: relpath, msg: err.Error(), kind: driver.NotFound}
	// 	}
	// 	return fmt.Errorf("delete file blob %s: %v", key, err)
	// }
	// // Files are moved to the recyclebin first, before thay can be removed from filesystem
	// // and there .meta files are placed next to the real file, so delete .meta file in same directory
	// metaFile := path + AttrsExt
	// // metaFile := strings.Replace(path, DataDir, MetaDir, 1) + AttrsExt
	// if err = os.Remove(metaFile); err != nil && !os.IsNotExist(err) {
	// 	return fmt.Errorf("delete file blob %s: %v", key, err)
	// }
	// return nil
}

// type fileError struct {
// 	relpath, msg string
// 	kind         driver.ErrorKind
// }

// func (e fileError) Error() string {
// 	return fmt.Sprintf("fileblob: object %s: %v", e.relpath, e.msg)
// }

// func (e fileError) BlobError() driver.ErrorKind {
// 	return e.kind
// }

type sqlFileError struct {
	key, msg string
	kind     driver.ErrorKind
}

func (e sqlFileError) Error() string {
	return fmt.Sprintf("sqlfileblob: key %s: %v", e.key, e.msg)
}

func (e sqlFileError) BlobError() driver.ErrorKind {
	return e.kind
}
