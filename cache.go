package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"path/filepath"

	"time"

	. "github.com/claudetech/loggo/default"
	"golang.org/x/oauth2"

	"sync"

	"os"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Cache is the cache
type Cache struct {
	session   *mgo.Session
	dbName    string
	tokenPath string
	chunkPath string
	pop       sync.Mutex
}

const (
	// StoreAction stores an object in cache
	StoreAction = iota
	// DeleteAction deletes an object in cache
	DeleteAction = iota
)

// APIObject is a Google Drive file object
type APIObject struct {
	ObjectID     string `bson:"_id,omitempty"`
	Name         string
	IsDir        bool
	Size         uint64
	LastModified time.Time
	DownloadURL  string
	Parents      []string
	CanTrash     bool
}

// PageToken is the last change id
type PageToken struct {
	ID    string `bson:"_id,omitempty"`
	Token string
}

// DownloadRequest is a download queue request
type DownloadRequest struct {
	ID      string `bson:"_id,omitempty"`
	Object  *APIObject
	Offset  int64
	Size    int64
	Active  bool
	Preload bool
}

// Chunk is a chunk that has been stored in cache
type Chunk struct {
	ID   string `bson:"_id,omitempty"`
	Path string
}

// NewCache creates a new cache instance
func NewCache(mongoURL, mongoUser, mongoPass, mongoDatabase, cacheBasePath, tempBasePath string, sqlDebug bool) (*Cache, error) {
	Log.Debugf("Opening cache connection")

	session, err := mgo.Dial(mongoURL)
	if nil != err {
		Log.Debugf("%v")
		return nil, fmt.Errorf("Could not open mongo db connection")
	}

	cache := Cache{
		session:   session,
		dbName:    mongoDatabase,
		tokenPath: filepath.Join(cacheBasePath, "token.json"),
		chunkPath: filepath.Join(tempBasePath, "chunks"),
	}

	// getting the db
	db := session.DB(mongoDatabase)

	// login
	if "" != mongoUser && "" != mongoPass {
		db.Login(mongoUser, mongoPass)
	}

	// create index
	col := db.C("api_objects")
	col.EnsureIndex(mgo.Index{Key: []string{"parents"}})
	col.EnsureIndex(mgo.Index{Key: []string{"name"}})
	col = db.C("downloads")
	col.EnsureIndex(mgo.Index{Key: []string{"active"}})
	col.EnsureIndex(mgo.Index{Key: []string{"preload"}})

	// clear database caches
	downloads := db.C("downloads")
	if _, err := downloads.RemoveAll(nil); nil != err {
		Log.Debugf("%v", err)
		Log.Warningf("Could not clear pending download requests")
	}
	chunks := db.C("chunks")
	if _, err := chunks.RemoveAll(nil); nil != err {
		Log.Debugf("%v", err)
		Log.Warningf("Could not clear old chunk entries")
	}
	if err := os.RemoveAll(cache.chunkPath); nil != err {
		Log.Warningf("Could not clear old chunk files")
	}

	return &cache, nil
}

// Close closes all handles
func (c *Cache) Close() error {
	Log.Debugf("Closing cache connection")
	c.session.Close()
	return nil
}

// LoadToken loads a token from cache
func (c *Cache) LoadToken() (*oauth2.Token, error) {
	Log.Debugf("Loading token from cache")

	tokenFile, err := ioutil.ReadFile(c.tokenPath)
	if nil != err {
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not read token file in %v", c.tokenPath)
	}

	var token oauth2.Token
	json.Unmarshal(tokenFile, &token)

	Log.Tracef("Got token from cache %v", token)

	return &token, nil
}

// StoreToken stores a token in the cache or updates the existing token element
func (c *Cache) StoreToken(token *oauth2.Token) error {
	Log.Debugf("Storing token to cache")

	tokenJSON, err := json.Marshal(token)
	if nil != err {
		Log.Debugf("%v", err)
		return fmt.Errorf("Could not generate token.json content")
	}

	if err := ioutil.WriteFile(c.tokenPath, tokenJSON, 0644); nil != err {
		Log.Debugf("%v", err)
		return fmt.Errorf("Could not generate token.json file")
	}

	return nil
}

// GetObject gets an object by id
func (c *Cache) GetObject(id string) (*APIObject, error) {
	Log.Tracef("Getting object %v", id)
	db := c.session.DB(c.dbName).C("api_objects")

	var object APIObject
	if err := db.Find(bson.M{"_id": id}).One(&object); nil != err {
		return nil, fmt.Errorf("Could not find object %v in cache", id)
	}

	Log.Tracef("Got object from cache %v", object)
	return &object, nil
}

// GetObjectsByParent get all objects under parent id
func (c *Cache) GetObjectsByParent(parent string) ([]*APIObject, error) {
	Log.Tracef("Getting children for %v", parent)
	db := c.session.DB(c.dbName).C("api_objects")

	var objects []*APIObject
	if err := db.Find(bson.M{"parents": parent}).All(&objects); nil != err {
		return nil, fmt.Errorf("Could not find children for parent %v in cache", parent)
	}

	Log.Tracef("Got objects from cache %v", objects)
	return objects, nil
}

// GetObjectByParentAndName finds a child element by name and its parent id
func (c *Cache) GetObjectByParentAndName(parent, name string) (*APIObject, error) {
	Log.Tracef("Getting object %v in parent %v", name, parent)
	db := c.session.DB(c.dbName).C("api_objects")

	var object APIObject
	if err := db.Find(bson.M{"parents": parent, "name": name}).One(&object); nil != err {
		return nil, fmt.Errorf("Could not find object with name %v in parent %v", name, parent)
	}

	Log.Tracef("Got object from cache %v", object)
	return &object, nil
}

// DeleteObject deletes an object by id
func (c *Cache) DeleteObject(id string) error {
	db := c.session.DB(c.dbName).C("api_objects")

	if err := db.Remove(bson.M{"_id": id}); nil != err {
		return fmt.Errorf("Could not delete object %v", id)
	}

	return nil
}

// UpdateObject updates an object
func (c *Cache) UpdateObject(object *APIObject) error {
	db := c.session.DB(c.dbName).C("api_objects")

	if _, err := db.Upsert(bson.M{"_id": object.ObjectID}, object); nil != err {
		return fmt.Errorf("Could not update/save object %v (%v)", object.ObjectID, object.Name)
	}

	return nil
}

// StoreStartPageToken stores the page token for changes
func (c *Cache) StoreStartPageToken(token string) error {
	Log.Debugf("Storing page token %v in cache", token)
	db := c.session.DB(c.dbName).C("page_token")

	if _, err := db.Upsert(bson.M{"_id": "t"}, &PageToken{ID: "t", Token: token}); nil != err {
		return fmt.Errorf("Could not store token %v", token)
	}

	return nil
}

// GetStartPageToken gets the start page token
func (c *Cache) GetStartPageToken() (string, error) {
	Log.Debugf("Getting start page token from cache")
	db := c.session.DB(c.dbName).C("page_token")

	var pageToken PageToken
	if err := db.Find(nil).One(&pageToken); nil != err {
		return "", fmt.Errorf("Could not get token from cache")
	}

	Log.Tracef("Got start page token %v", pageToken.Token)
	return pageToken.Token, nil
}

// NewDownloadRequest creates a new download request
func NewDownloadRequest(object *APIObject, offset, size int64, preload bool) *DownloadRequest {
	return &DownloadRequest{
		ID:      fmt.Sprintf("%v:%v", object.ObjectID, offset),
		Object:  object,
		Offset:  offset,
		Size:    size,
		Active:  false,
		Preload: preload,
	}
}

// AddToDownloadQueue adds a request to the download queue
func (c *Cache) AddToDownloadQueue(request *DownloadRequest) {
	db := c.session.DB(c.dbName).C("downloads")

	if err := db.Insert(request); nil != err && !mgo.IsDup(err) {
		Log.Debugf("%v", err)
		Log.Warningf("Could not insert download request %v", request.ID)
	}
}

// DownloadRunning check if a download is already running
func (c *Cache) DownloadRunning(id string) bool {
	db := c.session.DB(c.dbName).C("downloads")

	n, err := db.Find(bson.M{"_id": id, "active": "true"}).Count()
	if nil != err {
		return false
	}
	return n > 0
}

// ActivateDownload indicates that the download has been started
func (c *Cache) ActivateDownload(request *DownloadRequest) error {
	db := c.session.DB(c.dbName).C("downloads")

	request.Active = true
	if _, err := db.Upsert(bson.M{"_id": request.ID}, request); nil != err {
		return fmt.Errorf("Could not activate download request %v", request.ID)
	}
	return nil
}

// DeleteDownload removes the download request when the download is finished
func (c *Cache) DeleteDownload(id string) {
	db := c.session.DB(c.dbName).C("downloads")

	if err := db.Remove(bson.M{"_id": id}); nil != err {
		Log.Warningf("Could not delete download request %v", id)
	}
}

// PopDownloadQueue pops an element from the queue in correct order (this request is blocking)
func (c *Cache) PopDownloadQueue() *DownloadRequest {
	c.pop.Lock()
	defer c.pop.Unlock()

	response := make(chan *DownloadRequest)

	go func() {
		db := c.session.DB(c.dbName).C("downloads")

		var request DownloadRequest
		for {
			if err := db.Find(bson.M{"active": false}).Sort("preload").One(&request); nil == err {
				if err := db.Remove(bson.M{"_id": request.ID}); nil != err {
					Log.Warningf("Could not delete download request %v", request.ID)
				}
				break
			} else {
				time.Sleep(500 * time.Millisecond)
			}
		}
		response <- &request
	}()

	return <-response
}

// ChunkExists tests if a chunk exists
func (c *Cache) ChunkExists(id string) bool {
	db := c.session.DB(c.dbName).C("chunks")

	n, err := db.Find(bson.M{"_id": id}).Count()
	if nil != err {
		return false
	}
	return n > 0
}

// StoreChunk stores a chunk to disk/cache
func (c *Cache) StoreChunk(id string, content []byte) error {
	db := c.session.DB(c.dbName).C("chunks")

	chunk := Chunk{
		ID:   id,
		Path: filepath.Join(c.chunkPath, id),
	}
	if _, err := db.Upsert(bson.M{"_id": id}, &chunk); nil != err {
		return fmt.Errorf("Could not store chunk %v", id)
	}
	if err := os.MkdirAll(c.chunkPath, 0777); nil != err {
		return fmt.Errorf("Could not create chunk directory")
	}
	if err := ioutil.WriteFile(chunk.Path, content, 0777); nil != err {
		return fmt.Errorf("Could not write chunk %v to path %v", id, chunk.Path)
	}
	return nil
}

type chunkResponse struct {
	chunk *Chunk
	err   error
}

// GetChunk blocks till a chunk is finished and returns the content of the chunk
func (c *Cache) GetChunk(id string, fOffset, offset, size int64) ([]byte, error) {
	chunkChannel := make(chan chunkResponse)

	go func() {
		for !c.ChunkExists(id) {
			time.Sleep(100 * time.Millisecond)
		}

		db := c.session.DB(c.dbName).C("chunks")

		var chunk Chunk
		if err := db.Find(bson.M{"_id": id}).One(&chunk); nil != err {
			chunkChannel <- chunkResponse{
				err: fmt.Errorf("Could not chunk %v", id),
			}
			return
		}
		chunkChannel <- chunkResponse{
			chunk: &chunk,
		}
	}()

	response := <-chunkChannel

	if nil != response.err {
		return nil, response.err
	}

	f, err := os.Open(response.chunk.Path)
	if nil != err {
		Log.Tracef("%v", err)
		return nil, fmt.Errorf("Could not open chunk %v at %v", id, response.chunk.Path)
	}
	defer f.Close()

	buf := make([]byte, size)
	n, err := f.ReadAt(buf, fOffset)
	if n > 0 && (nil == err || io.EOF == err || io.ErrUnexpectedEOF == err) {
		eOffset := int64(math.Min(float64(size), float64(len(buf))))
		return buf[:eOffset], nil
	}

	return nil, fmt.Errorf("Could not find chunk %v", id)
}
