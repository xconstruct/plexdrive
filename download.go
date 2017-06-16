package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	. "github.com/claudetech/loggo/default"
)

// DownloadManager handles concurrent chunk downloads
type DownloadManager struct {
	Client    *http.Client
	Cache     *Cache
	ReadAhead int
	ChunkSize int64
}

// NewDownloadManager creates a new download manager
func NewDownloadManager(threadCount, chunkReadAhead int, chunkSize int64, client *http.Client, cache *Cache) (*DownloadManager, error) {

	manager := DownloadManager{
		Client:    client,
		Cache:     cache,
		ReadAhead: chunkReadAhead,
		ChunkSize: chunkSize,
	}

	if threadCount < 1 {
		return nil, fmt.Errorf("Number of threads for download manager must not be < 1")
	}

	for i := 0; i < threadCount; i++ {
		go manager.downloadThread()
	}

	return &manager, nil
}

// Download downloads a chunk with high priority
func (m *DownloadManager) Download(object *APIObject, offset, size int64) ([]byte, error) {
	fOffset := offset % m.ChunkSize
	offsetStart := offset - fOffset

	request := NewDownloadRequest(object, offsetStart, size, false)
	m.Cache.AddToDownloadQueue(request)

	readAheadOffset := offsetStart + m.ChunkSize
	for i := 0; i < m.ReadAhead && uint64(readAheadOffset) < object.Size; i++ {
		m.Cache.AddToDownloadQueue(NewDownloadRequest(object, readAheadOffset, size, true))
		readAheadOffset += m.ChunkSize
	}

	return m.getChunk(request, fOffset, offset, size)
}

func (m *DownloadManager) downloadThread() {
	for {
		m.loadChunk(m.Cache.PopDownloadQueue())
	}
}

func (m *DownloadManager) loadChunk(request *DownloadRequest) {
	if !m.Cache.ChunkExists(request.ID) && !m.Cache.DownloadRunning(request.ID) {
		Log.Debugf("Starting download request %v (offset: %v / size: %v)", request.ID, request.Offset, request.Object.Size)

		defer m.Cache.DeleteDownload(request.ID)
		if err := m.Cache.ActivateDownload(request); nil != err {
			Log.Warningf("%v", err)
			return
		}
		bytes, err := downloadFromAPI(m.Client, m.ChunkSize, 0, request)
		if nil != err {
			Log.Warningf("Could not load chunk %v", request.ID)
			return
		}
		if err := m.Cache.StoreChunk(request.ID, bytes); nil != err {
			Log.Warningf("%v", err)
		}
	} else {
		Log.Tracef("Download %v already running or chunk already exist", request.ID)
	}
}

func (m *DownloadManager) getChunk(request *DownloadRequest, fOffset, offset, size int64) ([]byte, error) {
	return m.Cache.GetChunk(request.ID, fOffset, offset, size)
}

func downloadFromAPI(client *http.Client, chunkSize, delay int64, request *DownloadRequest) ([]byte, error) {
	// sleep if request is throttled
	if delay > 0 {
		time.Sleep(time.Duration(delay) * time.Second)
	}

	fOffset := request.Offset % chunkSize
	offsetStart := request.Offset - fOffset
	offsetEnd := offsetStart + chunkSize

	Log.Tracef("Requesting object %v (%v) bytes %v - %v from API (preload: %v)",
		request.Object.ObjectID, request.Object.Name, offsetStart, offsetEnd, request.Preload)
	req, err := http.NewRequest("GET", request.Object.DownloadURL, nil)
	if nil != err {
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not create request object %v (%v) from API", request.Object.ObjectID, request.Object.Name)
	}

	req.Header.Add("Range", fmt.Sprintf("bytes=%v-%v", offsetStart, offsetEnd))

	Log.Tracef("Sending HTTP Request %v", req)

	res, err := client.Do(req)
	if nil != err {
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not request object %v (%v) from API", request.Object.ObjectID, request.Object.Name)
	}
	defer res.Body.Close()
	reader := res.Body

	if res.StatusCode != 206 {
		if res.StatusCode != 403 {
			Log.Debugf("Request\n----------\n%v\n----------\n", req)
			Log.Debugf("Response\n----------\n%v\n----------\n", res)
			return nil, fmt.Errorf("Wrong status code %v", res.StatusCode)
		}

		// throttle requests
		if delay > 8 {
			return nil, fmt.Errorf("Maximum throttle interval has been reached")
		}
		bytes, err := ioutil.ReadAll(reader)
		if nil != err {
			Log.Debugf("%v", err)
			return nil, fmt.Errorf("Could not read body of 403 error")
		}
		body := string(bytes)
		if strings.Contains(body, "dailyLimitExceeded") ||
			strings.Contains(body, "userRateLimitExceeded") ||
			strings.Contains(body, "rateLimitExceeded") ||
			strings.Contains(body, "backendError") {
			if 0 == delay {
				delay = 1
			} else {
				delay = delay * 2
			}
			return downloadFromAPI(client, chunkSize, delay, request)
		}

		// return an error if other 403 error occurred
		Log.Debugf("%v", body)
		return nil, fmt.Errorf("Could not read object %v (%v) / StatusCode: %v",
			request.Object.ObjectID, request.Object.Name, res.StatusCode)
	}

	bytes, err := ioutil.ReadAll(reader)
	if nil != err {
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not read objects %v (%v) API response", request.Object.ObjectID, request.Object.Name)
	}

	return bytes, nil
}
