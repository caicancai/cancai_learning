package main

import (
	"errors"
	"fmt"
	"github.com/tidwall/tinylru"
	"os"
	"path/filepath"
	"sync"
)

var ()

type LogFormat byte

const (
	Binary LogFormat = 0

	JSON LogFormat = 1
)

// Options represents the options that can be set when opening a new log.
type Options struct {
	NoSync bool

	SegmentSize int

	LogFormat LogFormat

	SegmentCacheSize int

	NoCopy bool

	DirPerms os.FileMode
	FilePerm os.FileMode
}

var DefaultOptions = &Options{
	NoSync:      false,
	SegmentSize: 20 * 1024 * 1024,
	LogFormat:   Binary,
	NoCopy:      false,
	DirPerms:    0755,
	FilePerm:    0640,
}

type Log struct {
	mu         sync.Mutex
	path       string
	opts       Options
	closed     bool
	corrupt    bool
	segments   []*segment
	firstIndex uint64
	lastIndex  uint64
	sfile      *os.File
	//wbatch     Batch
	scache tinylru.LRU
}

type segment struct {
	path  string
	index uint64
	ebuf  []byte
	epos  []bpos
}

type bpos struct {
	pos int
	end int
}

func Open(path string, opts Options) (*Log, error) {
	return nil, nil
}

func abs(path string) (string, error) {
	if path == ":memory:" {
		return "", errors.New("in-memory storage not supported")
	}
	return filepath.Abs(path)
}

func (l *Log) pushCache(segIdx int) {
	_, _, _, v, evicted :=
		l.scache.SetEvicted(segIdx, nil)
	if evicted {
		s := v.(*segment)
		s.ebuf = nil
		s.epos = nil
	}
}

func (l *Log) load() error {
	return nil
}

func segmentName(index uint64) string {
	return fmt.Sprintf("log.%02d", index)
}

func (l *Log) Close() error {
	return nil
}

func (l *Log) Write(index uint64, data []byte) error {
	return nil
}

func (l *Log) appendEntry(dst []byte, index uint64, data []byte) (out []byte,
	epos bpos) {
	return nil, bpos{}
}

func (l *Log) cycle() error {
	return nil
}

func appendJSONEntry(dst []byte, index uint64, data []byte) []byte {
	return nil
}

func appendJSONData(dst []byte, data []byte) []byte {
	return nil
}

func appendBinaryEntry(dst []byte, index uint64, data []byte) []byte {
	return nil
}

func appendUvarint(dst []byte, x uint64) []byte {
	return nil
}
