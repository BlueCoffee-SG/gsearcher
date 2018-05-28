package indexer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"stuqIndex"
	"utils"
)

// Indexer  description
type Indexer struct {
	indexs   map[string]*stuqIndex.Index
	pathname string
	Logger   *utils.Log4FE
}

// NewIndexer function description
// params : index Name
// return :
func NewIndexer(pathname, indexname string, logger *utils.Log4FE) *Indexer {
	this := &Indexer{pathname: pathname, indexs: make(map[string]*stuqIndex.Index), Logger: logger}
	if pathname == "./" {
		this.Logger.Error("[ERROR] pathname can not use [%v]", pathname)
		return nil
	}

	if !utils.Exist(pathname) {
		this.Logger.Info("[INFO] pathnem %v", pathname)
		os.RemoveAll(pathname)
	} else {
		this.LoadIndex(indexname)
	}

	return this
}

// AddIndex function description
// params : document
// return :
func (this *Indexer) AddIndex(indexname string, fields map[string]uint64) error {

	if _, ok := this.indexs[indexname]; ok {
		this.Logger.Error("[ERROR] index [%v] is exist", indexname)
		return fmt.Errorf("[ERROR] index [%v] is exist", indexname)
	}

	this.indexs[indexname] = stuqIndex.NewIndex(this.pathname, indexname, this.Logger)
	if this.indexs[indexname] == nil {
		return fmt.Errorf("[ERROR] create index [%v] error", indexname)
	}
	return this.indexs[indexname].MappingFields(fields)

}

func (this *Indexer) LoadIndex(indexname string) error {

	indexMetaFile := fmt.Sprintf("%v/%v.json", this.pathname, indexname)
	if !utils.Exist(indexMetaFile) {
		this.Logger.Error("[ERROR] Index [%v] in path [%v] not exist", indexname, this.pathname)
		return fmt.Errorf("Index [%v] in path [%v] not exist", indexname, this.pathname)
	}

	idx := stuqIndex.NewIndex(this.pathname, indexname, this.Logger)
	if idx == nil {
		this.Logger.Error("[ERROR] load index [%v] fail ...", indexname)
		return fmt.Errorf("Index [%v] in path [%v] not exist", indexname, this.pathname)
	}

	this.indexs[indexname] = idx
	return nil

}

func (this *Indexer) UpdateDocuments(indexname string, documents []string) error {

	for _, body := range documents {
		document := make(map[string]string)
		if err := json.Unmarshal([]byte(body), &document); err != nil {
			continue
		}
		this.indexs[indexname].AddDocument(document)
	}

	return this.indexs[indexname].Serialization()

}

func (this *Indexer) LoadData(indexname, datafilename, fieldType string, fieldnames []string) error {

	datafile, err := os.Open(datafilename)
	if err != nil {
		this.Logger.Error("[ERROR] Open File[%v] Error %v\n", datafilename, err)
		return err
	}
	defer datafile.Close()
	scanner := bufio.NewScanner(datafile)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	document := make(map[string]string)
	count := 0
	for scanner.Scan() {
		if fieldType == "text" {
			text := scanner.Text()
			subtexts := strings.Split(text, "\t")
			sublen := len(subtexts)
			for i, t := range fieldnames {
				if i >= sublen {
					document[t] = ""
				} else {
					document[t] = subtexts[i]
				}

			}
			this.indexs[indexname].AddDocument(document)
		}
		count++
		if count%10000 == 0 {
			this.Logger.Info("[INFO] Process %v Documents", count)
		}

	}
	this.Logger.Error("[ERROR] scanner[%v] error : %v", count, scanner.Err())

	return this.indexs[indexname].Serialization()

}

// SyncIndex function description
// params :
// return :
func (this *Indexer) SyncIndex() error {

	return nil
}
