package utils

import "os"

var DOCNODE_SIZE uint64 = 8

var IVT_SYNC_NUMBER uint64 = 50000

type DocInfo struct {
	DocId uint64 `json:"docid"`
}

type DocInfoSort []DocInfo

func (a DocInfoSort) Len() int      { return len(a) }
func (a DocInfoSort) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a DocInfoSort) Less(i, j int) bool {
	if a[i] == a[j] {
		return a[i].DocId < a[j].DocId
	}
	return a[i].DocId < a[j].DocId
}

const ROOT string = "./index/"

type TmpIvt struct {
	Term  string `json:"term"`
	DocID uint64 `json:"docid"`
}

type TmpIvtTermSort []TmpIvt

func (a TmpIvtTermSort) Len() int      { return len(a) }
func (a TmpIvtTermSort) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a TmpIvtTermSort) Less(i, j int) bool {
	if a[i] == a[j] {
		return a[i].Term > a[j].Term
	}
	return a[i].Term > a[j].Term
}

type KV interface {
	Set(key string, value uint64) error
	Push(key string, value uint64) error
	Save(fielname string) error
	Load(filename string) error
	Get(key string) (uint64, bool)
}

const (
	TString uint64 = 101
	TNumber uint64 = 102
	TStore  uint64 = 103
)

//indexStruct
type IdxStruct struct {
	Pathname     string            `json:"_pathname"`
	Indexname    string            `json:"_indexname"`
	DataFilepath string            `json:"_datafilepath"`
	FileType     string            `json:"_filetype"`
	ContentDesc  []string          `json:"_contentdesc"`
	Fields       map[string]string `json:"fields"`
}

//increment DataStruct
type IdxInc struct {
	Pathname  string   `json:"_pathname"`
	Indexname string   `json:"_indexname"`
	Contents  []string `json:"_contents"`
}

const (
	EQ   uint64 = 101
	UNEQ uint64 = 102
	OVER uint64 = 103
)

//filterStruct
type Filted struct {
	Fieldname string
	Value     uint64
	Filtedtyp uint64
}

func Exist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

func Interaction(a []DocInfo, b []DocInfo) ([]DocInfo, bool) {

	if a == nil || b == nil {
		return nil, false
	}

	lena := len(a)
	lenb := len(b)
	var c []DocInfo
	lenc := 0
	if lena < lenb {
		c = make([]DocInfo, lena)
	} else {
		c = make([]DocInfo, lenb)
	}
	ia := 0
	ib := 0
	for ia < lena && ib < lenb {
		if a[ia].DocId == b[ib].DocId {
			c[lenc] = a[ia]
			lenc++
			ia++
			ib++
			continue
		}
		if a[ia].DocId < b[ib].DocId {
			ia++
		} else {
			ib++
		}
	}

	if len(c) == 0 {
		return nil, false
	} else {
		return c[:lenc], true
	}

}

func Merge(a []DocInfo, b []DocInfo) ([]DocInfo, bool) {
	lena := len(a)
	lenb := len(b)
	lenc := 0
	c := make([]DocInfo, lena+lenb)
	ia := 0
	ib := 0
	if lena == 0 && lenb == 0 {
		return nil, false
	}

	for ia < lena && ib < lenb {

		if a[ia] == b[ib] {
			c[lenc] = a[ia]
			lenc++
			ia++
			ib++
			continue
		}

		if a[ia].DocId < b[ib].DocId {
			c[lenc] = a[ia]
			lenc++
			ia++
		} else {
			c[lenc] = b[ib]
			lenc++
			ib++
		}
	}

	if ia < lena {
		for ; ia < lena; ia++ {
			c[lenc] = a[ia]
			lenc++
		}

	} else {
		for ; ib < lenb; ib++ {
			c[lenc] = b[ib]
			lenc++
		}
	}

	return c[:lenc], true

}
