package stuqIndex

import (
	"fmt"
	"strconv"
	"utils"
)

type profile struct {
	maxDocID  uint64
	fieldName string
	pathname  string
	fieldType uint64
	pfl       *utils.Mmap
	detail    *utils.Mmap
	Logger    *utils.Log4FE
}

// newProfile function description
// params :
// return :
func newProfile(pathname, fieldname string, fieldtype uint64, logger *utils.Log4FE) *profile {

	this := &profile{pathname: pathname, fieldName: fieldname, fieldType: fieldtype, Logger: logger, maxDocID: 0}

	//open file
	pflfilename := fmt.Sprintf("%v%v.pfl", pathname, fieldname)
	dtlfilename := fmt.Sprintf("%v%v.dtl", pathname, fieldname)

	if fieldtype == utils.TString || fieldtype == utils.TStore {
		if utils.Exist(pflfilename) && utils.Exist(dtlfilename) {
			this.pfl, _ = utils.NewMmap(pflfilename, utils.MODE_APPEND)
			this.detail, _ = utils.NewMmap(dtlfilename, utils.MODE_APPEND)

		} else {
			this.pfl, _ = utils.NewMmap(pflfilename, utils.MODE_CREATE)
			this.detail, _ = utils.NewMmap(dtlfilename, utils.MODE_CREATE)
		}

	}

	if fieldtype == utils.TNumber {
		if utils.Exist(pflfilename) {
			this.pfl, _ = utils.NewMmap(pflfilename, utils.MODE_APPEND)
		} else {
			this.pfl, _ = utils.NewMmap(pflfilename, utils.MODE_CREATE)
		}
		this.detail = nil
	}

	return this
}

// addDocument function description
// params :
// return :
func (this *profile) addDocument(docid uint64, content string) error {

	//写入数据文件中
	if this.fieldType == utils.TString || this.fieldType == utils.TStore {

		offset := uint64(this.detail.GetPointer())
		this.pfl.AppendUInt64(offset)
		this.detail.AppendStringWithLen(content)
		this.pfl.Sync()
		this.detail.Sync()
		return nil

	}

	value, err := strconv.ParseUint(content, 0, 0)
	if err != nil {
		value = 0
	}
	this.pfl.AppendUInt64(value)
	this.pfl.Sync()
	return nil

}

func (this *profile) filted(docid, value, typ uint64) bool {

	offset := this.pfl.ReadUInt64(docid * 8)

	switch typ {
	case utils.EQ:
		return offset == value
	case utils.UNEQ:
		return offset != value
	case utils.OVER:
		return offset > value
	default:
		return false
	}

}

// getDetail function description
// params :
// return :
func (this *profile) getDetail(docid uint64) interface{} {

	offset := this.pfl.ReadUInt64(docid * 8)

	if this.fieldType == utils.TString || this.fieldType == utils.TStore {
		return this.detail.ReadStringWithLen(offset)
	}

	return offset

}

// sync function description
// params :
// return :
func (this *profile) sync() error {

	if this.fieldType == utils.TString || this.fieldType == utils.TStore {
		this.detail.Sync()
	}
	return this.pfl.Sync()

}
