package stuqIndex

import (
	"encoding/json"
	"fmt"
	"utils"
)

type field struct {
	Name     string `json:"name"`
	Type     uint64 `json:"type"`
	MaxDocID uint64 `json:"maxdocid"`
	pathname string

	pfl *profile
	ivt *invert

	Logger *utils.Log4FE `jons:"-"`
}

// newField function description
// params :
// return :
func newField(pathname, fieldname string, ftype uint64, max uint64, logger *utils.Log4FE) *field {

	this := &field{pathname: pathname,
		Name:     fieldname,
		Type:     ftype,
		Logger:   logger,
		MaxDocID: max,
		pfl:      nil,
		ivt:      nil}
	fieldMetaFilename := fmt.Sprintf("%v%v.json", pathname, fieldname)
	if utils.Exist(fieldMetaFilename) {
		if bvar, err := utils.ReadFromJson(fieldMetaFilename); err == nil {
			if jerr := json.Unmarshal(bvar, this); jerr != nil {
				return nil
			}
		} else {
			return nil
		}
	}
	this.pfl = newProfile(pathname, fieldname, ftype, logger)
	if ftype == utils.TString {
		this.ivt = newInvert(pathname, fieldname, ftype, logger)
	}

	return this
}

// addDocument function description
// params :
// return :
func (this *field) addDocument(docid uint64, content string) error {

	if docid != this.MaxDocID {
		return fmt.Errorf("docid error , max docid is [%v]", this.MaxDocID)
	}

	if err := this.pfl.addDocument(docid, content); err != nil {
		this.Logger.Error("[ERROR] profile add document error %v", err)
		return err
	}
	if this.ivt != nil {
		if err := this.ivt.addDocument(docid, content); err != nil {
			this.Logger.Error("[ERROR] invert add document error %v", err)
			return err
		}
	}

	this.MaxDocID++

	if this.MaxDocID%utils.IVT_SYNC_NUMBER == 0 {
		if this.ivt != nil {
			this.ivt.saveTempInvert()
		}
	}

	return nil

}

// serialization function description
// params :
// return :
func (this *field) serialization() error {

	if this.ivt != nil {
		if err := this.ivt.saveTempInvert(); err != nil {
			this.Logger.Error("[ERROR] save invert error  %v", err)
			return err
		}
		if err := this.ivt.mergeTmpInvert(); err != nil {
			this.Logger.Error("[ERROR] merge invert error  %v", err)
			return err
		}
	}
	if err := this.pfl.sync(); err != nil {
		this.Logger.Error("[ERROR] sync profile error  %v", err)
		return err
	}

	fieldMetaFilename := fmt.Sprintf("%v%v.json", this.pathname, this.Name)
	this.Logger.Info("[INFO] fieldMetaFilename %v", fieldMetaFilename)
	if err := utils.WriteToJson(this, fieldMetaFilename); err != nil {
		this.Logger.Error("[ERROR] save field json error  %v", err)
		return err
	}

	return nil

}

// searchTerm function description
// params :
// return :
func (this *field) searchTerm(term string) ([]utils.DocInfo, bool) {

	if this.ivt != nil {
		return this.ivt.searchTerm(term)
	}
	return nil, false
}

func (this *field) filted(docid, value, typ uint64) bool {

	if this.pfl != nil && this.Type == utils.TNumber {
		return this.pfl.filted(docid, value, typ)
	}
	return false

}

// getDetail function description
// params :
// return :
func (this *field) getDetail(docid uint64) (string, uint64, bool) {

	if this.pfl != nil && docid < this.MaxDocID {
		res := this.pfl.getDetail(docid)
		if this.Type == utils.TString || this.Type == utils.TStore {
			return fmt.Sprintf("%s", res), 0, true
		}
		resnum, _ := res.(uint64)
		return fmt.Sprintf("%v", res), resnum, true
	}
	return "", 0, false
}
