package stuqIndex

import (
	"encoding/json"
	"fmt"
	"strings"
	"utils"
)

type fieldMapping struct {
	fieldmap map[string]*field
}

type Index struct {
	Name       string `json:"indexname"`
	MaxDocID   uint64 `json:"maxdocid"`
	MaxSegment uint64 `json:"maxsegment"`
	pathname   string
	FieldInfos map[string]uint64 `json:"Fields"`
	allfields  []fieldMapping
	fields     map[string]*field
	Logger     *utils.Log4FE `json"-"`
}

// NewIndex function description
// params :
// return :
func NewIndex(pathname, indexname string, logger *utils.Log4FE) *Index {

	indexMetaFile := fmt.Sprintf("%v/%v.json", pathname, indexname)
	this := &Index{Name: indexname, pathname: pathname,
		Logger:     logger,
		FieldInfos: nil,
		fields:     make(map[string]*field),
		allfields:  make([]fieldMapping, 0)}
	if utils.Exist(indexMetaFile) {
		if bvar, err := utils.ReadFromJson(indexMetaFile); err == nil {
			if jerr := json.Unmarshal(bvar, this); jerr != nil {
				return nil
			}
		} else {
			return nil
		}
		this.Logger.Info("[INFO] this.MaxSegment %v", this.MaxSegment)
		for i := uint64(0); i < this.MaxSegment; i++ {
			fieldpath := fmt.Sprintf("%v/%v_%v_", pathname, indexname, i)
			fieldmap := fieldMapping{fieldmap: make(map[string]*field)}
			for fname, ftype := range this.FieldInfos {
				field := newField(fieldpath, fname, ftype, 0, logger)
				fieldmap.fieldmap[fname] = field

			}
			this.allfields = append(this.allfields, fieldmap)

		}

		//add inc update
		fieldpath := fmt.Sprintf("%v/%v_%v_", pathname, indexname, this.MaxSegment)
		for fname, ftype := range this.FieldInfos {
			field := newField(fieldpath, fname, ftype, this.MaxDocID, logger)
			this.fields[fname] = field

		}

	}

	return this

}

// MappingFields function description
// params :
// return :
func (this *Index) MappingFields(fieldinfos map[string]uint64) error {

	if this.FieldInfos == nil {

		this.FieldInfos = fieldinfos
		fieldpath := fmt.Sprintf("%v/%v_%v_", this.pathname, this.Name, this.MaxSegment)
		for fname, ftype := range this.FieldInfos {
			field := newField(fieldpath, fname, ftype, 0, this.Logger)
			this.fields[fname] = field
		}
		this.Logger.Info("[INFO] fields %v", this.FieldInfos)
		return nil
	}

	return fmt.Errorf("fields is exist")
}

// AddDocument function description
// params :
// return :
func (this *Index) AddDocument(document map[string]string) error {

	docid := this.MaxDocID
	this.MaxDocID++
	if this.FieldInfos == nil {
		return fmt.Errorf("no fields")
	}

	for k, field := range this.fields {
		if _, ok := document[k]; !ok {
			document[k] = ""
		}

		if err := field.addDocument(docid, document[k]); err != nil {
			this.Logger.Error("[ERROR] add document error %v", err)
			this.MaxDocID--
			return err
		}

	}

	return nil
}

// Serialization function description
// params :
// return :
func (this *Index) Serialization() error {

	if this.FieldInfos == nil {
		return fmt.Errorf("no fields")
	}

	for _, field := range this.fields {
		if err := field.serialization(); err != nil {
			this.Logger.Error("[ERROR] add document error %v", err)
			return err
		}

	}

	fieldmap := fieldMapping{fieldmap: this.fields}
	this.allfields = append(this.allfields, fieldmap)
	this.MaxSegment++
	fieldpath := fmt.Sprintf("%v/%v_%v_", this.pathname, this.Name, this.MaxSegment)
	for fname, ftype := range this.FieldInfos {
		field := newField(fieldpath, fname, ftype, this.MaxDocID, this.Logger)
		this.fields[fname] = field

	}

	indexMetaFile := fmt.Sprintf("%v/%v.json", this.pathname, this.Name)
	if err := utils.WriteToJson(this, indexMetaFile); err != nil {
		this.Logger.Error("[ERROR] save field json error  %v", err)
		return err
	}

	return nil
}

// Search function description
// params :
// return :
func (this *Index) Search(query string, filteds []utils.Filted) ([]utils.DocInfo, bool) {
	terms := utils.GSegmenter.Segment(query, false)
	resdocids := make([]utils.DocInfo, 0)
	flag := false
	//term搜索
	for _, term := range terms {
		subdocids := make([]utils.DocInfo, 0)
		for k, v := range this.FieldInfos {
			if v == utils.TString {
				for i := uint64(0); i < this.MaxSegment; i++ {
					fielddocids, ok := this.SearchTerm(term, k, i)
					if ok {
						subdocids, _ = utils.Merge(subdocids, fielddocids)
					}

				}

			}
		}
		if !flag {
			resdocids = subdocids
			flag = true
		} else {
			resdocids, _ = utils.Interaction(resdocids, subdocids)
		}
	}

	//结果集过滤
	docids := make([]utils.DocInfo, 0)
	for _, docid := range resdocids {

		flag := false
		for _, f := range filteds {
			if !this.fields[f.Fieldname].filted(docid.DocId, f.Value, f.Filtedtyp) {
				flag = true
				break
			}
		}
		if !flag {
			docids = append(docids, docid)
		}

	}

	if len(docids) == 0 {
		return nil, false
	}
	return docids, true
}

// SearchTerm function description
// params :
// return :
func (this *Index) SearchTerm(term, field string, segnum uint64) ([]utils.DocInfo, bool) {

	nospaceterm := strings.TrimSpace(term)
	if len(nospaceterm) > 0 {
		this.Logger.Info("[INFO] segment %v", segnum)
		return this.allfields[segnum].fieldmap[field].searchTerm(nospaceterm)
		//return this.fields[field].searchTerm(nospaceterm)
	}
	return nil, false
}

// GetDocument function description
// params :
// return :
func (this *Index) GetDocument(docid uint64) (map[string]string, bool) {

	if docid >= this.MaxDocID {
		return nil, false
	}
	document := make(map[string]string)

	for i := uint64(0); i < this.MaxSegment; i++ {
		f := this.allfields[i]

		for fname, field := range f.fieldmap {
			v, _, ok := field.getDetail(docid)
			if ok {
				document[fname] = v

			} else {
				break
			}
		}

	}

	return document, true
}
