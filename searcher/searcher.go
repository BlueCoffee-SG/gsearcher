package searcher

import (
	"fmt"
	"strconv"
	"stuqIndex"
	"utils"
)

type Searcher struct {
	index    map[string]*stuqIndex.Index
	pathname string
	Logger   *utils.Log4FE
}

func NewSearcher(pathname string, logger *utils.Log4FE) *Searcher {

	this := &Searcher{index: make(map[string]*stuqIndex.Index), Logger: logger, pathname: pathname}

	return this
}

//	LoadIndex function description
//	params :
//	return :
func (this *Searcher) LoadIndex(indexname string) error {

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

	this.index[indexname] = idx
	return nil

}

/**
 *	Search function description
 *	params : query
 *	return :
 **/
func (this *Searcher) Search(parms map[string]string) ([]map[string]string, bool, int, error) {

	indexname, hasindex := parms["_index"]
	ps, hasps := parms["_pagesize"]
	pg, haspg := parms["_page"]
	query, hasq := parms["q"]

	if !hasindex || !haspg || !hasps || !hasq {
		return nil, false, 0, fmt.Errorf("parms error")
	}

	if _, ok := this.index[indexname]; !ok {
		if err := this.LoadIndex(indexname); err != nil {
			return nil, false, 0, fmt.Errorf("load index error : %v ", err)
		}

	}
	filteds := make([]utils.Filted, 0)
	for k, v := range parms {

		if k == "_index" || k == "_page" || k == "_pagesize" || k == "q" {
			continue
		}
		var filted utils.Filted
		switch k[0] {
		case '-':
			filted.Filtedtyp = utils.EQ
		case '_':
			filted.Filtedtyp = utils.UNEQ
		case '>':
			filted.Filtedtyp = utils.OVER
		default:

		}
		value, err := strconv.ParseUint(v, 0, 0)
		if err != nil {
			continue
		}
		filted.Fieldname = k[1:]
		filted.Value = value
		filteds = append(filteds, filted)
	}

	docids, found := this.index[indexname].Search(query, filteds)

	pagesize, _ := strconv.Atoi(ps)
	pagenum, _ := strconv.Atoi(pg)
	if found {
		lens := len(docids)
		start := pagesize * (pagenum - 1)
		end := pagesize * pagenum

		if start >= lens {
			return nil, false, 0, fmt.Errorf("page overflow")
		}
		if end >= lens {
			end = lens
		}

		res := make([]map[string]string, 0)
		for _, docid := range docids[start:end] {
			info, ok := this.index[indexname].GetDocument(docid.DocId)
			if ok {
				res = append(res, info)
			}

		}
		if len(res) > 0 {
			return res, true, len(docids), nil
		}

	}
	return nil, false, 0, nil

}
