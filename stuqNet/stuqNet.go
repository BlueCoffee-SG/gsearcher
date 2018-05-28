package stuqNet

import (
	"encoding/json"
	"errors"
	"fmt"
	"indexer"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"searcher"
	"time"
	"utils"
)

const (
	METHOD_GET    string = "GET"
	METHOD_POST   string = "POST"
	METHOD_PUT    string = "PUT"
	METHOD_DELETE string = "DELETE"

	URL_CREATE uint64 = 101
	URL_SEARCH uint64 = 102
	URL_UPDATE uint64 = 103
)

type HttpService struct {
	Logger   *utils.Log4FE `json:"-"`
	searcher *searcher.Searcher
	indexer  *indexer.Indexer
	pathname string
	port     int
}

func NewHttpService(port int, pathname string, logger *utils.Log4FE) *HttpService {
	this := &HttpService{Logger: logger, port: port, searcher: nil, indexer: nil, pathname: pathname}
	return this
}

func (this *HttpService) Start() error {

	if this.searcher == nil {
		this.searcher = searcher.NewSearcher(this.pathname, this.Logger)
		this.Logger.Error("Server start fail: manager is nil")

	}
	this.Logger.Info("Server starting")
	addr := fmt.Sprintf(":%d", this.port)
	err := http.ListenAndServe(addr, this)
	if err != nil {
		this.Logger.Error("Server start fail: %v", err)
		return err
	}
	this.Logger.Info("Server started")
	return nil
}

func (this *HttpService) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	var startTime, endTime time.Time
	var err error
	var body []byte
	startTime = time.Now()
	//write to http头
	header := w.Header()
	header.Add("Content-Type", "application/json")
	header.Add("charset", "UTF-8")
	header.Add("Access-Control-Allow-Origin", "*")
	requestUrl := r.RequestURI
	result := make(map[string]interface{})
	result["_errorcode"] = 0
	parms, err := this.parseArgs(r)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, MakeErrorResult(-1, err.Error()))
		return
	}

	reqType, err := this.ParseURL(requestUrl)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, MakeErrorResult(-1, err.Error()))
		goto END
	}

	body, err = ioutil.ReadAll(r.Body)
	if err != nil && err != io.EOF {
		result["_errorcode"] = -1
		result["_errormessage"] = "读取请求数据出错，请重新提交"
		goto END
	}

	switch reqType {
	case URL_SEARCH:

		res, found, count, err := this.searcher.Search(parms)
		if err != nil {
			result["_errorcode"] = -1
			result["_errormessage"] = err.Error()
			goto END
		}
		if found {
			result["_count"] = count
			result["_detail"] = res
		} else {
			result["_count"] = 0
		}

	case URL_UPDATE:
		var idxinc utils.IdxInc
		if err := json.Unmarshal(body, &idxinc); err != nil {
			result["_errorcode"] = -1
			result["_errormessage"] = err.Error()
			goto END
		}
		if this.indexer == nil {

			this.indexer = indexer.NewIndexer(this.pathname, idxinc.Indexname, this.Logger)
			if this.indexer == nil {
				result["_errorcode"] = -1
				result["_errormessage"] = "create index error"
				goto END
			}
		}
		this.indexer.UpdateDocuments(idxinc.Indexname, idxinc.Contents)

	case URL_CREATE:
		var idxstruct utils.IdxStruct
		if err := json.Unmarshal(body, &idxstruct); err != nil {
			result["_errorcode"] = -1
			result["_errormessage"] = err.Error()
			goto END
		}
		if this.indexer == nil {

			this.indexer = indexer.NewIndexer(this.pathname, idxstruct.Indexname, this.Logger)
			if this.indexer == nil {
				result["_errorcode"] = -1
				result["_errormessage"] = "create index error"
				goto END
			}
		}

		fieldMap := make(map[string]uint64)
		for k, v := range idxstruct.Fields {
			switch v {
			case "string":
				fieldMap[k] = utils.TString
			case "number":
				fieldMap[k] = utils.TNumber
			case "onlyshow":
				fieldMap[k] = utils.TStore
			default:

			}
		}

		this.indexer.AddIndex(idxstruct.Indexname, fieldMap)

		if err := this.indexer.LoadData(idxstruct.Indexname, idxstruct.DataFilepath, idxstruct.FileType, idxstruct.ContentDesc); err != nil {
			result["_errorcode"] = -1
			result["_errormessage"] = err.Error()
			goto END
		}

		result["_status"] = "ok"

	}

END:
	if err != nil {
		this.Logger.Error("[ERROR] %v ", err)
	}

	endTime = time.Now()
	result["_cost"] = fmt.Sprintf("%v", endTime.Sub(startTime))

	resStr, _ := this.createJSON(result)
	io.WriteString(w, resStr)
	this.Logger.Info("[COST:%v] [URL : %v] ", fmt.Sprintf("%v", endTime.Sub(startTime)), r.RequestURI)
	return
}

func (this *HttpService) createJSON(result map[string]interface{}) (string, error) {

	r, err := json.Marshal(result)
	if err != nil {
		return "", err
	}

	return string(r), nil

}

func (this *HttpService) parseArgs(r *http.Request) (map[string]string, error) {
	err := r.ParseForm()
	if err != nil {
		return nil, err
	}

	res := make(map[string]string)
	for k, v := range r.Form {
		res[k] = v[0]
	}

	return res, nil
}

// ParseURL function description
// params :
// return :
func (this *HttpService) ParseURL(url string) (uint64, error) {

	urlPattern := "/(_search|_update|_create)\\?"
	urlRegexp, err := regexp.Compile(urlPattern)
	if err != nil {
		return 0, err
	}
	matchs := urlRegexp.FindStringSubmatch(url)
	if matchs == nil {
		return 0, errors.New("URL ERROR ")
	}

	resource := matchs[1]
	if resource == "_search" {
		return URL_SEARCH, nil
	}
	if resource == "_update" {
		return URL_UPDATE, nil
	}
	if resource == "_create" {
		return URL_CREATE, nil
	}

	return 0, errors.New("Error")

}

func MakeErrorResult(errcode int, errmsg string) string {
	data := map[string]interface{}{
		"error_code": errcode,
		"message":    errmsg,
	}
	result, err := json.Marshal(data)
	if err != nil {
		return fmt.Sprintf("{\"error_code\":%v,\"message\":\"%v\"}", errcode, errmsg)
	}
	return string(result)
}
