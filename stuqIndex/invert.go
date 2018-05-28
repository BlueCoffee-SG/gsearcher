package stuqIndex

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"utils"
)

type invert struct {
	curDocID   uint64
	segmentNum uint64
	fieldName  string
	pathname   string
	fieldType  uint64
	tmpIvt     []utils.TmpIvt
	idx        *utils.Mmap
	keys       utils.KV
	Logger     *utils.Log4FE
	wg         *sync.WaitGroup
}

type tmpMergeNode struct {
	Term   string
	DocIds []utils.DocInfo
}

// newInvert function description
// params :
// return :
func newInvert(pathname, fieldname string, fieldtype uint64, logger *utils.Log4FE) *invert {
	this := &invert{wg: new(sync.WaitGroup), tmpIvt: make([]utils.TmpIvt, 0),
		pathname: pathname, curDocID: 0, fieldName: fieldname, fieldType: fieldtype,
		Logger: logger, segmentNum: 0, keys: nil, idx: nil}

	idxfilename := fmt.Sprintf("%v%v.idx", pathname, fieldname)
	dicfilename := fmt.Sprintf("%v%v.dic", pathname, fieldname)

	if utils.Exist(idxfilename) && utils.Exist(dicfilename) {
		this.keys = utils.NewHashTable()
		this.keys.Load(dicfilename)
		this.Logger.Info("[INFO] Read %v success", dicfilename)
		this.idx, _ = utils.NewMmap(idxfilename, utils.MODE_APPEND)
		this.Logger.Info("[INFO] Read %v success", idxfilename)
	}

	return this
}

// searchTerm function description
// params :
// return :
func (this *invert) searchTerm(term string) ([]utils.DocInfo, bool) {

	nospaceterm := strings.TrimSpace(term)
	if len(nospaceterm) == 0 {
		return nil, false
	}

	if offset, ok := this.keys.Get(nospaceterm); ok {

		lens := this.idx.ReadInt64(int64(offset))
		this.Logger.Info("[INFO] offset %v  lens: %v trem:%v", offset, lens, nospaceterm)
		res := this.idx.ReadDocIdsArry(uint64(offset+8), uint64(lens))
		return res, true

	}
	this.Logger.Info("[INFO] term [%v] not found ", term)
	return nil, false

}

// addDocument function description
// params : docid编号，文档内容
// return :
func (this *invert) addDocument(docid uint64, content string) error {

	terms := utils.GSegmenter.Segment(content, true)

	for _, term := range terms {
		nospaceterm := strings.TrimSpace(term)
		if len(nospaceterm) > 0 {
			this.tmpIvt = append(this.tmpIvt, utils.TmpIvt{DocID: docid, Term: nospaceterm})
		}

	}

	return nil

}

// saveTempInvert function description
// params :
// return :
func (this *invert) saveTempInvert() error {
	filename := fmt.Sprintf("%v%v_%v.ivt", this.pathname, this.fieldName, this.segmentNum)
	this.segmentNum++
	sort.Sort(utils.TmpIvtTermSort(this.tmpIvt))

	fout, err := os.Create(filename)
	defer fout.Close()
	if err != nil {
		this.Logger.Error("[ERROR] creat [%v] error : %v", filename, err)
		return err
	}

	for _, tmpnode := range this.tmpIvt {
		info_json, err := json.Marshal(tmpnode)
		if err != nil {
			this.Logger.Error("[ERROR] Marshal err %v\n", tmpnode)
			return err
		}
		fout.WriteString(string(info_json) + "\n")
	}

	this.tmpIvt = make([]utils.TmpIvt, 0)

	return nil

}

// mergeTmpInvert function description
// params :
// return :
func (this *invert) mergeTmpInvert() error {

	mergeChanList := make([]chan tmpMergeNode, 0)
	for i := uint64(0); i < this.segmentNum; i++ {
		filename := fmt.Sprintf("%v%v_%v.ivt", this.pathname, this.fieldName, i)
		mergeChanList = append(mergeChanList, make(chan tmpMergeNode))
		this.wg.Add(1)
		go this.mapRoutine(filename, &mergeChanList[i])
	}

	this.wg.Add(1)
	filename := fmt.Sprintf("%v%v_%v.ivt", this.pathname, this.fieldName, this.segmentNum)
	go this.reduceRoutine(filename, &mergeChanList)

	this.Logger.Info("[INFO] Waiting [%v] Routines", this.segmentNum)
	this.wg.Wait()
	this.Logger.Info("[INFO] finish [%v] Routines", this.segmentNum)

	return nil
}

// reduceRoutine function description
// params :
// return :
func (this *invert) reduceRoutine(filename string, mergeChanList *[]chan tmpMergeNode) error {

	defer this.wg.Done()

	lens := len(*mergeChanList)
	maxTerm := ""
	closeCount := make([]bool, lens)
	nodes := make([]tmpMergeNode, 0)
	this.keys = utils.NewHashTable()

	idxFileName := fmt.Sprintf("%v%v.idx", this.pathname, this.fieldName)
	idxFd, err := os.OpenFile(idxFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer idxFd.Close()

	this.Logger.Info("[INFO] reduce [%v] indexs start ... ", lens)

	dicFileName := fmt.Sprintf("%v%v.dic", this.pathname, this.fieldName)
	dicFd, err1 := os.OpenFile(dicFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err1 != nil {
		return err1
	}
	defer dicFd.Close()

	totalOffset := uint64(0)

	for i, v := range *mergeChanList {
		vv, ok := <-v
		if ok {
			if maxTerm < vv.Term {
				maxTerm = vv.Term
			}
		} else {
			closeCount[i] = true
		}
		nodes = append(nodes, vv)
	}
	nextmax := ""

	//merge
	for {
		var resnode tmpMergeNode
		resnode.DocIds = make([]utils.DocInfo, 0)
		resnode.Term = maxTerm
		closeNum := 0
		for i, _ := range nodes {
			if maxTerm == nodes[i].Term {
				this.Logger.Info("[INFO] resnode %v", resnode)
				resnode.DocIds = append(resnode.DocIds, nodes[i].DocIds...)
				vv, ok := <-(*mergeChanList)[i]
				if ok {
					nodes[i].Term = vv.Term
					nodes[i].DocIds = vv.DocIds
				} else {
					closeCount[i] = true
				}
			}
			if !closeCount[i] {
				if nextmax <= nodes[i].Term {
					nextmax = nodes[i].Term
				}
				closeNum++
			}

		}
		sort.Sort(utils.DocInfoSort(resnode.DocIds))
		this.Logger.Info("[INFO] docids %v", resnode)

		lens := uint64(len(resnode.DocIds))
		lenBufer := make([]byte, 8)
		binary.LittleEndian.PutUint64(lenBufer, lens)

		idxFd.Write(lenBufer)
		buffer := new(bytes.Buffer)
		err = binary.Write(buffer, binary.LittleEndian, resnode.DocIds)
		if err != nil {
			this.Logger.Error("[ERROR] invert --> Serialization :: Error %v", err)
			return err
		}
		idxFd.Write(buffer.Bytes())
		this.keys.Push(resnode.Term, uint64(totalOffset))

		totalOffset = totalOffset + uint64(8) + lens*utils.DOCNODE_SIZE

		if closeNum == 0 {
			break
		}
		maxTerm = nextmax
		nextmax = ""
	}
	this.keys.Save(dicFileName)
	this.Logger.Info("[INFO] reduce [%v] indexs finish ... ", lens)

	return nil
}

// mapRoutine function description
// params :
// return :
func (this *invert) mapRoutine(filename string, tmpmergeChan *chan tmpMergeNode) error {

	defer this.wg.Done()

	datafile, err := os.Open(filename)
	if err != nil {
		this.Logger.Error("[ERROR] Open File[%v] Error %v\n", filename, err)
		return err
	}
	defer datafile.Close()
	this.Logger.Info("[INFO] map file[%v] index start ...", filename)
	scanner := bufio.NewScanner(datafile)
	var node tmpMergeNode
	if scanner.Scan() {
		var v utils.TmpIvt
		content := scanner.Text()
		json.Unmarshal([]byte(content), &v)
		node.Term = v.Term
		node.DocIds = make([]utils.DocInfo, 0)
		node.DocIds = append(node.DocIds, utils.DocInfo{DocId: v.DocID})

	}

	for scanner.Scan() {
		var v utils.TmpIvt
		content := scanner.Text()
		json.Unmarshal([]byte(content), &v)
		if v.Term != node.Term {
			*tmpmergeChan <- node
			node.Term = v.Term
			node.DocIds = make([]utils.DocInfo, 0)
			node.DocIds = append(node.DocIds, utils.DocInfo{DocId: v.DocID})
		} else {
			node.DocIds = append(node.DocIds, utils.DocInfo{DocId: v.DocID})
		}

	}
	*tmpmergeChan <- node
	close(*tmpmergeChan)
	os.Remove(filename)
	this.Logger.Info("[INFO] file[%v] process finish ...", filename)
	return nil

}
