package utils

import (
	"fmt"
)

type Segmenter struct {
	dictionary  string
	fssegmenter *FSSegmenter
}

// GSegmenter 分词器全局变量
var GSegmenter *Segmenter

func NewSegmenter(dic_name string) *Segmenter {
	this := &Segmenter{dictionary: dic_name}
	this.fssegmenter = NewFSSegmenter(dic_name)
	if this == nil {
		fmt.Errorf("ERROR segment is nil")
		return nil
	}
	return this

}

func (this *Segmenter) Segment(content string, search_mode bool) []string {

	terms, _ := this.fssegmenter.Segment(content, search_mode)

	termmap := make(map[string]bool)
	res := make([]string, 0)
	for _, term := range terms {
		if _, ok := termmap[term]; !ok {
			termmap[term] = true
			res = append(res, term)
		}
	}

	return res

}
