package main

import (
	"flag"
	"runtime"
	"stuqNet"
	"utils"
)

func main() {

	var port int
	var pathname string
	flag.IntVar(&port, "port", 9999, "端口")
	flag.StringVar(&pathname, "path", "pathname", "索引路径名称")
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	logger, _ := utils.New("stuqsearcher")
	logger.Info("Loading Segmenter ...")
	utils.GSegmenter = utils.NewSegmenter("./data/dictionary.txt")
	logger.Info("Starting Search Engine ...")
	httpservice := stuqNet.NewHttpService(port, pathname, logger)
	httpservice.Start()

}
