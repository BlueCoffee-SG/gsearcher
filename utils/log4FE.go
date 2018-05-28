package utils

import (
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
)

const (
	LogTrace uint64 = 9
	LogDebug uint64 = 8
	LogInfo  uint64 = 7
	LogWarn  uint64 = 6
	LogFatal uint64 = 5
	LogError uint64 = 4
)

type Log4FE struct {
	file_handle   *os.File
	logger_handle *log.Logger
	logLevel      uint64
}

func New(filepathname string) (log4FE *Log4FE, err error) {

	filename := fmt.Sprintf("%s.log", filepathname)

	// 初始化Log4FE
	out, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		filename = "./default.log"
		out, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
	}
	logtmp := log.New(out, "", log.LstdFlags)

	log4FE = &Log4FE{
		logLevel:      LogTrace,
		file_handle:   out,
		logger_handle: logtmp,
	}

	return log4FE, nil
}

func (this *Log4FE) Close() (err error) {
	return this.file_handle.Close()
}

func (this *Log4FE) SetLevel(level uint64) {
	this.logLevel = level
}

func (this *Log4FE) log(level string, format string, args ...interface{}) (err error) {
	msg := fmt.Sprintf(format, args...)
	fmt.Println(msg)

	_, filepath, filenum, _ := runtime.Caller(2)
	filename := path.Base(filepath)
	logmsg := fmt.Sprintf("[%s] %s : %d - %s", level, filename, filenum, msg)
	this.logger_handle.Println("", logmsg)

	return nil
}

func (this *Log4FE) Fatal(format string, args ...interface{}) (err error) {
	if this.logLevel >= LogFatal {
		return this.log("FATAL", format, args...)
	}
	return nil
}

func (this *Log4FE) Error(format string, args ...interface{}) (err error) {
	if this.logLevel >= LogError {
		return this.log("ERROR", format, args...)
	}
	return nil
}

func (this *Log4FE) Warn(format string, args ...interface{}) (err error) {
	if this.logLevel >= LogWarn {
		return this.log("WARN", format, args...)
	}
	return nil
}

func (this *Log4FE) Info(format string, args ...interface{}) (err error) {

	if this.logLevel >= LogInfo {
		return this.log("INFO", format, args...)
	}
	return nil

}

func (this *Log4FE) Debug(format string, args ...interface{}) (err error) {
	if this.logLevel >= LogDebug {
		return this.log("DEBUG", format, args...)
	}
	return nil
}

func (this *Log4FE) Trace(format string, args ...interface{}) (err error) {
	if this.logLevel >= LogTrace {
		return this.log("TRACE", format, args...)
	}
	return nil
}
