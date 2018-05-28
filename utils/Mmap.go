package utils

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

type Mmap struct {
	MmapBytes   []byte
	FileName    string
	FileLen     int64
	FilePointer int64
	MapType     int64
	FileFd      *os.File
}

const APPEND_DATA int64 = 1024 * 1024
const (
	MODE_APPEND = iota
	MODE_CREATE
)

func NewMmap(file_name string, mode int) (*Mmap, error) {

	this := &Mmap{MmapBytes: make([]byte, 0), FileName: file_name, FileLen: 0, MapType: 0, FilePointer: 0, FileFd: nil}

	file_mode := os.O_RDWR
	file_create_mode := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	if mode == MODE_CREATE {
		file_mode = os.O_RDWR | os.O_CREATE | os.O_TRUNC
	}

	f, err := os.OpenFile(file_name, file_mode, 0664)

	if err != nil {
		f, err = os.OpenFile(file_name, file_create_mode, 0664)
		if err != nil {
			return nil, err
		}
	}

	fi, err := f.Stat()
	if err != nil {
		fmt.Printf("ERR:%v", err)
	}
	this.FileLen = fi.Size()
	if mode == MODE_CREATE || this.FileLen == 0 {
		syscall.Ftruncate(int(f.Fd()), fi.Size()+APPEND_DATA)
		this.FileLen = APPEND_DATA
	}
	this.MmapBytes, err = syscall.Mmap(int(f.Fd()), 0, int(this.FileLen), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	if err != nil {
		fmt.Printf("MAPPING ERROR  %v \n", err)
		return nil, err
	}

	this.FileFd = f
	return this, nil
}

// SetFileEnd function description
// params :
// return :
func (this *Mmap) SetFileEnd(file_len int64) {
	this.FilePointer = file_len
}

func (this *Mmap) AppendStringWithLen(value string) error {
	this.AppendInt64(int64(len(value)))
	this.appendString(value)
	return nil //this.Sync()

}

func (this *Mmap) AppendBytes(value []byte) uint64 {

	offset := uint64(this.FilePointer)
	lens := int64(len(value))
	if err := this.checkFilePointer(lens); err != nil {
		return 0
	}

	dst := this.MmapBytes[this.FilePointer : this.FilePointer+lens]
	copy(dst, value)
	this.FilePointer += lens
	return offset

}

func (this *Mmap) ReadStringWithLen(start uint64) string {
	lens := this.ReadInt64(int64(start))
	return this.readString(int64(start+8), lens)

}

func (this *Mmap) ReadDocIdsArry(start, len uint64) []DocInfo {

	arry := *(*[]DocInfo)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&this.MmapBytes[start])),
		Len:  int(len),
		Cap:  int(len),
	}))
	return arry
}

func (this *Mmap) Sync() error {
	//var err error
	dh := this.header()
	_, _, err1 := syscall.Syscall(syscall.SYS_MSYNC, dh.Data, uintptr(dh.Len), syscall.MS_SYNC)
	if err1 != 0 {
		fmt.Printf("Sync Error ")
		return errors.New("Sync Error")
	}

	return nil
}

func (this *Mmap) Unmap() error {

	syscall.Munmap(this.MmapBytes)
	this.FileFd.Close()
	return nil
}

func (this *Mmap) readString(start, lens int64) string {

	return string(this.MmapBytes[start : start+lens])
}

func (this *Mmap) appendString(value string) error {

	lens := int64(len(value))
	if err := this.checkFilePointer(lens); err != nil {
		return err
	}

	dst := this.MmapBytes[this.FilePointer : this.FilePointer+lens]
	copy(dst, []byte(value))
	this.FilePointer += lens
	return nil //this.Sync()

}

// checkFilePointer function description : 检查插入某个长度以后是否会超出文件长度，如果会超出，重新设置文件
// params :
// return :
func (this *Mmap) checkFilePointer(check_value int64) error {

	if this.FilePointer+check_value >= this.FileLen {
		err := syscall.Ftruncate(int(this.FileFd.Fd()), this.FileLen+APPEND_DATA)
		if err != nil {
			fmt.Printf("ftruncate error : %v\n", err)
			return err
		}
		this.FileLen += APPEND_DATA
		syscall.Munmap(this.MmapBytes)
		this.MmapBytes, err = syscall.Mmap(int(this.FileFd.Fd()), 0, int(this.FileLen), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

		if err != nil {
			fmt.Printf("MAPPING ERROR  %v \n", err)
			return err
		}

	}

	return nil
}

func (this *Mmap) GetPointer() int64 {
	return this.FilePointer
}

func (this *Mmap) header() *reflect.SliceHeader {
	return (*reflect.SliceHeader)(unsafe.Pointer(&this.MmapBytes))
}

func (this *Mmap) ReadInt64(start int64) int64 {

	return int64(binary.LittleEndian.Uint64(this.MmapBytes[start : start+8]))
}

func (this *Mmap) ReadUInt64(start uint64) uint64 {

	return binary.LittleEndian.Uint64(this.MmapBytes[start : start+8])
}

func (this *Mmap) WriteUInt64(start int64, value uint64) error {

	binary.LittleEndian.PutUint64(this.MmapBytes[start:start+8], uint64(value))

	return nil //this.Sync()
}

func (this *Mmap) WriteInt64(start, value int64) error {
	binary.LittleEndian.PutUint64(this.MmapBytes[start:start+8], uint64(value))
	return nil //this.Sync()
}

func (this *Mmap) AppendInt64(value int64) error {

	if err := this.checkFilePointer(8); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(this.MmapBytes[this.FilePointer:this.FilePointer+8], uint64(value))
	this.FilePointer += 8
	return nil //this.Sync()
}

func (this *Mmap) AppendUInt64(value uint64) error {

	if err := this.checkFilePointer(8); err != nil {
		return err
	}

	binary.LittleEndian.PutUint64(this.MmapBytes[this.FilePointer:this.FilePointer+8], value)
	this.FilePointer += 8
	return nil //this.Sync()
}
