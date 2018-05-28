/*****************************************************************************
 *  file name : KV.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : KV封装
 *
******************************************************************************/

package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

const (
	TABLE_LEN  = 0x500
	HASH_O     = 0
	HASH_A     = 1
	HASH_B     = 2
	MaxASCII   = '\u007F'
	DefaultPos = -1
)

var (
	cryptTable   [TABLE_LEN]uint64
	BUCKETS_LIST = []int{17, 37, 79, 163, 331, 673, 1361, 2729, 5471, 10949, 21911, 43853, 87719, 175447, 350899, 701819, 1403641, 2807303, 5614657, 11229331, 22458671, 44917381, 89834777, 179669557, 359339171, 718678369, 1437356741, 2147483647}
)

type hashTable struct {
	hasData  bool
	entities []*Entity
}

type Entity struct {
	Hash0 uint64 //位置hash值
	HashA uint64 //一次hash值
	HashB uint64 //二次hash值
	Value uint64 //索引偏移
}

type HashMap struct {
	buckets   uint64 //桶长度
	tableLen  uint64 //实际数据长度
	available bool
	hashtable []hashTable
	entities  []Entity
}

func NewHashTable() *HashMap {
	this := &HashMap{tableLen: 0, entities: make([]Entity, 0), available: false}
	initCryptTable()
	return this
}

func (this *HashMap) Load(filename string) error {

	dicFile, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer dicFile.Close()

	fi, _ := dicFile.Stat()
	length := int(fi.Size())
	entityCnt := length / (8 * 4)
	entities := make([]Entity, entityCnt)
	this.buckets = calcBuckets(entityCnt)

	err = binary.Read(dicFile, binary.LittleEndian, entities)
	if err != nil {
		return err
	}

	this.entities = entities

	this.hashtable = make([]hashTable, this.buckets)

	for i, _ := range this.entities {
		pos := this.entities[i].Hash0 % uint64(this.buckets)
		//没有数据
		if this.hashtable[pos].hasData == false {
			this.hashtable[pos].entities = make([]*Entity, 0)
			this.hashtable[pos].entities = append(this.hashtable[pos].entities, &(this.entities[i]))
			this.hashtable[pos].hasData = true
			continue
		}

		//已经有数据了
		for _, node := range this.hashtable[pos].entities {
			//更新数据
			if node.HashA == this.entities[i].HashA && node.HashB == this.entities[i].HashB {
				node.Value = this.entities[i].Value
				continue
			}

		}
		//没有更新项，添加新数据
		this.hashtable[pos].entities = append(this.hashtable[pos].entities, &(this.entities[i]))

	}
	this.available = true
	return nil

}

func (this *HashMap) Save(filename string) error {

	// 写索引文件
	dicFile, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer dicFile.Close()
	buffer := new(bytes.Buffer)

	err = binary.Write(buffer, binary.LittleEndian, this.entities)
	if err != nil {
		return err
	}

	_, err = dicFile.Write(buffer.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (this *HashMap) Push(key string, value uint64) error {
	hash0 := hashKey(key, HASH_O)
	hasha := hashKey(key, HASH_A)
	hashb := hashKey(key, HASH_B)
	node := Entity{Hash0: hash0, HashA: hasha, HashB: hashb, Value: value}
	this.entities = append(this.entities, node)
	return nil

}

func (this *HashMap) Set(key string, value uint64) error {

	if !this.available {
		fmt.Errorf("not init ,can not available")
	}

	hash0 := hashKey(key, HASH_O)
	hasha := hashKey(key, HASH_A)
	hashb := hashKey(key, HASH_B)
	pos := hash0 % uint64(this.buckets)

	//没有数据
	if this.hashtable[pos].hasData == false {
		node := Entity{Hash0: hash0, HashA: hasha, HashB: hashb, Value: value}
		this.entities = append(this.entities, node)
		this.hashtable[pos].entities = make([]*Entity, 0)
		this.hashtable[pos].entities = append(this.hashtable[pos].entities, &node)
		this.hashtable[pos].hasData = true
		return nil
	}

	//已经有数据了

	for _, node := range this.hashtable[pos].entities {
		//更新数据
		if node.HashA == hasha && node.HashB == hashb {
			node.Value = value
			return nil
		}

	}
	//没有更新项，添加新数据
	node := Entity{Hash0: hash0, HashA: hasha, HashB: hashb, Value: value}
	this.entities = append(this.entities, node)
	this.hashtable[pos].entities = append(this.hashtable[pos].entities, &node)
	return nil

}

func (this *HashMap) Get(key string) (uint64, bool) {

	hash0 := hashKey(key, HASH_O)
	hasha := hashKey(key, HASH_A)
	hashb := hashKey(key, HASH_B)
	pos := hash0 % uint64(this.buckets)

	if this.hashtable[pos].hasData == false {
		return 0, false
	}

	if len(this.hashtable[pos].entities) == 1 {
		return this.hashtable[pos].entities[0].Value, true
	}

	for _, node := range this.hashtable[pos].entities {
		//更新数据
		if node.HashA == hasha && node.HashB == hashb {
			return node.Value, true
		}

	}
	return 0, false

}

func calcBuckets(maxsize int) uint64 {
	var buckets int
	if maxsize != 0 {
		buckets = custMinBuckets(maxsize)
		for _, size := range BUCKETS_LIST {
			if buckets < size {
				buckets = size
				break
			}
		}
	}
	return uint64(buckets)

}

// 查找合适的桶的个数
func custMinBuckets(size int) int {
	var buckets int
	v := size
	// round 到最近的2的倍数
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	// size * 4 /3
	b := size * 4 / 3
	if b > v {
		buckets = b
	} else {
		buckets = v
	}
	return buckets
}

// 初始化hash计算需要的基础map table
func initCryptTable() {
	var seed, index1, index2 uint64 = 0x00100001, 0, 0
	i := 0
	for index1 = 0; index1 < 0x100; index1 += 1 {
		for index2, i = index1, 0; i < 5; index2 += 0x100 {
			seed = (seed*125 + 3) % 0x2aaaab
			temp1 := (seed & 0xffff) << 0x10
			seed = (seed*125 + 3) % 0x2aaaab
			temp2 := seed & 0xffff
			cryptTable[index2] = temp1 | temp2
			i += 1
		}
	}
}

// hash, 以及相关校验hash值
func hashKey(lpszString string, dwHashType int) uint64 {
	i, ch := 0, 0
	var seed1, seed2 uint64 = 0x7FED7FED, 0xEEEEEEEE
	var key uint8
	strLen := len(lpszString)
	for i < strLen {
		key = lpszString[i]
		ch = int(toUpper(rune(key)))
		i += 1
		seed1 = cryptTable[(dwHashType<<8)+ch] ^ (seed1 + seed2)
		seed2 = uint64(ch) + seed1 + seed2 + (seed2 << 5) + 3
	}
	return uint64(seed1)
}

func toUpper(r rune) rune {
	if r <= MaxASCII {
		if 'a' <= r && r <= 'z' {
			r -= 'a' - 'A'
		}
		return r
	}
	return r
}
