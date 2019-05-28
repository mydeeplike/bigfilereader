package bigfilereader

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type BigFileReader struct {
	ReadMutex sync.Mutex
	pos int
	fp *os.File
	rd *bufio.Reader
	LineCallback func(s []byte) []byte
	MergeCallback func(mergeStr []byte)
	WriteMutex sync.Mutex

}

func New(fileName string) (*BigFileReader, error) {
	var err error
	obj := &BigFileReader{}
	obj.fp, err = os.OpenFile(fileName, os.O_RDONLY, 0644)
	obj.rd = bufio.NewReader(obj.fp)
	obj.ReadMutex = sync.Mutex{}
	obj.WriteMutex = sync.Mutex{}
	return obj, err
}

func (this *BigFileReader)Close() {
	this.fp.Close()
}

// 每次读取一整块数据
func (this *BigFileReader)ReadBlock(readMaxSize int) ([]byte, error) {

	// 最小 1M
	// if readMaxSize < 1000000000 {
	// 	readMaxSize = 1000000000
	// }

	// 单线程读取
	this.ReadMutex.Lock()
	defer this.ReadMutex.Unlock()

	fp := this.fp
	buf := make([]byte, readMaxSize)
	n, err := fp.Read(buf)
	if err != nil {
		return nil, err
	}
	// 从后往前查找 \n
	var m int64
	for m = int64(n) - 1; m >= 0; m-- {
		if buf[m] == '\n' {
			fp.Seek(m - int64(n) + 1, 1) // 回退字节数，设置在\n的后面
			break
		}
	}
	if m <= 0 {
		return buf[0:n], nil
	} else {
		// 包含换行
		return buf[:m+1], nil
	}
}

/*

	obj := bigFile.New("1.log")
	for s, err := obj.ReadLine(); err != nil {
		fmt.Println(s)
	}

 */
// 过滤掉空行，如果返回空数据的时候，终止掉！
// 效率低下！除了方便，没有优点
func (this *BigFileReader)ReadLine() (string, error) {

	// 单线程读取
	this.ReadMutex.Lock()
	defer this.ReadMutex.Unlock()

	line, err := this.rd.ReadString('\n')
	if err == nil || err == io.EOF {
		line = strings.TrimSpace(line)

		// 如果读取到文件末尾，关闭文件
		if err == io.EOF {
			this.fp.Close()
		}

		return line, err
	}
	return "", err
}


func (this *BigFileReader)Run(works int, readBlockSize int, writeBlockSize int) {

	// 打开文件流
	defer this.Close()


	startTime := time.Now()

	// 显示进度
	processNum := uint64(0)
	go func() {
		for {
			endTime := time.Now().Sub(startTime).Round(time.Second).String()
			fmt.Printf("\relapsed: %v, num: %v", endTime, processNum)
			time.Sleep(1 * time.Second)
		}
	}()

	// works := 1
	wg := sync.WaitGroup{}
	wg.Add(works)
	for i := 0; i < works; i++ {
		go func() {
			for {
				r, err := this.ReadBlock(readBlockSize);
				if err != nil {
					break
				}
				arr := bytes.Split(r, []byte("\n"))
				mergeArr := make([]byte, 0, writeBlockSize)
				for _, line := range arr {
					//line = bytes.TrimSpace(line)
					if len(line) == 0 {
						continue
					}
					mergeArr = append(mergeArr, this.LineCallback(line)...)
					if len(mergeArr) > writeBlockSize {
						this.MergeCallback(mergeArr)
						mergeArr = mergeArr[0:0]
					}
					atomic.AddUint64(&processNum, 1)
				}
				if len(mergeArr) > 0 {
					this.MergeCallback(mergeArr)
					//mergeArr = mergeArr[0:0]
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()

	endTime := time.Now().Sub(startTime).Round(time.Second).String()
	fmt.Printf("\rElapsed: %v, Total: %v, done!\n", endTime, processNum)
}
