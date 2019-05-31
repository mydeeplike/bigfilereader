package bigfilereader

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

// 第 1 种方式：推荐
func TestScan1(t *testing.T) {
	big, _ := New("1.log")
	defer big.Close()

	big.LineCallback = func(b []byte) []byte {
		s := string(b)
		arr := strings.Split(s, ",")
		t := fmt.Sprintf(`('%v','%v'),`, arr[0], arr[1])

		/* 对于大数据，这种比字符串相加快很多！
		buf := bytes.NewBuffer([100]byte{}[:])
		buf.WriteString("xxx")
		buf.Bytes()
		*/
		return []byte(t)
	}

	big.MergeCallback = func(mergeBytes []byte) {
		mergeBytes = bytes.TrimRight(mergeBytes, ",")
		sql1 := fmt.Sprintf("INSERT INTO xxx (id, name) VALUES %v", string(mergeBytes))
		fmt.Printf(sql1)
	}

	big.Run(1, 100000000, 50)

	// 4 : 线程数，100000000: 每次读取的文件块大小，2000000：每次合并的大小
	// big.Run(4, 100000000, 2000000)
}


// 第 2 种方式：不推荐
func TestScan2(t *testing.T) {
	big, _ := New("1.log")
	defer big.Close()

	works := 2
	wg := sync.WaitGroup{}
	wg.Add(works)
	for i := 0; i < works; i++ {
		go func() {
			for {
				r, err := big.ReadBlock(20);
				if err != nil {
					break
				}
				buf := bufio.NewReader(bytes.NewReader(r))
				for {
					line, err := buf.ReadString('\n')
					if err == nil || err == io.EOF {
						line = strings.TrimSpace(line)
						if line != "" {
							fmt.Println(line)
						}
					}
					if err != nil {
						break
					}
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	t.Log("done")
}


// 第 3 种方式：单线程读取: 效率低下，不推荐
func TestScan3(t *testing.T) {
	obj, _ := New("1.log")
	for {
		s, err := obj.ReadLine();
		if err != nil {
			break
		}
		fmt.Println(s)
	}
}
