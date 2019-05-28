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

func TestScan1(t *testing.T) {
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

func TestScan2(t *testing.T) {
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
}

// 单线程读取: 效率低下
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

func Test1(t *testing.T) {
	s := "abc"
	println(s[0 : len(s)-1])
}
