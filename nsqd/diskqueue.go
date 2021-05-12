package nsqd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path"
)

// diskQueue implements a filesystem backed FIFO queue
type diskQueue struct {
	name            string
	dataPath        string
	maxBytesPerFile int64 //一个文件能存放的最大容量，如果超过了就要再新建一个文件去存

	readChan          chan []byte // exposed via ReadChan()
	writeChan         chan []byte
	writeResponseChan chan error

	readPos      int64
	writePos     int64
	readFileNum  int64
	writeFileNum int64

	// 读取文件和把读取到的内容实际发送给接收方是两个步骤，
	// 下面两个变量用来记录这两个步骤的中间状态
	nextReadPos     int64
	nextReadFileNum int64

	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader
	writeBuf  bytes.Buffer
}

func NewDiskQueue(name string) *diskQueue {
	d := diskQueue{
		name:              name,
		maxBytesPerFile:   100 * 1024,
		readChan:          make(chan []byte),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
	}
	d.dataPath, _ = os.Getwd()
	go d.ioLoop()
	return &d
}

// ReadChan returns the receive-only []byte channel for reading data
func (d *diskQueue) ReadChan() <-chan []byte {
	return d.readChan
}

// Put writes a []byte to the queue
func (d *diskQueue) Put(data []byte) error {
	d.writeChan <- data
	return <-d.writeResponseChan
}

func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var r chan []byte

	for {
		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			//这里就显示出了nextReadPos的作用了，当比较结果不一致的话，说明上一轮循环已经读取过一次文件了，
			//但是下面的select分支并没有选择 r <- dataRead，所以这一轮我们就不需要再次读取了
			if d.nextReadPos == d.readPos && d.nextReadFileNum == d.readFileNum {
				dataRead, err = d.readOne()
				if err != nil {
					log.Printf("DISKQUEUE(%s) reading at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
			}
			r = d.readChan
		} else {
			r = nil
		}

		select {
		case r <- dataRead:
			d.moveForward()
		case dataWrite := <-d.writeChan:
			d.writeResponseChan <- d.writeOne(dataWrite) //把错误直接抛给上层
		}
	}
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		log.Printf("DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		d.reader = bufio.NewReader(d.readFile)
	}

	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	// we only advance next* because we have not yet sent this to consumers
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum
	if d.nextReadPos >= d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}

	return readBuf, nil
}

// jump to the next read file and rename the current (bad) file
func (d *diskQueue) handleReadError() {
	if d.readFileNum == d.writeFileNum {
		// if you can't properly read from the current write file it's safe to
		// assume that something is fucked and we should skip the current file too
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0
	}

	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"

	log.Printf("DISKQUEUE(%s) jump to next file and saving bad file as %s",
		d.name, badRenameFn)

	err := os.Rename(badFn, badRenameFn)
	if err != nil {
		log.Printf(
			"DISKQUEUE(%s) failed to rename bad diskqueue file %s to %s",
			d.name, badFn, badRenameFn)
	}

	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0
}

func (d *diskQueue) moveForward() {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	// see if we need to clean up the old file
	if oldReadFileNum != d.nextReadFileNum {
		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn)
		if err != nil {
			log.Printf("DISKQUEUE(%s) failed to Remove(%s) - %s", d.name, fn, err)
		}
	}
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (d *diskQueue) writeOne(data []byte) error {
	var err error

	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		log.Printf("DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	dataLen := int32(len(data))

	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen) //先把消息长度写进去
	if err != nil {
		return err
	}

	_, err = d.writeBuf.Write(data) //再把data写进去
	if err != nil {
		return err
	}

	// only write to the file once
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes

	//如果该文件写满了，就换下一个文件写
	if d.writePos >= d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return nil
}

func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}
