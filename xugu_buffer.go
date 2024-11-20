package xugu

import (
	"errors"
	"net"
	"time"
)

var (
	ErrBusyBuffer = errors.New("busy buffer")
)

type buffer struct {
	buf     []byte // buf 是一个字节缓冲区，长度和容量相等。
	conn    net.Conn
	idx     int64
	length  int
	timeout time.Duration
}

// newBuffer 分配并返回一个新的缓冲区。
func newBuffer(nc net.Conn) buffer {
	return buffer{
		buf:  make([]byte, 2048),
		conn: nc,
	}
}

func (b *buffer) peekChar() byte {
	if b.idx > int64(len(b.buf[b.idx:b.length])) {
		b.readNext(1, false)
		b.idx-- //peekchar 只查看当前字符，不移动指针，但是readNext会移动指针，所以需要-1
	}
	ret := b.buf[b.idx]
	return ret
}
func (b *buffer) reset() {
	b.idx = 0
	b.length = 0
	b.buf = make([]byte, 2048)
}

func (b *buffer) readNext(need int, reverse bool) ([]byte, error) {
	if need == 0 {
		return nil, nil
	}
	//长度不够返回
	if len(b.buf[b.idx:b.length]) < need {
		buffer := make([]byte, need+1)
		b.buf = append(b.buf[:b.length], buffer...)
		n, err := b.conn.Read(b.buf[b.length:])
		if err != nil {
			// if err == io.EOF {

			// }
			return nil, err
		}

		b.length += n

		for b.length-int(b.idx) < need {

			n, err := b.conn.Read(b.buf[b.length:])
			if err != nil {
				// if err == io.EOF {

				// }

				return nil, err
			}
			//nTmp += n
			b.length += n
		}

	}

	offset := b.idx
	b.idx += int64(need)
	if GlobalIsBig {
		reverse = false
	}
	if reverse {
		tmp := reverseBytes(b.buf[offset:b.idx])

		return tmp, nil
	} else {

		return b.buf[offset:b.idx], nil
	}

}
