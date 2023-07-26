package wire

import (
	"sync"

	"github.com/quic-go/quic-go/internal/protocol"
)

var pool sync.Pool

var datagramPool sync.Pool

func init() {
	pool.New = func() interface{} {
		return &StreamFrame{
			Data:     make([]byte, 0, protocol.MaxPacketBufferSize),
			fromPool: true,
		}
	}

	datagramPool.New = func() any {
		return &DatagramFrame{
			Data:     make([]byte, 0, protocol.MaxDatagramFrameSize),
			fromPool: true,
		}
	}
}

func GetStreamFrame() *StreamFrame {
	f := pool.Get().(*StreamFrame)
	return f
}

func putStreamFrame(f *StreamFrame) {
	if !f.fromPool {
		return
	}
	if protocol.ByteCount(cap(f.Data)) != protocol.MaxPacketBufferSize {
		panic("wire.PutStreamFrame called with packet of wrong size!")
	}
	pool.Put(f)
}

func GetDatagramFrame() *DatagramFrame {
	f := datagramPool.Get().(*DatagramFrame)
	return f
}

func putDatagramFrame(f *DatagramFrame) {
	if !f.fromPool {
		return
	}
	if protocol.ByteCount(cap(f.Data)) != protocol.MaxDatagramFrameSize {
		panic("wire.PutStreamFrame called with packet of wrong size!")
	}
	datagramPool.Put(f)
}
