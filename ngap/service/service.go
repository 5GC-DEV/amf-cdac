// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package service

import (
	"encoding/hex"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"git.cs.nctu.edu.tw/calee/sctp"
	"github.com/5GC-DEV/ngap-cdac"
	"github.com/omec-project/amf/logger"
	"github.com/omec-project/amf/metrics"
)

type Packet struct {
	Conn net.Conn
	Data []byte
	TSN  uint32
}

type WorkerPool struct {
	ringBuffer *RingBuffer
	handler    NGAPHandler
}

type cell struct {
	sequence uint64
	data     Packet
}

//	type RingBuffer struct {
//		buffer   []Packet
//		head     int
//		tail     int
//		count    int
//		size     int
//		mutex    sync.Mutex
//		notEmpty *sync.Cond
//		notFull  *sync.Cond
//	}
type RingBuffer struct {
	buffer     []cell
	bufferMask uint64 // size-1, size must be a power of two

	// enqueuePos is shared across multiple producer goroutines
	// (one per gNB connection), so it needs atomic + CAS.
	enqueuePos uint64
	_          [56]byte // padding, avoids false sharing with dequeuePos

	// dequeuePos is shared across multiple consumer goroutines.
	dequeuePos uint64
	_          [56]byte
}

var ringBuffer *RingBuffer

type NGAPHandler struct {
	HandleMessage      func(conn net.Conn, msg []byte)
	HandleNotification func(conn net.Conn, notification sctp.Notification)
}

const readBufSize uint32 = 524288 // 131072

// set default read timeout to 2 seconds
var readTimeout syscall.Timeval = syscall.Timeval{Sec: 2, Usec: 0}

var (
	sctpListener *sctp.SCTPListener
	connections  sync.Map
)

var sctpConfig sctp.SocketConfig = sctp.SocketConfig{
	InitMsg:   sctp.InitMsg{NumOstreams: 3, MaxInstreams: 5, MaxAttempts: 2, MaxInitTimeout: 2},
	RtoInfo:   &sctp.RtoInfo{SrtoAssocID: 0, SrtoInitial: 500, SrtoMax: 1500, StroMin: 100},
	AssocInfo: &sctp.AssocInfo{AsocMaxRxt: 4},
}

func InitWorkerPool(handler NGAPHandler) {
	ringBuffer = NewRingBuffer(8192)

	wp := NewWorkerPool(ringBuffer, handler)
	wp.Start(5)
}

func NewWorkerPool(rb *RingBuffer, handler NGAPHandler) *WorkerPool {
	return &WorkerPool{
		ringBuffer: rb,
		handler:    handler,
	}
}

func (wp *WorkerPool) Start(n int) {
	for i := 0; i < n; i++ {
		go wp.worker(i)
	}
}

func Run(addresses []string, port int, handler NGAPHandler) {
	ips := []net.IPAddr{}

	for _, addr := range addresses {
		if netAddr, err := net.ResolveIPAddr("ip", addr); err != nil {
			logger.NgapLog.Errorf("error resolving address '%s': %v\n", addr, err)
		} else {
			logger.NgapLog.Debugf("resolved address '%s' to %s\n", addr, netAddr)
			ips = append(ips, *netAddr)
		}
	}

	addr := &sctp.SCTPAddr{
		IPAddrs: ips,
		Port:    port,
	}

	go listenAndServe(addr, handler)
}

func listenAndServe(addr *sctp.SCTPAddr, handler NGAPHandler) {
	if listener, err := sctpConfig.Listen("sctp", addr); err != nil {
		logger.NgapLog.Errorf("failed to listen: %+v", err)
		return
	} else {
		sctpListener = listener
	}

	logger.NgapLog.Infof("Listen on %s", sctpListener.Addr())

	for {
		newConn, err := sctpListener.AcceptSCTP()
		if err != nil {
			switch err {
			case syscall.EINTR, syscall.EAGAIN:
				logger.NgapLog.Debugf("AcceptSCTP: %+v", err)
			default:
				logger.NgapLog.Errorf("failed to accept: %+v", err)
			}
			continue
		}

		var info *sctp.SndRcvInfo
		if infoTmp, err := newConn.GetDefaultSentParam(); err != nil {
			logger.NgapLog.Errorf("get default sent param error: %+v, accept failed", err)
			if err = newConn.Close(); err != nil {
				logger.NgapLog.Errorf("close error: %+v", err)
			}
			continue
		} else {
			info = infoTmp
			logger.NgapLog.Debugf("get default sent param[value: %+v]", info)
		}

		info.PPID = ngap.PPID
		if err := newConn.SetDefaultSentParam(info); err != nil {
			logger.NgapLog.Errorf("set default sent param error: %+v, accept failed", err)
			if err = newConn.Close(); err != nil {
				logger.NgapLog.Errorf("close error: %+v", err)
			}
			continue
		} else {
			logger.NgapLog.Debugf("set default sent param[value: %+v]", info)
		}

		events := sctp.SCTP_EVENT_DATA_IO | sctp.SCTP_EVENT_SHUTDOWN | sctp.SCTP_EVENT_ASSOCIATION
		if err := newConn.SubscribeEvents(events); err != nil {
			logger.NgapLog.Errorf("failed to accept: %+v", err)
			if err = newConn.Close(); err != nil {
				logger.NgapLog.Errorf("close error: %+v", err)
			}
			continue
		} else {
			logger.NgapLog.Debugln("subscribe SCTP event[DATA_IO, SHUTDOWN_EVENT, ASSOCIATION_CHANGE]")
		}

		if err := newConn.SetReadBuffer(int(readBufSize)); err != nil {
			logger.NgapLog.Errorf("set read buffer error: %+v, accept failed", err)
			if err = newConn.Close(); err != nil {
				logger.NgapLog.Errorf("close error: %+v", err)
			}
			continue
		} else {
			logger.NgapLog.Debugf("Set read buffer to %d bytes", readBufSize)
		}

		if err := newConn.SetReadTimeout(readTimeout); err != nil {
			logger.NgapLog.Errorf("set read timeout error: %+v, accept failed", err)
			if err = newConn.Close(); err != nil {
				logger.NgapLog.Errorf("close error: %+v", err)
			}
			continue
		} else {
			logger.NgapLog.Debugf("set read timeout: %+v", readTimeout)
		}

		logger.NgapLog.Infof("[AMF] SCTP Accept from: %s", newConn.RemoteAddr().String())
		connections.Store(newConn, newConn)

		go handleConnection(newConn, readBufSize, handler)
	}
}

func Stop() {
	logger.NgapLog.Infoln("close SCTP server...")
	if err := sctpListener.Close(); err != nil {
		logger.NgapLog.Error(err)
		logger.NgapLog.Infof("SCTP server may not close normally.")
	}

	connections.Range(func(key, value interface{}) bool {
		conn := value.(net.Conn)
		if err := conn.Close(); err != nil {
			logger.NgapLog.Error(err)
		}
		return true
	})

	logger.NgapLog.Infof("SCTP server closed")
}

// func handleConnection(conn *sctp.SCTPConn, bufsize uint32, handler NGAPHandler) {
// 	defer func() {
// 		// if AMF call Stop(), then conn.Close() will return EBADF because conn has been closed inside Stop()
// 		if err := conn.Close(); err != nil && err != syscall.EBADF {
// 			logger.NgapLog.Errorf("close connection error: %+v", err)
// 		}
// 		connections.Delete(conn)
// 	}()
// 	// start := time.Now()
// 	// recvTime := time.Now()

// 	for {
// 		buf := make([]byte, bufsize)
// 		readStart := time.Now()
// 		n, info, notification, err := conn.SCTPRead(buf)
// 		readEnd := time.Now()
// 		metrics.ObserveSCTPReadDuration(readEnd.Sub(readStart))

// 		logger.NgapLog.Infof("SCTPRead start=%s end=%s duration=%v", readStart.Format(time.RFC3339Nano), readEnd.Format(time.RFC3339Nano), readEnd.Sub(readStart))
// 		// recvTime := time.Now()
// 		// logger.NgapLog.Infof("SCTPRead returned at %s bytes=%d", recvTime.Format(time.RFC3339Nano), n)
// 		if err != nil {
// 			switch err {
// 			case io.EOF, io.ErrUnexpectedEOF:
// 				logger.NgapLog.Debugln("read EOF from client")
// 				return
// 			case syscall.EAGAIN:
// 				logger.NgapLog.Debugln("SCTP read timeout")
// 				continue
// 			case syscall.EINTR:
// 				logger.NgapLog.Debugf("SCTPRead: %+v", err)
// 				continue
// 			default:
// 				logger.NgapLog.Errorf("handle connection[addr: %+v] error: %+v", conn.RemoteAddr(), err)
// 				return
// 			}
// 		}

// 		if notification != nil {
// 			if handler.HandleNotification != nil {
// 				handler.HandleNotification(conn, notification)
// 			} else {
// 				logger.NgapLog.Warnf("received sctp notification[type 0x%x] but not handled", notification.Type())
// 			}
// 		} else {
// 			if info == nil || info.PPID != ngap.PPID {
// 				logger.NgapLog.Warnln("received SCTP PPID != 60, discard this packet")
// 				continue
// 			}

// 			// logger.NgapLog.Debugf("Read %d bytes", n)
// 			logger.NgapLog.Debugf("Packet content: %+v", hex.Dump(buf[:n]))

// 			if info.SSN != 0 {
// 				logger.NgapLog.Infof("Time=%s SSN=%d", time.Now().Format(time.RFC3339Nano), info.SSN)
// 			}
// 			if info.TSN != 0 {
// 				logger.NgapLog.Infof("TSN: %d", info.TSN)
// 				logger.NgapLog.Infof("Time=%s TSN=%d", time.Now().Format(time.RFC3339Nano), info.TSN)
// 			}
// 			// TODO: concurrent on per-UE message
// 			if conn.RemoteAddr() != nil {
// 				logger.NgapLog.Infof("SCTP packet received from %s", conn.RemoteAddr())
// 			}
// 			if info.PPID != 0 {
// 				logger.NgapLog.Infof("ppid:%d", info.PPID)
// 			}
// 			handleStart := time.Now()
// 			logger.NgapLog.Infof("HandleMessage start=%s", handleStart.Format(time.RFC3339Nano))
// handler.HandleMessage(conn, buf[:n])
// 			handleEnd := time.Now()
// 			metrics.ObserveHandleMessageDuration(handleEnd.Sub(handleStart))
// 			logger.NgapLog.Infof("HandleMessage end=%s duration=%v", handleEnd.Format(time.RFC3339Nano), handleEnd.Sub(handleStart))
// 		}
// 	}
// }

func handleConnection(conn *sctp.SCTPConn, bufsize uint32, handler NGAPHandler) {
	defer func() {
		// if AMF call Stop(), then conn.Close() will return EBADF because conn has been closed inside Stop()
		if err := conn.Close(); err != nil && err != syscall.EBADF {
			logger.NgapLog.Errorf("close connection error: %+v", err)
		}
		connections.Delete(conn)
	}()

	for {
		buf := make([]byte, bufsize)

		readStart := time.Now()
		n, info, notification, err := conn.SCTPRead(buf)
		readEnd := time.Now()

		metrics.ObserveSCTPReadDuration(readEnd.Sub(readStart))

		logger.NgapLog.Infof("SCTPRead start=%s end=%s duration=%v", readStart.Format(time.RFC3339Nano), readEnd.Format(time.RFC3339Nano), readEnd.Sub(readStart))

		if err != nil {
			switch err {
			case io.EOF, io.ErrUnexpectedEOF:
				logger.NgapLog.Debugln("read EOF from client")
				return

			case syscall.EAGAIN:
				logger.NgapLog.Debugln("SCTP read timeout")
				continue

			case syscall.EINTR:
				logger.NgapLog.Debugf("SCTPRead: %+v", err)
				continue

			default:
				logger.NgapLog.Errorf("handle connection[addr: %+v] error: %+v", conn.RemoteAddr(), err)
				return
			}
		}

		if notification != nil {
			if handler.HandleNotification != nil {
				handler.HandleNotification(conn, notification)
			} else {
				logger.NgapLog.Warnf("received sctp notification[type 0x%x] but not handled", notification.Type())
			}
		} else {
			if info == nil || info.PPID != ngap.PPID {
				logger.NgapLog.Warnln("received SCTP PPID != 60, discard this packet")
				continue
			}

			logger.NgapLog.Debugf("Packet content: %+v", hex.Dump(buf[:n]))

			if info.SSN != 0 {
				logger.NgapLog.Infof("Time=%s SSN=%d", time.Now().Format(time.RFC3339Nano), info.SSN)
			}

			if info.TSN != 0 {
				logger.NgapLog.Infof("TSN: %d", info.TSN)
				logger.NgapLog.Infof("Time=%s TSN=%d", time.Now().Format(time.RFC3339Nano), info.TSN)
			}

			if conn.RemoteAddr() != nil {
				logger.NgapLog.Infof("SCTP packet received from %s", conn.RemoteAddr())
			}

			if info.PPID != 0 {
				logger.NgapLog.Infof("ppid:%d", info.PPID)
			}

			// Copy packet before pushing into ring buffer
			packetData := make([]byte, n)
			copy(packetData, buf[:n])

			packet := Packet{
				Conn: conn,
				Data: packetData,
				TSN:  info.TSN,
			}

			// Producer pushes packet to ring buffer
			pushStart := time.Now()
			// ringBuffer.Push(packet)
			ringBuffer.PushBlocking(packet)
			pushEnd := time.Now()
			logger.NgapLog.Infof("push start=%s end=%s duration=%v", pushStart.Format(time.RFC3339Nano), pushEnd.Format(time.RFC3339Nano), pushEnd.Sub(pushStart))
			logger.NgapLog.Debugf("Packet queued to ring buffer, size=%d", n)
			// Immediately continue to next SCTPRead()
		}
	}
}

func (wp *WorkerPool) worker(id int) {
	for {
		// packet := wp.ringBuffer.Pop()
		packet := wp.ringBuffer.PopBlocking(id)

		handleStart := time.Now()

		logger.NgapLog.Infof("Worker-%d HandleMessage start=%s for the packet TSN=%d", id, handleStart.Format(time.RFC3339Nano), packet.TSN)

		wp.handler.HandleMessage(packet.Conn, packet.Data)

		handleEnd := time.Now()

		metrics.ObserveHandleMessageDuration(handleEnd.Sub(handleStart))

		logger.NgapLog.Infof("Worker-%d HandleMessage end=%s for the packet TSN=%d duration=%v", id, handleEnd.Format(time.RFC3339Nano), packet.TSN, handleEnd.Sub(handleStart))
	}
}

//	func NewRingBuffer(size int) *RingBuffer {
//		rb := &RingBuffer{
//			buffer: make([]Packet, size),
//			size:   size,
//		}
//		rb.notEmpty = sync.NewCond(&rb.mutex)
//		rb.notFull = sync.NewCond(&rb.mutex)
//		return rb
//	}
//
// NewRingBuffer creates a ring buffer. size MUST be a power of two.
func NewRingBuffer(size int) *RingBuffer {
	if size <= 0 || size&(size-1) != 0 {
		panic("RingBuffer size must be a power of two")
	}
	rb := &RingBuffer{
		buffer:     make([]cell, size),
		bufferMask: uint64(size - 1),
	}
	for i := range rb.buffer {
		rb.buffer[i].sequence = uint64(i)
	}
	return rb
}

// func (rb *RingBuffer) Push(packet Packet) {
// 	rb.mutex.Lock()
// 	defer rb.mutex.Unlock()

// 	// Wait if buffer is full
// 	for rb.count == rb.size {
// 		waitStart := time.Now()
// 		logger.NgapLog.Warnf("Producer waiting: RingBuffer FULL (count=%d size=%d)", rb.count, rb.size)
// 		rb.notFull.Wait()
// 		waitEnd := time.Now()
// 		logger.NgapLog.Warnf("Producer awakened after %v (count=%d size=%d)", waitEnd.Sub(waitStart), rb.count, rb.size)
// 	}

// 	rb.buffer[rb.tail] = packet

// 	rb.tail = (rb.tail + 1) % rb.size

// 	rb.count++

//		rb.notEmpty.Signal()
//	}
func (rb *RingBuffer) Push(packet Packet) bool {
	pos := atomic.LoadUint64(&rb.enqueuePos)
	for {
		c := &rb.buffer[pos&rb.bufferMask]
		seq := atomic.LoadUint64(&c.sequence)
		diff := int64(seq) - int64(pos)
		index := pos & rb.bufferMask
		// logger.NgapLog.Debugf("[PUSH-TRY] TSN=%d pos=%d index=%d seq=%d", packet.TSN, pos, index, seq)
		switch {
		case diff == 0:
			// Slot free for writing. Try to claim it — this is the
			// contention point between the 2+ producer goroutines.
			if atomic.CompareAndSwapUint64(&rb.enqueuePos, pos, pos+1) {
				c.data = packet
				atomic.StoreUint64(&c.sequence, pos+1) // publish to consumers
				logger.NgapLog.Infof("[PUSH-OK ] TSN=%d pos=%d index=%d newSeq=%d", packet.TSN, pos, index, pos+1)
				return true
			}
			// logger.NgapLog.Debugf("[PUSH-RETRY] TSN=%d oldPos=%d newPos=%d", packet.TSN, pos, atomic.LoadUint64(&rb.enqueuePos))
			pos = atomic.LoadUint64(&rb.enqueuePos) // lost the race, retry
		case diff < 0:
			return false // full

		default:
			pos = atomic.LoadUint64(&rb.enqueuePos)
		}
	}
}

func (rb *RingBuffer) PushBlocking(packet Packet) {
	spins := 0
	for !rb.Push(packet) {
		backoff(&spins)
	}
}

// func (rb *RingBuffer) Pop() Packet {
// 	rb.mutex.Lock()
// 	defer rb.mutex.Unlock()

// 	// Wait if buffer empty
// 	for rb.count == 0 {
// 		waitStart := time.Now()
// 		logger.NgapLog.Infof("Worker waiting: RingBuffer EMPTY")
// 		rb.notEmpty.Wait()
// 		waitEnd := time.Now()
// 		logger.NgapLog.Infof("Worker awakened after %v (count=%d)", waitEnd.Sub(waitStart), rb.count)
// 	}

// 	packet := rb.buffer[rb.head]

// 	rb.head = (rb.head + 1) % rb.size

// 	rb.count--

// 	rb.notFull.Signal()

//		return packet
//	}
func (rb *RingBuffer) Pop(workerID int) (Packet, bool) {
	pos := atomic.LoadUint64(&rb.dequeuePos)

	for {
		c := &rb.buffer[pos&rb.bufferMask]
		seq := atomic.LoadUint64(&c.sequence)
		diff := int64(seq) - int64(pos+1)
		index := pos & rb.bufferMask
		// logger.NgapLog.Debugf("[POP-TRY ] worker=%d pos=%d index=%d seq=%d", workerID, pos, index, seq)
		switch {
		case diff == 0:
			// Slot published and ready to read. Try to claim it —
			// this is the contention point between consumers.
			if atomic.CompareAndSwapUint64(&rb.dequeuePos, pos, pos+1) {
				packet := c.data
				logger.NgapLog.Infof("[POP-OK ] worker=%d TSN=%d pos=%d index=%d", workerID, packet.TSN, pos, index)
				atomic.StoreUint64(&c.sequence, pos+rb.bufferMask+1) // free slot for producer
				return packet, true
			}
			// logger.NgapLog.Debugf("[POP-RETRY] worker=%d oldPos=%d newPos=%d", workerID, pos, atomic.LoadUint64(&rb.dequeuePos))
			pos = atomic.LoadUint64(&rb.dequeuePos) // lost race, retry
		case diff < 0:
			return Packet{}, false // empty
		default:
			pos = atomic.LoadUint64(&rb.dequeuePos)
		}
	}
}

func (rb *RingBuffer) PopBlocking(workerID int) Packet {
	spins := 0
	for {
		if p, ok := rb.Pop(workerID); ok {
			return p
		}
		backoff(&spins)
	}
}

func backoff(spins *int) {
	*spins++
	switch {
	case *spins < 30:
		runtime.Gosched()
	case *spins < 200:
		time.Sleep(50 * time.Microsecond)
	default:
		time.Sleep(1 * time.Millisecond)
	}
}
