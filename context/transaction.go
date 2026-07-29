// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package context

import (
	"sync"
	"time"

	"github.com/omec-project/amf/logger"
)

const (
	sqnGracePeriod = 100 * time.Millisecond
	sqnModulus     = 256 // sqn is a single byte, wraps mod 256
)

type EventChannel struct {
	Message       chan interface{}
	Event         chan string
	AmfUe         *AmfUe
	NasHandler    func(*AmfUe, NasMsg)
	NgapHandler   func(*AmfUe, NgapMsg)
	SbiHandler    func(s1, s2 string, msg interface{}) (interface{}, string, interface{}, interface{})
	ConfigHandler func(s1, s2, s3 string, msg interface{})
	seqMu         sync.Mutex
	prevSqn       int      // last sqn actually processed; -1 = none yet
	held          *NgapMsg // at most one out-of-order message buffered
	heldTimer     *time.Timer
}

func (tx *EventChannel) UpdateNgapHandler(handler func(*AmfUe, NgapMsg)) {
	tx.AmfUe.TxLog.Infof("updated ngaphandler")
	tx.NgapHandler = handler
}

func (tx *EventChannel) UpdateNasHandler(handler func(*AmfUe, NasMsg)) {
	tx.AmfUe.TxLog.Infof("updated nashandler")
	tx.NasHandler = handler
}

func (tx *EventChannel) UpdateSbiHandler(handler func(s1, s2 string, msg interface{}) (interface{}, string, interface{}, interface{})) {
	if tx.AmfUe == nil {
		logger.ContextLog.Error("eventchannel is nil while updating Sbi handler")
		return
	} else {
		tx.AmfUe.TxLog.Infof("updated sbihandler")
		tx.SbiHandler = handler
	}
}

func (tx *EventChannel) UpdateConfigHandler(handler func(s1, s2, s3 string, msg interface{})) {
	tx.AmfUe.TxLog.Infof("updated confighandler")
	tx.ConfigHandler = handler
}

func (tx *EventChannel) Start() {
	for {
		select {
		case msg := <-tx.Message:
			switch msg := msg.(type) {
			case NasMsg:
				tx.NasHandler(tx.AmfUe, msg)
			case NgapMsg:
				tx.NgapHandler(tx.AmfUe, msg)
			case SbiMsg:
				p_1, p_2, p_3, p_4 := tx.SbiHandler(msg.UeContextId, msg.ReqUri, msg.Msg)
				res := SbiResponseMsg{
					RespData:       p_1,
					LocationHeader: p_2,
					ProblemDetails: p_3,
					TransferErr:    p_4,
				}
				msg.Result <- res
			case ConfigMsg:
				tx.ConfigHandler(msg.Supi, msg.Sst, msg.Sd, msg.Msg)
			}
		case event := <-tx.Event:
			if event == "quit" {
				tx.AmfUe.TxLog.Infof("closed ue goroutine")
				return
			}
		}
	}
}

func (tx *EventChannel) SubmitMessage(msg interface{}) {
	tx.Message <- msg
}

func (tx *EventChannel) SubmitNgapMessage(msg NgapMsg) {
	recvTime := time.Now()
	if msg.Sqn < 0 {
		tx.Message <- msg
		return
	}

	var toSendNow []NgapMsg
	var decision string

	tx.seqMu.Lock()
	expected := (tx.prevSqn + 1) % sqnModulus
	prevSqnBefore := tx.prevSqn

	switch {
	case tx.prevSqn == -1 || msg.Sqn == expected:
		// In order (or very first message for this UE). Accept it and
		// advance prevSqn immediately.
		decision = "immediate"
		tx.prevSqn = msg.Sqn
		toSendNow = append(toSendNow, msg)

		// If we were already holding a message waiting on exactly this
		// gap, it's now unblocked — release it too, in order.
		if tx.held != nil {
			nextExpected := (tx.prevSqn + 1) % sqnModulus
			if tx.held.Sqn == nextExpected {
				if tx.heldTimer != nil {
					tx.heldTimer.Stop()
					tx.heldTimer = nil
				}
				toSendNow = append(toSendNow, *tx.held)
				tx.prevSqn = tx.held.Sqn
				tx.held = nil
			}
		}

	case tx.held != nil:
		// Out of order, and we're already holding a different
		// out-of-order message. This shouldn't normally happen with
		// only a single held slot; log it and let this one through
		// rather than silently dropping it.
		decision = "passthrough-collision"
		tx.AmfUe.TxLog.Warnf(
			"held buffer already occupied (held sqn=%d), passing sqn=%d through unordered",
			tx.held.Sqn, msg.Sqn)
		tx.prevSqn = msg.Sqn
		toSendNow = append(toSendNow, msg)

	default:
		// Out of order condition : buffer it and start the grace-period timer.
		decision = "held"
		heldCopy := msg
		tx.held = &heldCopy
		tx.heldTimer = time.AfterFunc(sqnGracePeriod, func() {
			tx.releaseHeldAfterTimeout(heldCopy.Sqn)
		})
	}
	tx.AmfUe.TxLog.Infof("SUBMIT sqn=%d prevSqnBefore=%d expected=%d decision=%s time=%s", msg.Sqn, prevSqnBefore, expected, decision, recvTime.Format(time.RFC3339Nano))
	tx.seqMu.Unlock()
	// Send outside the lock so a blocked/slow channel send never holds
	// seqMu and stalls other producers submitting for this UE.
	for _, m := range toSendNow {
		tx.AmfUe.TxLog.Infof("DISPATCH-TO-CHANNEL sqn=%d time=%s", m.Sqn, time.Now().Format(time.RFC3339Nano))
		tx.Message <- m
	}
}

func (tx *EventChannel) releaseHeldAfterTimeout(expectedSqn int) {
	fireTime := time.Now()
	tx.seqMu.Lock()
	var toSend *NgapMsg
	if tx.held != nil && tx.held.Sqn == expectedSqn {
		tx.AmfUe.TxLog.Infof("grace period expired waiting for predecessor of sqn=%d, processing out of order", expectedSqn)
		tx.AmfUe.TxLog.Infof("TIMEOUT-RELEASE sqn=%d prevSqnBefore=%d time=%s", expectedSqn, tx.prevSqn, fireTime.Format(time.RFC3339Nano))
		tx.prevSqn = tx.held.Sqn
		toSend = tx.held
		tx.held = nil
		tx.heldTimer = nil
	}
	tx.seqMu.Unlock()

	if toSend != nil {
		tx.Message <- *toSend
	}
}
