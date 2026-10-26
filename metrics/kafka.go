// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	mi "github.com/5GC-DEV/util-cdac/metricinfo"
	"github.com/omec-project/amf/factory"
)

// Writer is retained for source compatibility with existing AMF call sites.
// Metrics are now exposed directly by the AMF Prometheus endpoint; Kafka is not used.
type Writer struct{}

var StatWriter Writer

func InitialiseKafkaStream(_ *factory.Configuration) error {
	return nil
}

func GetWriter() Writer { return StatWriter }

func (Writer) SendMessage(_ []byte) error { return nil }

func (Writer) PublishUeCtxtEvent(ctxt mi.CoreSubscriber, op mi.SubscriberOp) error {
	operation := "modify"
	if op == mi.SubsOpAdd {
		operation = "add"
	} else if op == mi.SubsOpDel {
		operation = "delete"
	}
	UpdateCoreSubscriber(ctxt.Imsi, ctxt.IPAddress, ctxt.AmfSubState, operation)
	IncrementAmfSvcStats(ctxt.AmfId, operation)
	return nil
}

func (Writer) PublishNfStatusEvent(event mi.MetricEvent) error {
	connected := event.NfStatusData.NfStatus == mi.NfStatusConnected
	SetNfStatus(event.NfStatusData.NfName, string(event.NfStatusData.NfType), connected)
	return nil
}
