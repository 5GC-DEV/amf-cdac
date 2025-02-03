// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

/*
 * AMF Statistics exposing to promethus
 *
 */

package metrics

import (
	"fmt"
	"net/http"

	"github.com/omec-project/amf/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// AmfStats captures AMF level stats
type AmfStats struct {
	ngapMsg           *prometheus.CounterVec
	gnbSessionProfile *prometheus.GaugeVec
	// by cdac tvm
	ueReg *prometheus.CounterVec
}

var amfStats *AmfStats

func initAmfStats() *AmfStats {
	return &AmfStats{
		ngapMsg: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ngap_messages_total",
			Help: "ngap interface counters",
		}, []string{"amf_id", "msg_type", "direction", "result", "reason"}),

		gnbSessionProfile: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gnb_session_profile",
			Help: "gNB session Profile",
		}, []string{"id", "ip", "state", "tac"}),
		// Counter of total UE Registrations - by cdac tvm
		ueReg: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "amf_ue_registrations_total",
			Help: "Counter of total UE Registrations",
		}, []string{"amf_id", "reg_type", "result"}),
	}
}

func (ps *AmfStats) register() error {
	prometheus.Unregister(ps.ngapMsg)

	if err := prometheus.Register(ps.ngapMsg); err != nil {
		return err
	} else {
		fmt.Print("---ngap_messages_total metric registered successfully")
	}

	if err := prometheus.Register(ps.gnbSessionProfile); err != nil {
		return err
	} else {
		fmt.Print("---gnb_session_profile metric registered successfully")
	}
	// by cdac tvm
	if err := prometheus.Register(ps.ueReg); err != nil {
		return err
	} else {
		fmt.Print("---amf_ue_registrations_total metric registered successfully")
	}
	return nil
}

func init() {
	amfStats = initAmfStats()

	if err := amfStats.register(); err != nil {
		logger.AppLog.Errorln("AMF Stats register failed", err)
	}
}

// InitMetrics initialises AMF stats
func InitMetrics() {
	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe("0.0.0.0:9089", nil); err != nil {
		logger.InitLog.Errorf("could not open metrics port: %v", err)
	}
}

// IncrementNgapMsgStats increments message level stats
func IncrementNgapMsgStats(amfID, msgType, direction, result, reason string) {
	fmt.Print("---exposing metric ngap_messages_total")
	amfStats.ngapMsg.WithLabelValues(amfID, msgType, direction, result, reason).Inc()
}

// SetGnbSessProfileStats maintains Session profile info
func SetGnbSessProfileStats(id, ip, state, tac string, count uint64) {
	fmt.Print("---exposing metric gnb_session_profile")
	amfStats.gnbSessionProfile.WithLabelValues(id, ip, state, tac).Set(float64(count))
}

// IncrementUeRegStats increments registration level stats - by cdac tvm
func IncrementUeRegStats(amfID, regType, result string) {
	fmt.Print("---exposing metric amf_ue_registrations_total")
	amfStats.ueReg.WithLabelValues(amfID, regType, result).Inc()
}
