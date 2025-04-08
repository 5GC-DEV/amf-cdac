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
	"net/http"

	"github.com/omec-project/amf/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// AmfStats captures AMF level stats
type AmfStats struct {
	ngapMsg           *prometheus.CounterVec
	gnbSessionProfile *prometheus.GaugeVec
	ueReg             *prometheus.CounterVec
	ueDeregistered    *prometheus.CounterVec
	ueConnRelease     *prometheus.CounterVec
	gnbDisconnect     *prometheus.CounterVec
	ueAuthFail        *prometheus.CounterVec
	pagingFail        *prometheus.CounterVec
	xnHandoverFail    *prometheus.CounterVec
	n2HandoverFail    *prometheus.CounterVec
	nfNonReachable    *prometheus.CounterVec
	noOfUeConnect     *prometheus.GaugeVec
	noOfGnbConnect    *prometheus.GaugeVec
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

		ueReg: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "amf_ue_registrations_total",
			Help: "Counter of total UE Registrations",
		}, []string{"amf_id", "result"}),

		ueDeregistered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ue_deregistered_total",
			Help: "ue deregistration stats",
		}, []string{"amf_id", "msg_type", "direction", "result"}),

		ueConnRelease: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ue_connection_release_total",
			Help: "Total number of ue release",
		}, []string{"amf_id", "ran_Ue_Ngap_Id", "direction", "result"}),

		gnbDisconnect: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gnb_disconnection_total",
			Help: "gnb disconnection counters",
		}, []string{"id", "ip", "result"}),

		ueAuthFail: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ue_authentication_failure_total",
			Help: "ue authentication fail counters ",
		}, []string{"amf_id", "suci", "ausf_id", "result"}),

		pagingFail: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ue_paging_failures_total",
			Help: "ue paging failure counters ",
		}, []string{"amf_id", "suci", "result"}),

		xnHandoverFail: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "xn_handover_failures_total",
			Help: "xn handover failure counters",
		}, []string{"amf_id", "suci", "target_gnbip", "result"}),

		n2HandoverFail: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "n2_handover_failures_total",
			Help: "n2 handover failure counters",
		}, []string{"amf_id", "suci", "target_gnbip", "result"}),

		nfNonReachable: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "nf_nonreachable_total",
			Help: "nf non reachable counters",
		}, []string{"amf_id", "nrf_uri", "result"}),

		noOfUeConnect: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ue_connected_total",
			Help: "UE connections total",
		}, []string{"id", "supi", "guti"}),

		noOfGnbConnect: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gnb_connected_total",
			Help: "GNB connections total",
		}, []string{"id", "gnb_id", "gnb_ip"}),
	}
}

func (ps *AmfStats) register() error {
	prometheus.Unregister(ps.ngapMsg)

	if err := prometheus.Register(ps.ngapMsg); err != nil {
		return err
	}
	if err := prometheus.Register(ps.gnbSessionProfile); err != nil {
		return err
	}
	if err := prometheus.Register(ps.ueReg); err != nil {
		return err
	}
	if err := prometheus.Register(ps.ueDeregistered); err != nil {
		return err
	}
	if err := prometheus.Register(ps.ueConnRelease); err != nil {
		return err
	}
	if err := prometheus.Register(ps.gnbDisconnect); err != nil {
		return err
	}
	if err := prometheus.Register(ps.ueAuthFail); err != nil {
		return err
	}
	if err := prometheus.Register(ps.pagingFail); err != nil {
		return err
	}
	if err := prometheus.Register(ps.xnHandoverFail); err != nil {
		return err
	}
	if err := prometheus.Register(ps.n2HandoverFail); err != nil {
		return err
	}
	if err := prometheus.Register(ps.nfNonReachable); err != nil {
		return err
	}
	if err := prometheus.Register(ps.noOfUeConnect); err != nil {
		return err
	}
	if err := prometheus.Register(ps.noOfGnbConnect); err != nil {
		return err
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
	if err := http.ListenAndServe(":9089", nil); err != nil {
		logger.InitLog.Errorf("could not open metrics port: %v", err)
	}
}

// IncrementNgapMsgStats increments message level stats
func IncrementNgapMsgStats(amfID, msgType, direction, result, reason string) {
	amfStats.ngapMsg.WithLabelValues(amfID, msgType, direction, result, reason).Inc()
}

// SetGnbSessProfileStats maintains Session profile info
func SetGnbSessProfileStats(id, ip, state, tac string, count uint64) {
	amfStats.gnbSessionProfile.WithLabelValues(id, ip, state, tac).Set(float64(count))
}

// IncrementUeRegStats increments registration level stats
func IncrementUeRegStats(amfID, result string) {
	amfStats.ueReg.WithLabelValues(amfID, result).Inc()
}

// IncrementUeDeregStats increments ue deregistration stats
func IncrementUeDeregStats(amfID, msgType, direction, result string) {
	amfStats.ueDeregistered.WithLabelValues(amfID, msgType, direction, result).Inc()
}

// IncrementUeConnRelStats increments ue connection release stats
func IncrementUeConnRelStats(amfID, ranUeNgapId, direction, result string) {
	amfStats.ueConnRelease.WithLabelValues(amfID, ranUeNgapId, direction, result).Inc()
}

// IncrementGnbDisconnStats increments gnb disconnection level stats
func IncrementGnbDisconnStats(id, ip, result string) {
	amfStats.gnbDisconnect.WithLabelValues(id, ip, result).Inc()
}

// IncrementUeAuthFailStats increments ue authentication failure level stats
func IncrementUeAuthFailStats(amfID, suci, ausfid, result string) {
	amfStats.ueAuthFail.WithLabelValues(amfID, suci, ausfid, result).Inc()
}

// IncrementUePagingFailStats increments ue paging failure level stats
func IncrementUePagingFailStats(amfID, suci, result string) {
	amfStats.pagingFail.WithLabelValues(amfID, suci, result).Inc()
}

// IncrementXnHandoverFailStats increments xn handover failure level stats
func IncrementXnHandoverFailStats(amfID, suci, gnbip, result string) {
	amfStats.xnHandoverFail.WithLabelValues(amfID, suci, gnbip, result).Inc()
}

// IncrementN2HandoverFailStats increments n2 handover failure level stats
func IncrementN2HandoverFailStats(amfID, suci, gnbip, result string) {
	amfStats.n2HandoverFail.WithLabelValues(amfID, suci, gnbip, result).Inc()
}

// IncrementNfNonReachableStats increments NF Non Reachable level stats
func IncrementNfNonReachableStats(amfID, nrfuri, result string) {
	amfStats.nfNonReachable.WithLabelValues(amfID, nrfuri, result).Inc()
}

// SetNoOfUeConnectionStats maintains total ue connections info
func SetNoOfUeConnectionStats(id, suci, guti string, count uint64) {
	amfStats.noOfUeConnect.WithLabelValues(id, suci, guti).Set(float64(count))
}

// SetNoOfGnbConnectionStats maintains total gNB connections info
func SetNoOfGnbConnectionStats(id, gnbid, gnbip string, count uint64) {
	amfStats.noOfGnbConnect.WithLabelValues(id, gnbid, gnbip).Set(float64(count))
}
