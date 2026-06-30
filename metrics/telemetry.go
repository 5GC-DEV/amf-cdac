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

	"github.com/5GC-DEV/nas-cdac"
	"github.com/omec-project/amf/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// AmfStats captures AMF level stats
type AmfStats struct {
	ngapMsg             *prometheus.CounterVec
	gnbSessionProfile   *prometheus.GaugeVec
	ueReg               *prometheus.CounterVec
	ueDeregistered      *prometheus.CounterVec
	ueConnRelease       *prometheus.CounterVec
	ueConnReleaseTotal  prometheus.Counter
	gnbDisconnect       *prometheus.CounterVec
	gnbDisconnectTotal  prometheus.Counter
	ueAuthFail          *prometheus.CounterVec
	ueAuthFailTotal     prometheus.Counter
	pagingFail          *prometheus.CounterVec
	pagingFailTotal     prometheus.Counter
	xnHandoverFail      *prometheus.CounterVec
	xnHandoverFailTotal prometheus.Counter
	n2HandoverFail      *prometheus.CounterVec
	n2HandoverFailTotal prometheus.Counter
	nfNonReachable      *prometheus.CounterVec
	noOfUeConnect       *prometheus.GaugeVec
	noOfActiveSub       prometheus.Gauge
	noOfActiveGnb       prometheus.Gauge
	gnbConnect          prometheus.Counter
	authRequest         *prometheus.CounterVec
	authRequestTotal    prometheus.Counter
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
			Name: "amf_ue_connection_release",
			Help: "count of ue release",
		}, []string{"amf_id", "ran_Ue_Ngap_Id", "direction", "result"}),

		gnbDisconnect: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "amf_gnb_disconnected",
			Help: "gnb disconnection counters",
		}, []string{"id", "ip", "result"}),

		ueAuthFail: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "amf_ue_authentication_fail",
			Help: "ue authentication fail counters ",
		}, []string{"amf_id", "suci", "ausf_id", "result"}),

		pagingFail: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "amf_ue_paging_failures",
			Help: "ue paging failure counters ",
		}, []string{"amf_id", "suci", "result"}),

		xnHandoverFail: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "amf_xn_handover_failures",
			Help: "xn handover failure counters",
		}, []string{"amf_id", "suci", "target_gnbip", "result"}),

		n2HandoverFail: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "amf_n2_handover_failures",
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

		noOfActiveSub: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "amf_active_subscribers",
			Help: "current number of active subscribers in the core",
		}),

		noOfActiveGnb: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "amf_active_gnb",
			Help: "current number of active gNB's in the core",
		}),

		gnbConnect: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "amf_gnb_connected_total",
			Help: "Counter of total gNB connections",
		}),

		authRequest: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "amf_auth_request",
			Help: "Counter of authentication request send",
		}, []string{"supi"}),

		ueConnReleaseTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "amf_ue_connection_release_total",
			Help: "Total count of ue release",
		}),

		authRequestTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "amf_auth_request_total",
			Help: "Total count of authentication request send",
		}),

		gnbDisconnectTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "amf_gnb_disconnected_total",
			Help: "Total count of gnb disconnection",
		}),

		ueAuthFailTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "amf_ue_authentication_fail_total",
			Help: "Total count of ue authentication failures",
		}),

		pagingFailTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "amf_ue_paging_failures_total",
			Help: "Total count of ue paging failures",
		}),

		xnHandoverFailTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "amf_xn_handover_failures_total",
			Help: "Total count of xn handover failures",
		}),

		n2HandoverFailTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "amf_n2_handover_failures_total",
			Help: "Total count of n2 handover failures",
		}),
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
	if err := prometheus.Register(ps.noOfActiveSub); err != nil {
		return err
	}
	if err := prometheus.Register(ps.noOfActiveGnb); err != nil {
		return err
	}
	if err := prometheus.Register(ps.gnbConnect); err != nil {
		return err
	}
	if err := prometheus.Register(ps.authRequest); err != nil {
		return err
	}
	if err := prometheus.Register(ps.ueConnReleaseTotal); err != nil {
		return err
	}
	if err := prometheus.Register(ps.authRequestTotal); err != nil {
		return err
	}
	if err := prometheus.Register(ps.gnbDisconnectTotal); err != nil {
		return err
	}
	if err := prometheus.Register(ps.ueAuthFailTotal); err != nil {
		return err
	}
	if err := prometheus.Register(ps.pagingFailTotal); err != nil {
		return err
	}
	if err := prometheus.Register(ps.xnHandoverFailTotal); err != nil {
		return err
	}
	if err := prometheus.Register(ps.n2HandoverFailTotal); err != nil {
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

// SetNoOfActiveSubStats maintains total active subscribers info
func SetNoOfActiveSubStats(count uint64) {
	amfStats.noOfActiveSub.Set(float64(count))
}

// SetNoOfActiveGnbStats maintains total active subscribers info
func SetNoOfActiveGnbStats(count uint64) {
	amfStats.noOfActiveGnb.Set(float64(count))
}

// IncrementGnbConnStats maintains gnb connection level stats
func IncrementGnbConnStats() {
	amfStats.gnbConnect.Inc()
}

// IncrementAuthReqStats maintains gnb connection level stats
func IncrementAuthReqStats(supi string) {
	amfStats.authRequest.WithLabelValues(supi).Inc()
}

// IncrementUeConnRelStatsTotal maintains total ue connection release stats
func IncrementUeConnRelStatsTotal() {
	amfStats.ueConnReleaseTotal.Inc()
}

// IncrementAuthReqStatsTotal maintains total ue auth request send stats
func IncrementAuthReqStatsTotal() {
	amfStats.authRequestTotal.Inc()
}

// IncrementAuthReqStatsTotal maintains total ue auth request send stats
func IncrementGnbDisconnStatsTotal() {
	amfStats.gnbDisconnectTotal.Inc()
}

// IncrementUeAuthFailStatstotal maintains total ue authentication fail stats
func IncrementUeAuthFailStatstotal() {
	amfStats.ueAuthFailTotal.Inc()
}

// IncrementUeAuthFailStatstotal maintains total ue authentication fail stats
func IncrementUePagingFailStatsTotal() {
	amfStats.pagingFailTotal.Inc()
}

// IncrementXnHandoverFailStatsTotal maintains total ue authentication fail stats
func IncrementXnHandoverFailStatsTotal() {
	amfStats.xnHandoverFailTotal.Inc()
}

// IncrementN2HandoverFailStatsTotal maintains total ue authentication fail stats
func IncrementN2HandoverFailStatsTotal() {
	amfStats.n2HandoverFailTotal.Inc()
}

func InitStats(amfID string) {
	logger.InitLog.Info("Entering InitStats")
	amfStats.ueReg.WithLabelValues(amfID, "success").Add(0)
	amfStats.ueReg.WithLabelValues(amfID, "failure").Add(0)
	amfStats.ueDeregistered.WithLabelValues(amfID, string(nas.MsgTypeDeregistrationRequestUEOriginatingDeregistration), "out", "success").Add(0)
	amfStats.ngapMsg.WithLabelValues(amfID, "InitialUEMessage", "In", "", "").Add(0)
	amfStats.ueConnRelease.WithLabelValues(amfID, "", "In", "success").Add(0)
	amfStats.ueAuthFail.WithLabelValues(amfID, "", "", "success").Add(0)
	amfStats.pagingFail.WithLabelValues(amfID, "", "success").Add(0)
	amfStats.xnHandoverFail.WithLabelValues(amfID, "", "", "success").Add(0)
	amfStats.n2HandoverFail.WithLabelValues(amfID, "", "", "success").Add(0)
	amfStats.nfNonReachable.WithLabelValues(amfID, "", "success").Add(0)
	amfStats.gnbDisconnect.WithLabelValues("", "", "success").Add(0)
	amfStats.gnbConnect.Add(0)
	amfStats.authRequest.WithLabelValues("").Add(0)
	amfStats.ueConnReleaseTotal.Add(0)
	amfStats.authRequestTotal.Add(0)
	amfStats.gnbDisconnectTotal.Add(0)
	amfStats.ueAuthFailTotal.Add(0)
	amfStats.pagingFailTotal.Add(0)
	amfStats.xnHandoverFailTotal.Add(0)
	amfStats.n2HandoverFailTotal.Add(0)
}
