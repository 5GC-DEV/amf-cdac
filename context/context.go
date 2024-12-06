// SPDX-FileCopyrightText: 2022-present Intel Corporation
// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package context

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/omec-project/amf/factory"
	"github.com/omec-project/amf/logger"
	"github.com/omec-project/openapi/models"
	"github.com/omec-project/util/drsm"
	"github.com/omec-project/util/idgenerator"
)

var (
	amfContext                                                = AMFContext{}
	tmsiGenerator                    *idgenerator.IDGenerator = nil
	amfUeNGAPIDGenerator             *idgenerator.IDGenerator = nil
	amfStatusSubscriptionIDGenerator *idgenerator.IDGenerator = nil
	mutex                            sync.Mutex
)

func init() {
	AMF_Self().LadnPool = make(map[string]*LADN)
	AMF_Self().EventSubscriptionIDGenerator = idgenerator.NewGenerator(1, math.MaxInt32)
	AMF_Self().Name = "amf"
	AMF_Self().UriScheme = models.UriScheme_HTTPS
	AMF_Self().RelativeCapacity = 0xff
	AMF_Self().ServedGuamiList = make([]models.Guami, 0, MaxNumOfServedGuamiList)
	AMF_Self().PlmnSupportList = make([]factory.PlmnSupportItem, 0, MaxNumOfPLMNs)
	AMF_Self().NfService = make(map[models.ServiceName]models.NfService)
	AMF_Self().NetworkName.Full = "free5GC"
	if !AMF_Self().EnableDbStore {
		tmsiGenerator = idgenerator.NewGenerator(1, math.MaxInt32)
		amfStatusSubscriptionIDGenerator = idgenerator.NewGenerator(1, math.MaxInt32)
		amfUeNGAPIDGenerator = idgenerator.NewGenerator(1, MaxValueOfAmfUeNgapId)
	}
}

type AMFContext struct {
	Drsm                            drsm.DrsmInterface
	EventSubscriptionIDGenerator    *idgenerator.IDGenerator
	EventSubscriptions              sync.Map
	UePool                          sync.Map         // map[supi]*AmfUe
	RanUePool                       sync.Map         // map[AmfUeNgapID]*RanUe
	AmfRanPool                      sync.Map         // map[net.Conn]*AmfRan
	LadnPool                        map[string]*LADN // dnn as key
	SupportTaiLists                 []models.Tai
	ServedGuamiList                 []models.Guami
	PlmnSupportList                 []factory.PlmnSupportItem
	RelativeCapacity                int64
	NfId                            string
	Name                            string
	NfService                       map[models.ServiceName]models.NfService // nfservice that amf support
	UriScheme                       models.UriScheme
	BindingIPv4                     string
	SBIPort                         int
	Key                             string
	PEM                             string
	NgapPort                        int
	SctpGrpcPort                    int
	RegisterIPv4                    string
	HttpIPv6Address                 string
	TNLWeightFactor                 int64
	SupportDnnLists                 []string
	AMFStatusSubscriptions          sync.Map // map[subscriptionID]models.SubscriptionData
	NfStatusSubscriptions           sync.Map // map[NfInstanceID]models.NrfSubscriptionData.SubscriptionId
	NrfUri                          string
	SecurityAlgorithm               SecurityAlgorithm
	NetworkName                     factory.NetworkName
	NgapIpList                      []string // NGAP Server IP
	T3502Value                      int      // unit is second
	T3512Value                      int      // unit is second
	Non3gppDeregistrationTimerValue int      // unit is second
	// read-only fields
	T3513Cfg                 factory.TimerValue
	T3522Cfg                 factory.TimerValue
	T3550Cfg                 factory.TimerValue
	T3560Cfg                 factory.TimerValue
	T3565Cfg                 factory.TimerValue
	EnableSctpLb             bool
	EnableDbStore            bool
	EnableNrfCaching         bool
	NrfCacheEvictionInterval time.Duration
}

type AMFContextEventSubscription struct {
	IsAnyUe           bool
	IsGroupUe         bool
	UeSupiList        []string
	Expiry            *time.Time
	EventSubscription models.AmfEventSubscription
}

type SecurityAlgorithm struct {
	IntegrityOrder []uint8 // slice of security.AlgIntegrityXXX
	CipheringOrder []uint8 // slice of security.AlgCipheringXXX
}

func NewPlmnSupportItem() (item factory.PlmnSupportItem) {
	item.SNssaiList = make([]models.Snssai, 0, MaxNumOfSlice)
	return
}

func (context *AMFContext) TmsiAllocate() int32 {
	var val int32
	var err error
	if context.EnableDbStore {
		val, err = context.Drsm.AllocateInt32ID()
	} else {
		var tmp int64
		tmp, err = AllocateUniqueID(&tmsiGenerator, "tmsi")
		val = int32(tmp)
	}
	if err != nil {
		logger.ContextLog.Errorf("Allocate TMSI error: %+v", err)
		return -1
	}
	logger.ContextLog.Infof("Allocate TMSI : %v", val)
	return val
}

func (context *AMFContext) AllocateAmfUeNgapID() (int64, error) {
	var val int64
	var err error
	if context.EnableDbStore {
		var tmp int32
		tmp, err = context.Drsm.AllocateInt32ID()
		val = int64(tmp)
	} else {
		val, err = AllocateUniqueID(&amfUeNGAPIDGenerator, "amfUeNgapID")
	}
	if err != nil {
		logger.ContextLog.Errorf("Allocate NgapID error: %+v", err)
		return -1, err
	}

	logger.ContextLog.Infof("Allocate AmfUeNgapID : %v", val)
	return val, nil
}

func (context *AMFContext) AllocateGutiToUe(ue *AmfUe) {
	servedGuami := context.ServedGuamiList[0]
	ue.Tmsi = context.TmsiAllocate()

	plmnID := servedGuami.PlmnId.Mcc + servedGuami.PlmnId.Mnc
	tmsiStr := fmt.Sprintf("%08x", ue.Tmsi)
	ue.Guti = plmnID + servedGuami.AmfId + tmsiStr
}

func (context *AMFContext) ReAllocateGutiToUe(ue *AmfUe) {
	var err error
	servedGuami := context.ServedGuamiList[0]
	if context.EnableDbStore {
		err = context.Drsm.ReleaseInt32ID(ue.Tmsi)
	} else {
		tmsiGenerator.FreeID(int64(ue.Tmsi))
	}
	if err != nil {
		logger.ContextLog.Errorf("Error releasing tmsi: %v", err)
	}
	ue.Tmsi = context.TmsiAllocate()

	plmnID := servedGuami.PlmnId.Mcc + servedGuami.PlmnId.Mnc
	tmsiStr := fmt.Sprintf("%08x", ue.Tmsi)
	ue.Guti = plmnID + servedGuami.AmfId + tmsiStr
}

func (context *AMFContext) AllocateRegistrationArea(ue *AmfUe, anType models.AccessType) {
	// clear the previous registration area if need
	if len(ue.RegistrationArea[anType]) > 0 {
		ue.RegistrationArea[anType] = nil
	}

	// allocate a new tai list as a registration area to ue
	// TODO: algorithm to choose TAI list

	taiList := make([]models.Tai, len(context.SupportTaiLists))
	copy(taiList, context.SupportTaiLists)
	for i := range taiList {
		tmp, err := strconv.ParseUint(taiList[i].Tac, 10, 32)
		if err != nil {
			logger.ContextLog.Errorf("Could not convert TAC to int: %v", err)
		}
		taiList[i].Tac = fmt.Sprintf("%06x", tmp)
		logger.ContextLog.Infof("Supported Tai List in AMF Plmn: %v, Tac: %v", taiList[i].PlmnId, taiList[i].Tac)
	}
	for _, supportTai := range taiList {
		if reflect.DeepEqual(supportTai, ue.Tai) {
			ue.RegistrationArea[anType] = append(ue.RegistrationArea[anType], supportTai)
			break
		}
	}
}

func (context *AMFContext) NewAMFStatusSubscription(subscriptionData models.SubscriptionData) (subscriptionID string) {
	var id int32
	var err error
	if context.EnableDbStore {
		id, err = context.Drsm.AllocateInt32ID()
	} else {
		var tmp int64
		tmp, err = amfStatusSubscriptionIDGenerator.Allocate()
		id = int32(tmp)
	}
	if err != nil {
		logger.ContextLog.Errorf("Allocate subscriptionID error: %+v", err)
		return ""
	}

	subscriptionID = strconv.Itoa(int(id))
	context.AMFStatusSubscriptions.Store(subscriptionID, subscriptionData)
	return
}

// Return Value: (subscriptionData *models.SubScriptionData, ok bool)
func (context *AMFContext) FindAMFStatusSubscription(subscriptionID string) (*models.SubscriptionData, bool) {
	if value, ok := context.AMFStatusSubscriptions.Load(subscriptionID); ok {
		subscriptionData := value.(models.SubscriptionData)
		return &subscriptionData, ok
	} else {
		return nil, false
	}
}

func (context *AMFContext) DeleteAMFStatusSubscription(subscriptionID string) {
	context.AMFStatusSubscriptions.Delete(subscriptionID)
	if id, err := strconv.ParseInt(subscriptionID, 10, 64); err != nil {
		logger.ContextLog.Error(err)
	} else {
		if context.EnableDbStore {
			err = context.Drsm.ReleaseInt32ID(int32(id))
		} else {
			amfStatusSubscriptionIDGenerator.FreeID(id)
		}
		if err != nil {
			logger.ContextLog.Error(err)
		}
	}
}

func (context *AMFContext) NewEventSubscription(subscriptionID string, subscription *AMFContextEventSubscription) {
	context.EventSubscriptions.Store(subscriptionID, subscription)
}

func (context *AMFContext) FindEventSubscription(subscriptionID string) (*AMFContextEventSubscription, bool) {
	if value, ok := context.EventSubscriptions.Load(subscriptionID); ok {
		return value.(*AMFContextEventSubscription), ok
	} else {
		return nil, false
	}
}

func (context *AMFContext) DeleteEventSubscription(subscriptionID string) {
	context.EventSubscriptions.Delete(subscriptionID)
	if id, err := strconv.ParseInt(subscriptionID, 10, 32); err != nil {
		logger.ContextLog.Error(err)
	} else {
		context.EventSubscriptionIDGenerator.FreeID(id)
	}
}

func (context *AMFContext) AddAmfUeToUePool(ue *AmfUe, supi string) {
	if len(supi) == 0 {
		logger.ContextLog.Errorf("Supi is nil")
	}
	ue.Supi = supi
	context.UePool.Store(ue.Supi, ue)
}

func (context *AMFContext) NewAmfUe(supi string) *AmfUe {
	mutex.Lock()
	defer mutex.Unlock()
	ue := AmfUe{}
	ue.init()

	if supi != "" {
		context.AddAmfUeToUePool(&ue, supi)
	}

	context.AllocateGutiToUe(&ue)

	return &ue
}

func (context *AMFContext) AmfUeFindByUeContextID(ueContextID string) (*AmfUe, bool) {
	if strings.HasPrefix(ueContextID, "imsi") {
		return context.AmfUeFindBySupi(ueContextID)
	}
	if strings.HasPrefix(ueContextID, "imei") {
		return context.AmfUeFindByPei(ueContextID)
	}
	if strings.HasPrefix(ueContextID, "5g-guti") {
		guti := ueContextID[strings.LastIndex(ueContextID, "-")+1:]
		return context.AmfUeFindByGuti(guti)
	}
	return nil, false
}

func (context *AMFContext) AmfUeFindBySupi(supi string) (ue *AmfUe, ok bool) {
	if value, loadOk := context.UePool.Load(supi); loadOk {
		ue = value.(*AmfUe)
		ok = loadOk
	} else if context.EnableDbStore {
		ue, ok = DbFetchUeBySupi(supi)
		if ue != nil && ok {
			logger.ContextLog.Infoln("Ue with supi found in DB : ", supi)
			context.UePool.Store(ue.Supi, ue)
		} else {
			logger.ContextLog.Infoln("Ue with Supi not found locally and in DB: ", supi)
		}
	} else {
		logger.ContextLog.Infoln("Ue with Supi not found : ", supi)
	}

	return
}

func (context *AMFContext) AmfUeFindByPei(pei string) (ue *AmfUe, ok bool) {
	context.UePool.Range(func(key, value interface{}) bool {
		candidate := value.(*AmfUe)
		if ok = (candidate.Pei == pei); ok {
			ue = candidate
			return false
		}
		return true
	})
	return
}

func (context *AMFContext) AmfUeFindBySuci(suci string) (ue *AmfUe, ok bool) {
	context.UePool.Range(func(key, value interface{}) bool {
		candidate := value.(*AmfUe)
		if ok = (candidate.Suci == suci); ok {
			ue = candidate
			return false
		}
		return true
	})
	return
}

func (context *AMFContext) AmfUeDeleteBySuci(suci string) (ue *AmfUe, ok bool) {
	context.UePool.Range(func(key, value interface{}) bool {
		candidate := value.(*AmfUe)
		if ok = (candidate.Suci == suci); ok {
			context.UePool.Delete(candidate.Supi)
			candidate.TxLog.Infof("uecontext removed based on suci")
			candidate.Remove()
			return false
		}
		return true
	})
	return
}

func (context *AMFContext) NewAmfRan(conn net.Conn) *AmfRan {
	ran := AmfRan{}
	ran.SupportedTAList = NewSupportedTAIList()
	ran.Conn = conn
	ran.GnbIp = conn.RemoteAddr().String()
	ran.Log = logger.NgapLog.With(logger.FieldRanAddr, conn.RemoteAddr().String())
	context.AmfRanPool.Store(conn, &ran)
	return &ran
}

// use net.Conn to find RAN context, return *AmfRan and ok bit
func (context *AMFContext) AmfRanFindByConn(conn net.Conn) (*AmfRan, bool) {
	if value, ok := context.AmfRanPool.Load(conn); ok {
		return value.(*AmfRan), ok
	}
	return nil, false
}

func (context *AMFContext) NewAmfRanAddr(remoteAddr string) *AmfRan {
	ran := AmfRan{}
	ran.SupportedTAList = NewSupportedTAIList()
	ran.GnbIp = remoteAddr
	ran.Log = logger.NgapLog.With(logger.FieldRanAddr, remoteAddr)
	context.AmfRanPool.Store(remoteAddr, &ran)
	logger.ContextLog.Infoln("New Amf Ran added with address: %v", remoteAddr)
	return &ran
}

func (context *AMFContext) NewAmfRanId(GnbId string) *AmfRan {
	ran := AmfRan{}
	ran.SupportedTAList = NewSupportedTAIList()
	ran.GnbId = GnbId
	ran.Log = logger.NgapLog.With(logger.FieldRanId, GnbId)
	context.AmfRanPool.Store(GnbId, &ran)
	logger.ContextLog.Infoln("New Amf Ran added with GnbId: %v", GnbId)
	return &ran
}

func (context *AMFContext) AmfRanFindByGnbId(gnbId string) (*AmfRan, bool) {
	if value, ok := context.AmfRanPool.Load(gnbId); ok {
		return value.(*AmfRan), ok
	}
	return nil, false
}

// use ranNodeID to find RAN context, return *AmfRan and ok bit
func (context *AMFContext) AmfRanFindByRanID(ranNodeID models.GlobalRanNodeId) (*AmfRan, bool) {
	var ran *AmfRan
	var ok bool

	logger.ContextLog.Infof("Target RAN Node ID - GNbId: %v, MCC: %v, MNC: %v", ranNodeID.GNbId.GNBValue, ranNodeID.PlmnId.Mcc, ranNodeID.PlmnId.Mnc)

	context.AmfRanPool.Range(func(key, value interface{}) bool {
		amfRan := value.(*AmfRan)

		if amfRan == nil {
			logger.ContextLog.Warnf("Found nil AmfRan in pool at key: %v", key)
		} else {
			logger.ContextLog.Infof("AmfRan details - Key: %v, RanId: %+v, RanPresent: %v", key, amfRan.RanId, amfRan.RanPresent)
		}

		if amfRan != nil {
			logger.ContextLog.Infof("AMF gNB Id: %v & AMF name: %v", amfRan.GnbId, amfRan.Name)
			if amfRan.RanId != nil {
				if amfRan.RanId.GNbId != nil {
					logger.ContextLog.Infof("AmfRan in pool - GNbId: %v", amfRan.RanId.GNbId.GNBValue)
				} else {
					logger.ContextLog.Warnf("AmfRan in pool - GNbId is nil")
				}

				if amfRan.RanId.PlmnId != nil {
					logger.ContextLog.Infof("AmfRan in pool - MCC: %v, MNC: %v", amfRan.RanId.PlmnId.Mcc, amfRan.RanId.PlmnId.Mnc)
				} else {
					logger.ContextLog.Warnf("AmfRan in pool - PlmnId is nil")
				}
			} else {
				logger.ContextLog.Warnf("AmfRan in pool - RanId is nil")
			}
		}

		logger.ContextLog.Infof("AMF gNB RanPresent: %v", amfRan.RanPresent)

		switch amfRan.RanPresent {
		case RanPresentGNbId:
			if amfRan.RanId != nil && amfRan.RanId.GNbId != nil {
				if amfRan.RanId.GNbId.GNBValue == ranNodeID.GNbId.GNBValue {
					ran = amfRan
					ok = true
					logger.ContextLog.Infof("amfRan.RanId.GNbId.GNBValue == ranNodeID.GNbId.GNBValue")
					return false
				}
			} else {
				logger.ContextLog.Warnf("AmfRanFindByRanID - amfRan.RanId is nil")
			}
		case RanPresentNgeNbId:
			if amfRan.RanId.NgeNbId == ranNodeID.NgeNbId {
				ran = amfRan
				ok = true
				logger.ContextLog.Infof("amfRan.RanId.NgeNbId == ranNodeID.NgeNbId")
				return false
			}
		case RanPresentN3IwfId:
			if amfRan.RanId.N3IwfId == ranNodeID.N3IwfId {
				ran = amfRan
				ok = true
				logger.ContextLog.Infof("amfRan.RanId.N3IwfId == ranNodeID.N3IwfId")
				return false
			}
		}

		logger.ContextLog.Infof("End of switch amfRan.RanPresent {} - No match in switch")

		if amfRan.RanId != nil && amfRan.RanId.GNbId != nil {
			if amfRan.RanId.GNbId.GNBValue == ranNodeID.GNbId.GNBValue {
				ran = amfRan
				ok = true
				logger.ContextLog.Infof("Fallback match: amfRan.RanId.GNbId.GNBValue == ranNodeID.GNbId.GNBValue")
				return false
			}
		} else if amfRan != nil && ranNodeID.GNbId != nil {
			logger.ContextLog.Infof("Fallback match: Checking with GnbId: %v", amfRan.GnbId[7:])
			if amfRan.GnbId[7:] == ranNodeID.GNbId.GNBValue {
				ran = amfRan
				ok = true
				logger.ContextLog.Infof("Fallback match: amfRan.GnbId == ranNodeID.GNbId.GNBValue")
				return false
			}
		}

		logger.ContextLog.Infof("End of switch amfRan.RanPresent {}")
		return true
	})
	logger.ContextLog.Infof("End of AmfRanFindByRanID()")
	return ran, ok
}

func (context *AMFContext) DeleteAmfRan(conn net.Conn) {
	context.AmfRanPool.Delete(conn)
}

func (context *AMFContext) DeleteAmfRanId(gnbId string) {
	context.AmfRanPool.Delete(gnbId)
}

func (context *AMFContext) InSupportDnnList(targetDnn string) bool {
	for _, dnn := range context.SupportDnnLists {
		if dnn == targetDnn {
			return true
		}
	}
	return false
}

func (context *AMFContext) InPlmnSupportList(snssai models.Snssai) bool {
	for _, plmnSupportItem := range context.PlmnSupportList {
		for _, supportSnssai := range plmnSupportItem.SNssaiList {
			if reflect.DeepEqual(supportSnssai, snssai) {
				return true
			}
		}
	}
	return false
}

func mapToByte(data map[string]interface{}) (ret []byte) {
	ret, err := json.Marshal(data)
	if err != nil {
		logger.ContextLog.Error(err)
	}
	return
}

func (context *AMFContext) AmfUeFindByGutiLocal(guti string) (ue *AmfUe, ok bool) {
	context.UePool.Range(func(key, value interface{}) bool {
		candidate := value.(*AmfUe)
		if ok = (candidate.Guti == guti); ok {
			ue = candidate
			return false
		}
		return true
	})

	return
}

func (context *AMFContext) AmfUeFindBySupiLocal(supi string) (ue *AmfUe, ok bool) {
	context.UePool.Range(func(key, value interface{}) bool {
		candidate := value.(*AmfUe)
		if ok = (candidate.Supi == supi); ok {
			ue = candidate
			return false
		}
		return true
	})

	return
}

func (context *AMFContext) AmfUeFindByGuti(guti string) (ue *AmfUe, ok bool) {
	ue, ok = context.AmfUeFindByGutiLocal(guti)
	if ok {
		logger.ContextLog.Infoln("Guti found locally : ", guti)
	} else if context.EnableDbStore {
		ue, ok = DbFetchUeByGuti(guti)
		if ue != nil && ok {
			logger.ContextLog.Infoln("Ue with Guti found in DB : ", guti)
			context.UePool.Store(ue.Supi, ue)
		} else {
			logger.ContextLog.Infoln("Ue with Guti not found locally and in DB: ", guti)
		}
	} else {
		logger.ContextLog.Infoln("Ue with Guti not found : ", guti)
	}
	return
}

func (context *AMFContext) AmfUeFindByPolicyAssociationID(polAssoId string) (ue *AmfUe, ok bool) {
	context.UePool.Range(func(key, value interface{}) bool {
		candidate := value.(*AmfUe)
		if ok = (candidate.PolicyAssociationId == polAssoId); ok {
			ue = candidate
			return false
		}
		return true
	})
	return
}

func (context *AMFContext) RanUeFindByAmfUeNgapIDLocal(amfUeNgapID int64) *RanUe {
	if value, ok := context.RanUePool.Load(amfUeNgapID); ok {
		return value.(*RanUe)
	} else {
		return nil
	}
}

func (context *AMFContext) RanUeFindByAmfUeNgapID(amfUeNgapID int64) *RanUe {
	ranUe := context.RanUeFindByAmfUeNgapIDLocal(amfUeNgapID)
	if ranUe != nil {
		return ranUe
	} else {
		if context.EnableDbStore {
			ranUe = DbFetchRanUeByAmfUeNgapID(amfUeNgapID)
			if ranUe != nil {
				context.RanUePool.Store(ranUe.AmfUeNgapId, ranUe)
				return ranUe
			}
		}
	}

	logger.ContextLog.Errorf("ranUe not found with AmfUeNgapID")
	return nil
}

func (context *AMFContext) GetIPv4Uri() string {
	return fmt.Sprintf("%s://%s:%d", context.UriScheme, context.RegisterIPv4, context.SBIPort)
}

func (context *AMFContext) InitNFService(serivceName []string, version string) {
	tmpVersion := strings.Split(version, ".")
	versionUri := "v" + tmpVersion[0]
	for index, nameString := range serivceName {
		name := models.ServiceName(nameString)
		context.NfService[name] = models.NfService{
			ServiceInstanceId: strconv.Itoa(index),
			ServiceName:       name,
			Versions: &[]models.NfServiceVersion{
				{
					ApiFullVersion:  version,
					ApiVersionInUri: versionUri,
				},
			},
			Scheme:          context.UriScheme,
			NfServiceStatus: models.NfServiceStatus_REGISTERED,
			ApiPrefix:       context.GetIPv4Uri(),
			IpEndPoints: &[]models.IpEndPoint{
				{
					Ipv4Address: context.RegisterIPv4,
					Transport:   models.TransportProtocol_TCP,
					Port:        int32(context.SBIPort),
				},
			},
		}
	}
}

// Reset AMF Context
func (context *AMFContext) Reset() {
	context.AmfRanPool.Range(func(key, value interface{}) bool {
		context.UePool.Delete(key)
		return true
	})
	for key := range context.LadnPool {
		delete(context.LadnPool, key)
	}
	context.RanUePool.Range(func(key, value interface{}) bool {
		context.RanUePool.Delete(key)
		return true
	})
	context.UePool.Range(func(key, value interface{}) bool {
		context.UePool.Delete(key)
		return true
	})
	context.EventSubscriptions.Range(func(key, value interface{}) bool {
		context.DeleteEventSubscription(key.(string))
		return true
	})
	for key := range context.NfService {
		delete(context.NfService, key)
	}
	context.SupportTaiLists = context.SupportTaiLists[:0]
	context.PlmnSupportList = context.PlmnSupportList[:0]
	context.ServedGuamiList = context.ServedGuamiList[:0]
	context.RelativeCapacity = 0xff
	context.NfId = ""
	context.UriScheme = models.UriScheme_HTTPS
	context.SBIPort = 0
	context.BindingIPv4 = ""
	context.RegisterIPv4 = ""
	context.HttpIPv6Address = ""
	context.Name = "amf"
	context.NrfUri = ""
}

// Create new AMF context
func AMF_Self() *AMFContext {
	return &amfContext
}
