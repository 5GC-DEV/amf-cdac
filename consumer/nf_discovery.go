// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	amf_context "github.com/omec-project/amf/context"
	"github.com/omec-project/amf/logger"
	"github.com/omec-project/amf/util"
	"github.com/omec-project/openapi/Nnrf_NFDiscovery"
	"github.com/omec-project/openapi/models"
	nrfCache "github.com/omec-project/openapi/nrfcache"
)

func SendSearchNFInstances(nrfUri string, targetNfType, requestNfType models.NfType,
	param *Nnrf_NFDiscovery.SearchNFInstancesParamOpts,
) (models.SearchResult, error) {
	logger.ConsumerLog.Infof("**** Initiating NF Instance search to NRF[%s] with target NF type: %s, request NF type: %s",
		nrfUri, targetNfType, requestNfType)
	if amf_context.AMF_Self().EnableNrfCaching {
		logger.ConsumerLog.Infof("**** NRF caching is enabled. Searching NF instances using cache.")
		return nrfCache.SearchNFInstances(nrfUri, targetNfType, requestNfType, param)
	} else {
		logger.ConsumerLog.Infof("**** NRF caching is disabled. Sending direct NF discovery request to NRF.")
		return SendNfDiscoveryToNrf(nrfUri, targetNfType, requestNfType, param)
	}
}

func SendNfDiscoveryToNrf(nrfUri string, targetNfType, requestNfType models.NfType,
	param *Nnrf_NFDiscovery.SearchNFInstancesParamOpts,
) (models.SearchResult, error) {
	logger.ConsumerLog.Infof("*** Starting NF discovery to NRF URI: %s", nrfUri)
	logger.ConsumerLog.Infof("*** Target NF Type: %s, Request NF Type: %s", targetNfType, requestNfType)
	logger.ConsumerLog.Infof("*** Search parameters: %+v", param)
	// Set client and set url
	configuration := Nnrf_NFDiscovery.NewConfiguration()
	configuration.SetBasePath(nrfUri)
	client := Nnrf_NFDiscovery.NewAPIClient(configuration)
	logger.ConsumerLog.Infof("*** Sending SearchNFInstances request to NRF...")
	result, res, err := client.NFInstancesStoreApi.SearchNFInstances(context.TODO(), targetNfType, requestNfType, param)
	if res != nil && res.StatusCode == http.StatusTemporaryRedirect {
		logger.ConsumerLog.Infof("*** Received response with status code: %d", res.StatusCode)
		err = fmt.Errorf("temporary Redirect For Non NRF Consumer")
	}
	resultJSON, jsonErr := json.MarshalIndent(result, "", "  ")
	if jsonErr != nil {
		logger.ConsumerLog.Warnln("*** Error marshalling result to JSON:", jsonErr)
	} else {
		logger.ConsumerLog.Infof("*** Result details: %s", resultJSON)
	}
	defer func() {
		if bodyCloseErr := res.Body.Close(); bodyCloseErr != nil {
			err = fmt.Errorf("SearchNFInstances' response body cannot close: %+w", bodyCloseErr)
			logger.ConsumerLog.Errorf("***  Error closing response body: %+v", bodyCloseErr)
		}
	}()

	amfSelf := amf_context.AMF_Self()
	logger.ConsumerLog.Infof("**** Checking if the AMF has active NF status subscriptions")
	var nrfSubData models.NrfSubscriptionData
	var problemDetails *models.ProblemDetails
	for _, nfProfile := range result.NfInstances {
		logger.ConsumerLog.Infof("*** Processing NF instance ID: %s", nfProfile.NfInstanceId)
		// checking whether the AMF subscribed to this target nfinstanceid or not
		if _, ok := amfSelf.NfStatusSubscriptions.Load(nfProfile.NfInstanceId); !ok {
			logger.ConsumerLog.Infof("**** No active subscription found for NF instance ID: %s. Creating subscription...", nfProfile.NfInstanceId)
			nrfSubscriptionData := models.NrfSubscriptionData{
				NfStatusNotificationUri: fmt.Sprintf("%s/namf-callback/v1/nf-status-notify", amfSelf.GetIPv4Uri()),
				SubscrCond:              &models.NfInstanceIdCond{NfInstanceId: nfProfile.NfInstanceId},
				ReqNfType:               requestNfType,
			}
			nrfSubData, problemDetails, err = SendCreateSubscription(nrfUri, nrfSubscriptionData)
			if problemDetails != nil {
				logger.ConsumerLog.Errorf("SendCreateSubscription to NRF, Problem[%+v]", problemDetails)
			} else if err != nil {
				logger.ConsumerLog.Errorf("SendCreateSubscription Error[%+v]", err)
			}
			amfSelf.NfStatusSubscriptions.Store(nfProfile.NfInstanceId, nrfSubData.SubscriptionId)
		} else {
			logger.ConsumerLog.Infof("**** Active subscription already exists for NF instance ID: %s", nfProfile.NfInstanceId)
		}
	}
	logger.ConsumerLog.Infof("***  NF discovery completed. Returning results.")
	return result, err
}

func SearchUdmSdmInstance(ue *amf_context.AmfUe, nrfUri string, targetNfType, requestNfType models.NfType,
	param *Nnrf_NFDiscovery.SearchNFInstancesParamOpts,
) error {
	resp, localErr := SendSearchNFInstances(nrfUri, targetNfType, requestNfType, param)
	if localErr != nil {
		return localErr
	}

	// select the first UDM_SDM, TODO: select base on other info
	var sdmUri string
	for _, nfProfile := range resp.NfInstances {
		ue.UdmId = nfProfile.NfInstanceId
		sdmUri = util.SearchNFServiceUri(nfProfile, models.ServiceName_NUDM_SDM, models.NfServiceStatus_REGISTERED)
		if sdmUri != "" {
			break
		}
	}
	ue.NudmSDMUri = sdmUri
	if ue.NudmSDMUri == "" {
		err := fmt.Errorf("AMF can not select an UDM by NRF")
		logger.ConsumerLog.Errorln(err.Error())
		return err
	}
	return nil
}

func SearchNssfNSSelectionInstance(ue *amf_context.AmfUe, nrfUri string, targetNfType, requestNfType models.NfType,
	param *Nnrf_NFDiscovery.SearchNFInstancesParamOpts,
) error {
	resp, localErr := SendSearchNFInstances(nrfUri, targetNfType, requestNfType, param)
	if localErr != nil {
		return localErr
	}

	// select the first NSSF, TODO: select base on other info
	var nssfUri string
	for _, nfProfile := range resp.NfInstances {
		ue.NssfId = nfProfile.NfInstanceId
		nssfUri = util.SearchNFServiceUri(nfProfile, models.ServiceName_NNSSF_NSSELECTION, models.NfServiceStatus_REGISTERED)
		if nssfUri != "" {
			break
		}
	}
	ue.NssfUri = nssfUri
	if ue.NssfUri == "" {
		return fmt.Errorf("AMF can not select an NSSF by NRF")
	}
	return nil
}

func SearchAmfCommunicationInstance(ue *amf_context.AmfUe, nrfUri string, targetNfType,
	requestNfType models.NfType, param *Nnrf_NFDiscovery.SearchNFInstancesParamOpts,
) (err error) {
	resp, localErr := SendSearchNFInstances(nrfUri, targetNfType, requestNfType, param)
	if localErr != nil {
		err = localErr
		return
	}

	// select the first AMF, TODO: select base on other info
	var amfUri string
	for _, nfProfile := range resp.NfInstances {
		ue.TargetAmfProfile = &nfProfile
		amfUri = util.SearchNFServiceUri(nfProfile, models.ServiceName_NAMF_COMM, models.NfServiceStatus_REGISTERED)
		if amfUri != "" {
			break
		}
	}
	ue.TargetAmfUri = amfUri
	if ue.TargetAmfUri == "" {
		err = fmt.Errorf("AMF can not select an target AMF by NRF")
	}
	return
}
