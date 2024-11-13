// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package consumer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/antihax/optional"
	amf_context "github.com/omec-project/amf/context"
	"github.com/omec-project/amf/logger"
	"github.com/omec-project/openapi"
	"github.com/omec-project/openapi/Nnssf_NSSelection"
	"github.com/omec-project/openapi/models"
)

func NSSelectionGetForRegistration(ue *amf_context.AmfUe, requestedNssai []models.MappingOfSnssai) (
	*models.ProblemDetails, error,
) {
	configuration := Nnssf_NSSelection.NewConfiguration()
	configuration.SetBasePath(ue.NssfUri)
	client := Nnssf_NSSelection.NewAPIClient(configuration)

	amfSelf := amf_context.AMF_Self()
	sliceInfo := models.SliceInfoForRegistration{
		SubscribedNssai: ue.SubscribedNssai,
	}

	for _, snssai := range requestedNssai {
		sliceInfo.RequestedNssai = append(sliceInfo.RequestedNssai, *snssai.ServingSnssai)
		if snssai.HomeSnssai != nil {
			sliceInfo.MappingOfNssai = append(sliceInfo.MappingOfNssai, snssai)
		}
	}

	var paramOpt Nnssf_NSSelection.NSSelectionGetParamOpts
	if e, err := json.Marshal(sliceInfo); err != nil {
		logger.ConsumerLog.Warnf("json marshal failed: %+v", err)
	} else {
		paramOpt = Nnssf_NSSelection.NSSelectionGetParamOpts{
			SliceInfoRequestForRegistration: optional.NewInterface(string(e)),
		}
	}
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	res, httpResp, localErr := client.NetworkSliceInformationDocumentApi.NSSelectionGet(ctx,
		models.NfType_AMF, amfSelf.NfId, &paramOpt)
	if localErr == nil {
		ue.NetworkSliceInfo = &res
		for _, allowedNssai := range res.AllowedNssaiList {
			ue.AllowedNssai[allowedNssai.AccessType] = allowedNssai.AllowedSnssaiList
		}
		ue.ConfiguredNssai = res.ConfiguredNssai
	} else if httpResp != nil {
		if httpResp.Status != localErr.Error() {
			err := localErr
			return nil, err
		}
		problem := localErr.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails)
		return &problem, nil
	} else {
		return nil, openapi.ReportError("NSSF No Response")
	}

	return nil, nil
}

func NSSelectionGetForPduSession(ue *amf_context.AmfUe, snssai models.Snssai) (
	*models.AuthorizedNetworkSliceInfo, *models.ProblemDetails, error,
) {
	ue.GmmLog.Infof("**** Starting NS Selection for PDU Session. SNSSAI: %+v", snssai)
	configuration := Nnssf_NSSelection.NewConfiguration()
	configuration.SetBasePath(ue.NssfUri)
	client := Nnssf_NSSelection.NewAPIClient(configuration)

	amfSelf := amf_context.AMF_Self()
	sliceInfoForPduSession := models.SliceInfoForPduSession{
		SNssai:            &snssai,
		RoamingIndication: models.RoamingIndication_NON_ROAMING, // not support roaming
	}

	e, err := json.Marshal(sliceInfoForPduSession)
	if err != nil {
		logger.ConsumerLog.Warnf("json marshal failed: %+v", err)
	}
	paramOpt := Nnssf_NSSelection.NSSelectionGetParamOpts{
		SliceInfoRequestForPduSession: optional.NewInterface(string(e)),
	}
	ue.GmmLog.Infof("****  Sending NSSelection request to NSSF: [%s]", ue.NssfUri)
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	res, httpResp, localErr := client.NetworkSliceInformationDocumentApi.NSSelectionGet(ctx,
		models.NfType_AMF, amfSelf.NfId, &paramOpt)
	if localErr == nil {
		ue.GmmLog.Infof("*** NS Selection succeeded. Received response: %+v", res)
		return &res, nil, nil
	} else if httpResp != nil {
		ue.GmmLog.Errorf("****  NS Selection failed. HTTP Status: %s, Error: %+v", httpResp.Status, localErr)
		if httpResp.Status != localErr.Error() {
			ue.GmmLog.Warnf("*** HTTP status mismatch. Returning error without ProblemDetails.")
			return nil, nil, localErr
		}
		problem := localErr.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails)
		ue.GmmLog.Errorf("**** ProblemDetails received: %+v", problem)
		return nil, &problem, nil
	} else {
		ue.GmmLog.Errorf("****  NSSF did not respond. No HTTP response received.")
		return nil, nil, openapi.ReportError("NSSF No Response")
	}
}
