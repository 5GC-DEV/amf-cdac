// SPDX-FileCopyrightText: 2021 Open Networking Foundation <info@opennetworking.org>
// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package consumer

import (
	"context"
	"time"

	"github.com/antihax/optional"
	amf_context "github.com/omec-project/amf/context"
	"github.com/omec-project/openapi"
	"github.com/omec-project/openapi/Nudm_SubscriberDataManagement"
	"github.com/omec-project/openapi/models"
)

func PutUpuAck(ue *amf_context.AmfUe, upuMacIue string) error {
	configuration := Nudm_SubscriberDataManagement.NewConfiguration()
	configuration.SetBasePath(ue.NudmSDMUri)
	client := Nudm_SubscriberDataManagement.NewAPIClient(configuration)

	ackInfo := models.AcknowledgeInfo{
		UpuMacIue: upuMacIue,
	}
	upuOpt := Nudm_SubscriberDataManagement.PutUpuAckParamOpts{
		AcknowledgeInfo: optional.NewInterface(ackInfo),
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	_, err := client.ProvidingAcknowledgementOfUEParametersUpdateApi.PutUpuAck(ctx, ue.Supi, &upuOpt)
	return err
}

func SDMGetAmData(ue *amf_context.AmfUe) (problemDetails *models.ProblemDetails, err error) {
	configuration := Nudm_SubscriberDataManagement.NewConfiguration()
	configuration.SetBasePath(ue.NudmSDMUri)
	client := Nudm_SubscriberDataManagement.NewAPIClient(configuration)

	getAmDataParamOpt := Nudm_SubscriberDataManagement.GetAmDataParamOpts{
		PlmnId: optional.NewInterface(ue.PlmnId.Mcc + ue.PlmnId.Mnc),
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()

	data, httpResp, localErr := client.AccessAndMobilitySubscriptionDataRetrievalApi.GetAmData(
		ctx, ue.Supi, &getAmDataParamOpt)
	if localErr == nil {
		ue.AccessAndMobilitySubscriptionData = &data
		ue.Gpsi = data.Gpsis[0] // TODO: select GPSI
	} else if httpResp != nil {
		if httpResp.Status != localErr.Error() {
			err = localErr
			return
		}
		problem := localErr.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails)
		problemDetails = &problem
	} else {
		err = openapi.ReportError("server no response")
	}
	return
}

func SDMGetSmfSelectData(ue *amf_context.AmfUe) (problemDetails *models.ProblemDetails, err error) {
	configuration := Nudm_SubscriberDataManagement.NewConfiguration()
	configuration.SetBasePath(ue.NudmSDMUri)
	client := Nudm_SubscriberDataManagement.NewAPIClient(configuration)

	paramOpt := Nudm_SubscriberDataManagement.GetSmfSelectDataParamOpts{
		PlmnId: optional.NewInterface(ue.PlmnId.Mcc + ue.PlmnId.Mnc),
	}
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	data, httpResp, localErr := client.SMFSelectionSubscriptionDataRetrievalApi.GetSmfSelectData(ctx, ue.Supi, &paramOpt)
	if localErr == nil {
		ue.SmfSelectionData = &data
	} else if httpResp != nil {
		if httpResp.Status != localErr.Error() {
			err = localErr
			return
		}
		problem := localErr.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails)
		problemDetails = &problem
	} else {
		err = openapi.ReportError("server no response")
	}

	return
}

func SDMGetUeContextInSmfData(ue *amf_context.AmfUe) (problemDetails *models.ProblemDetails, err error) {
	configuration := Nudm_SubscriberDataManagement.NewConfiguration()
	configuration.SetBasePath(ue.NudmSDMUri)
	client := Nudm_SubscriberDataManagement.NewAPIClient(configuration)
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()

	data, httpResp, localErr := client.UEContextInSMFDataRetrievalApi.GetUeContextInSmfData(ctx, ue.Supi, nil)
	if localErr == nil {
		ue.UeContextInSmfData = &data
	} else if httpResp != nil {
		if httpResp.Status != localErr.Error() {
			err = localErr
			return
		}
		problem := localErr.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails)
		problemDetails = &problem
	} else {
		err = openapi.ReportError("server no response")
	}

	return
}

func SDMSubscribe(ue *amf_context.AmfUe) (problemDetails *models.ProblemDetails, err error) {
	configuration := Nudm_SubscriberDataManagement.NewConfiguration()
	configuration.SetBasePath(ue.NudmSDMUri)
	client := Nudm_SubscriberDataManagement.NewAPIClient(configuration)

	amfSelf := amf_context.AMF_Self()
	sdmSubscription := models.SdmSubscription{
		NfInstanceId: amfSelf.NfId,
		PlmnId:       &ue.PlmnId,
	}
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()

	_, httpResp, localErr := client.SubscriptionCreationApi.Subscribe(ctx, ue.Supi, sdmSubscription)
	if localErr == nil {
		return
	} else if httpResp != nil {
		if httpResp.Status != localErr.Error() {
			err = localErr
			return
		}
		problem := localErr.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails)
		problemDetails = &problem
	} else {
		err = openapi.ReportError("server no response")
	}
	return
}

func SDMGetSliceSelectionSubscriptionData(ue *amf_context.AmfUe) (problemDetails *models.ProblemDetails, err error) {
	configuration := Nudm_SubscriberDataManagement.NewConfiguration()
	configuration.SetBasePath(ue.NudmSDMUri)
	client := Nudm_SubscriberDataManagement.NewAPIClient(configuration)

	paramOpt := Nudm_SubscriberDataManagement.GetNssaiParamOpts{
		PlmnId: optional.NewInterface(ue.PlmnId.Mcc + ue.PlmnId.Mnc),
	}
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()
	/*nssai, httpResp, localErr := client.SliceSelectionSubscriptionDataRetrievalApi.GetNssai(ctx, ue.Supi, &paramOpt)
	if localErr == nil {
		for _, defaultSnssai := range nssai.DefaultSingleNssais {
			subscribedSnssai := models.SubscribedSnssai{
				SubscribedSnssai: &models.Snssai{
					Sst: defaultSnssai.Sst,
					Sd:  defaultSnssai.Sd,
				},
				DefaultIndication: true,
			}
			ue.SubscribedNssai = append(ue.SubscribedNssai, subscribedSnssai)
		}
		for _, snssai := range nssai.SingleNssais {
			subscribedSnssai := models.SubscribedSnssai{
				SubscribedSnssai: &models.Snssai{
					Sst: snssai.Sst,
					Sd:  snssai.Sd,
				},
				DefaultIndication: false,
			}
			ue.SubscribedNssai = append(ue.SubscribedNssai, subscribedSnssai)
		}
		ue.GmmLog.Infof("Final SubscribedNssai for SUPI[%s]: %+v", ue.Supi, ue.SubscribedNssai)*/
	nssai, httpResp, localErr := client.SliceSelectionSubscriptionDataRetrievalApi.GetNssai(ctx, ue.Supi, &paramOpt)
	if localErr == nil {
		// Log raw UDM response
		ue.GmmLog.Infof("UDM DefaultSingleNssais: %+v", nssai.DefaultSingleNssais)
		ue.GmmLog.Infof("UDM SingleNssais: %+v", nssai.SingleNssais)

		// Add slices into ue.SubscribedNssai
		for _, defaultSnssai := range nssai.DefaultSingleNssais {
			ue.GmmLog.Infof("Adding default slice: SST=%d SD=%s", defaultSnssai.Sst, defaultSnssai.Sd)
			subscribedSnssai := models.SubscribedSnssai{
				SubscribedSnssai: &models.Snssai{
					Sst: defaultSnssai.Sst,
					Sd:  defaultSnssai.Sd,
				},
				DefaultIndication: true,
			}
			ue.SubscribedNssai = append(ue.SubscribedNssai, subscribedSnssai)
		}

		for _, snssai := range nssai.SingleNssais {
			ue.GmmLog.Infof("Adding non-default slice: SST=%d SD=%s", snssai.Sst, snssai.Sd)
			subscribedSnssai := models.SubscribedSnssai{
				SubscribedSnssai: &models.Snssai{
					Sst: snssai.Sst,
					Sd:  snssai.Sd,
				},
				DefaultIndication: false,
			}
			ue.SubscribedNssai = append(ue.SubscribedNssai, subscribedSnssai)
		}

		ue.GmmLog.Infof("Final SubscribedNssai for SUPI[%s]: %+v", ue.Supi, ue.SubscribedNssai)
	} else if httpResp != nil {
		if httpResp.Status != localErr.Error() {
			err = localErr
			return problemDetails, err
		}
		problem := localErr.(openapi.GenericOpenAPIError).Model().(models.ProblemDetails)
		problemDetails = &problem
	} else {
		err = openapi.ReportError("Could not contact UDM at %v, %+v", ue.NudmSDMUri, localErr)
	}
	return problemDetails, err
}
