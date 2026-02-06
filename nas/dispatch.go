// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package nas

import (
	"errors"
	"fmt"

	"github.com/5GC-DEV/nas-cdac"
	"github.com/5GC-DEV/openapi-cdac/models"
	"github.com/5GC-DEV/util-cdac/fsm"
	"github.com/omec-project/amf/context"
	"github.com/omec-project/amf/gmm"
)

func Dispatch(ue *context.AmfUe, accessType models.AccessType, procedureCode int64, msg *nas.Message) error {
	if msg.GmmMessage == nil {
		return errors.New("gmm message is nil")
	}

	if msg.GsmMessage != nil {
		return errors.New("GSM Message should include in GMM Message")
	}

	ue.StateMu.RLock()
	if ue.State[accessType] == nil {
		return fmt.Errorf("UE State is empty (accessType=%q). Can't send GSM Message", accessType)
	}
	ue.StateMu.RUnlock()

	return gmm.GmmFSM.SendEvent(ue.State[accessType], gmm.GmmMessageEvent, fsm.ArgsType{
		gmm.ArgAmfUe:         ue,
		gmm.ArgAccessType:    accessType,
		gmm.ArgNASMessage:    msg.GmmMessage,
		gmm.ArgProcedureCode: procedureCode,
	})
}
