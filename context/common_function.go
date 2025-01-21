// Copyright 2019 free5GC.org
//
// SPDX-License-Identifier: Apache-2.0
//

package context

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/mohae/deepcopy"
	"github.com/omec-project/amf/logger"
	"github.com/omec-project/openapi/models"
)

func CompareUserLocation(loc1 models.UserLocation, loc2 models.UserLocation) bool {
	if loc1.EutraLocation != nil && loc2.EutraLocation != nil {
		eutraloc1 := deepcopy.Copy(*loc1.EutraLocation).(models.EutraLocation)
		eutraloc2 := deepcopy.Copy(*loc2.EutraLocation).(models.EutraLocation)
		eutraloc1.UeLocationTimestamp = nil
		eutraloc2.UeLocationTimestamp = nil
		return reflect.DeepEqual(eutraloc1, eutraloc2)
	}
	if loc1.N3gaLocation != nil && loc2.N3gaLocation != nil {
		return reflect.DeepEqual(loc1, loc2)
	}
	if loc1.NrLocation != nil && loc2.NrLocation != nil {
		nrloc1 := deepcopy.Copy(*loc1.NrLocation).(models.NrLocation)
		nrloc2 := deepcopy.Copy(*loc2.NrLocation).(models.NrLocation)
		nrloc1.UeLocationTimestamp = nil
		nrloc2.UeLocationTimestamp = nil
		return reflect.DeepEqual(nrloc1, nrloc2)
	}

	return false
}

func InTaiList(servedTai models.Tai, taiList []models.Tai) bool {
	for _, tai := range taiList {
		if reflect.DeepEqual(tai, servedTai) {
			return true
		}
	}
	return false
}

// Added Function for Comparing the plmn list from RAN and the CORE for the proper RanConfiguration update procedure
func InPlmnList(gnbplmnlist []interface{}, amfplmnlist []interface{}) bool {
	return reflect.DeepEqual(gnbplmnlist, amfplmnlist)
}

// Added Function for Comparing the Slice list from RAN and the CORE for the proper NgSetup update procedure
func InSliceList(gnbslicelist [][]interface{}, amfslicelist [][]interface{}) bool {
	for _, gnbItem := range gnbslicelist {
		for _, amfItem := range amfslicelist {
			if reflect.DeepEqual(gnbItem, amfItem) {
				return true
			}
		}
	}
	return false
}

// Function for converting hexstring to octalbytes
func ConvertHexToOctalbytes(hexstring string) []byte {
	if len(hexstring)%2 != 0 {
		fmt.Println("hexstring should have an even length")
		return nil
	}

	var sdlistgnb []byte
	for i := 0; i < len(hexstring); i += 2 {
		hexPair := hexstring[i : i+2]

		// Convert hex pair to an integer
		intValue, err := strconv.ParseInt(hexPair, 16, 64)
		if err != nil {
			fmt.Println("error in parsing hex pair:", hexPair)
			return nil
		}

		// Convert integer to octal string
		octalString := strconv.FormatInt(intValue, 8)

		// Convert octal string to integer
		octalValue, err := strconv.ParseInt(octalString, 8, 64)
		if err != nil {
			fmt.Println("error in parsing octal string:", octalString)
			return nil
		}

		// Append the byte value of octal to the list
		sdlistgnb = append(sdlistgnb, byte(octalValue))
	}

	return sdlistgnb
}

func TacInAreas(targetTac string, areas []models.Area) bool {
	for _, area := range areas {
		for _, tac := range area.Tacs {
			if targetTac == tac {
				return true
			}
		}
	}
	return false
}

func AttachSourceUeTargetUe(sourceUe, targetUe *RanUe) {
	if sourceUe == nil {
		logger.ContextLog.Error("Source Ue is Nil")
		return
	}
	if targetUe == nil {
		logger.ContextLog.Error("Target Ue is Nil")
		return
	}
	amfUe := sourceUe.AmfUe
	if amfUe == nil {
		logger.ContextLog.Error("AmfUe is Nil")
		return
	}
	targetUe.AmfUe = amfUe
	targetUe.SourceUe = sourceUe
	sourceUe.TargetUe = targetUe
}

func DetachSourceUeTargetUe(ranUe *RanUe) {
	if ranUe == nil {
		logger.ContextLog.Error("ranUe is Nil")
		return
	}
	if ranUe.TargetUe != nil {
		targetUe := ranUe.TargetUe

		ranUe.TargetUe = nil
		targetUe.SourceUe = nil
	} else if ranUe.SourceUe != nil {
		source := ranUe.SourceUe

		ranUe.SourceUe = nil
		source.TargetUe = nil
	}
}
