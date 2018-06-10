package main

import (
	"encoding/json"
	"log"
	"strconv"

	"github.com/thomas-bamilo/emarsysapicontact/apicall"
	"github.com/thomas-bamilo/emarsysapicontact/customerrow"
	"github.com/thomas-bamilo/emarsysapicontact/dbinteract"
	"github.com/thomas-bamilo/sql/connectdb"
)

func main() {

	// define map to store BOB customer data
	bobEmailMap := make(map[string]customerrow.CustomerRow)

	// connect to BOB database
	dbBob := connectdb.ConnectToBob()
	defer dbBob.Close()

	// fill map with BOB customer data
	dbinteract.GetBobEmailMap(dbBob, bobEmailMap)

	// define map to store BI customer data
	mainMap := make(map[string]customerrow.CustomerRow)
	lastCancelReasonMap := make(map[string]customerrow.CustomerRow)
	lastBadCancelReasonMap := make(map[string]customerrow.CustomerRow)
	lastReturnReasonMap := make(map[string]customerrow.CustomerRow)
	lastBadReturnReasonMap := make(map[string]customerrow.CustomerRow)
	lastOrderStatusMap := make(map[string]customerrow.CustomerRow)
	hourOfDayMap := make(map[string]customerrow.CustomerRow)
	dayOfWeekMap := make(map[string]customerrow.CustomerRow)
	dayOfMonthMap := make(map[string]customerrow.CustomerRow)
	monthOfYearMap := make(map[string]customerrow.CustomerRow)

	// connect to BI database
	dbBi := connectdb.ConnectToBi()
	defer dbBi.Close()

	// fill map with BI customer data
	log.Println(`MainBiQuery`)
	dbinteract.MainBiQuery(dbBi, mainMap)
	log.Println(`LastCancelReasonQuery`)
	dbinteract.LastCancelReasonQuery(dbBi, lastCancelReasonMap)
	log.Println(`LastBadCancelReasonQuery`)
	dbinteract.LastBadCancelReasonQuery(dbBi, lastBadCancelReasonMap)
	log.Println(`LastReturnReasonQuery`)
	dbinteract.LastReturnReasonQuery(dbBi, lastReturnReasonMap)
	log.Println(`LastBadReturnReasonQuery`)
	dbinteract.LastBadReturnReasonQuery(dbBi, lastBadReturnReasonMap)
	log.Println(`LastOrderStatusQuery`)
	dbinteract.LastOrderStatusQuery(dbBi, lastOrderStatusMap)
	log.Println(`HourOfDayQuery`)
	dbinteract.HourOfDayQuery(dbBi, hourOfDayMap)
	log.Println(`DayOfWeekQuery`)
	dbinteract.DayOfWeekQuery(dbBi, dayOfWeekMap)
	log.Println(`DayOfMonthQuery`)
	dbinteract.DayOfMonthQuery(dbBi, dayOfMonthMap)
	log.Println(`MonthOfYearQuery`)
	dbinteract.MonthOfYearQuery(dbBi, monthOfYearMap)

	// join all map together into one customerTable
	log.Println(`JoinQuery`)
	customerTable := dbinteract.JoinQuery(mainMap,
		lastCancelReasonMap,
		lastBadCancelReasonMap,
		lastReturnReasonMap,
		lastBadReturnReasonMap,
		lastOrderStatusMap,
		hourOfDayMap,
		dayOfWeekMap,
		dayOfMonthMap,
		monthOfYearMap,
		bobEmailMap,
	)

	log.Println(`DivideCustomerTableInChunk`)
	arrayOfCustomerTableChunk := customerrow.DivideCustomerTableInChunk(customerTable)

	log.Println(`Number of batches: ` + strconv.Itoa(len(arrayOfCustomerTableChunk)))
	for i := 0; i < len(arrayOfCustomerTableChunk); i++ {

		log.Println(`Batch number: ` + strconv.Itoa(i))
		// marshal each chunk of customerTable to JSON
		partialCustomerTableJSON, err := json.Marshal(arrayOfCustomerTableChunk[i])
		checkError(err)

		// stringify partialCustomerTableJSON and add additional information
		partialCustomerTableJSONStr := `{"key_id": "3", "contacts": ` + string(partialCustomerTableJSON) + `}`

		// call API to upload customer data to Emarsys
		apicall.ApiCall(partialCustomerTableJSONStr)

	}

	log.Println(`End`)

}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
