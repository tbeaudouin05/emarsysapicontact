package dbinteract

import (
	"database/sql"
	"log"
	"strconv"

	"github.com/thomas-bamilo/emarsysapicontact/customerrow"
)

// GetBobEmailMap maps id_customer to email for each customer - NB: Emarsys data is joined on customer email, not id_customer
func GetBobEmailMap(dbBob *sql.DB, bobEmailMap map[string]customerrow.CustomerRow) {
	log.Println("getBobEmailMap")
	// query BOB database to retrieve id_customer and customer_email
	rows, err := dbBob.Query(`
		SELECT 
		c.id_customer
		,c.email 
		FROM customer c
		LEFT JOIN sales_order so
		ON so.fk_customer = c.id_customer
		WHERE so.created_at >= DATE_SUB(NOW(), INTERVAL 90 DAY);
		  `)
	checkError(err)
	defer rows.Close()

	var iDCustomer string
	var customerRow customerrow.CustomerRow

	// for each row of the retrieved customer data, change it to JSON and call Emarsys API
	for rows.Next() {

		err := rows.Scan(
			&iDCustomer,
			&customerRow.Email,
		)
		checkError(err)

		bobEmailMap[iDCustomer] = customerRow

	}
}

// MainBiQuery gets customer data from BI database which does not require the function ROW_NUMBER() OVER(PARTITION BY...
func MainBiQuery(dbBi *sql.DB, mainMap map[string]customerrow.CustomerRow) {

	// query BI database to retrieve customer data
	rows, err := dbBi.Query(`SELECT
		g_il.customer_id AS '4819' 
		,'' AS '18124' -- lci.cluster_id
		,COALESCE(CONVERT(char(10), GetDate(),126),'') AS '18125' 
		,COALESCE(g_il.avg_item_price,'') AS '18126' 
		,COALESCE(g_il.avg_basket_size,'') AS '18130' 
		,COALESCE(CONVERT(char(10), g_il.last_refund_reject_date,126),'') AS '18139' 
		,'' AS '18140' -- last_delivered_date
		,COALESCE(CONVERT(char(10), g_il.last_voucher_date,126),'') AS '18142' 
		,'' AS '18143' -- last_voucher_type
		,COALESCE(CONVERT(char(10), g_il.last_cart_rule_date,126),'') AS '18144'
		,COALESCE(g_il.refund_reject_ratio,'') AS '18145' 
		,COALESCE(g_il.order_voucher_ratio,'') AS '18146' 
		,COALESCE(g_il.order_cart_rule_ratio,'') AS '18147' 
		,'' AS '18148' -- main_voucher_type
		,COALESCE(CONVERT(char(10), g_il.last_ship_broken_sla_date,126),'') AS '18149' 
		,COALESCE(CONVERT(char(10), g_il.last_refund_broken_sla_date,126),'') AS '18152' 
		,COALESCE(g_il.bad_cancellation_ratio,'') AS '18151' 
		,COALESCE(g_il.bad_return_ratio,'') AS '18153'
		,COALESCE(g_il.ship_broken_sla_ratio,'') AS '18154' 
		,COALESCE(g_il.refund_broken_sla_ratio,'') AS '18155' 
		,COALESCE(g_il.order_count,'')
	  
		FROM
	  
	  (	  
	  SELECT  
		 il.customer_id AS 'customer_id'
		,SUM(il.unit_price) / COUNT(ALL il.id_sales_order_item) AS 'avg_item_price'
		,COUNT(ALL il.id_sales_order_item) / COUNT(DISTINCT il.order_nr) AS 'avg_basket_size'
		,MAX(il.refund_reject_at) AS  'last_refund_reject_date'
		,MAX(CASE WHEN il.coupon_money_value = 0 THEN NULL ELSE il.created_at END) AS 'last_voucher_date'
		,MAX(CASE WHEN il.cart_rule_discount = 0 THEN NULL ELSE il.created_at END) AS 'last_cart_rule_date'
		,CAST(COUNT(ALL il.canceled_at) AS DECIMAL(7,1))/COUNT(ALL il.id_sales_order_item) AS 'cancel_ratio'
		,CAST(COUNT(ALL il.returned_at) AS DECIMAL(7,1))/COUNT(ALL il.id_sales_order_item) AS 'return_ratio'
		,CAST(COUNT(ALL il.refund_reject_at) AS DECIMAL(7,1))/COUNT(ALL il.id_sales_order_item) AS 'refund_reject_ratio'
		,CAST(COUNT(DISTINCT CASE WHEN il.coupon_money_value = 0 THEN NULL ELSE il.order_nr END) AS DECIMAL(7,1))/COUNT(DISTINCT il.order_nr) AS 'order_voucher_ratio'
		,CAST(COUNT(DISTINCT CASE WHEN il.cart_rule_discount = 0 THEN NULL ELSE il.order_nr END) AS DECIMAL(7,1))/COUNT(DISTINCT il.order_nr) AS 'order_cart_rule_ratio'
		,NULL AS 'main_voucher_type'
		,MAX(CASE WHEN il.is_ship_broken_sla = 1 THEN DATEADD(day,3,il.created_at)
				  ELSE NULL END) AS 'last_ship_broken_sla_date'
		,MAX(CASE WHEN il.is_refund_broken_sla = 1 THEN DATEADD(day,7,il.ready_for_refund_at)
				  ELSE NULL END) AS 'last_refund_broken_sla_date'
		,CAST(COUNT(CASE WHEN il.is_bad_cancel_reason = 1 THEN il.id_sales_order_item ELSE NULL END) AS DECIMAL(7,1)) / COUNT(il.id_sales_order_item) AS 'bad_cancellation_ratio'
		,CAST(COUNT(CASE WHEN il.is_bad_return_reason = 1 THEN il.id_sales_order_item ELSE NULL END) AS DECIMAL(7,1)) / COUNT(il.id_sales_order_item) AS 'bad_return_ratio'
		,CAST(COUNT(CASE WHEN il.is_ship_broken_sla = 1 THEN il.id_sales_order_item ELSE NULL END) AS DECIMAL(7,1)) / COUNT(il.id_sales_order_item) AS 'ship_broken_sla_ratio'
		,CAST(COUNT(CASE WHEN il.is_refund_broken_sla = 1 THEN il.id_sales_order_item ELSE NULL END) AS DECIMAL(7,1)) / COUNT(il.id_sales_order_item) AS 'refund_broken_sla_ratio'
		,COUNT(DISTINCT il.order_nr) AS  'order_count'
	,MAX(il.created_at) AS  'last_order_date'
	  
	   FROM
	  
	  (SELECT 
	  
		si.id_sales_order_item
		,so.customer_id
		,so.status
		,si.ready_for_refund_at
		,si.shipped_at
		,si.canceled_at
		,si.returned_at
		,si.created_at
		,so.order_nr
		,si.unit_price
		,si.coupon_money_value
		,si.cart_rule_discount
		,si.refund_reject_at
	  
		,CASE WHEN DATEDIFF(HOUR,si.created_at,GETDATE()) / 24 > 3 AND si.shipped_at IS NULL THEN 1
			  WHEN DATEDIFF(HOUR, si.created_at, si.shipped_at)/24 > 3 THEN 1
			  ELSE NULL END AS 'is_ship_broken_sla'
	  
		,CASE WHEN DATEDIFF(HOUR, si.ready_for_refund_at, GETDATE())/24 > 7 AND si.refund_completed_at IS NULL THEN 1
				  WHEN DATEDIFF(HOUR, si.ready_for_refund_at, si.refund_completed_at)/24 > 7 THEN 1
				  ELSE NULL END AS 'is_refund_broken_sla'
	  
		,CASE WHEN si.return_reason ='content issue' THEN 1
			  WHEN si.return_reason ='Damaged item' THEN 1
			  WHEN si.return_reason ='Expiring date' THEN 1
			  WHEN si.return_reason ='Fake A' THEN 1
			  WHEN si.return_reason ='Fake C' THEN 1
			  WHEN si.return_reason ='Fake Product' THEN 1
			  WHEN si.return_reason ='Guarantee issue / wrong details base on our site' THEN 1
			  WHEN si.return_reason ='Internal - wrong item' THEN 1
			  WHEN si.return_reason ='Lost in Shipment' THEN 1
			  WHEN si.return_reason ='Merchant - Defective' THEN 1
			  WHEN si.return_reason ='merchant - wrong item' THEN 1
			  WHEN si.return_reason ='Merchant-Not complete product' THEN 1
			  WHEN si.return_reason ='Poor quality of the product' THEN 1
			  WHEN si.return_reason ='wrong color' THEN 1
			  WHEN si.return_reason ='Wrong product information' THEN 1
			  ELSE 0 END AS 'is_bad_return_reason'
	  
		,CASE  WHEN si.cancel_reason ='cancellation - unable to send via 3PL/own-rider' THEN 1
			  WHEN si.cancel_reason ='Customer - Late fulfillment' THEN 1
			  WHEN si.cancel_reason ='Customer - Some items were out of stock' THEN 1
			  WHEN si.cancel_reason ='Customer - Wrong product information' THEN 1
			  WHEN si.cancel_reason ='Internal-Defective' THEN 1
			  WHEN si.cancel_reason ='Internal-Error' THEN 1
			  WHEN si.cancel_reason ='Late Fulfilment by Seller' THEN 1
			  WHEN si.cancel_reason ='Lost in Warehouse' THEN 1
			  WHEN si.cancel_reason ='Merchant - Defective' THEN 1
			  WHEN si.cancel_reason ='Merchant - Order replacement' THEN 1
			  WHEN si.cancel_reason ='Merchant - Out of stock' THEN 1
			  WHEN si.cancel_reason ='Merchant - Price was wrong (Bamilo mistake)' THEN 1
			  WHEN si.cancel_reason ='Merchant - Price was wrong (Merchant mistake)' THEN 1
			  WHEN si.cancel_reason ='Merchant-Late Fulfilment' THEN 1
			  WHEN si.cancel_reason ='Merchant-Wrong' THEN 1
			  WHEN si.cancel_reason ='Out of stock(Cancellation - Internal)' THEN 1
			  WHEN si.cancel_reason ='Price Was wrong-Merchant' THEN 1
			  WHEN si.cancel_reason ='SC/Bob Error' THEN 1
			  ELSE 0 END AS 'is_bad_cancel_reason'
	  
	  
		FROM StagingDB_Replica.Gathering.tblSalesOrder so
	  
		LEFT JOIN StagingDB_Replica.Gathering.tblSalesItem si
		ON        so.id_sales_order = si.fk_sales_order
		
	  WHERE si.finance_verified_at IS NOT NULL) il
	  
		GROUP BY il.customer_id

	) g_il

  WHERE g_il.last_order_date >= DATEADD(DAY, -30, GETDATE())
		  `)
	checkError(err)
	//defer rows.Close()

	var iDCustomer string
	var customerRow customerrow.CustomerRow

	// for each row = customer of retrieve the customer data, map email from BOB, change it to JSON and call Emarsys API

	// continue until there is no new row
	for rows.Next() {
		err := rows.Scan(
			&iDCustomer,
			&customerRow.IDCluster,
			&customerRow.LastBiRefresh,
			&customerRow.AvgItemPrice,
			&customerRow.AvgBasketSize,
			&customerRow.LastRefundRejectDate,
			&customerRow.LastDeliveredDate,
			&customerRow.LastVoucherDate,
			&customerRow.LastVoucherType,
			&customerRow.LastCartRuleDate,
			&customerRow.RefundRejectRatio,
			&customerRow.OrderVoucherRatio,
			&customerRow.OrderCartRuleRatio,
			&customerRow.MainVoucherType,
			&customerRow.LastShipBrokenSLADate,
			&customerRow.LastRefundBrokenSLADate,
			&customerRow.BadCancellationRatio,
			&customerRow.BadReturnRatio,
			&customerRow.ShipBrokenSLARatio,
			&customerRow.RefundBrokenSLARatio,
			&customerRow.OrderCount,
		)
		checkError(err)

		mainMap[iDCustomer] = customerRow
	}

}

// LastCancelReasonQuery gets the last_cancel_date and the last_cancel_reason of each customer
func LastCancelReasonQuery(dbBi *sql.DB, lastCancelReasonMap map[string]customerrow.CustomerRow) {

	// query BI database to retrieve customer data
	rows, err := dbBi.Query(`SELECT
		lcdr.customer_id
		,CONVERT(char(10), lcdr.last_cancel_date,126) AS '18138' 
		,COALESCE(lcdr.last_cancel_reason,'') AS '18129' 

		FROM (

		SELECT 
		lcrr.customer_id
		,lcrr.last_cancel_reason
		,lcrr.last_cancel_date

		FROM (
	  
		SELECT so.customer_id
				,MAX(si.cancel_reason) AS 'last_cancel_reason'
				,si.canceled_at AS  'last_cancel_date'
				,ROW_NUMBER() OVER(PARTITION BY so.customer_id
									   ORDER BY si.canceled_at  DESC) AS rk
	  
			FROM StagingDB_Replica.Gathering.tblSalesOrder so
			LEFT JOIN StagingDB_Replica.Gathering.tblSalesItem si
			ON        so.id_sales_order = si.fk_sales_order
			WHERE si.finance_verified_at IS NOT NULL
      AND si.canceled_at IS NOT NULL
	  
		GROUP BY 
		so.customer_id
		,si.canceled_at
	  
		) lcrr
	  
	   WHERE lcrr.rk = 1  
		
		)  lcdr
		`)
	checkError(err)
	//defer rows.Close()

	var iDCustomer string
	var customerRow customerrow.CustomerRow

	// continue until there is no new row
	for rows.Next() {
		err := rows.Scan(
			&iDCustomer,
			&customerRow.LastCancelDate,
			&customerRow.LastCancelReason,
		)
		checkError(err)

		lastCancelReasonMap[iDCustomer] = customerRow
	}

}

// LastBadCancelReasonQuery gets the last_bad_cancel_date and the last_bad_cancel_reason of each customer
func LastBadCancelReasonQuery(dbBi *sql.DB, lastBadCancelReasonMap map[string]customerrow.CustomerRow) {

	// query BI database to retrieve customer data
	rows, err := dbBi.Query(`SELECT 
			lbcrd.customer_id
			,CONVERT(char(10), lbcrd.last_bad_cancel_date,126) AS '18131' 
			,COALESCE(lbcrd.last_bad_cancel_reason,'') AS '18132'
		  
			FROM (
		  
			SELECT 
			 lbcrr.customer_id
			,lbcrr.last_bad_cancel_reason
			,lbcrr.last_bad_cancel_date
		  
			FROM (
		  
			SELECT 
		  
			   inter_1.customer_id
			  ,MAX(CASE WHEN inter_1.bad_cancel_date IS NOT NULL THEN inter_1.cancel_reason ELSE NULL END) AS 'last_bad_cancel_reason'
			  ,inter_1.bad_cancel_date AS 'last_bad_cancel_date'
			  ,ROW_NUMBER() OVER(PARTITION BY inter_1.customer_id
										   ORDER BY inter_1.bad_cancel_date  DESC) AS rk
			  FROM (
			  
		  
			  SELECT so.customer_id
					,si.cancel_reason AS 'cancel_reason'
					,CASE  WHEN si.cancel_reason ='cancellation - unable to send via 3PL/own-rider' THEN si.canceled_at
				  WHEN si.cancel_reason ='Customer - Late fulfillment' THEN si.canceled_at
				  WHEN si.cancel_reason ='Customer - Some items were out of stock' THEN si.canceled_at
				  WHEN si.cancel_reason ='Customer - Wrong product information' THEN si.canceled_at
				  WHEN si.cancel_reason ='Internal-Defective' THEN si.canceled_at
				  WHEN si.cancel_reason ='Internal-Error' THEN si.canceled_at
				  WHEN si.cancel_reason ='Late Fulfilment by Seller' THEN si.canceled_at
				  WHEN si.cancel_reason ='Lost in Warehouse' THEN si.canceled_at
				  WHEN si.cancel_reason ='Merchant - Defective' THEN si.canceled_at
				  WHEN si.cancel_reason ='Merchant - Order replacement' THEN si.canceled_at
				  WHEN si.cancel_reason ='Merchant - Out of stock' THEN si.canceled_at
				  WHEN si.cancel_reason ='Merchant - Price was wrong (Bamilo mistake)' THEN si.canceled_at
				  WHEN si.cancel_reason ='Merchant - Price was wrong (Merchant mistake)' THEN si.canceled_at
				  WHEN si.cancel_reason ='Merchant-Late Fulfilment' THEN si.canceled_at
				  WHEN si.cancel_reason ='Merchant-Wrong' THEN si.canceled_at
				  WHEN si.cancel_reason ='Out of stock(Cancellation - Internal)' THEN si.canceled_at
				  WHEN si.cancel_reason ='Price Was wrong-Merchant' THEN si.canceled_at
				  WHEN si.cancel_reason ='SC/Bob Error' THEN si.canceled_at
				  ELSE NULL END AS 'bad_cancel_date'
				   
		  
				FROM StagingDB_Replica.Gathering.tblSalesOrder so
				LEFT JOIN StagingDB_Replica.Gathering.tblSalesItem si
				ON        so.id_sales_order = si.fk_sales_order
				WHERE si.finance_verified_at IS NOT NULL
		  AND si.canceled_at IS NOT NULL) inter_1
		  
			GROUP BY 
			inter_1.customer_id
			,inter_1.bad_cancel_date
		  
			) lbcrr
		  
		  WHERE rk = 1
		  AND lbcrr.last_bad_cancel_date IS NOT NULL
		  
			) lbcrd	
		`)
	checkError(err)
	//defer rows.Close()

	var iDCustomer string
	var customerRow customerrow.CustomerRow

	// continue until there is no new row
	for rows.Next() {
		err := rows.Scan(
			&iDCustomer,
			&customerRow.LastBadCancelDate,
			&customerRow.LastBadCancelReason,
		)
		checkError(err)

		lastBadCancelReasonMap[iDCustomer] = customerRow
	}

}

// LastReturnReasonQuery gets the last_return_date and the last_return_reason of each customer
func LastReturnReasonQuery(dbBi *sql.DB, lastReturnReasonMap map[string]customerrow.CustomerRow) {

	// query BI database to retrieve customer data
	rows, err := dbBi.Query(`SELECT
		lrdr.customer_id AS '4819'
		,CONVERT(char(10), lrdr.last_return_date,126) AS '18135' 
		,COALESCE(lrdr.last_return_reason,'') AS '18134' 
	 
		FROM (

		SELECT 
		lrrr.customer_id
		,lrrr.last_return_reason
		,lrrr.last_return_date

		FROM (
	  
		SELECT so.customer_id
				,MAX(si.return_reason) AS 'last_return_reason'
				,si.returned_at AS  'last_return_date'
				,ROW_NUMBER() OVER(PARTITION BY so.customer_id
									   ORDER BY si.returned_at  DESC) AS rk
	  
			FROM StagingDB_Replica.Gathering.tblSalesOrder so
			LEFT JOIN StagingDB_Replica.Gathering.tblSalesItem si
			ON        so.id_sales_order = si.fk_sales_order
			WHERE si.finance_verified_at IS NOT NULL
      		AND si.returned_at IS NOT NULL
	  
		GROUP BY 
		so.customer_id
		,si.returned_at
	  
		) lrrr
	  
	   WHERE lrrr.rk = 1
	  
		) lrdr	
		`)
	checkError(err)
	//defer rows.Close()

	var iDCustomer string
	var customerRow customerrow.CustomerRow

	// continue until there is no new row
	for rows.Next() {
		err := rows.Scan(
			&iDCustomer,
			&customerRow.LastReturnDate,
			&customerRow.LastReturnReason,
		)
		checkError(err)

		lastReturnReasonMap[iDCustomer] = customerRow
	}

}

// LastBadReturnReasonQuery gets the last_bad_return_date and the last_bad_return_reason of each customer
func LastBadReturnReasonQuery(dbBi *sql.DB, lastBadReturnReasonMap map[string]customerrow.CustomerRow) {

	// query BI database to retrieve customer data
	rows, err := dbBi.Query(`SELECT 
		lbrdr.customer_id
		,CONVERT(char(10), lbrdr.last_bad_return_date,126) AS '18136' 
		,COALESCE(lbrdr.last_bad_return_reason,'') AS '18137' 
		
		FROM (
	  
		 SELECT 
		 lbrrr.customer_id
		,lbrrr.last_bad_return_reason
		,lbrrr.last_bad_return_date
	  
		FROM (
	  
		SELECT 
	  
		   inter_2.customer_id
		  ,MAX(CASE WHEN inter_2.bad_return_date IS NOT NULL THEN inter_2.return_reason ELSE NULL END) AS 'last_bad_return_reason'
		  ,inter_2.bad_return_date AS 'last_bad_return_date'
		  ,ROW_NUMBER() OVER(PARTITION BY inter_2.customer_id
									   ORDER BY inter_2.bad_return_date  DESC) AS rk
		  FROM (
		  
	  
		  SELECT so.customer_id
				,si.return_reason AS 'return_reason'
				,CASE WHEN si.return_reason ='content issue' THEN si.returned_at
			  WHEN si.return_reason ='Damaged item' THEN si.returned_at
			  WHEN si.return_reason ='Expiring date' THEN si.returned_at
			  WHEN si.return_reason ='Fake A' THEN si.returned_at
			  WHEN si.return_reason ='Fake C' THEN si.returned_at
			  WHEN si.return_reason ='Fake Product' THEN si.returned_at
			  WHEN si.return_reason ='Guarantee issue / wrong details base on our site' THEN si.returned_at
			  WHEN si.return_reason ='Internal - wrong item' THEN si.returned_at
			  WHEN si.return_reason ='Lost in Shipment' THEN si.returned_at
			  WHEN si.return_reason ='Merchant - Defective' THEN si.returned_at
			  WHEN si.return_reason ='merchant - wrong item' THEN si.returned_at
			  WHEN si.return_reason ='Merchant-Not complete product' THEN si.returned_at
			  WHEN si.return_reason ='Poor quality of the product' THEN si.returned_at
			  WHEN si.return_reason ='wrong color' THEN si.returned_at
			  WHEN si.return_reason ='Wrong product information' THEN si.returned_at
			  ELSE NULL END AS 'bad_return_date'
			   
	  
			FROM StagingDB_Replica.Gathering.tblSalesOrder so
			LEFT JOIN StagingDB_Replica.Gathering.tblSalesItem si
			ON        so.id_sales_order = si.fk_sales_order
			WHERE si.finance_verified_at IS NOT NULL
      AND si.returned_at IS NOT NULL) inter_2
	  
		GROUP BY 
		inter_2.customer_id
		,inter_2.bad_return_date
	  
		) lbrrr
	  
	  WHERE lbrrr.rk = 1
		AND lbrrr.last_bad_return_date IS NOT NULL
		
		) lbrdr	
		`)
	checkError(err)
	defer rows.Close()

	var iDCustomer string
	var customerRow customerrow.CustomerRow

	// continue until there is no new row
	for rows.Next() {
		err := rows.Scan(
			&iDCustomer,
			&customerRow.LastBadReturnDate,
			&customerRow.LastBadReturnReason,
		)
		checkError(err)

		lastBadReturnReasonMap[iDCustomer] = customerRow
	}

}

// LastOrderStatusQuery gets the last_order_status of each customer
func LastOrderStatusQuery(dbBi *sql.DB, lastOrderStatusMap map[string]customerrow.CustomerRow) {

	// query BI database to retrieve customer data
	rows, err := dbBi.Query(`SELECT 
		los.customer_id AS '4819' 
		,los.last_order_status AS '18141' 
		
		
		FROM
	  
	 (
	  
			SELECT 
		loss.customer_id
		,loss.last_order_status
		FROM (
	  
		SELECT so.customer_id
				,MAX(so.status) AS 'last_order_status'
				,ROW_NUMBER() OVER(PARTITION BY so.customer_id
									   ORDER BY so.created_at  DESC) AS rk
	  
			FROM StagingDB_Replica.Gathering.tblSalesOrder so
			LEFT JOIN StagingDB_Replica.Gathering.tblSalesItem si
			ON        so.id_sales_order = si.fk_sales_order
			WHERE si.finance_verified_at IS NOT NULL
	  
		GROUP BY 
		so.customer_id
		,so.created_at
	  
		) loss
	  
	   WHERE loss.rk = 1
		
		) los
		`)
	checkError(err)
	defer rows.Close()

	var iDCustomer string
	var customerRow customerrow.CustomerRow

	// continue until there is no new row
	for rows.Next() {
		err := rows.Scan(
			&iDCustomer,
			&customerRow.LastOrderStatus,
		)
		checkError(err)

		lastOrderStatusMap[iDCustomer] = customerRow
	}

}

// HourOfDayQuery gets the hour_of_day_most_order and the order_count_hod_most_order of each customer
func HourOfDayQuery(dbBi *sql.DB, hourOfDayMap map[string]customerrow.CustomerRow) {

	// query BI database to retrieve customer data
	rows, err := dbBi.Query(`SELECT 
		hod.customer_id AS '4819' 
		,hod.hour_of_day_most_order AS '18156' 
		,hod.order_count_hod_most_order	  
		
		FROM (
	  
	  SELECT 
		hodmo.customer_id
		,hodmo.hour_of_day_most_order
		,hodmo.order_count_hod_most_order
		FROM (
	  
		SELECT so.customer_id
				,DATEPART(HOUR,so.created_at) AS 'hour_of_day_most_order'
				,COUNT( DISTINCT so.order_nr) AS  'order_count_hod_most_order'
				,ROW_NUMBER() OVER(PARTITION BY so.customer_id
									   ORDER BY COUNT(DISTINCT so.order_nr) DESC) AS rk
			FROM StagingDB_Replica.Gathering.tblSalesOrder so
			LEFT JOIN StagingDB_Replica.Gathering.tblSalesItem si
			ON        so.id_sales_order = si.fk_sales_order
			WHERE si.finance_verified_at IS NOT NULL
	  
		GROUP BY 
		so.customer_id
		,DATEPART(HOUR,so.created_at)
	  
		) hodmo
	  
	   WHERE hodmo.rk = 1
	  
	  
		) hod
		`)
	checkError(err)
	defer rows.Close()

	var iDCustomer string
	var customerRow customerrow.CustomerRow

	// continue until there is no new row
	for rows.Next() {
		err := rows.Scan(
			&iDCustomer,
			&customerRow.HourOfDayMostOrder,
			&customerRow.OrderCountHodMostOrder,
		)
		checkError(err)

		hourOfDayMap[iDCustomer] = customerRow
	}

}

// DayOfWeekQuery gets the day_of_week_most_order and the order_count_dow_most_order of each customer
func DayOfWeekQuery(dbBi *sql.DB, dayOfWeekMap map[string]customerrow.CustomerRow) {

	// query BI database to retrieve customer data
	rows, err := dbBi.Query(`SELECT 

		dow.customer_id AS '4819' 
		,dow.day_of_week_most_order AS '18158' 
		,dow.order_count_dow_most_order
		
		FROM (
	  
		SELECT 
		dowmo.customer_id
		,dowmo.day_of_week_most_order
		,dowmo.order_count_dow_most_order

		FROM (
	  
		SELECT so.customer_id
				,DATEPART(dw,so.created_at) AS 'day_of_week_most_order'
				,COUNT(DISTINCT so.order_nr) AS  'order_count_dow_most_order'
				,ROW_NUMBER() OVER(PARTITION BY so.customer_id
									   ORDER BY COUNT(DISTINCT so.order_nr) DESC) AS rk
			FROM StagingDB_Replica.Gathering.tblSalesOrder so
			LEFT JOIN StagingDB_Replica.Gathering.tblSalesItem si
			ON        so.id_sales_order = si.fk_sales_order
			WHERE si.finance_verified_at IS NOT NULL
	  
		GROUP BY 
		so.customer_id
		,DATEPART(dw,so.created_at)
	  
		) dowmo
	  
	   WHERE dowmo.rk = 1
	  
		) dow
		`)
	checkError(err)
	defer rows.Close()

	var iDCustomer string
	var customerRow customerrow.CustomerRow

	// continue until there is no new row
	for rows.Next() {
		err := rows.Scan(
			&iDCustomer,
			&customerRow.DayOfWeekMostOrder,
			&customerRow.OrderCountDowMostOrder,
		)
		checkError(err)

		dayOfWeekMap[iDCustomer] = customerRow
	}

}

// DayOfMonthQuery gets the day_of_month_most_order and the order_count_dom_most_order of each customer
func DayOfMonthQuery(dbBi *sql.DB, dayOfMonthMap map[string]customerrow.CustomerRow) {

	// query BI database to retrieve customer data
	rows, err := dbBi.Query(`SELECT 
		dom.customer_id AS '4819' 
		,dom.day_of_month_most_order AS '18160'
		,dom.order_count_dom_most_order
	  
		FROM (
	  
		  SELECT 
		dommo.customer_id
		,dommo.day_of_month_most_order
		,dommo.order_count_dom_most_order
		FROM (
	  
		SELECT so.customer_id
				,DAY(so.created_at) AS 'day_of_month_most_order'
				,COUNT(DISTINCT so.order_nr) AS  'order_count_dom_most_order'
				,ROW_NUMBER() OVER(PARTITION BY so.customer_id
									   ORDER BY COUNT(DISTINCT so.order_nr) DESC) AS rk
			FROM StagingDB_Replica.Gathering.tblSalesOrder so
			LEFT JOIN StagingDB_Replica.Gathering.tblSalesItem si
			ON        so.id_sales_order = si.fk_sales_order
			WHERE si.finance_verified_at IS NOT NULL
	  
		GROUP BY 
		so.customer_id
		,DAY(so.created_at)
	  
		) dommo
	  
	   WHERE dommo.rk = 1
	  
		) dom
		`)
	checkError(err)
	defer rows.Close()

	var iDCustomer string
	var customerRow customerrow.CustomerRow

	// continue until there is no new row
	for rows.Next() {
		err := rows.Scan(
			&iDCustomer,
			&customerRow.DayOfMonthMostOrder,
			&customerRow.OrderCountDomMostOrder,
		)
		checkError(err)

		dayOfMonthMap[iDCustomer] = customerRow
	}

}

// MonthOfYearQuery gets the month_of_year_most_order and the order_count_moy_most_order of each customer
func MonthOfYearQuery(dbBi *sql.DB, monthOfYearMap map[string]customerrow.CustomerRow) {

	// query BI database to retrieve customer data
	rows, err := dbBi.Query(`SELECT 

		moy.customer_id AS '4819' 
		,moy.month_of_year_most_order AS '18162' 
		,moy.order_count_moy_most_order
	  
		FROM (
	   
			SELECT 
		moymo.customer_id
		,moymo.month_of_year_most_order
		,moymo.order_count_moy_most_order

		FROM (
	  
		SELECT so.customer_id
				,DATEPART(month, so.created_at) AS 'month_of_year_most_order'
				,COUNT(DISTINCT so.order_nr) AS  'order_count_moy_most_order'
				,ROW_NUMBER() OVER(PARTITION BY so.customer_id
									   ORDER BY COUNT(DISTINCT so.order_nr) DESC) AS rk
			FROM StagingDB_Replica.Gathering.tblSalesOrder so
			LEFT JOIN StagingDB_Replica.Gathering.tblSalesItem si
			ON        so.id_sales_order = si.fk_sales_order
			WHERE si.finance_verified_at IS NOT NULL
	  
		GROUP BY 
		so.customer_id
		,DATEPART(month, so.created_at)
	  
		) moymo
	  
	   WHERE moymo.rk = 1
	  
		) moy
		`)
	checkError(err)
	defer rows.Close()

	var iDCustomer string
	var customerRow customerrow.CustomerRow

	// continue until there is no new row
	for rows.Next() {
		err := rows.Scan(
			&iDCustomer,
			&customerRow.MonthOfYearMostOrder,
			&customerRow.OrderCountMoyMostOrder,
		)
		checkError(err)

		monthOfYearMap[iDCustomer] = customerRow
	}

}

// JoinQuery joins all the maps together into one customer table
func JoinQuery(mainMap, lastCancelReasonMap, lastBadCancelReasonMap, lastReturnReasonMap, lastBadReturnReasonMap, lastOrderStatusMap, hourOfDayMap, dayOfWeekMap, dayOfMonthMap, monthOfYearMap, bobEmailMap map[string]customerrow.CustomerRow) (customerTable []customerrow.CustomerRow) {

	var customerRow customerrow.CustomerRow

	for k := range mainMap {
		customerRow.Email = bobEmailMap[k].Email
		customerRow.IDCustomer = k
		customerRow.IDCluster = mainMap[k].IDCluster
		customerRow.LastBiRefresh = mainMap[k].LastBiRefresh
		customerRow.AvgItemPrice = mainMap[k].AvgItemPrice
		customerRow.AvgBasketSize = mainMap[k].AvgBasketSize
		customerRow.LastCancelDate = lastCancelReasonMap[k].LastCancelDate
		customerRow.LastCancelReason = lastCancelReasonMap[k].LastCancelReason
		customerRow.LastBadCancelDate = lastBadCancelReasonMap[k].LastBadCancelDate
		customerRow.LastBadCancelReason = lastBadCancelReasonMap[k].LastBadCancelReason
		customerRow.LastReturnDate = lastReturnReasonMap[k].LastReturnDate
		customerRow.LastReturnReason = lastReturnReasonMap[k].LastReturnReason
		customerRow.LastBadReturnDate = lastBadReturnReasonMap[k].LastBadReturnDate
		customerRow.LastBadReturnReason = lastBadReturnReasonMap[k].LastBadReturnReason
		customerRow.LastRefundRejectDate = mainMap[k].LastRefundRejectDate
		customerRow.LastDeliveredDate = mainMap[k].LastDeliveredDate
		customerRow.LastOrderStatus = lastOrderStatusMap[k].LastOrderStatus
		customerRow.LastVoucherDate = mainMap[k].LastVoucherDate
		customerRow.LastVoucherType = mainMap[k].LastVoucherType
		customerRow.LastCartRuleDate = mainMap[k].LastCartRuleDate
		customerRow.RefundRejectRatio = mainMap[k].RefundRejectRatio
		customerRow.OrderVoucherRatio = mainMap[k].OrderVoucherRatio
		customerRow.OrderCartRuleRatio = mainMap[k].OrderCartRuleRatio
		customerRow.MainVoucherType = mainMap[k].MainVoucherType
		customerRow.LastShipBrokenSLADate = mainMap[k].LastShipBrokenSLADate
		customerRow.LastRefundBrokenSLADate = mainMap[k].LastRefundBrokenSLADate
		customerRow.BadCancellationRatio = mainMap[k].BadCancellationRatio
		customerRow.BadReturnRatio = mainMap[k].BadReturnRatio
		customerRow.ShipBrokenSLARatio = mainMap[k].ShipBrokenSLARatio
		customerRow.RefundBrokenSLARatio = mainMap[k].RefundBrokenSLARatio
		customerRow.HourOfDayMostOrder = hourOfDayMap[k].HourOfDayMostOrder
		customerRow.RatioHodMostOrder = strconv.FormatFloat(hourOfDayMap[k].OrderCountHodMostOrder/mainMap[k].OrderCount, 'E', -1, 64)
		customerRow.DayOfWeekMostOrder = dayOfWeekMap[k].DayOfWeekMostOrder
		customerRow.RatioDowMostOrder = strconv.FormatFloat(dayOfWeekMap[k].OrderCountDowMostOrder/mainMap[k].OrderCount, 'E', -1, 64)
		customerRow.DayOfMonthMostOrder = dayOfMonthMap[k].DayOfMonthMostOrder
		customerRow.RatioDomMostOrder = strconv.FormatFloat(dayOfMonthMap[k].OrderCountDomMostOrder/mainMap[k].OrderCount, 'E', -1, 64)
		customerRow.MonthOfYearMostOrder = monthOfYearMap[k].MonthOfYearMostOrder
		customerRow.RatioMoyMostOrder = strconv.FormatFloat(monthOfYearMap[k].OrderCountMoyMostOrder/mainMap[k].OrderCount, 'E', -1, 64)

		customerTable = append(customerTable, customerRow)
	}

	return customerTable

}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
