package customerrow

// EmarsysConf struct defines how config.yaml should be built
type CustomerRow struct {
	Email                   string `json:"3"`
	IDCustomer              string `json:"4819"`
	IDCluster               string `json:"18124"`
	LastBiRefresh           string `json:"18125"`
	AvgItemPrice            string `json:"18126"`
	AvgBasketSize           string `json:"18130"`
	LastCancelDate          string `json:"18138"`
	LastCancelReason        string `json:"18129"`
	LastBadCancelDate       string `json:"18131"`
	LastBadCancelReason     string `json:"18132"`
	LastReturnDate          string `json:"18135"`
	LastReturnReason        string `json:"18134"`
	LastBadReturnDate       string `json:"18136"`
	LastBadReturnReason     string `json:"18137"`
	LastRefundRejectDate    string `json:"18139"`
	LastDeliveredDate       string `json:"18140"`
	LastOrderStatus         string `json:"18141"`
	LastVoucherDate         string `json:"18142"`
	LastVoucherType         string `json:"18143"`
	LastCartRuleDate        string `json:"18144"`
	RefundRejectRatio       string `json:"18145"`
	OrderVoucherRatio       string `json:"18146"`
	OrderCartRuleRatio      string `json:"18147"`
	MainVoucherType         string `json:"18148"`
	LastShipBrokenSLADate   string `json:"18149"`
	LastRefundBrokenSLADate string `json:"18152"`
	BadCancellationRatio    string `json:"18151"`
	BadReturnRatio          string `json:"18153"`
	ShipBrokenSLARatio      string `json:"18154"`
	RefundBrokenSLARatio    string `json:"18155"`
	HourOfDayMostOrder      string `json:"18156"`
	RatioHodMostOrder       string `json:"18157"`
	DayOfWeekMostOrder      string `json:"18158"`
	RatioDowMostOrder       string `json:"18159"`
	DayOfMonthMostOrder     string `json:"18160"`
	RatioDomMostOrder       string `json:"18161"`
	MonthOfYearMostOrder    string `json:"18162"`
	RatioMoyMostOrder       string `json:"18163"`

	OrderCount             float64
	OrderCountHodMostOrder float64
	OrderCountDowMostOrder float64
	OrderCountDomMostOrder float64
	OrderCountMoyMostOrder float64
}

// DivideCustomerTableInChunk divides an array of
func DivideCustomerTableInChunk(customerTable []CustomerRow) (arrayOfCustomerTableChunk [][]CustomerRow) {

	chunkSize := 999

	for i := 0; i < len(customerTable); i += chunkSize {
		end := i + chunkSize

		if end > len(customerTable) {
			end = len(customerTable)
		}

		arrayOfCustomerTableChunk = append(arrayOfCustomerTableChunk, customerTable[i:end])
	}

	return arrayOfCustomerTableChunk
}
