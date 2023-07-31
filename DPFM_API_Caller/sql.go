package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-quantity-unit-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-quantity-unit-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"strings"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) QuantityUnit(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.QuantityUnit {

	where := strings.Join([]string{
		fmt.Sprintf("WHERE quantityUnit.QuantityUnit = \"%s\ ", input.QuantityUnit.QuantityUnit),
	}, "")

	rows, err := c.db.Query(
		`SELECT 
    	quantityUnit.QuantityUnit
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_quantityUnit_quantityUnit_data as quantityUnit 
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToQuantityUnit(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
