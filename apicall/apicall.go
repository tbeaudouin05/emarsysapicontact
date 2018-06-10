package apicall

import (
	"fmt"

	"github.com/thomas-bamilo/emarsysapicontact/emarsysconfig"
)

func ApiCall(customerTableJSONStr string) {

	emarsysConfig := emarsysconfig.EmarsysConfig{}
	emarsysConfig.ReadYamlEmarsysConfig()
	fmt.Println(emarsysConfig.Send("PUT", "contact", customerTableJSONStr))
}
