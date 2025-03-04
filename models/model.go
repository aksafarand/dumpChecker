package models

const (
	Huawei4gDumpDir    = "./dumpfiles/huawei/4g"
	Huawei2gDumpDir    = "./dumpfiles/huawei/2g"
	HuaweiVendorResult = "./output/huawei"

	Nokia4gDumpDir    = "./dumpfiles/nokia/4g"
	Nokia2gDumpDir    = "./dumpfiles/nokia/2g"
	NokiaVendorResult = "./output/nokia"
	ConfigDir         = "./config/"
)

type ConfigRecord struct {
	TableName       string
	ParamName       string
	AttributeColumn string
	DataType        string
	Operator        string
	ProposedValue   string
}
