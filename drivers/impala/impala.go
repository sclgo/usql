// Package impala defines and registers usql's Apache Impala driver.
//
// See: https://github.com/sclgo/impala-go
// Group: bad
package impala

import (
	_ "github.com/sclgo/impala-go" // DRIVER
	"github.com/xo/usql/drivers"
	"github.com/xo/usql/drivers/metadata/impala"
)

func init() {
	drivers.Register("impala", drivers.Driver{
		NewMetadataReader: impala.New,
	})
}
