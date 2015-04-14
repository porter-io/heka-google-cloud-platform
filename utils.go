package cloudplatform

import (
	"time"
)

func getTimestamp(t int64) string {
	return time.Unix(0, t).UTC().Format(time.RFC3339)
}

