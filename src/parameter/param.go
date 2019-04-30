package parameter

import "time"

func GetCurrentTimeStamp() uint64 {
	return uint64(time.Now().Unix()) //current time in seconds since epoch
}