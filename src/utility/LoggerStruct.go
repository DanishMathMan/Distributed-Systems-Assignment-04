package utility

import (
	"encoding/json"
	"log"
)

type LogStruct struct {
	Timestamp  int64
	Identifier int64 // identifier is the processe's port number
	Message    string
}

// LogAsJson logs a log struct as a json string to the standard logger which in our case is the log file
func LogAsJson(logStruct LogStruct, trailingComma bool) {
	j, err := json.MarshalIndent(logStruct, "", "\t")
	if err != nil {
		log.Panicln(err)
	}
	w := log.Writer()
	w.Write(j)
	var trail string
	if trailingComma {
		trail = ",\n"
	} else {
		trail = "\n"
	}
	w.Write([]byte(trail))
}
