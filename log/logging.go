package log

import (
	"io"
	"log"
)

var (
	// Info Logger
	Info *log.Logger

	// Warning Logger
	Warn *log.Logger

	// Error Logger
	Error *log.Logger
)

// Initialises the the logger
func Init(
	infoHandle io.Writer,
	warningHandle io.Writer,
	errorHandle io.Writer) {

	Info = log.New(infoHandle,
		"INFO: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	Warn = log.New(warningHandle,
		"WARNING: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	Error = log.New(errorHandle,
		"ERROR: ",
		log.Ldate|log.Ltime|log.Lshortfile)

}
