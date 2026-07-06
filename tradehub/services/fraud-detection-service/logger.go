package main


import "log"

func logInfo(format string, args ...interface{}) {
log.Printf("[INFO] "+format, args...)
}

func logError(format string, args ...interface{}) {
log.Printf("[ERROR] "+format, args...)
}
