package raft

import "log"
import "os"
import "bufio"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func init() {
    file, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
    if err != nil {
        log.Fatal("无法打开日志文件:", err)
    }
	writer := bufio.NewWriter(file)
	log.SetOutput(writer)
}
