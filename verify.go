package main

import (
	"fmt"
	log "github.com/cihub/seelog"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
)

type CheckErrorAction int

const (
	ACTION_FATAL_QUIT CheckErrorAction = iota
	ACTION_LOG_ERROR
)

//Notice:
//  1. when dev, set to ACTION_FATAL_QUIT, so can check error quickly,
//     then can add error logical for the place that once thought could not go wrong
//  2. when released, set to ACTION_LOG_ERROR, so just log error

var verifyAction = ACTION_LOG_ERROR

// GetCallStackInfo return fileName, lineNo, funName
func GetCallStackInfo(skip int) (string, int, string) {
	var funName string
	pc, fileName, lineNo, ok := runtime.Caller(skip)
	if ok {
		funName = strings.TrimPrefix(filepath.Ext(runtime.FuncForPC(pc).Name()), ".")
	} else {
		funName = "<Unknown>"
		fileName, lineNo = "<Unknown>", -1
	}
	return fileName, lineNo, funName
}

// skip 表示跳过几个调用堆栈, 获取真正有意义的代码调用位置
func checkAndHandleError(err error, msg string, action CheckErrorAction, skip int) {
	if err != nil {
		fileName, lineNo, funName := GetCallStackInfo(skip)

		switch action {
		case ACTION_FATAL_QUIT:
			log.Errorf("%s:%d: (%s) FAIL(%s), msg=%s", fileName, lineNo, funName, reflect.TypeOf(err).String(), msg)
			//log.Fatalf("") //"error at: %s:%d, msg=%s, err=%s", fileName, lineNo, msg, err)
			panic(fmt.Sprintf("error at: %s:%d, msg=%s, err=%s", fileName, lineNo, msg, err))
		case ACTION_LOG_ERROR:
			log.Warnf("%s:%d: (%s) FAIL(%s), msg=%s", fileName, lineNo, funName, reflect.TypeOf(err).String(), msg)
			//flog.Infof("error at: %s:%d, msg=%s, err=%s", fileName, lineNo, msg, err)
		}
	}
}

func Verify(err error) error {
	if err != nil {
		checkAndHandleError(err, err.Error(), verifyAction, 3)
	}
	return err
}

func VerifyWithResult[T any](result T, err error) T {
	if err != nil {
		checkAndHandleError(err, err.Error(), verifyAction, 3)
	}
	return result
}
