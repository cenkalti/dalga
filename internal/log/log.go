package log

import (
	stdlog "log"
)

var debugging bool

func EnableDebug() {
	debugging = true
}

func Debugln(args ...interface{}) {
	if debugging {
		stdlog.Println(args...)
	}
}

func Debugf(fmt string, args ...interface{}) {
	if debugging {
		stdlog.Printf(fmt, args...)
	}
}

func Println(args ...interface{}) {
	stdlog.Println(args...)
}

func Printf(fmt string, args ...interface{}) {
	stdlog.Printf(fmt, args...)
}

func Fatal(msg interface{}) {
	stdlog.Fatal(msg)
}
