package log

import stdlog "log"

var (
	debugging bool
	disabled  bool
)

func EnableDebug() {
	debugging = true
}

func Disable() {
	disabled = true
}

func Debugln(args ...interface{}) {
	if debugging && !disabled {
		stdlog.Println(args...)
	}
}

func Debugf(fmt string, args ...interface{}) {
	if debugging && !disabled {
		stdlog.Printf(fmt, args...)
	}
}

func Println(args ...interface{}) {
	if !disabled {
		stdlog.Println(args...)
	}
}

func Printf(fmt string, args ...interface{}) {
	if !disabled {
		stdlog.Printf(fmt, args...)
	}
}

func Fatal(msg interface{}) {
	if !disabled {
		stdlog.Fatal(msg)
	}
}
