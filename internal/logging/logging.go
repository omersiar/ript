package logging

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
)

var levelNames = map[Level]string{
	DEBUG: "DEBUG",
	INFO:  "INFO",
	WARN:  "WARN",
	ERROR: "ERROR",
}

type Logger struct {
	level  Level
	logger *log.Logger
}

var globalLogger *Logger

func Init(level string) {
	levelMap := map[string]Level{
		"debug": DEBUG,
		"info":  INFO,
		"warn":  WARN,
		"error": ERROR,
	}

	logLevel := INFO
	if l, ok := levelMap[strings.ToLower(level)]; ok {
		logLevel = l
	}

	globalLogger = &Logger{
		level:  logLevel,
		logger: log.New(os.Stdout, "", log.LstdFlags),
	}
}

func (l *Logger) log(level Level, msg string, args ...interface{}) {
	if level < l.level {
		return
	}

	prefix := fmt.Sprintf("[%s]", levelNames[level])
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	l.logger.Printf("%s %s\n", prefix, msg)
}

func Debug(msg string, args ...interface{}) {
	if globalLogger == nil {
		Init("info")
	}
	globalLogger.log(DEBUG, msg, args...)
}

func Info(msg string, args ...interface{}) {
	if globalLogger == nil {
		Init("info")
	}
	globalLogger.log(INFO, msg, args...)
}

func Warn(msg string, args ...interface{}) {
	if globalLogger == nil {
		Init("info")
	}
	globalLogger.log(WARN, msg, args...)
}

func Error(msg string, args ...interface{}) {
	if globalLogger == nil {
		Init("info")
	}
	globalLogger.log(ERROR, msg, args...)
}

func Fatal(msg string, args ...interface{}) {
	if globalLogger == nil {
		Init("info")
	}
	globalLogger.log(ERROR, msg, args...)
	os.Exit(1)
}

func SetOutput(w io.Writer) {
	if globalLogger != nil {
		globalLogger.logger = log.New(w, "", log.LstdFlags)
	}
}
