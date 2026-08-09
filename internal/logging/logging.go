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

func parseLevel(level string) Level {
	switch strings.ToLower(level) {
	case "debug":
		return DEBUG
	case "warn":
		return WARN
	case "error":
		return ERROR
	case "info", "":
		return INFO
	default:
		return INFO
	}
}

func newLogger(level Level, w io.Writer) *Logger {
	if w == nil {
		w = os.Stdout
	}

	return &Logger{
		level:  level,
		logger: log.New(w, "", log.LstdFlags),
	}
}

func Init(level string) {
	globalLogger = newLogger(parseLevel(level), os.Stdout)
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
