package main

import (
	"fmt"
	"regexp"
	"strings"
)

func newTopicMatcher(prefix string, search string, regexMode bool) (func(string) bool, error) {
	prefix = strings.TrimSpace(prefix)
	search = strings.TrimSpace(search)
	if prefix == "" && search == "" {
		return func(string) bool { return true }, nil
	}

	var compiled *regexp.Regexp
	if search != "" && regexMode {
		re, err := regexp.Compile(search)
		if err != nil {
			return nil, fmt.Errorf("invalid --search regex: %w", err)
		}
		compiled = re
	}

	needle := strings.ToLower(search)
	return func(topicName string) bool {
		if prefix != "" && !strings.HasPrefix(topicName, prefix) {
			return false
		}
		if search == "" {
			return true
		}
		if compiled != nil {
			return compiled.MatchString(topicName)
		}
		return strings.Contains(strings.ToLower(topicName), needle)
	}, nil
}
