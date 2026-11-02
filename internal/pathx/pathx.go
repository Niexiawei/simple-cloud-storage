package pathx

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

func Prefix(value string) (string, error) {
	value = strings.TrimFunc(value, func(r rune) bool { return r == '/' || unicode.IsSpace(r) })
	if value == "" {
		return "", fmt.Errorf("prefix is empty")
	}
	return clean(value, true)
}

func Key(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("key is empty")
	}
	if strings.HasPrefix(value, "/") {
		return "", fmt.Errorf("key must be relative")
	}
	return clean(value, false)
}

func Join(prefix, key string) (string, error) {
	p, err := Prefix(prefix)
	if err != nil {
		return "", err
	}
	k, err := Key(key)
	if err != nil {
		return "", err
	}
	return p + "/" + k, nil
}

func clean(value string, prefix bool) (string, error) {
	if !utf8.ValidString(value) {
		return "", fmt.Errorf("path is not valid UTF-8")
	}
	if strings.Contains(value, "\\") {
		return "", fmt.Errorf("backslash is not allowed")
	}
	parts := strings.Split(value, "/")
	for _, part := range parts {
		if part == "" {
			return "", fmt.Errorf("path contains an empty segment")
		}
		if part == "." || part == ".." {
			return "", fmt.Errorf("relative path segment %q is not allowed", part)
		}
		for _, r := range part {
			if unicode.IsControl(r) {
				return "", fmt.Errorf("path contains a control character")
			}
		}
	}
	if !prefix && strings.HasSuffix(value, "/") {
		return "", fmt.Errorf("key must name a file")
	}
	return strings.Join(parts, "/"), nil
}
