package idgen

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"unicode"
)

// GenerateID returns an ID with format: 'prefix/<random-4-bytes-hex>'.
func GenerateID(prefix string) string {
	idBuf := make([]byte, 4)
	if _, err := rand.Read(idBuf); err != nil {
		panic(err)
	}
	return sanitizePrefix(prefix) + "-" + hex.EncodeToString(idBuf)
}

// sanitizePrefix removes any characters that are not alphanumeric or hyphens,
// and converts to lowercase.
func sanitizePrefix(prefix string) string {
	var b strings.Builder
	b.Grow(len(prefix))

	for _, r := range prefix {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' {
			b.WriteRune(unicode.ToLower(r))
		}
	}

	return b.String()
}
