package feat

import "fmt"

func iDWithPeriod(base string, period int) string {
	return fmt.Sprintf("%s_%d", base, period)
}
