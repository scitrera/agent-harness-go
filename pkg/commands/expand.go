package commands

import (
	"strconv"
	"strings"
)

// Expand substitutes argument tokens in a command body:
//
//	$ARGUMENTS      -> the full raw argument string
//	$1 .. $9        -> whitespace-split positional arguments ("" when absent)
//
// Tokens with no corresponding argument expand to the empty string. $ARGUMENTS
// is listed first so it is never partially matched by a positional token.
func Expand(body, args string) string {
	fields := strings.Fields(args)
	repl := make([]string, 0, 2+9*2)
	repl = append(repl, "$ARGUMENTS", args)
	for i := 1; i <= 9; i++ {
		val := ""
		if i <= len(fields) {
			val = fields[i-1]
		}
		repl = append(repl, "$"+strconv.Itoa(i), val)
	}
	return strings.NewReplacer(repl...).Replace(body)
}
