package executor

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
)

// normalizeStatementDigest computes a MySQL-style statement digest from a SQL
// query. It returns:
//   - digest: SHA-256 hex string of the normalized token stream (64 chars)
//   - digestText: human-readable normalized form with literals replaced by `?`
//
// The normalization rules approximate MySQL's behavior:
//   - Comments are stripped (-- ..., # ..., /* ... */)
//   - Strings, numbers, and NULL are replaced with `?`
//   - Identifiers are quoted with backticks
//   - Keywords are uppercased
//   - Whitespace is collapsed to single spaces around tokens
//   - Repeating value tuples like `(1,2),(3,4),...` are collapsed to `(...) /* , ... */`
//   - NULL in IS NULL / IS NOT NULL and NOT NULL constraints is kept as a keyword
//
// maxDigestLength mirrors performance_schema_max_digest_length: the token
// stream is truncated to maxDigestLength/2 tokens before rendering. Pass 0
// to disable truncation.
//
// This is a best-effort approximation; exact byte-for-byte equality with
// MySQL's digest is not guaranteed because MySQL's tokenizer applies many
// dialect-specific rules (PARSER_TOKEN_*) that are out of scope.
func normalizeStatementDigest(query string, maxDigestLength int) (digest string, digestText string) {
	tokens := tokenizeForDigest(query)
	tokens = reclassifyNullTokens(tokens)
	tokens = collapseUnaryOperators(tokens)
	tokens = collapseInsideParenLiterals(tokens)
	tokens = collapseValueLists(tokens)
	tokens = collapseTopLevelLiteralList(tokens)
	// Truncate token stream to maxDigestLength/2 tokens, matching MySQL's
	// internal 2-bytes-per-token representation of performance_schema_max_digest_length.
	if maxDigestLength > 0 {
		maxTokens := maxDigestLength / 2
		if len(tokens) > maxTokens {
			tokens = tokens[:maxTokens]
		}
	}
	digestText = strings.TrimSpace(joinDigestTokens(tokens))
	h := sha256.Sum256([]byte(digestText))
	digest = hex.EncodeToString(h[:])
	return digest, digestText
}

// collapseUnaryOperators strips unary '+' and '-' operators that immediately
// precede a literal (number/string/null) in a unary-operator context.
//
// A '+'/'-' operator is considered unary when the preceding non-operator token
// (looking back past any chain of +/- operators) is one of:
//   - start of the token stream (implicit: after SELECT, etc.)
//   - a '(' punctuation
//   - a ',' punctuation
//   - another operator (!=, <=, >= etc. — but not +/- themselves in binary context)
//   - a keyword such as SELECT, WHERE, AND, OR, NOT, BY, HAVING, SET, RETURN, THEN, ELSE
//
// Crucially, unary stripping only applies when the operator directly precedes a
// LITERAL token (number/string/null). When it precedes an identifier (e.g. -a),
// the operator is kept.
func collapseUnaryOperators(tokens []digestToken) []digestToken {
	// We make multiple passes until no changes occur (handles chains like +++1).
	for {
		changed := false
		out := make([]digestToken, 0, len(tokens))
		i := 0
		for i < len(tokens) {
			t := tokens[i]
			// Check if this is a '+' or '-' operator followed by a literal
			if t.kind == tokOperator && (t.text == "+" || t.text == "-") && i+1 < len(tokens) && isLiteralTok(tokens[i+1]) {
				// Look back at the last non-unary-op token in `out` to determine context
				if isUnaryContext(out) {
					// Strip this unary operator
					changed = true
					i++ // skip the operator, keep the literal
					continue
				}
			}
			out = append(out, t)
			i++
		}
		tokens = out
		if !changed {
			break
		}
	}
	return tokens
}

// isUnaryContext returns true if the given token sequence (already emitted)
// indicates that a following '+'/'-' operator is in unary position.
func isUnaryContext(out []digestToken) bool {
	if len(out) == 0 {
		return true
	}
	prev := out[len(out)-1]
	switch prev.kind {
	case tokPunct:
		return prev.text == "(" || prev.text == ","
	case tokOperator:
		// Any binary comparison operator or logical operator puts next +/- in unary context
		// But NOT a preceding literal (binary context): that's handled by caller checking isLiteralTok
		return true
	case tokKeyword:
		// These keywords put the next token in an "expression start" context
		switch prev.text {
		case "SELECT", "WHERE", "AND", "OR", "NOT", "BY", "HAVING",
			"SET", "RETURN", "THEN", "ELSE", "ON", "WHEN",
			"BETWEEN", "IN", "EXISTS", "CASE", "UNION", "ALL",
			"VALUES", "VALUE":
			return true
		}
	}
	return false
}

// reclassifyNullTokens changes tokNull tokens to tokKeyword when they appear
// in IS NULL, IS NOT NULL, or NOT NULL column-constraint contexts. In MySQL
// digests, the NULL keyword in these constructs is kept as-is rather than
// replaced with '?'.
func reclassifyNullTokens(tokens []digestToken) []digestToken {
	for i, t := range tokens {
		if t.kind != tokNull {
			continue
		}
		// Look backwards for IS or NOT keyword
		j := i - 1
		// Skip over any other tokens to find IS or NOT
		if j >= 0 && tokens[j].kind == tokKeyword &&
			(tokens[j].text == "IS" || tokens[j].text == "NOT") {
			tokens[i] = digestToken{kind: tokKeyword, text: "NULL"}
		}
	}
	return tokens
}

// collapseInsideParenLiterals replaces "(L , L , L [, L]*)" — where each L is
// a literal token (number/string/null already) — with "(...)". This also
// handles the single-element IN-list form "(L)" when preceded by the IN keyword
// (MySQL collapses IN (single_value) to IN (...) as well).
func collapseInsideParenLiterals(tokens []digestToken) []digestToken {
	out := make([]digestToken, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		if tokens[i].kind == tokPunct && tokens[i].text == "(" {
			// Check if the previous token is IN keyword (for single-value IN-list collapsing)
			prevIsIN := len(out) > 0 && out[len(out)-1].kind == tokKeyword && out[len(out)-1].text == "IN"
			// Scan inner tokens until matching ')'
			depth := 1
			j := i + 1
			ok := true
			litCount := 0
			commaCount := 0
			for j < len(tokens) && depth > 0 {
				t := tokens[j]
				if t.kind == tokPunct {
					if t.text == "(" {
						depth++
						ok = false
					} else if t.text == ")" {
						depth--
						if depth == 0 {
							break
						}
					} else if t.text == "," && depth == 1 {
						commaCount++
					} else {
						ok = false
					}
				} else if t.kind == tokNumber || t.kind == tokString || t.kind == tokNull {
					if depth == 1 {
						litCount++
					}
				} else {
					ok = false
				}
				j++
			}
			// Collapse multi-literal lists: "(L,L,...,L)" -> "(...)"
			// Also collapse single-literal IN-lists: "IN (L)" -> "IN (...)"
			if ok && depth == 0 && litCount >= 1 && litCount == commaCount+1 &&
				(commaCount >= 1 || (prevIsIN && litCount == 1)) {
				// "(L,L,...,L)" -> "(...)"
				out = append(out, digestToken{kind: tokPunct, text: "("})
				out = append(out, digestToken{kind: tokKeyword, text: "..."})
				out = append(out, digestToken{kind: tokPunct, text: ")"})
				i = j + 1
				continue
			}
		}
		out = append(out, tokens[i])
		i++
	}
	return out
}

// collapseTopLevelLiteralList replaces runs of "L , L [, L]*" with "L , ...",
// matching MySQL's behaviour for things like:
//   "SELECT 1, 2, 3 FROM t1"                  -> "SELECT ?, ... FROM `t1`"
//   "SELECT * FROM (SELECT a, 1, 1 FROM t1)"  -> "SELECT * FROM (SELECT `a`, ?, ... FROM `t1`)"
//
// This applies at all nesting depths, not just the top level, because MySQL
// collapses literal sequences inside subquery bodies as well.
func collapseTopLevelLiteralList(tokens []digestToken) []digestToken {
	out := make([]digestToken, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		t := tokens[i]
		if isLiteralTok(t) {
			// Try to extend: literal , literal , ...
			j := i + 1
			litCount := 1
			for j+1 < len(tokens) && tokens[j].kind == tokPunct && tokens[j].text == "," && isLiteralTok(tokens[j+1]) {
				litCount++
				j += 2
			}
			if litCount >= 2 {
				out = append(out, t) // first literal
				out = append(out, digestToken{kind: tokCompactComma, text: ","})
				out = append(out, digestToken{kind: tokKeyword, text: "..."})
				i = j
				continue
			}
		}
		out = append(out, t)
		i++
	}
	return out
}

func isLiteralTok(t digestToken) bool {
	return t.kind == tokNumber || t.kind == tokString || t.kind == tokNull
}

// digestToken represents a single SQL token classified for digest purposes.
type digestToken struct {
	kind digestTokKind
	text string // textual form for digest_text rendering
}

type digestTokKind int

const (
	tokKeyword digestTokKind = iota
	tokIdent         // backtick-quoted identifier (the `text` is the unescaped name)
	tokNumber        // becomes `?`
	tokString        // becomes `?`
	tokNull          // becomes `?`
	tokPunct         // ( ) , ; . etc.
	tokOperator      // + - * / = < > etc.
	tokWhitespace
	tokCompactComma  // a comma from a collapsed literal list; renders without leading space
)

// sqlKeywords is a small set of common MySQL keywords. Tokens matching this
// set (case-insensitive) are uppercased and not backtick-quoted.
var sqlKeywords = map[string]bool{
	"SELECT": true, "FROM": true, "WHERE": true, "AND": true, "OR": true, "NOT": true,
	"INSERT": true, "INTO": true, "VALUES": true, "VALUE": true, "UPDATE": true,
	"DELETE": true, "SET": true, "AS": true, "ON": true, "USING": true,
	"INNER": true, "OUTER": true, "LEFT": true, "RIGHT": true, "FULL": true,
	"JOIN": true, "CROSS": true, "STRAIGHT_JOIN": true,
	"GROUP": true, "BY": true, "ORDER": true, "HAVING": true, "LIMIT": true, "OFFSET": true,
	"UNION": true, "ALL": true, "DISTINCT": true, "ANY": true, "SOME": true, "EXISTS": true,
	"CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
	"IF": true, "IFNULL": true, "NULLIF": true, "COALESCE": true,
	"CREATE": true, "DROP": true, "ALTER": true, "TABLE": true, "DATABASE": true,
	"SCHEMA": true, "INDEX": true, "VIEW": true, "TRIGGER": true, "PROCEDURE": true,
	"FUNCTION": true, "EVENT": true, "USER": true, "ROLE": true,
	"PRIMARY": true, "KEY": true, "UNIQUE": true, "FOREIGN": true, "REFERENCES": true,
	"CHECK": true, "CONSTRAINT": true, "DEFAULT": true, "AUTO_INCREMENT": true,
	"NULL": true, "TRUE": true, "FALSE": true, "UNKNOWN": true,
	"INT": true, "INTEGER": true, "BIGINT": true, "SMALLINT": true, "TINYINT": true,
	"MEDIUMINT": true, "DECIMAL": true, "NUMERIC": true, "FLOAT": true, "DOUBLE": true,
	"REAL": true, "BIT": true, "BOOLEAN": true, "BOOL": true, "DATE": true, "TIME": true,
	"DATETIME": true, "TIMESTAMP": true, "YEAR": true, "CHAR": true, "VARCHAR": true,
	"BINARY": true, "VARBINARY": true, "BLOB": true, "TEXT": true, "ENUM": true, "SET_": true,
	"CHARACTER": true, "JSON": true, "GEOMETRY": true, "POINT": true,
	"USE": true, "BEGIN": true, "COMMIT": true, "ROLLBACK": true, "SAVEPOINT": true,
	"START": true, "TRANSACTION": true, "WORK": true, "CHAIN": true, "RELEASE": true,
	"GRANT": true, "REVOKE": true, "WITH": true, "RECURSIVE": true,
	"PREPARE": true, "EXECUTE": true, "DEALLOCATE": true,
	"CALL": true, "RETURN": true, "RETURNS": true, "DECLARE": true,
	"BETWEEN": true, "LIKE": true, "ILIKE": true, "IN": true, "IS": true, "REGEXP": true, "RLIKE": true,
	"DESC": true, "ASC": true, "ESCAPE": true, "EXPLAIN": true, "ANALYZE": true,
	"TRUNCATE": true, "RENAME": true, "REPLACE": true, "MODIFY": true, "ADD": true, "COLUMN": true,
	"FOR": true, "EACH": true, "ROW": true, "BEFORE": true, "AFTER": true, "OF": true,
	"SHOW": true, "VARIABLES": true, "STATUS": true, "GLOBAL": true, "SESSION": true, "LOCAL": true,
	"IGNORE": true, "DUPLICATE": true, "DELAYED": true, "LOW_PRIORITY": true, "HIGH_PRIORITY": true,
	"DIV": true, "MOD": true, "XOR": true,
	"DUAL": true,
}

// tokenizeForDigest splits a SQL string into digest tokens, stripping comments.
func tokenizeForDigest(q string) []digestToken {
	var out []digestToken
	r := []rune(q)
	n := len(r)
	i := 0
	for i < n {
		c := r[i]
		// Whitespace
		if unicode.IsSpace(c) {
			i++
			continue
		}
		// Line comment "-- "
		if c == '-' && i+1 < n && r[i+1] == '-' && (i+2 >= n || unicode.IsSpace(r[i+2])) {
			for i < n && r[i] != '\n' {
				i++
			}
			continue
		}
		// Hash comment
		if c == '#' {
			for i < n && r[i] != '\n' {
				i++
			}
			continue
		}
		// Block comment
		if c == '/' && i+1 < n && r[i+1] == '*' {
			i += 2
			for i+1 < n && !(r[i] == '*' && r[i+1] == '/') {
				i++
			}
			if i+1 < n {
				i += 2
			} else {
				i = n
			}
			continue
		}
		// Backtick identifier
		if c == '`' {
			j := i + 1
			for j < n && r[j] != '`' {
				j++
			}
			name := string(r[i+1 : j])
			out = append(out, digestToken{kind: tokIdent, text: name})
			if j < n {
				j++
			}
			i = j
			continue
		}
		// String literal
		if c == '\'' || c == '"' {
			quote := c
			j := i + 1
			for j < n {
				if r[j] == '\\' && j+1 < n {
					j += 2
					continue
				}
				if r[j] == quote {
					if j+1 < n && r[j+1] == quote {
						j += 2
						continue
					}
					break
				}
				j++
			}
			if j < n {
				j++
			}
			out = append(out, digestToken{kind: tokString})
			i = j
			continue
		}
		// Number (incl decimals, exponent, optional negative handled at op level)
		if unicode.IsDigit(c) || (c == '.' && i+1 < n && unicode.IsDigit(r[i+1])) {
			j := i
			for j < n && (unicode.IsDigit(r[j]) || r[j] == '.') {
				j++
			}
			if j < n && (r[j] == 'e' || r[j] == 'E') {
				j++
				if j < n && (r[j] == '+' || r[j] == '-') {
					j++
				}
				for j < n && unicode.IsDigit(r[j]) {
					j++
				}
			}
			out = append(out, digestToken{kind: tokNumber})
			i = j
			continue
		}
		// Identifier / keyword (letters, digits, underscore, $, also unicode letters)
		if unicode.IsLetter(c) || c == '_' || c == '$' {
			j := i
			for j < n && (unicode.IsLetter(r[j]) || unicode.IsDigit(r[j]) || r[j] == '_' || r[j] == '$') {
				j++
			}
			word := string(r[i:j])
			upper := strings.ToUpper(word)
			if upper == "NULL" {
				out = append(out, digestToken{kind: tokNull})
			} else if sqlKeywords[upper] {
				out = append(out, digestToken{kind: tokKeyword, text: upper})
			} else {
				out = append(out, digestToken{kind: tokIdent, text: word})
			}
			i = j
			continue
		}
		// User variable: @varname -> @? (MySQL normalizes user variable names)
		if c == '@' {
			j := i + 1
			// Skip optional second @ (system variables like @@global.var)
			if j < n && r[j] == '@' {
				// System variable - not a user variable, fall through to operator
			} else if j < n && (unicode.IsLetter(r[j]) || r[j] == '_' || r[j] == '$' || unicode.IsDigit(r[j])) {
				// User variable @varname - consume the name and emit as literal (normalized to @?)
				for j < n && (unicode.IsLetter(r[j]) || unicode.IsDigit(r[j]) || r[j] == '_' || r[j] == '$' || r[j] == '.') {
					j++
				}
				// Emit @ followed by ? (user variable name is normalized to ?)
				out = append(out, digestToken{kind: tokOperator, text: "@"})
				out = append(out, digestToken{kind: tokNumber}) // renders as ?
				i = j
				continue
			}
		}
		// Punctuation single chars
		switch c {
		case '(', ')', ',', ';', '.':
			out = append(out, digestToken{kind: tokPunct, text: string(c)})
			i++
			continue
		}
		// Multi-char operators
		if c == '<' && i+1 < n && (r[i+1] == '=' || r[i+1] == '>') {
			out = append(out, digestToken{kind: tokOperator, text: string(r[i : i+2])})
			i += 2
			continue
		}
		if c == '>' && i+1 < n && r[i+1] == '=' {
			out = append(out, digestToken{kind: tokOperator, text: ">="})
			i += 2
			continue
		}
		if c == '!' && i+1 < n && r[i+1] == '=' {
			out = append(out, digestToken{kind: tokOperator, text: "!="})
			i += 2
			continue
		}
		if c == ':' && i+1 < n && r[i+1] == '=' {
			out = append(out, digestToken{kind: tokOperator, text: ":="})
			i += 2
			continue
		}
		if c == '|' && i+1 < n && r[i+1] == '|' {
			out = append(out, digestToken{kind: tokOperator, text: "||"})
			i += 2
			continue
		}
		if c == '&' && i+1 < n && r[i+1] == '&' {
			out = append(out, digestToken{kind: tokOperator, text: "&&"})
			i += 2
			continue
		}
		// Single-char operator fallback
		out = append(out, digestToken{kind: tokOperator, text: string(c)})
		i++
	}
	return out
}

// collapseValueLists collapses repeating "(...) , (...) , ..." patterns into
// a single "(...) /* , ... */" group, mirroring MySQL's digest reduction.
func collapseValueLists(tokens []digestToken) []digestToken {
	// Find each group sequence: '(' ... ')' ',' '(' ... ')' [ ',' '(' ... ')' ]*
	// When length >= 2 groups, replace with first group + " /* , ... */".
	out := make([]digestToken, 0, len(tokens))
	i := 0
	for i < len(tokens) {
		if tokens[i].kind == tokPunct && tokens[i].text == "(" {
			// Try to find the matching ")"
			depth := 1
			j := i + 1
			for j < len(tokens) && depth > 0 {
				if tokens[j].kind == tokPunct && tokens[j].text == "(" {
					depth++
				} else if tokens[j].kind == tokPunct && tokens[j].text == ")" {
					depth--
					if depth == 0 {
						break
					}
				}
				j++
			}
			if depth != 0 {
				out = append(out, tokens[i])
				i++
				continue
			}
			// We have a (...) group from i..j inclusive.
			// Look for repeating ", (..." pattern.
			groupCount := 1
			k := j + 1
			for k < len(tokens) && tokens[k].kind == tokPunct && tokens[k].text == "," {
				if k+1 < len(tokens) && tokens[k+1].kind == tokPunct && tokens[k+1].text == "(" {
					// Find matching ")"
					d := 1
					m := k + 2
					for m < len(tokens) && d > 0 {
						if tokens[m].kind == tokPunct && tokens[m].text == "(" {
							d++
						} else if tokens[m].kind == tokPunct && tokens[m].text == ")" {
							d--
							if d == 0 {
								break
							}
						}
						m++
					}
					if d != 0 {
						break
					}
					groupCount++
					k = m + 1
					continue
				}
				break
			}
			if groupCount >= 2 {
				// Append the first group, then a synthetic " /* , ... */ " marker.
				out = append(out, tokens[i:j+1]...)
				out = append(out, digestToken{kind: tokKeyword, text: "/* , ... */"})
				i = k
				continue
			}
		}
		out = append(out, tokens[i])
		i++
	}
	return out
}

// joinDigestTokens renders the token stream as the final digest_text string.
// Spacing rules:
//   - No space after '(' or '.'
//   - No space before ';' or '.'
//   - No space before/after tokCompactComma (collapsed literal-list comma)
//   - Space before ',' and ')' when inside a CREATE TABLE column list
//   - Single space between most other tokens
func joinDigestTokens(tokens []digestToken) string {
	// Detect CREATE TABLE DDL to apply column-list spacing inside its outer parens.
	isCreateTableDDL := len(tokens) >= 2 &&
		tokens[0].kind == tokKeyword && tokens[0].text == "CREATE"
	if isCreateTableDDL {
		found := false
		end := len(tokens)
		if end > 4 {
			end = 4
		}
		for _, t := range tokens[1:end] {
			if t.kind == tokKeyword && t.text == "TABLE" {
				found = true
				break
			}
		}
		isCreateTableDDL = found
	}

	// Find the outer paren range for CREATE TABLE column definitions.
	outerParenOpen := -1
	outerParenClose := -1
	if isCreateTableDDL {
		for i, t := range tokens {
			if t.kind == tokPunct && t.text == "(" {
				outerParenOpen = i
				depth := 1
				for j := i + 1; j < len(tokens); j++ {
					if tokens[j].kind == tokPunct && tokens[j].text == "(" {
						depth++
					} else if tokens[j].kind == tokPunct && tokens[j].text == ")" {
						depth--
						if depth == 0 {
							outerParenClose = j
							break
						}
					}
				}
				break
			}
		}
	}

	// typeAliases maps keyword aliases that MySQL normalizes in digests.
	// INT → INTEGER, CHAR → CHARACTER, DATABASE → SCHEMA.
	typeAliases := map[string]string{
		"INT":      "INTEGER",
		"CHAR":     "CHARACTER",
		"DATABASE": "SCHEMA",
	}

	var b strings.Builder
	for i, t := range tokens {
		var rendered string
		switch t.kind {
		case tokKeyword:
			if norm, ok := typeAliases[t.text]; ok {
				rendered = norm
			} else {
				rendered = t.text
			}
		case tokIdent:
			rendered = "`" + strings.ReplaceAll(t.text, "`", "``") + "`"
		case tokNumber, tokString, tokNull:
			rendered = "?"
		case tokPunct:
			rendered = t.text
		case tokOperator:
			rendered = t.text
		case tokCompactComma:
			rendered = ","
		}
		if i > 0 {
			prev := tokens[i-1]
			inColList := outerParenOpen >= 0 && i > outerParenOpen && i <= outerParenClose
			if inColList {
				// Inside CREATE TABLE column list: space after '(', before ')' and ','
				if i == outerParenOpen+1 {
					// First token after outer '(': add space
					b.WriteByte(' ')
				} else if t.kind == tokPunct && t.text == ")" && i == outerParenClose {
					// Closing outer ')': add space before it
					b.WriteByte(' ')
				} else if t.kind == tokPunct && t.text == "," {
					// Comma between column definitions: space before ','
					b.WriteByte(' ')
				} else if needsSpace(prev, t) {
					b.WriteByte(' ')
				}
			} else if needsSpace(prev, t) {
				b.WriteByte(' ')
			}
		}
		b.WriteString(rendered)
	}
	return b.String()
}

func needsSpace(prev, cur digestToken) bool {
	// No space before tokCompactComma (it glues directly to preceding token)
	if cur.kind == tokCompactComma {
		return false
	}
	// No space after '('
	if prev.kind == tokPunct && prev.text == "(" {
		return false
	}
	// No space before ')'
	if cur.kind == tokPunct && cur.text == ")" {
		return false
	}
	// No space after '@' (user variable prefix: @? renders without space)
	if prev.kind == tokOperator && prev.text == "@" {
		return false
	}
	return true
}
