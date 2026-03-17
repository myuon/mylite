package executor

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// jsonNormalize parses a Go value into a normalized JSON interface{} value.
// Returns the parsed JSON value suitable for JSON operations.
func jsonNormalize(val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	s := toString(val)
	s = strings.TrimSpace(s)
	var result interface{}
	if err := json.Unmarshal([]byte(s), &result); err != nil {
		return nil, fmt.Errorf("invalid JSON value: %s", s)
	}
	return result, nil
}

// jsonMarshal converts a Go value to a JSON string using MySQL conventions.
func jsonMarshal(val interface{}) string {
	if val == nil {
		return "null"
	}
	b, err := json.Marshal(val)
	if err != nil {
		return "null"
	}
	return string(b)
}

// jsonMarshalMySQL converts a Go value to a MySQL-compatible JSON string.
// MySQL formats: no spaces after : or , in compact form, uses specific number formatting.
func jsonMarshalMySQL(val interface{}) string {
	return jsonMarshalMySQLIndent(val, false)
}

func jsonMarshalMySQLIndent(val interface{}, pretty bool) string {
	if val == nil {
		return "null"
	}
	switch v := val.(type) {
	case uint64:
		return strconv.FormatUint(v, 10)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case float64:
		if v == 0 {
			return "0"
		}
		// MySQL: integers are rendered without decimal point
		if v == math.Trunc(v) && !math.IsInf(v, 0) && !math.IsNaN(v) && math.Abs(v) < 1e18 {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		b, _ := json.Marshal(v)
		return string(b)
	case []interface{}:
		if len(v) == 0 {
			return "[]"
		}
		if pretty {
			var parts []string
			for _, elem := range v {
				parts = append(parts, jsonMarshalMySQLIndent(elem, true))
			}
			inner := strings.Join(parts, ",\n")
			// Indent each line
			lines := strings.Split(inner, "\n")
			for i, l := range lines {
				lines[i] = "  " + l
			}
			return "[\n" + strings.Join(lines, "\n") + "\n]"
		}
		var parts []string
		for _, elem := range v {
			parts = append(parts, jsonMarshalMySQL(elem))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]interface{}:
		if len(v) == 0 {
			return "{}"
		}
		// MySQL sorts keys alphabetically
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if pretty {
			var parts []string
			for _, k := range keys {
				kb, _ := json.Marshal(k)
				parts = append(parts, string(kb)+": "+jsonMarshalMySQLIndent(v[k], true))
			}
			inner := strings.Join(parts, ",\n")
			lines := strings.Split(inner, "\n")
			for i, l := range lines {
				lines[i] = "  " + l
			}
			return "{\n" + strings.Join(lines, "\n") + "\n}"
		}
		var parts []string
		for _, k := range keys {
			kb, _ := json.Marshal(k)
			parts = append(parts, string(kb)+": "+jsonMarshalMySQL(v[k]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// toJSONValue converts an executor value to a JSON-compatible value for embedding.
func toJSONValue(val interface{}) interface{} {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case int64:
		return float64(v)
	case uint64:
		return float64(v)
	case float64:
		return v
	case bool:
		return v
	case string:
		// Try to parse as JSON first
		var parsed interface{}
		if err := json.Unmarshal([]byte(v), &parsed); err == nil {
			return parsed
		}
		return v
	default:
		return toString(val)
	}
}

// evalJSONExtract implements JSON_EXTRACT(json_doc, path[, path...])
func (e *Executor) evalJSONExtract(v *sqlparser.JSONExtractExpr) (interface{}, error) {
	docVal, err := e.evalExpr(v.JSONDoc)
	if err != nil {
		return nil, err
	}
	if docVal == nil {
		return nil, nil
	}
	doc, err := jsonNormalize(docVal)
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, p := range v.PathList {
		pv, err := e.evalExpr(p)
		if err != nil {
			return nil, err
		}
		if pv == nil {
			return nil, nil
		}
		paths = append(paths, toString(pv))
	}

	if len(paths) == 1 {
		result := jsonExtractPath(doc, paths[0])
		if result == nil {
			return nil, nil
		}
		return jsonMarshalMySQL(result), nil
	}

	// Multiple paths: return JSON array of results
	var results []interface{}
	for _, p := range paths {
		result := jsonExtractPath(doc, p)
		results = append(results, result)
	}
	return jsonMarshalMySQL(results), nil
}

// jsonExtractPath extracts a value from a JSON document using a JSON path.
func jsonExtractPath(doc interface{}, path string) interface{} {
	if path == "$" {
		return doc
	}
	// Remove leading $
	if !strings.HasPrefix(path, "$") {
		return nil
	}
	remaining := path[1:]
	return jsonExtractPathInternal(doc, remaining)
}

func jsonExtractPathInternal(doc interface{}, path string) interface{} {
	if path == "" {
		return doc
	}

	if strings.HasPrefix(path, ".") {
		path = path[1:]
		// Handle quoted key: ."key"
		if strings.HasPrefix(path, "\"") {
			end := strings.Index(path[1:], "\"")
			if end < 0 {
				return nil
			}
			key := path[1 : end+1]
			remaining := path[end+2:]
			obj, ok := doc.(map[string]interface{})
			if !ok {
				return nil
			}
			val, exists := obj[key]
			if !exists {
				return nil
			}
			return jsonExtractPathInternal(val, remaining)
		}
		// Handle wildcard: .*
		if strings.HasPrefix(path, "*") {
			remaining := path[1:]
			obj, ok := doc.(map[string]interface{})
			if !ok {
				return nil
			}
			keys := make([]string, 0, len(obj))
			for k := range obj {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var results []interface{}
			for _, k := range keys {
				val := jsonExtractPathInternal(obj[k], remaining)
				if val != nil {
					results = append(results, val)
				}
			}
			if len(results) == 0 {
				return nil
			}
			return results
		}
		// Handle regular key
		var key string
		rest := path
		for i, c := range path {
			if c == '.' || c == '[' {
				key = path[:i]
				rest = path[i:]
				break
			}
		}
		if key == "" {
			key = rest
			rest = ""
		}
		obj, ok := doc.(map[string]interface{})
		if !ok {
			return nil
		}
		val, exists := obj[key]
		if !exists {
			return nil
		}
		return jsonExtractPathInternal(val, rest)
	}

	if strings.HasPrefix(path, "[") {
		end := strings.Index(path, "]")
		if end < 0 {
			return nil
		}
		indexStr := path[1:end]
		remaining := path[end+1:]

		if indexStr == "*" {
			arr, ok := doc.([]interface{})
			if !ok {
				return nil
			}
			var results []interface{}
			for _, elem := range arr {
				val := jsonExtractPathInternal(elem, remaining)
				if val != nil {
					results = append(results, val)
				}
			}
			return results
		}

		idx, err := strconv.Atoi(indexStr)
		if err != nil {
			return nil
		}
		arr, ok := doc.([]interface{})
		if !ok {
			// MySQL-compatible behavior used by JSON generated columns:
			// scalar JSON values can be addressed as $[0].
			if idx == 0 {
				return jsonExtractPathInternal(doc, remaining)
			}
			return nil
		}
		if idx < 0 || idx >= len(arr) {
			return nil
		}
		return jsonExtractPathInternal(arr[idx], remaining)
	}

	// Handle ** wildcard (recursive descent)
	if strings.HasPrefix(path, "**") {
		remaining := path[2:]
		return jsonRecursiveSearch(doc, remaining)
	}

	return nil
}

func jsonRecursiveSearch(doc interface{}, path string) interface{} {
	var results []interface{}

	val := jsonExtractPathInternal(doc, path)
	if val != nil {
		results = append(results, val)
	}

	switch v := doc.(type) {
	case map[string]interface{}:
		for _, child := range v {
			r := jsonRecursiveSearch(child, path)
			if r != nil {
				if arr, ok := r.([]interface{}); ok {
					results = append(results, arr...)
				} else {
					results = append(results, r)
				}
			}
		}
	case []interface{}:
		for _, child := range v {
			r := jsonRecursiveSearch(child, path)
			if r != nil {
				if arr, ok := r.([]interface{}); ok {
					results = append(results, arr...)
				} else {
					results = append(results, r)
				}
			}
		}
	}

	if len(results) == 0 {
		return nil
	}
	if len(results) == 1 {
		return results[0]
	}
	return results
}

// evalJSONAttributes handles JSON_VALID, JSON_TYPE, JSON_DEPTH, JSON_LENGTH
func (e *Executor) evalJSONAttributes(v *sqlparser.JSONAttributesExpr) (interface{}, error) {
	docVal, err := e.evalExpr(v.JSONDoc)
	if err != nil {
		return nil, err
	}

	switch v.Type {
	case sqlparser.ValidAttributeType:
		// JSON_VALID
		if docVal == nil {
			return nil, nil
		}
		s := toString(docVal)
		var js interface{}
		if err := json.Unmarshal([]byte(s), &js); err != nil {
			return int64(0), nil
		}
		return int64(1), nil

	case sqlparser.TypeAttributeType:
		// JSON_TYPE
		if docVal == nil {
			return nil, nil
		}
		doc, err := jsonNormalize(docVal)
		if err != nil {
			return nil, mysqlError(3141, "22032", "Invalid JSON text in argument 1 to function json_type: \"Invalid value.\" at position 0.")
		}
		return jsonTypeName(doc), nil

	case sqlparser.DepthAttributeType:
		// JSON_DEPTH
		if docVal == nil {
			return nil, nil
		}
		doc, err := jsonNormalize(docVal)
		if err != nil {
			return nil, err
		}
		return int64(jsonDepth(doc)), nil

	case sqlparser.LengthAttributeType:
		// JSON_LENGTH
		if docVal == nil {
			return nil, nil
		}
		doc, err := jsonNormalize(docVal)
		if err != nil {
			return nil, err
		}

		// Handle optional path argument
		if v.Path != nil {
			pathVal, err := e.evalExpr(v.Path)
			if err != nil {
				return nil, err
			}
			if pathVal == nil {
				return nil, nil
			}
			doc = jsonExtractPath(doc, toString(pathVal))
			if doc == nil {
				return nil, nil
			}
		}

		switch d := doc.(type) {
		case map[string]interface{}:
			return int64(len(d)), nil
		case []interface{}:
			return int64(len(d)), nil
		default:
			return int64(1), nil
		}
	}
	return nil, fmt.Errorf("unsupported JSON attribute type")
}

func jsonTypeName(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	switch v := val.(type) {
	case bool:
		_ = v
		return "BOOLEAN"
	case float64:
		if v == math.Trunc(v) && !math.IsInf(v, 0) && math.Abs(v) < 1e18 {
			return "INTEGER"
		}
		return "DOUBLE"
	case string:
		return "STRING"
	case []interface{}:
		return "ARRAY"
	case map[string]interface{}:
		return "OBJECT"
	default:
		return "OPAQUE"
	}
}

func jsonDepth(val interface{}) int {
	switch v := val.(type) {
	case map[string]interface{}:
		if len(v) == 0 {
			return 1
		}
		maxChild := 0
		for _, child := range v {
			d := jsonDepth(child)
			if d > maxChild {
				maxChild = d
			}
		}
		return maxChild + 1
	case []interface{}:
		if len(v) == 0 {
			return 1
		}
		maxChild := 0
		for _, child := range v {
			d := jsonDepth(child)
			if d > maxChild {
				maxChild = d
			}
		}
		return maxChild + 1
	default:
		return 1
	}
}

// evalJSONObject implements JSON_OBJECT(key, value, ...)
func (e *Executor) evalJSONObject(v *sqlparser.JSONObjectExpr) (interface{}, error) {
	obj := make(map[string]interface{})
	for _, p := range v.Params {
		keyVal, err := e.evalExpr(p.Key)
		if err != nil {
			return nil, err
		}
		if keyVal == nil {
			return nil, mysqlError(3158, "22032", "JSON documents may not contain NULL member names.")
		}
		valVal, err := e.evalExpr(p.Value)
		if err != nil {
			return nil, err
		}
		obj[toString(keyVal)] = toJSONValue(valVal)
	}
	return jsonMarshalMySQL(obj), nil
}

// evalJSONArray implements JSON_ARRAY(val, ...)
func (e *Executor) evalJSONArray(v *sqlparser.JSONArrayExpr) (interface{}, error) {
	arr := make([]interface{}, 0, len(v.Params))
	for _, p := range v.Params {
		val, err := e.evalExpr(p)
		if err != nil {
			return nil, err
		}
		arr = append(arr, toJSONValue(val))
	}
	return jsonMarshalMySQL(arr), nil
}

// evalJSONContains implements JSON_CONTAINS(target, candidate[, path])
func (e *Executor) evalJSONContains(v *sqlparser.JSONContainsExpr) (interface{}, error) {
	targetVal, err := e.evalExpr(v.Target)
	if err != nil {
		return nil, err
	}
	if targetVal == nil {
		return nil, nil
	}
	candidateVal, err := e.evalExpr(v.Candidate)
	if err != nil {
		return nil, err
	}
	if candidateVal == nil {
		return nil, nil
	}

	target, err := jsonNormalize(targetVal)
	if err != nil {
		return nil, err
	}
	candidate, err := jsonNormalize(candidateVal)
	if err != nil {
		return nil, err
	}

	if len(v.PathList) > 0 {
		pathVal, err := e.evalExpr(v.PathList[0])
		if err != nil {
			return nil, err
		}
		target = jsonExtractPath(target, toString(pathVal))
		if target == nil {
			return nil, nil
		}
	}

	if jsonContains(target, candidate) {
		return int64(1), nil
	}
	return int64(0), nil
}

func jsonContains(target, candidate interface{}) bool {
	if target == nil && candidate == nil {
		return true
	}
	if target == nil || candidate == nil {
		return false
	}

	switch t := target.(type) {
	case map[string]interface{}:
		c, ok := candidate.(map[string]interface{})
		if !ok {
			return false
		}
		for k, cv := range c {
			tv, exists := t[k]
			if !exists || !jsonContains(tv, cv) {
				return false
			}
		}
		return true
	case []interface{}:
		switch c := candidate.(type) {
		case []interface{}:
			for _, cv := range c {
				found := false
				for _, tv := range t {
					if jsonContains(tv, cv) {
						found = true
						break
					}
				}
				if !found {
					return false
				}
			}
			return true
		default:
			for _, tv := range t {
				if jsonContains(tv, candidate) {
					return true
				}
			}
			return false
		}
	default:
		return jsonEqual(target, candidate)
	}
}

func jsonEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Normalize numbers
	fa, aIsNum := toJSONNumber(a)
	fb, bIsNum := toJSONNumber(b)
	if aIsNum && bIsNum {
		return fa == fb
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func toJSONNumber(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// evalJSONContainsPath implements JSON_CONTAINS_PATH(json_doc, one_or_all, path[, path...])
func (e *Executor) evalJSONContainsPath(v *sqlparser.JSONContainsPathExpr) (interface{}, error) {
	docVal, err := e.evalExpr(v.JSONDoc)
	if err != nil {
		return nil, err
	}
	if docVal == nil {
		return nil, nil
	}
	doc, err := jsonNormalize(docVal)
	if err != nil {
		return nil, err
	}

	modeVal, err := e.evalExpr(v.OneOrAll)
	if err != nil {
		return nil, err
	}
	mode := strings.ToLower(toString(modeVal))

	for _, p := range v.PathList {
		pv, err := e.evalExpr(p)
		if err != nil {
			return nil, err
		}
		path := toString(pv)
		found := jsonExtractPath(doc, path) != nil

		if mode == "one" && found {
			return int64(1), nil
		}
		if mode == "all" && !found {
			return int64(0), nil
		}
	}

	if mode == "one" {
		return int64(0), nil
	}
	return int64(1), nil
}

// evalJSONKeys implements JSON_KEYS(json_doc[, path])
func (e *Executor) evalJSONKeys(v *sqlparser.JSONKeysExpr) (interface{}, error) {
	docVal, err := e.evalExpr(v.JSONDoc)
	if err != nil {
		return nil, err
	}
	if docVal == nil {
		return nil, nil
	}
	doc, err := jsonNormalize(docVal)
	if err != nil {
		return nil, err
	}

	if v.Path != nil {
		pathVal, err := e.evalExpr(v.Path)
		if err != nil {
			return nil, err
		}
		doc = jsonExtractPath(doc, toString(pathVal))
		if doc == nil {
			return nil, nil
		}
	}

	obj, ok := doc.(map[string]interface{})
	if !ok {
		return nil, nil
	}

	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	arr := make([]interface{}, len(keys))
	for i, k := range keys {
		arr[i] = k
	}
	return jsonMarshalMySQL(arr), nil
}

// evalJSONSearch implements JSON_SEARCH(json_doc, one_or_all, search_str[, escape_char[, path...]])
func (e *Executor) evalJSONSearch(v *sqlparser.JSONSearchExpr) (interface{}, error) {
	docVal, err := e.evalExpr(v.JSONDoc)
	if err != nil {
		return nil, err
	}
	if docVal == nil {
		return nil, nil
	}
	doc, err := jsonNormalize(docVal)
	if err != nil {
		return nil, err
	}

	modeVal, err := e.evalExpr(v.OneOrAll)
	if err != nil {
		return nil, err
	}
	mode := strings.ToLower(toString(modeVal))

	searchVal, err := e.evalExpr(v.SearchStr)
	if err != nil {
		return nil, err
	}
	if searchVal == nil {
		return nil, nil
	}
	searchStr := toString(searchVal)

	escapeChar := '\\'
	if v.EscapeChar != nil {
		escVal, err := e.evalExpr(v.EscapeChar)
		if err != nil {
			return nil, err
		}
		if escVal != nil {
			escStr := toString(escVal)
			if len(escStr) > 0 {
				escapeChar = rune(escStr[0])
			}
		}
	}

	// Build regex from LIKE-style pattern
	pattern := likeToRegex(searchStr, escapeChar)

	var searchPaths []string
	for _, p := range v.PathList {
		pv, err := e.evalExpr(p)
		if err != nil {
			return nil, err
		}
		searchPaths = append(searchPaths, toString(pv))
	}

	var results []string
	jsonSearchInternal(doc, "$", pattern, mode, searchPaths, &results)

	if len(results) == 0 {
		return nil, nil
	}
	if mode == "one" {
		return "\"" + results[0] + "\"", nil
	}
	// Return as JSON array of strings
	arr := make([]interface{}, len(results))
	for i, r := range results {
		arr[i] = r
	}
	return jsonMarshalMySQL(arr), nil
}

func likeToRegex(pattern string, escape rune) *regexp.Regexp {
	var sb strings.Builder
	sb.WriteString("^")
	runes := []rune(pattern)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		if c == escape && i+1 < len(runes) {
			sb.WriteString(regexp.QuoteMeta(string(runes[i+1])))
			i++
		} else if c == '%' {
			sb.WriteString(".*")
		} else if c == '_' {
			sb.WriteString(".")
		} else {
			sb.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	sb.WriteString("$")
	re, _ := regexp.Compile("(?i)" + sb.String())
	return re
}

func jsonSearchInternal(doc interface{}, currentPath string, pattern *regexp.Regexp, mode string, searchPaths []string, results *[]string) {
	if mode == "one" && len(*results) > 0 {
		return
	}

	// Check if this path matches search path constraints
	if len(searchPaths) > 0 {
		match := false
		for _, sp := range searchPaths {
			if pathMatches(currentPath, sp) {
				match = true
				break
			}
		}
		if !match {
			return
		}
	}

	switch v := doc.(type) {
	case string:
		if pattern.MatchString(v) {
			*results = append(*results, currentPath)
		}
	case map[string]interface{}:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			childPath := currentPath + "." + k
			jsonSearchInternal(v[k], childPath, pattern, mode, searchPaths, results)
		}
	case []interface{}:
		for i, elem := range v {
			childPath := fmt.Sprintf("%s[%d]", currentPath, i)
			jsonSearchInternal(elem, childPath, pattern, mode, searchPaths, results)
		}
	}
}

func pathMatches(path, pattern string) bool {
	// Simple path matching - for now just check prefix
	if pattern == "$" {
		return true
	}
	return strings.HasPrefix(path, pattern)
}

// evalJSONRemove implements JSON_REMOVE(json_doc, path[, path...])
func (e *Executor) evalJSONRemove(v *sqlparser.JSONRemoveExpr) (interface{}, error) {
	docVal, err := e.evalExpr(v.JSONDoc)
	if err != nil {
		return nil, err
	}
	if docVal == nil {
		return nil, nil
	}
	doc, err := jsonNormalize(docVal)
	if err != nil {
		return nil, err
	}

	for _, p := range v.PathList {
		pv, err := e.evalExpr(p)
		if err != nil {
			return nil, err
		}
		path := toString(pv)
		doc = jsonRemovePath(doc, path)
	}
	return jsonMarshalMySQL(doc), nil
}

func jsonRemovePath(doc interface{}, path string) interface{} {
	if path == "$" {
		return doc // Can't remove root
	}
	if !strings.HasPrefix(path, "$") {
		return doc
	}
	remaining := path[1:]
	return jsonRemoveInternal(doc, remaining)
}

func jsonRemoveInternal(doc interface{}, path string) interface{} {
	if path == "" {
		return doc
	}

	// Simple key removal: .key
	if strings.HasPrefix(path, ".") {
		rest := path[1:]
		// Check for nested path
		nextDot := strings.IndexAny(rest, ".[")
		if nextDot < 0 {
			// Simple key at current level
			if obj, ok := doc.(map[string]interface{}); ok {
				delete(obj, rest)
			}
			return doc
		}
		key := rest[:nextDot]
		remaining := rest[nextDot:]
		if obj, ok := doc.(map[string]interface{}); ok {
			if child, exists := obj[key]; exists {
				obj[key] = jsonRemoveInternal(child, remaining)
			}
		}
		return doc
	}

	// Array index removal: [N]
	if strings.HasPrefix(path, "[") {
		end := strings.Index(path, "]")
		if end < 0 {
			return doc
		}
		indexStr := path[1:end]
		idx, err := strconv.Atoi(indexStr)
		if err != nil {
			return doc
		}
		remaining := path[end+1:]

		if remaining == "" {
			// Remove element at index
			if arr, ok := doc.([]interface{}); ok {
				if idx >= 0 && idx < len(arr) {
					newArr := make([]interface{}, 0, len(arr)-1)
					newArr = append(newArr, arr[:idx]...)
					newArr = append(newArr, arr[idx+1:]...)
					return newArr
				}
			}
			return doc
		}

		// Nested removal within array element
		if arr, ok := doc.([]interface{}); ok {
			if idx >= 0 && idx < len(arr) {
				arr[idx] = jsonRemoveInternal(arr[idx], remaining)
			}
		}
		return doc
	}

	return doc
}

// evalJSONValueModifier implements JSON_SET, JSON_INSERT, JSON_REPLACE, JSON_ARRAY_APPEND, JSON_ARRAY_INSERT
func (e *Executor) evalJSONValueModifier(v *sqlparser.JSONValueModifierExpr) (interface{}, error) {
	docVal, err := e.evalExpr(v.JSONDoc)
	if err != nil {
		return nil, err
	}
	if docVal == nil {
		return nil, nil
	}
	doc, err := jsonNormalize(docVal)
	if err != nil {
		return nil, err
	}

	for _, p := range v.Params {
		pathVal, err := e.evalExpr(p.Key)
		if err != nil {
			return nil, err
		}
		valVal, err := e.evalExpr(p.Value)
		if err != nil {
			return nil, err
		}

		path := toString(pathVal)
		newVal := toJSONValue(valVal)

		switch v.Type {
		case sqlparser.JSONSetType:
			doc = jsonSetPath(doc, path, newVal, true, true)
		case sqlparser.JSONInsertType:
			doc = jsonSetPath(doc, path, newVal, true, false) // insert only if not exists
		case sqlparser.JSONReplaceType:
			doc = jsonSetPath(doc, path, newVal, false, true) // replace only if exists
		case sqlparser.JSONArrayAppendType:
			doc = jsonArrayAppend(doc, path, newVal)
		case sqlparser.JSONArrayInsertType:
			doc = jsonArrayInsert(doc, path, newVal)
		}
	}

	return jsonMarshalMySQL(doc), nil
}

func jsonSetPath(doc interface{}, path string, val interface{}, createNew, replaceExisting bool) interface{} {
	if path == "$" {
		if replaceExisting {
			return val
		}
		return doc
	}
	if !strings.HasPrefix(path, "$") {
		return doc
	}
	remaining := path[1:]
	return jsonSetPathInternalFull(doc, remaining, val, createNew, replaceExisting)
}

func jsonSetPathInternalFull(doc interface{}, path string, val interface{}, createNew, replaceExisting bool) interface{} {
	if path == "" {
		if replaceExisting {
			return val
		}
		return doc
	}

	if strings.HasPrefix(path, ".") {
		path = path[1:]
		var key, rest string
		for i, c := range path {
			if c == '.' || c == '[' {
				key = path[:i]
				rest = path[i:]
				break
			}
		}
		if key == "" {
			key = path
			rest = ""
		}

		if obj, ok := doc.(map[string]interface{}); ok {
			if rest == "" {
				_, exists := obj[key]
				if exists && replaceExisting {
					obj[key] = val
				} else if !exists && createNew {
					obj[key] = val
				}
			} else {
				child, exists := obj[key]
				if exists {
					obj[key] = jsonSetPathInternalFull(child, rest, val, createNew, replaceExisting)
				} else if createNew {
					// Create intermediate objects
					obj[key] = jsonSetPathInternalFull(nil, rest, val, createNew, replaceExisting)
				}
			}
			return doc
		}
		return doc
	}

	if strings.HasPrefix(path, "[") {
		end := strings.Index(path, "]")
		if end < 0 {
			return doc
		}
		indexStr := path[1:end]
		rest := path[end+1:]

		idx, err := strconv.Atoi(indexStr)
		if err != nil {
			return doc
		}

		arr, ok := doc.([]interface{})
		if !ok {
			return doc
		}

		if rest == "" {
			if idx >= 0 && idx < len(arr) {
				if replaceExisting {
					arr[idx] = val
				}
			} else if createNew {
				// Append to end if index is out of range
				arr = append(arr, val)
				return arr
			}
			return doc
		}

		if idx >= 0 && idx < len(arr) {
			arr[idx] = jsonSetPathInternalFull(arr[idx], rest, val, createNew, replaceExisting)
		}
		return doc
	}

	return doc
}

func jsonSetPathInternal(doc interface{}, path string, val interface{}) {
	// Used to update a nested value
	if path == "" {
		return
	}
	if strings.HasPrefix(path, ".") {
		key := path[1:]
		if obj, ok := doc.(map[string]interface{}); ok {
			obj[key] = val
		}
		return
	}
	if strings.HasPrefix(path, "[") {
		end := strings.Index(path, "]")
		if end < 0 {
			return
		}
		indexStr := path[1:end]
		idx, err := strconv.Atoi(indexStr)
		if err != nil {
			return
		}
		if arr, ok := doc.([]interface{}); ok {
			if idx >= 0 && idx < len(arr) {
				arr[idx] = val
			}
		}
	}
}

func jsonArrayAppend(doc interface{}, path string, val interface{}) interface{} {
	if path == "$" {
		if arr, ok := doc.([]interface{}); ok {
			return append(arr, val)
		}
		return []interface{}{doc, val}
	}
	target := jsonExtractPath(doc, path)
	if target == nil {
		return doc
	}
	var newArr []interface{}
	if arr, ok := target.([]interface{}); ok {
		newArr = append(arr, val)
	} else {
		newArr = []interface{}{target, val}
	}
	remaining := path[1:] // Remove $
	jsonSetPathInternal(doc, remaining, newArr)
	return doc
}

func jsonArrayInsert(doc interface{}, path string, val interface{}) interface{} {
	if !strings.HasPrefix(path, "$") {
		return doc
	}
	// Find the array and index in the path
	lastBracket := strings.LastIndex(path, "[")
	if lastBracket < 0 {
		return doc
	}
	end := strings.Index(path[lastBracket:], "]")
	if end < 0 {
		return doc
	}
	indexStr := path[lastBracket+1 : lastBracket+end]
	idx, err := strconv.Atoi(indexStr)
	if err != nil {
		return doc
	}
	arrayPath := path[:lastBracket]
	if arrayPath == "$" {
		arrayPath = "$"
	}
	target := jsonExtractPath(doc, arrayPath)
	if arr, ok := target.([]interface{}); ok {
		if idx >= len(arr) {
			arr = append(arr, val)
		} else if idx < 0 {
			arr = append([]interface{}{val}, arr...)
		} else {
			arr = append(arr[:idx], append([]interface{}{val}, arr[idx:]...)...)
		}
		if arrayPath == "$" {
			return arr
		}
		remaining := arrayPath[1:]
		jsonSetPathInternal(doc, remaining, arr)
	}
	return doc
}

// evalJSONValueMerge implements JSON_MERGE_PRESERVE, JSON_MERGE_PATCH, JSON_MERGE
func (e *Executor) evalJSONValueMerge(v *sqlparser.JSONValueMergeExpr) (interface{}, error) {
	docVal, err := e.evalExpr(v.JSONDoc)
	if err != nil {
		return nil, err
	}
	if docVal == nil {
		return nil, nil
	}
	doc, err := jsonNormalize(docVal)
	if err != nil {
		return nil, err
	}

	for _, d := range v.JSONDocList {
		val, err := e.evalExpr(d)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		other, err := jsonNormalize(val)
		if err != nil {
			return nil, err
		}

		switch v.Type {
		case sqlparser.JSONMergePatchType:
			doc = jsonMergePatch(doc, other)
		default: // JSONMergeType, JSONMergePreserveType
			doc = jsonMergePreserve(doc, other)
		}
	}

	return jsonMarshalMySQL(doc), nil
}

func jsonMergePreserve(a, b interface{}) interface{} {
	aObj, aIsObj := a.(map[string]interface{})
	bObj, bIsObj := b.(map[string]interface{})

	if aIsObj && bIsObj {
		result := make(map[string]interface{})
		for k, v := range aObj {
			result[k] = v
		}
		for k, v := range bObj {
			if existing, ok := result[k]; ok {
				result[k] = jsonMergePreserve(existing, v)
			} else {
				result[k] = v
			}
		}
		return result
	}

	// Wrap non-arrays into arrays and concatenate
	var aArr, bArr []interface{}
	if arr, ok := a.([]interface{}); ok {
		aArr = arr
	} else {
		aArr = []interface{}{a}
	}
	if arr, ok := b.([]interface{}); ok {
		bArr = arr
	} else {
		bArr = []interface{}{b}
	}
	return append(aArr, bArr...)
}

func jsonMergePatch(a, b interface{}) interface{} {
	bObj, bIsObj := b.(map[string]interface{})
	if !bIsObj {
		return b
	}

	aObj, aIsObj := a.(map[string]interface{})
	if !aIsObj {
		aObj = make(map[string]interface{})
	}

	result := make(map[string]interface{})
	for k, v := range aObj {
		result[k] = v
	}
	for k, v := range bObj {
		if v == nil {
			delete(result, k)
		} else {
			if existing, ok := result[k]; ok {
				result[k] = jsonMergePatch(existing, v)
			} else {
				result[k] = v
			}
		}
	}
	return result
}

// evalJSONQuote implements JSON_QUOTE(string)
func (e *Executor) evalJSONQuote(v *sqlparser.JSONQuoteExpr) (interface{}, error) {
	val, err := e.evalExpr(v.StringArg)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	s := toString(val)
	b, _ := json.Marshal(s)
	return string(b), nil
}

// evalJSONUnquote implements JSON_UNQUOTE(json_val)
func (e *Executor) evalJSONUnquote(v *sqlparser.JSONUnquoteExpr) (interface{}, error) {
	val, err := e.evalExpr(v.JSONValue)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	s := toString(val)
	// If it's a JSON string (starts and ends with "), unquote it
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		var result string
		if err := json.Unmarshal([]byte(s), &result); err != nil {
			return s, nil
		}
		return result, nil
	}
	return s, nil
}

// evalJSONPretty implements JSON_PRETTY(json_val)
func (e *Executor) evalJSONPretty(v *sqlparser.JSONPrettyExpr) (interface{}, error) {
	val, err := e.evalExpr(v.JSONVal)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	doc, err := jsonNormalize(val)
	if err != nil {
		return nil, err
	}
	return jsonMarshalMySQLIndent(doc, true), nil
}

// evalJSONStorageSize implements JSON_STORAGE_SIZE(json_val)
func (e *Executor) evalJSONStorageSize(v *sqlparser.JSONStorageSizeExpr) (interface{}, error) {
	val, err := e.evalExpr(v.JSONVal)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	s := toString(val)
	return int64(len(s)), nil
}

// evalJSONStorageFree implements JSON_STORAGE_FREE(json_val)
func (e *Executor) evalJSONStorageFree(v *sqlparser.JSONStorageFreeExpr) (interface{}, error) {
	// JSON_STORAGE_FREE always returns 0 for our in-memory implementation
	val, err := e.evalExpr(v.JSONVal)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	return int64(0), nil
}

// evalJSONOverlaps implements JSON_OVERLAPS(json_doc1, json_doc2)
func (e *Executor) evalJSONOverlaps(v *sqlparser.JSONOverlapsExpr) (interface{}, error) {
	val1, err := e.evalExpr(v.JSONDoc1)
	if err != nil {
		return nil, err
	}
	if val1 == nil {
		return nil, nil
	}
	val2, err := e.evalExpr(v.JSONDoc2)
	if err != nil {
		return nil, err
	}
	if val2 == nil {
		return nil, nil
	}

	doc1, err := jsonNormalize(val1)
	if err != nil {
		return nil, err
	}
	doc2, err := jsonNormalize(val2)
	if err != nil {
		return nil, err
	}

	if jsonOverlaps(doc1, doc2) {
		return int64(1), nil
	}
	return int64(0), nil
}

func jsonOverlaps(a, b interface{}) bool {
	aArr, aIsArr := a.([]interface{})
	bArr, bIsArr := b.([]interface{})
	aObj, aIsObj := a.(map[string]interface{})
	bObj, bIsObj := b.(map[string]interface{})

	if aIsArr && bIsArr {
		for _, av := range aArr {
			for _, bv := range bArr {
				if jsonEqual(av, bv) {
					return true
				}
			}
		}
		return false
	}
	if aIsArr && !bIsArr {
		for _, av := range aArr {
			if jsonEqual(av, b) {
				return true
			}
		}
		return false
	}
	if !aIsArr && bIsArr {
		for _, bv := range bArr {
			if jsonEqual(a, bv) {
				return true
			}
		}
		return false
	}
	if aIsObj && bIsObj {
		for k, av := range aObj {
			if bv, ok := bObj[k]; ok {
				if jsonEqual(av, bv) {
					return true
				}
			}
		}
		return false
	}
	return jsonEqual(a, b)
}

// evalMemberOf implements val MEMBER OF(json_array)
func (e *Executor) evalMemberOf(v *sqlparser.MemberOfExpr) (interface{}, error) {
	val, err := e.evalExpr(v.Value)
	if err != nil {
		return nil, err
	}
	arrVal, err := e.evalExpr(v.JSONArr)
	if err != nil {
		return int64(0), nil
	}
	if arrVal == nil {
		return nil, nil
	}

	arr, err := jsonNormalize(arrVal)
	if err != nil {
		return int64(0), nil
	}

	jsonArr, ok := arr.([]interface{})
	if !ok {
		return int64(0), nil
	}

	target := toJSONValue(val)
	for _, elem := range jsonArr {
		if jsonEqual(elem, target) {
			return int64(1), nil
		}
	}
	return int64(0), nil
}

// evalJSONValue implements JSON_VALUE(json_doc, path [RETURNING type] [ON EMPTY] [ON ERROR])
func (e *Executor) evalJSONValue(v *sqlparser.JSONValueExpr) (interface{}, error) {
	docVal, err := e.evalExpr(v.JSONDoc)
	if err != nil {
		return nil, err
	}
	if docVal == nil {
		return nil, nil
	}

	pathVal, err := e.evalExpr(v.Path)
	if err != nil {
		return nil, err
	}

	doc, err := jsonNormalize(docVal)
	if err != nil {
		// Error handling
		if v.ErrorOnResponse != nil {
			return e.jtOnResponse(v.ErrorOnResponse)
		}
		return nil, nil
	}

	result := jsonExtractPath(doc, toString(pathVal))

	if result == nil {
		// Empty handling
		if v.EmptyOnResponse != nil {
			return e.jtOnResponse(v.EmptyOnResponse)
		}
		return nil, nil
	}

	// Convert to string if it's a scalar
	switch r := result.(type) {
	case string:
		return r, nil
	case float64:
		if r == math.Trunc(r) && !math.IsInf(r, 0) {
			return strconv.FormatInt(int64(r), 10), nil
		}
		return strconv.FormatFloat(r, 'f', -1, 64), nil
	case bool:
		if r {
			return "true", nil
		}
		return "false", nil
	case nil:
		return nil, nil
	default:
		return jsonMarshalMySQL(result), nil
	}
}

func (e *Executor) jtOnResponse(resp *sqlparser.JtOnResponse) (interface{}, error) {
	switch resp.ResponseType {
	case sqlparser.NullJSONType:
		return nil, nil
	case sqlparser.ErrorJSONType:
		return nil, fmt.Errorf("JSON value error")
	case sqlparser.DefaultJSONType:
		if resp.Expr != nil {
			return e.evalExpr(resp.Expr)
		}
		return nil, nil
	}
	return nil, nil
}

// evalJSONSchemaValid implements JSON_SCHEMA_VALID(schema, document)
func (e *Executor) evalJSONSchemaValid(v *sqlparser.JSONSchemaValidFuncExpr) (interface{}, error) {
	schemaVal, err := e.evalExpr(v.Schema)
	if err != nil {
		return nil, err
	}
	if schemaVal == nil {
		return nil, nil
	}
	docVal, err := e.evalExpr(v.Document)
	if err != nil {
		return nil, err
	}
	if docVal == nil {
		return nil, nil
	}

	// Check for excessive nesting depth on raw strings first (before parsing)
	schemaStr := toString(schemaVal)
	docStr := toString(docVal)
	if jsonRawDepth(schemaStr) > 100 || jsonRawDepth(docStr) > 100 {
		return nil, mysqlError(3157, "22032", "The JSON document exceeds the maximum depth.")
	}

	// Parse schema
	var schema interface{}
	if err := json.Unmarshal([]byte(schemaStr), &schema); err != nil {
		return nil, mysqlError(3141, "22032", "Invalid JSON text in argument 1 to function json_schema_valid.")
	}
	// Parse document
	var doc interface{}
	if err := json.Unmarshal([]byte(docStr), &doc); err != nil {
		return nil, mysqlError(3141, "22032", "Invalid JSON text in argument 2 to function json_schema_valid.")
	}

	schemaObj, ok := schema.(map[string]interface{})
	if !ok {
		return nil, mysqlError(3141, "22032", "Invalid JSON text in argument 1 to function json_schema_valid.")
	}

	// Check for $ref (JSON Schema references not supported)
	if jsonSchemaHasRef(schemaObj) {
		return nil, mysqlError(3986, "42000", "This version of MySQL doesn't yet support 'references in JSON Schema'")
	}

	if jsonSchemaValidate(schemaObj, doc) {
		return int64(1), nil
	}
	return int64(0), nil
}

// evalJSONSchemaValidationReport implements JSON_SCHEMA_VALIDATION_REPORT(schema, document)
func (e *Executor) evalJSONSchemaValidationReport(v *sqlparser.JSONSchemaValidationReportFuncExpr) (interface{}, error) {
	schemaVal, err := e.evalExpr(v.Schema)
	if err != nil {
		return nil, err
	}
	if schemaVal == nil {
		return nil, nil
	}
	docVal, err := e.evalExpr(v.Document)
	if err != nil {
		return nil, err
	}
	if docVal == nil {
		return nil, nil
	}

	// Parse schema
	var schema interface{}
	if err := json.Unmarshal([]byte(toString(schemaVal)), &schema); err != nil {
		return nil, mysqlError(3141, "22032", "Invalid JSON text in argument 1 to function json_schema_validation_report.")
	}
	// Parse document
	var doc interface{}
	if err := json.Unmarshal([]byte(toString(docVal)), &doc); err != nil {
		return nil, mysqlError(3141, "22032", "Invalid JSON text in argument 2 to function json_schema_validation_report.")
	}

	schemaObj, ok := schema.(map[string]interface{})
	if !ok {
		return nil, mysqlError(3141, "22032", "Invalid JSON text in argument 1 to function json_schema_validation_report.")
	}

	reason := jsonSchemaValidateReport(schemaObj, doc)
	if reason == "" {
		return `{"valid": true}`, nil
	}

	failedKw := jsonSchemaFailedKeyword(schemaObj, doc)
	// Build JSON with specific key order matching MySQL
	return fmt.Sprintf(`{"valid": false, "reason": %s, "schema-location": "#", "document-location": "#", "schema-failed-keyword": %s}`,
		jsonMarshalMySQL(reason), jsonMarshalMySQL(failedKw)), nil
}

// jsonSchemaValidate does basic JSON Schema validation
// jsonRawDepth computes the nesting depth of a raw JSON string (without parsing).
func jsonRawDepth(s string) int {
	maxDepth := 0
	depth := 0
	inString := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if inString {
			if ch == '\\' {
				i++
			} else if ch == '"' {
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '[', '{':
			depth++
			if depth > maxDepth {
				maxDepth = depth
			}
		case ']', '}':
			depth--
		}
	}
	return maxDepth
}

// jsonSchemaHasRef checks if a JSON schema object contains "$ref" keys (unsupported).
func jsonSchemaHasRef(schema map[string]interface{}) bool {
	for k, v := range schema {
		if k == "$ref" {
			return true
		}
		if subObj, ok := v.(map[string]interface{}); ok {
			if jsonSchemaHasRef(subObj) {
				return true
			}
		}
	}
	return false
}

func jsonSchemaValidate(schema map[string]interface{}, doc interface{}) bool {
	return jsonSchemaValidateReport(schema, doc) == ""
}

func jsonSchemaValidateReport(schema map[string]interface{}, doc interface{}) string {
	// type validation
	if typeVal, ok := schema["type"]; ok {
		typeName := toString(typeVal)
		if !jsonSchemaCheckType(typeName, doc) {
			return "The JSON document location '#' failed requirement 'type' at JSON Schema location '#'"
		}
	}

	// enum validation
	if enumVal, ok := schema["enum"]; ok {
		if arr, ok := enumVal.([]interface{}); ok {
			found := false
			for _, ev := range arr {
				if jsonEqual(ev, doc) {
					found = true
					break
				}
			}
			if !found {
				return "The JSON document location '#' failed requirement 'enum' at JSON Schema location '#'"
			}
		}
	}

	// minimum/maximum
	if minVal, ok := schema["minimum"]; ok {
		docNum, docIsNum := toJSONNumber(doc)
		minNum, minIsNum := toJSONNumber(minVal)
		if docIsNum && minIsNum && docNum < minNum {
			return "The JSON document location '#' failed requirement 'minimum' at JSON Schema location '#'"
		}
	}
	if maxVal, ok := schema["maximum"]; ok {
		docNum, docIsNum := toJSONNumber(doc)
		maxNum, maxIsNum := toJSONNumber(maxVal)
		if docIsNum && maxIsNum && docNum > maxNum {
			return "The JSON document location '#' failed requirement 'maximum' at JSON Schema location '#'"
		}
	}

	// maxLength/minLength
	if maxLen, ok := schema["maxLength"]; ok {
		if s, ok := doc.(string); ok {
			ml, _ := toJSONNumber(maxLen)
			if float64(len(s)) > ml {
				return "The JSON document location '#' failed requirement 'maxLength' at JSON Schema location '#'"
			}
		}
	}
	if minLen, ok := schema["minLength"]; ok {
		if s, ok := doc.(string); ok {
			ml, _ := toJSONNumber(minLen)
			if float64(len(s)) < ml {
				return "The JSON document location '#' failed requirement 'minLength' at JSON Schema location '#'"
			}
		}
	}

	// pattern
	if patVal, ok := schema["pattern"]; ok {
		if s, ok := doc.(string); ok {
			re, err := regexp.Compile(toString(patVal))
			if err == nil && !re.MatchString(s) {
				return "The JSON document location '#' failed requirement 'pattern' at JSON Schema location '#'"
			}
		}
	}

	// properties (for objects)
	if props, ok := schema["properties"]; ok {
		if propsObj, ok := props.(map[string]interface{}); ok {
			if docObj, ok := doc.(map[string]interface{}); ok {
				// required
				if req, ok := schema["required"]; ok {
					if reqArr, ok := req.([]interface{}); ok {
						for _, r := range reqArr {
							if _, exists := docObj[toString(r)]; !exists {
								return fmt.Sprintf("The JSON document location '#' failed requirement 'required' at JSON Schema location '#'")
							}
						}
					}
				}
				for k, propSchema := range propsObj {
					if val, exists := docObj[k]; exists {
						if ps, ok := propSchema.(map[string]interface{}); ok {
							if reason := jsonSchemaValidateReport(ps, val); reason != "" {
								return reason
							}
						}
					}
				}
			}
		}
	}

	// items (for arrays)
	if items, ok := schema["items"]; ok {
		if itemsSchema, ok := items.(map[string]interface{}); ok {
			if arr, ok := doc.([]interface{}); ok {
				for _, elem := range arr {
					if reason := jsonSchemaValidateReport(itemsSchema, elem); reason != "" {
						return reason
					}
				}
			}
		}
	}

	return ""
}

func jsonSchemaCheckType(typeName string, doc interface{}) bool {
	switch typeName {
	case "object":
		_, ok := doc.(map[string]interface{})
		return ok
	case "array":
		_, ok := doc.([]interface{})
		return ok
	case "string":
		_, ok := doc.(string)
		return ok
	case "number", "numeric":
		_, ok := doc.(float64)
		return ok
	case "integer":
		f, ok := doc.(float64)
		return ok && f == math.Trunc(f)
	case "boolean":
		_, ok := doc.(bool)
		return ok
	case "null":
		return doc == nil
	}
	return true
}

func jsonSchemaFailedKeyword(schema map[string]interface{}, doc interface{}) string {
	if typeVal, ok := schema["type"]; ok {
		if !jsonSchemaCheckType(toString(typeVal), doc) {
			return "type"
		}
	}
	if _, ok := schema["enum"]; ok {
		return "enum"
	}
	if _, ok := schema["minimum"]; ok {
		return "minimum"
	}
	if _, ok := schema["maximum"]; ok {
		return "maximum"
	}
	if _, ok := schema["required"]; ok {
		return "required"
	}
	return "type"
}
