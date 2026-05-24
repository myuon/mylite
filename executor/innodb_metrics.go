package executor

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/myuon/mylite/storage"
)

// innoDBMetricDef defines a single InnoDB metric entry for INFORMATION_SCHEMA.INNODB_METRICS.
type innoDBMetricDef struct {
	name      string
	subsystem string
	mtype     string
}

type innoDBMetricState struct {
	enabled     bool
	count       int64
	maxCount    *int64
	minCount    *int64
	countReset  int64
	maxReset    *int64
	minReset    *int64
	timeEnabled *time.Time
}

type innoDBMetricsRuntime struct {
	mu     sync.RWMutex
	byName map[string]*innoDBMetricState
}

func newInnoDBMetricsRuntime() *innoDBMetricsRuntime {
	rt := &innoDBMetricsRuntime{byName: make(map[string]*innoDBMetricState, len(innoDBMetrics))}
	for _, m := range innoDBMetrics {
		rt.byName[m.name] = &innoDBMetricState{}
	}
	return rt
}

func (rt *innoDBMetricsRuntime) state(name string) *innoDBMetricState {
	if rt == nil {
		return &innoDBMetricState{}
	}
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	if st, ok := rt.byName[name]; ok {
		return st
	}
	return &innoDBMetricState{}
}

func (e *Executor) innodbMetricsRuntime() *innoDBMetricsRuntime {
	if e.innodbMetrics == nil {
		e.innodbMetrics = newInnoDBMetricsRuntime()
	}
	return e.innodbMetrics
}

func innodbMonitorPatternInvalid(val string) bool {
	if val == "" {
		return true
	}
	if val == "_" {
		return true
	}
	return strings.ContainsRune(val, '*')
}

var innodbMonitorModuleAliases = map[string]string{
	"trx": "transaction",
	"log": "recovery",
}

func innodbMonitorResolveModule(val string) (string, bool) {
	const prefix = "module_"
	lower := strings.ToLower(val)
	if !strings.HasPrefix(lower, prefix) {
		return "", false
	}
	mod := lower[len(prefix):]
	if alias, ok := innodbMonitorModuleAliases[mod]; ok {
		return alias, true
	}
	return mod, true
}

func innodbMonitorLike(pattern, name string) bool {
	pi, ni := 0, 0
	for pi < len(pattern) {
		switch pattern[pi] {
		case '%':
			if pi+1 == len(pattern) {
				return true
			}
			for ni <= len(name) {
				if innodbMonitorLike(pattern[pi+1:], name[ni:]) {
					return true
				}
				ni++
			}
			return false
		case '_':
			if ni >= len(name) {
				return false
			}
			pi++
			ni++
		default:
			if ni >= len(name) || name[ni] != pattern[pi] {
				return false
			}
			pi++
			ni++
		}
	}
	return ni == len(name)
}

func innodbMonitorMatchPattern(pattern string, m innoDBMetricDef) bool {
	pattern = strings.TrimSpace(pattern)
	if strings.EqualFold(pattern, "all") {
		return true
	}
	if mod, ok := innodbMonitorResolveModule(pattern); ok {
		return strings.EqualFold(m.subsystem, mod)
	}
	if strings.ContainsAny(pattern, "%_") {
		return innodbMonitorLike(pattern, m.name)
	}
	return strings.EqualFold(pattern, m.name)
}

func (e *Executor) innodbMonitorMatchIndices(varName, pattern string) ([]int, error) {
	pattern = strings.TrimSpace(pattern)
	if strings.EqualFold(pattern, "default") {
		return nil, nil
	}
	if innodbMonitorPatternInvalid(pattern) {
		return nil, mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of '%s'", varName, pattern))
	}
	var idx []int
	for i, m := range innoDBMetrics {
		if innodbMonitorMatchPattern(pattern, m) {
			idx = append(idx, i)
		}
	}
	if len(idx) == 0 {
		return nil, mysqlError(1231, "42000", fmt.Sprintf("Variable '%s' can't be set to the value of '%s'", varName, pattern))
	}
	return idx, nil
}

func (e *Executor) applyInnoDBMonitorEnable(pattern string) error {
	idx, err := e.innodbMonitorMatchIndices("innodb_monitor_enable", pattern)
	if err != nil || idx == nil {
		return err
	}
	rt := e.innodbMetricsRuntime()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	now := time.Now()
	for _, i := range idx {
		name := innoDBMetrics[i].name
		st := rt.byName[name]
		st.enabled = true
		st.timeEnabled = &now
	}
	return nil
}

func (e *Executor) applyInnoDBMonitorDisable(pattern string) error {
	idx, err := e.innodbMonitorMatchIndices("innodb_monitor_disable", pattern)
	if err != nil || idx == nil {
		return err
	}
	rt := e.innodbMetricsRuntime()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, i := range idx {
		rt.byName[innoDBMetrics[i].name].enabled = false
	}
	return nil
}

func (e *Executor) applyInnoDBMonitorResetAll(pattern string) error {
	idx, err := e.innodbMonitorMatchIndices("innodb_monitor_reset_all", pattern)
	if err != nil || idx == nil {
		return err
	}
	rt := e.innodbMetricsRuntime()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, i := range idx {
		st := rt.byName[innoDBMetrics[i].name]
		if st.enabled {
			continue
		}
		st.count = 0
		st.maxCount = nil
		st.minCount = nil
		st.countReset = 0
		st.maxReset = nil
		st.minReset = nil
	}
	return nil
}

func (e *Executor) applyInnoDBMonitorReset(pattern string) error {
	idx, err := e.innodbMonitorMatchIndices("innodb_monitor_reset", pattern)
	if err != nil || idx == nil {
		return err
	}
	rt := e.innodbMetricsRuntime()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, i := range idx {
		st := rt.byName[innoDBMetrics[i].name]
		st.countReset = 0
		st.maxReset = nil
		st.minReset = nil
	}
	return nil
}

func (e *Executor) bumpInnoDBMetric(name string, delta int64) {
	if delta == 0 {
		return
	}
	rt := e.innodbMetricsRuntime()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	st, ok := rt.byName[name]
	if !ok || !st.enabled {
		return
	}
	st.count += delta
	st.countReset += delta
	if st.maxCount == nil || st.count > *st.maxCount {
		v := st.count
		st.maxCount = &v
	}
	if st.maxReset == nil || st.countReset > *st.maxReset {
		v := st.countReset
		st.maxReset = &v
	}
}

func (e *Executor) tableUsesInnoDB(dbName, tableName string) bool {
	db, err := e.Catalog.GetDatabase(dbName)
	if err != nil {
		return false
	}
	tbl, err := db.GetTable(tableName)
	if err != nil || tbl == nil {
		return false
	}
	eng := strings.ToUpper(tbl.Engine)
	return eng == "" || eng == "INNODB"
}

func (e *Executor) touchInnoDBTableHandleOpened(dbName, tableName string) {
	if e.tableUsesInnoDB(dbName, tableName) {
		e.bumpInnoDBMetric("metadata_table_handles_opened", 1)
	}
}

func (e *Executor) touchInnoDBTableHandleClosed(dbName, tableName string) {
	if e.tableUsesInnoDB(dbName, tableName) {
		e.bumpInnoDBMetric("metadata_table_handles_closed", 1)
	}
}

func (e *Executor) setInnoDBMetricValue(name string, value int64) {
	rt := e.innodbMetricsRuntime()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	st, ok := rt.byName[name]
	if !ok || !st.enabled {
		return
	}
	st.count = value
	st.countReset = value
	if st.maxCount == nil || value > *st.maxCount {
		v := value
		st.maxCount = &v
	}
	if st.maxReset == nil || value > *st.maxReset {
		v := value
		st.maxReset = &v
	}
	if st.minCount == nil || value < *st.minCount {
		v := value
		st.minCount = &v
	}
	if st.minReset == nil || value < *st.minReset {
		v := value
		st.minReset = &v
	}
}

func (e *Executor) recordInnoDBTrxActive() {
	if e.txnActiveSet == nil {
		return
	}
	e.txnActiveSet.mu.RLock()
	n := int64(len(e.txnActiveSet.active))
	e.txnActiveSet.mu.RUnlock()
	e.setInnoDBMetricValue("trx_active_transactions", n)
}

func (e *Executor) bumpInnoDBDMLReads(n int64) {
	e.bumpInnoDBMetric("dml_reads", n)
}

func (e *Executor) infoSchemaInnoDBMetrics() []storage.Row {
	rt := e.innodbMetricsRuntime()
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	rows := make([]storage.Row, 0, len(innoDBMetrics))
	for _, m := range innoDBMetrics {
		st := rt.byName[m.name]
		status := "disabled"
		if st.enabled {
			status = "enabled"
		}
		var maxCount interface{}
		if st.maxCount != nil {
			maxCount = *st.maxCount
		} else if st.enabled && st.count == 0 && (m.mtype == "status_counter" || m.mtype == "counter") {
			maxCount = int64(0)
		}
		var minCount interface{}
		if st.minCount != nil {
			minCount = *st.minCount
		}
		var maxReset interface{}
		if st.maxReset != nil {
			maxReset = *st.maxReset
		} else if st.countReset == 0 && m.mtype == "status_counter" && (st.enabled || st.count > 0) {
			maxReset = int64(0)
		}
		var minReset interface{}
		if st.minReset != nil {
			minReset = *st.minReset
		}
		var avgCount interface{}
		var avgReset interface{}
		var timeEnabled interface{}
		if st.timeEnabled != nil {
			timeEnabled = st.timeEnabled.Format("2006-01-02 15:04:05")
		}
		rows = append(rows, storage.Row{
			"NAME":              m.name,
			"COUNT":             st.count,
			"MAX_COUNT":         maxCount,
			"MIN_COUNT":         minCount,
			"AVG_COUNT":         avgCount,
			"COUNT_RESET":       st.countReset,
			"MAX_COUNT_RESET":   maxReset,
			"MIN_COUNT_RESET":   minReset,
			"AVG_COUNT_RESET":   avgReset,
			"TIME_ENABLED":      timeEnabled,
			"TIME_DISABLED":     nil,
			"TIME_ELAPSED":      nil,
			"TIME_RESET":        nil,
			"TYPE":              m.mtype,
			"STATUS":            status,
			"SUBSYSTEM":         m.subsystem,
			"COMMENT":           "",
		})
	}
	return rows
}

func (e *Executor) handleInnoDBMonitorSet(varName, val string) error {
	switch varName {
	case "innodb_monitor_enable":
		return e.applyInnoDBMonitorEnable(val)
	case "innodb_monitor_disable":
		return e.applyInnoDBMonitorDisable(val)
	case "innodb_monitor_reset_all":
		return e.applyInnoDBMonitorResetAll(val)
	case "innodb_monitor_reset":
		return e.applyInnoDBMonitorReset(val)
	default:
		return nil
	}
}
