package server

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"runtime"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	gomysql "github.com/go-mysql-org/go-mysql/server"
	"github.com/myuon/mylite/executor"
	"vitess.io/vitess/go/vt/sqlparser"
)


// Server is the MySQL-compatible server.
type Server struct {
	executor    *executor.Executor
	executorMu  sync.RWMutex
	Addr        string
	listener    net.Listener
	mu          sync.Mutex
	closed      bool
	conns       map[net.Conn]struct{}
	logger      *log.Logger
}

// SetExecutor safely replaces the server's executor under a write lock.
func (s *Server) SetExecutor(exec *executor.Executor) {
	s.executorMu.Lock()
	s.executor = exec
	s.executorMu.Unlock()
}

// getExecutor returns the server's executor under a read lock.
func (s *Server) getExecutor() *executor.Executor {
	s.executorMu.RLock()
	defer s.executorMu.RUnlock()
	return s.executor
}

func New(exec *executor.Executor, addr string) *Server {
	return &Server{
		executor: exec,
		Addr:     addr,
		conns:    make(map[net.Conn]struct{}),
		logger:   log.Default(),
	}
}

// SetLogger replaces the server's logger. Pass a logger writing to io.Discard
// to silence all server log output.
func (s *Server) SetLogger(l *log.Logger) {
	s.logger = l
}

// DiscardLogger returns a logger that discards all output.
func DiscardLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}

// stmtCounter is a global monotonically increasing ID for prepared statements.
var stmtCounter uint64

// Handler implements go-mysql server.Handler interface.
type Handler struct {
	srv      *Server
	executor *executor.Executor
	mu       sync.Mutex
	stmtsMu  sync.Mutex
	stmts    map[uint64]string
}

func (h *Handler) UseDB(dbName string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.executor.Execute(fmt.Sprintf("USE `%s`", dbName))
	return err
}

func (h *Handler) HandleQuery(query string) (*mysql.Result, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Set query info in process list
	pl := h.executor.GetProcessList()
	connID := h.executor.GetConnectionID()
	if pl != nil {
		pl.SetQuery(connID, query, "starting")
		defer pl.ClearQuery(connID)
	}

	result, err := h.executor.Execute(query)
	if err != nil {
		errMsg := err.Error()
		if strings.HasPrefix(errMsg, "ERROR ") {
			// Parse "ERROR <code> (<state>): <message>" into a proper MyError
			// so the go-mysql server sends the correct error code and state to the client.
			if myErr := parseMySQLError(errMsg); myErr != nil {
				return nil, myErr
			}
			return nil, fmt.Errorf("%s", errMsg)
		}
		return nil, fmt.Errorf("ERROR 1064 (42000): %v", err)
	}

	if result.IsResultSet {
		return resultToMySQL(result)
	}

	return &mysql.Result{
		AffectedRows: result.AffectedRows,
		InsertId:     result.InsertID,
	}, nil
}

func (h *Handler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}

func (h *Handler) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	// Count ? placeholders to get param count
	paramCount := strings.Count(query, "?")

	// Determine column count by parsing the query (without executing it).
	// Replace ? with a literal placeholder value so the parser can handle it.
	parseQuery := strings.ReplaceAll(query, "?", "0")
	columnCount := 0
	if stmt, err := sqlparser.NewTestParser().Parse(parseQuery); err == nil {
		if sel, ok := stmt.(*sqlparser.Select); ok {
			// For star, we can't know without executing; use 1 as a conservative estimate.
			// Most drivers only use columnCount to pre-allocate, so an over/under-estimate is tolerable.
			columnCount = len(sel.SelectExprs.Exprs)
		}
	}

	// Assign a unique ID and store the query
	stmtID := atomic.AddUint64(&stmtCounter, 1)
	h.stmtsMu.Lock()
	if h.stmts == nil {
		h.stmts = make(map[uint64]string)
	}
	h.stmts[stmtID] = query
	h.stmtsMu.Unlock()

	return paramCount, columnCount, stmtID, nil
}

func (h *Handler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	stmtID, ok := context.(uint64)
	if !ok {
		return nil, fmt.Errorf("invalid prepared statement context")
	}

	h.stmtsMu.Lock()
	storedQuery, found := h.stmts[stmtID]
	h.stmtsMu.Unlock()
	if !found {
		return nil, fmt.Errorf("prepared statement not found")
	}

	// Replace ? placeholders with the provided argument values
	finalQuery := replacePlaceholders(storedQuery, args)

	// Set query info in process list
	pl := h.executor.GetProcessList()
	connID := h.executor.GetConnectionID()
	if pl != nil {
		pl.SetQuery(connID, finalQuery, "starting")
		defer pl.ClearQuery(connID)
	}

	result, err := h.executor.Execute(finalQuery)
	if err != nil {
		errMsg := err.Error()
		if strings.HasPrefix(errMsg, "ERROR ") {
			if myErr := parseMySQLError(errMsg); myErr != nil {
				return nil, myErr
			}
			return nil, fmt.Errorf("%s", errMsg)
		}
		return nil, fmt.Errorf("ERROR 1064 (42000): %v", err)
	}

	if result.IsResultSet {
		return resultToMySQLBinary(result)
	}

	return &mysql.Result{
		AffectedRows: result.AffectedRows,
		InsertId:     result.InsertID,
	}, nil
}

func (h *Handler) HandleStmtClose(context interface{}) error {
	stmtID, ok := context.(uint64)
	if !ok {
		return nil
	}
	h.stmtsMu.Lock()
	delete(h.stmts, stmtID)
	h.stmtsMu.Unlock()
	return nil
}

// replacePlaceholders substitutes ? markers in a SQL query with the provided args
// converted to SQL literal strings. It iterates through the query character by character,
// replacing each ? with the next argument value.
func replacePlaceholders(query string, args []interface{}) string {
	var sb strings.Builder
	argIdx := 0
	for i := 0; i < len(query); i++ {
		if query[i] == '?' && argIdx < len(args) {
			sb.WriteString(argToSQL(args[argIdx]))
			argIdx++
		} else {
			sb.WriteByte(query[i])
		}
	}
	return sb.String()
}

// argToSQL converts a Go interface value to a SQL literal string.
// Values passed by the go-mysql library include mysql.TypedBytes for string/blob types
// and various integer types depending on the MySQL column type.
func argToSQL(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case mysql.TypedBytes:
		// String, varchar, blob, date, datetime, etc. come as TypedBytes.
		escaped := strings.ReplaceAll(string(val.Bytes), "'", "''")
		return "'" + escaped + "'"
	case int64:
		return strconv.FormatInt(val, 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int16:
		return strconv.FormatInt(int64(val), 10)
	case int8:
		return strconv.FormatInt(int64(val), 10)
	case int:
		return strconv.Itoa(val)
	case uint64:
		return strconv.FormatUint(val, 10)
	case uint32:
		return strconv.FormatUint(uint64(val), 10)
	case uint16:
		return strconv.FormatUint(uint64(val), 10)
	case uint8:
		return strconv.FormatUint(uint64(val), 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case bool:
		if val {
			return "1"
		}
		return "0"
	case string:
		// Escape single quotes within the string
		escaped := strings.ReplaceAll(val, "'", "''")
		return "'" + escaped + "'"
	case []byte:
		escaped := strings.ReplaceAll(string(val), "'", "''")
		return "'" + escaped + "'"
	case time.Time:
		return "'" + val.Format("2006-01-02 15:04:05") + "'"
	default:
		escaped := strings.ReplaceAll(fmt.Sprintf("%v", val), "'", "''")
		return "'" + escaped + "'"
	}
}

func (h *Handler) HandleOtherCommand(cmd byte, data []byte) error {
	// Handle COM_SHUTDOWN gracefully: return an error to the client (don't kill the server).
	// COM_SHUTDOWN = 8; we acknowledge it with an error so the client exits cleanly.
	if cmd == mysql.COM_SHUTDOWN {
		return mysql.NewError(mysql.ER_SPECIFIC_ACCESS_DENIED_ERROR, "Access denied; you need (at least one of) the SUPER privilege(s) for this operation")
	}
	return fmt.Errorf("unsupported command: %d", cmd)
}

// Start begins listening and serving connections.
func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", s.Addr, err)
	}

	s.logger.Printf("mylite server listening on %s", s.Addr)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Check if server was closed
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			s.logger.Printf("accept error: %v", err)
			continue
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go s.handleConnection(conn)
	}
}

// Close stops the server and all active connections.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	for c := range s.conns {
		c.Close()
	}
	s.conns = make(map[net.Conn]struct{})
	s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			s.logger.Printf("panic in connection handler: %v\n%s", r, buf[:n])
		}
	}()
	connExec := s.getExecutor().Clone()
	handler := &Handler{srv: s, executor: connExec}
	connID := connExec.GetConnectionID()

	// Register connection in the process list
	pl := connExec.GetProcessList()
	if pl != nil {
		pl.RegisterWithID(connID, "root", conn.RemoteAddr().String(), connExec.CurrentDB)
	}

	defer func() {
		connExec.OnDisconnect()
		// Unregister from process list
		if pl != nil {
			pl.Unregister(connID)
		}
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
	}()

	// Create a MySQL connection with no auth
	mysqlConn, err := gomysql.NewConn(conn, "root", "", handler)
	if err != nil {
		s.logger.Printf("handshake error: %v", err)
		return
	}
	for {
		err := mysqlConn.HandleCommand()
		if err != nil {
			return
		}
	}
}

// resultToMySQLBinary builds a mysql.Result using binary row encoding,
// required for responses to COM_STMT_EXECUTE (prepared statement execute).
func resultToMySQLBinary(result *executor.Result) (*mysql.Result, error) {
	if len(result.Rows) == 0 {
		r, err := mysql.BuildSimpleResultset(
			makeFields(result.Columns),
			[][]interface{}{},
			true,
		)
		if err != nil {
			return nil, err
		}
		return &mysql.Result{Resultset: r}, nil
	}

	// Fix empty strings for binary result path too
	for i, row := range result.Rows {
		for j, val := range row {
			if s, ok := val.(string); ok && s == "" {
				result.Rows[i][j] = []byte{}
			}
		}
	}
	r, err := mysql.BuildSimpleResultset(
		makeFields(result.Columns),
		result.Rows,
		true,
	)
	if err != nil {
		return nil, err
	}
	return &mysql.Result{Resultset: r}, nil
}

// fixEmptyStrings converts empty string values to []byte{} to work around
// a go-mysql library bug where empty strings are encoded as NULL due to
// unsafe.StringData("") returning nil.
func fixEmptyStrings(rows [][]interface{}) [][]interface{} {
	for i, row := range rows {
		for j, val := range row {
			if s, ok := val.(string); ok && s == "" {
				rows[i][j] = []byte{}
			}
		}
	}
	return rows
}

// normalizeRows ensures all rows have consistent Go types per column.
// go-mysql's BuildSimpleResultset will error if two non-null values in the same
// column have different MySQL types (e.g. int64 vs string).  We resolve this by
// converting every value in a column to string as soon as we detect a mismatch.
func normalizeRows(rows [][]interface{}) [][]interface{} {
	if len(rows) == 0 {
		return rows
	}
	// Convert bool values to int64, divisionResult to formatted string,
	// and enumValue to plain string (go-mysql doesn't know about enumValue).
	for i, row := range rows {
		for j, val := range row {
			if b, ok := val.(bool); ok {
				if b {
					rows[i][j] = int64(1)
				} else {
					rows[i][j] = int64(0)
				}
			} else if sd, ok := val.(executor.SysVarDouble); ok {
				rows[i][j] = strconv.FormatFloat(sd.Value, 'f', 6, 64)
			} else if sv, ok := val.(executor.ScaledValue); ok {
				// Format with the correct number of decimal places to preserve scale.
				rows[i][j] = fmt.Sprintf("%.*f", sv.Scale, sv.Value)
			} else if d, ok := val.(executor.DivisionResult); ok {
				rows[i][j] = fmt.Sprintf("%.*f", d.Precision, d.Value)
			} else if ar, ok := val.(executor.AvgResult); ok {
				rows[i][j] = fmt.Sprintf("%.*f", ar.Scale, ar.Value)
			} else if ev, ok := val.(executor.EnumValue); ok {
				rows[i][j] = string(ev)
			} else if hb, ok := val.(executor.HexBytes); ok {
				// HexBytes stores hex-encoded data (e.g. "31" for x'31').
				// Decode to raw bytes for the wire protocol.
				if decoded, err := hex.DecodeString(string(hb)); err == nil {
					rows[i][j] = decoded
				} else {
					rows[i][j] = string(hb)
				}
			}
		}
	}
	numCols := len(rows[0])
	// For each column determine the MySQL type category of the first non-null value.
	// Categories: 0=unknown, 1=signed int, 2=float, 3=string/bytes, 4=time, 5=unsigned int
	colCat := make([]int, numCols)
	colMixed := make([]bool, numCols)

	categoryOf := func(v interface{}) int {
		switch v.(type) {
		case int, int8, int16, int32, int64:
			return 1
		case uint, uint8, uint16, uint32, uint64:
			return 5
		case float32, float64:
			return 2
		case string, []byte:
			return 3
		case time.Time:
			return 4
		}
		return 3 // treat unknown as string
	}

	for _, row := range rows {
		for j, val := range row {
			if j >= numCols {
				break
			}
			if val == nil {
				continue
			}
			cat := categoryOf(val)
			if colCat[j] == 0 {
				colCat[j] = cat
			} else if colCat[j] != cat {
				colMixed[j] = true
			}
		}
	}

	// If no column has mixed types, return as-is.
	needsNorm := false
	for _, m := range colMixed {
		if m {
			needsNorm = true
			break
		}
	}
	if !needsNorm {
		// Fix empty strings even when no type normalization is needed.
		// go-mysql's BuildSimpleResultset encodes Go's "" as NULL because
		// unsafe.StringData("") returns nil. Convert "" to []byte{} instead.
		for i, row := range rows {
			for j, val := range row {
				if s, ok := val.(string); ok && s == "" {
					rows[i][j] = []byte{}
				}
			}
		}
		return rows
	}

	// Build new rows with mixed-type columns converted to string.
	out := make([][]interface{}, len(rows))
	for i, row := range rows {
		newRow := make([]interface{}, len(row))
		copy(newRow, row)
		for j, val := range row {
			if j < numCols && colMixed[j] && val != nil {
				newRow[j] = fmt.Sprintf("%v", val)
			}
			// Fix empty strings (see comment above)
			if s, ok := newRow[j].(string); ok && s == "" {
				newRow[j] = []byte{}
			}
		}
		out[i] = newRow
	}
	return out
}

func resultToMySQL(result *executor.Result) (*mysql.Result, error) {
	if len(result.Rows) == 0 {
		// Empty result set
		r, err := mysql.BuildSimpleResultset(
			makeFields(result.Columns),
			[][]interface{}{},
			false,
		)
		if err != nil {
			return nil, err
		}
		return &mysql.Result{
			Resultset: r,
		}, nil
	}

	r, err := mysql.BuildSimpleResultset(
		makeFields(result.Columns),
		fixEmptyStrings(normalizeRows(result.Rows)),
		false,
	)
	if err != nil {
		return nil, err
	}
	return &mysql.Result{
		Resultset: r,
	}, nil
}

func makeFields(columns []string) []string {
	return columns
}

// parseMySQLError parses an error message in the format "ERROR <code> (<state>): <message>"
// and returns a *mysql.MyError with the correct code, state, and message.
// Returns nil if the format doesn't match.
func parseMySQLError(errMsg string) *mysql.MyError {
	// Match "ERROR <code> (<state>): <message>"
	if !strings.HasPrefix(errMsg, "ERROR ") {
		return nil
	}
	rest := errMsg[6:] // skip "ERROR "
	// Parse code
	spaceIdx := strings.IndexByte(rest, ' ')
	if spaceIdx < 0 {
		return nil
	}
	code, err := strconv.Atoi(rest[:spaceIdx])
	if err != nil {
		return nil
	}
	rest = rest[spaceIdx+1:]
	// Parse (state)
	if len(rest) < 2 || rest[0] != '(' {
		return nil
	}
	closeIdx := strings.IndexByte(rest, ')')
	if closeIdx < 0 {
		return nil
	}
	state := rest[1:closeIdx]
	rest = rest[closeIdx+1:]
	// Skip ": "
	if len(rest) >= 2 && rest[0] == ':' && rest[1] == ' ' {
		rest = rest[2:]
	}
	return &mysql.MyError{
		Code:    uint16(code),
		State:   state,
		Message: rest,
	}
}
