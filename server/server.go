package server

import (
	"fmt"
	"log"
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
	Executor *executor.Executor
	Addr     string
	listener net.Listener
	mu       sync.Mutex
	closed   bool
}

func New(exec *executor.Executor, addr string) *Server {
	return &Server{
		Executor: exec,
		Addr:     addr,
	}
}

// stmtCounter is a global monotonically increasing ID for prepared statements.
var stmtCounter uint64

// Handler implements go-mysql server.Handler interface.
type Handler struct {
	exec     *executor.Executor
	mu       sync.Mutex
	stmtsMu  sync.Mutex
	stmts    map[uint64]string
}

func (h *Handler) UseDB(dbName string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.exec.Execute(fmt.Sprintf("USE `%s`", dbName))
	return err
}

func (h *Handler) HandleQuery(query string) (*mysql.Result, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	result, err := h.exec.Execute(query)
	if err != nil {
		errMsg := err.Error()
		if strings.HasPrefix(errMsg, "ERROR ") {
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

	result, err := h.exec.Execute(finalQuery)
	if err != nil {
		errMsg := err.Error()
		if strings.HasPrefix(errMsg, "ERROR ") {
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
	return fmt.Errorf("unsupported command: %d", cmd)
}

// Start begins listening and serving connections.
func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", s.Addr, err)
	}

	log.Printf("mylite server listening on %s", s.Addr)

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
			log.Printf("accept error: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

// Close stops the server.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	handler := &Handler{exec: s.Executor}

	// Create a MySQL connection with no auth
	mysqlConn, err := gomysql.NewConn(conn, "root", "", handler)
	if err != nil {
		log.Printf("handshake error: %v", err)
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

// normalizeRows ensures all rows have consistent Go types per column.
// go-mysql's BuildSimpleResultset will error if two non-null values in the same
// column have different MySQL types (e.g. int64 vs string).  We resolve this by
// converting every value in a column to string as soon as we detect a mismatch.
func normalizeRows(rows [][]interface{}) [][]interface{} {
	if len(rows) == 0 {
		return rows
	}
	numCols := len(rows[0])
	// For each column determine the MySQL type category of the first non-null value.
	// Categories: 0=unknown, 1=int, 2=float, 3=string/bytes, 4=time
	colCat := make([]int, numCols)
	colMixed := make([]bool, numCols)

	categoryOf := func(v interface{}) int {
		switch v.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return 1
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
		normalizeRows(result.Rows),
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
