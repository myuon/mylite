package server

import (
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	gomysql "github.com/go-mysql-org/go-mysql/server"
	"github.com/myuon/mylite/executor"
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

// Handler implements go-mysql server.Handler interface.
type Handler struct {
	exec *executor.Executor
	mu   sync.Mutex
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
	return 0, 0, nil, fmt.Errorf("prepared statements not yet supported")
}

func (h *Handler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return nil, fmt.Errorf("prepared statements not yet supported")
}

func (h *Handler) HandleStmtClose(context interface{}) error {
	return nil
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
		result.Rows,
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
