package clickhousetest

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	// temp directory to store db data.
	defaultDir = "clickhousetest"
	// clickhouse binary name to search in path.
	binName = "clickhouse"
	// clickhouse allows overriding main config files by adding new configs
	// to the below path.
)

// Server holds the connections and metadata (such as db directory, clickhouse path, etc)
// for the ephemeral clickhouse server.
type Server struct {
	opts    Options
	binPath string
	dbDir   string
	port    int
	cmd     *exec.Cmd
	conn    clickhouse.Conn
}

type Options struct {
	ExecMode  bool
	DBOptions clickhouse.Options
}

// Start creates an ephemeral clickhouse server and does the relevant connections
// required for CreateDatabase & other methods.
func Start(ctx context.Context, o Options) (*Server, error) {
	// If exec mode is false, just connect to existing clickhouse.
	if !o.ExecMode {
		s := &Server{opts: o}
		err := s.startNoExec(ctx)
		if err != nil {
			return nil, fmt.Errorf("start server : %w", err)
		}

		return s, nil
	}

	// TODO: assign random port here.
	o.DBOptions = clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	}

	// prepare data directory
	dir, err := os.MkdirTemp("", defaultDir)
	if err != nil {
		return nil, fmt.Errorf("create temp dir : %w", err)
	}

	log.Println("dir", dir)

	// try looking for the clickhouse-server executable path.
	bin, err := exec.LookPath(binName)
	if err != nil {
		return nil, fmt.Errorf("find bin path : %w", err)
	}

	// TODO: figure out a way to create & pass a custom
	// config file with custom port. Also, specify directory in config
	// & do away with cmd.Dir assignement
	cmd := exec.Command(bin, "server")
	cmd.Dir = dir
	s := &Server{
		//port:    port,
		cmd:     cmd,
		dbDir:   dir,
		binPath: bin,
	}
	err = s.start(ctx)
	if err != nil {
		return nil, fmt.Errorf("start server : %w", err)
	}

	return s, nil
}

func (s *Server) startNoExec(ctx context.Context) error {
	var err error
	// Wait for a max of 10 seconds to connect to the db.
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		s.conn, err = s.connectDB(ctx, s.opts.DBOptions)
		if err != nil {
			continue
		}
		break
	}
	return nil
}

// cleanup stops the server & deletes the db directory.
func (s *Server) cleanup() error {
	// stop the clickhouse server
	err := s.cmd.Process.Kill()
	if err != nil {
		return err
	}

	// remove the db directory
	err = os.RemoveAll(s.dbDir)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) start(ctx context.Context) error {
	var err error
	defer func() {
		if err != nil {
			s.cleanup()
		}
	}()
	err = s.cmd.Start()
	if err != nil {
		return err
	}

	// Wait for a max of 10 seconds to connect to the db.
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		s.conn, err = s.connectDB(ctx, s.opts.DBOptions)
		if err != nil {
			continue
		}

		return nil
	}

	return fmt.Errorf("clickhouse not accepting connections")
}

func (s *Server) Stop() error {
	if s.opts.ExecMode {
		if err := s.cleanup(); err != nil {
			return fmt.Errorf("cleanup temp files : %w", err)
		}
	}

	if s.conn != nil {
		err := s.conn.Close()
		if err != nil {
			return fmt.Errorf("close db conn : %w", err)
		}
	}

	return nil
}

func (s *Server) NewDatabase(ctx context.Context) (clickhouse.Conn, error) {
	var (
		dbOpts = s.opts.DBOptions
		err    error
	)

	dbOpts.Auth.Database, err = s.CreateDatabase(ctx)
	if err != nil {
		return nil, err
	}
	conn, err := s.connectDB(ctx, dbOpts)
	if err != nil {
		return nil, fmt.Errorf("connect to db : %w", err)
	}

	return conn, nil
}

// CreateDatabase creates a new random database and returns its dsn.
func (s *Server) CreateDatabase(ctx context.Context) (string, error) {
	db := randomString(8)
	err := s.conn.Exec(ctx, "CREATE DATABASE "+db+";")
	if err != nil {
		return "", fmt.Errorf("exec create db query : %w", err)
	}

	return db, nil
}

func (s *Server) connectDB(ctx context.Context, o clickhouse.Options) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&o)
	if err != nil {
		return nil, err
	}

	err = conn.Ping(ctx)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func randomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[:length]
}

// WIP

// minimal config to specifiy port.
// rest of the config is picked from clickhouse's config path preferences.
var (
	serverCfg = `
	<config>
    <port>%d</port>
	</config>
	`
)

const (
	configPath = "/etc/clickhouse-server/config.d"
)

// func (s *Server) createConfg() {
// 	// get an unused port, added to the config file.
// 	// port, err := findUnusedTCPPort()
// 	// if err != nil {
// 	// 	return nil, err
// 	// }

// 	// // write the config file with specific port.
// 	// err = os.Mkdir(configPath, 0644)
// 	// if err != nil {
// 	// 	return nil, err
// 	// // }
// 	// cfgFile := fmt.Sprintf(serverCfg, port)
// 	// err = ioutil.WriteFile(configPath+"/config-temp.xml", []byte(cfgFile), 0644)
// 	// if err != nil {
// 	// 	return nil, err
// 	// }
// }

func findUnusedTCPPort() (int, error) {
	l, err := net.ListenTCP("tcp", &net.TCPAddr{
		IP: net.IPv4(127, 0, 0, 1),
	})
	if err != nil {
		return 0, fmt.Errorf("find unused tcp port: %w", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		return 0, fmt.Errorf("find unused tcp port: %w", err)
	}
	return port, nil
}
