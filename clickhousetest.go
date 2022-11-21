package clickhousetest

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

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
	// temp directory to store db data.
	defaultDir = "clickhousetest"
	// clickhouse binary name to search in path.
	binName = "clickhouse"
	// clickhouse allows overriding main config files by adding new configs
	// to the below path.
	configPath = "/etc/clickhouse-server/config.d"
)

// Server holds the connections and metadata (such as db directory, clickhouse path, etc)
// for the ephemeral clickhouse server.
type Server struct {
	binPath string
	dbDir   string
	port    int
	cmd     *exec.Cmd
	conn    clickhouse.Conn
}

// Start creates an ephemeral clickhouse server and does the relevant connections
// required for CreateDatabase & other methods.
func Start(ctx context.Context) (*Server, error) {
	// prepare data directory
	dir, err := os.MkdirTemp("", defaultDir)
	if err != nil {
		return nil, fmt.Errorf("create temp dir : %w", err)
	}

	// try looking for the clickhouse-server executable path.
	bin, err := exec.LookPath(binName)
	if err != nil {
		return nil, fmt.Errorf("find bin path : %w", err)
	}

	// TODO: figure out a way to create & pass a custom
	// config file with custom port.

	// get an unused port, added to the config file.
	// port, err := findUnusedTCPPort()
	// if err != nil {
	// 	return nil, err
	// }

	// // write the config file with specific port.
	// err = os.Mkdir(configPath, 0644)
	// if err != nil {
	// 	return nil, err
	// // }
	// cfgFile := fmt.Sprintf(serverCfg, port)
	// err = ioutil.WriteFile(configPath+"/config-temp.xml", []byte(cfgFile), 0644)
	// if err != nil {
	// 	return nil, err
	// }

	s := &Server{
		//port:    port,
		cmd:     exec.Command(bin, "server"),
		dbDir:   dir,
		binPath: bin,
	}
	err = s.start(ctx)
	if err != nil {
		return nil, fmt.Errorf("start server : %w", err)
	}

	return s, nil
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
		s.conn, err = s.connectDB(ctx, "default")
		if err != nil {
			continue
		}

		return nil
	}

	return fmt.Errorf("clickhouse not accepting connections")
}

func (s *Server) Stop() error {
	if err := s.cleanup(); err != nil {
		return fmt.Errorf("cleanup temp files : %w", err)
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
	db, err := s.CreateDatabase(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := s.connectDB(ctx, db)
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

func (s *Server) connectDB(ctx context.Context, db string) (clickhouse.Conn, error) {
	// TODO: replace default port with custom, used while
	// running the server.
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: db,
			Username: "default",
			Password: "",
		},
	})
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
