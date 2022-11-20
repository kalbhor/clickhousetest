package clickhousetest

import (
	"context"
	"fmt"
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

// server holds the connections and metadata (such as db directory, clickhouse path, etc)
// for the ephemeral clickhouse server.
type Server struct {
	binPath string
	dbDir   string
	port    int
	cmd     *exec.Cmd
	conn    clickhouse.Conn
}

func New() (*Server, error) {
	// prepare data directory
	dir, err := os.MkdirTemp("", defaultDir)
	if err != nil {
		return nil, err
	}

	// try looking for the clickhouse-server executable path.
	bin, err := exec.LookPath(binName)
	if err != nil {
		return nil, err
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

	return &Server{
		//port:    port,
		cmd:     exec.Command(bin, "server"),
		dbDir:   dir,
		binPath: bin,
	}, nil
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

func (s *Server) Start(ctx context.Context) error {
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

	err = s.connectDB()
	if err != nil {
		return err
	}
	// TODO: create a temp db here.

	return nil
}

func (s *Server) Stop() error {
	return s.cleanup()
}

func (s *Server) connectDB() error {
	// TODO: replace default port with custom, used while
	// running the server.
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Debug: true,
		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:      time.Duration(10) * time.Second,
		MaxOpenConns:     5,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Duration(10) * time.Minute,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
	})
	if err != nil {
		return err
	}
	s.conn = conn

	return nil
}

// func findUnusedTCPPort() (int, error) {
// 	l, err := net.ListenTCP("tcp", &net.TCPAddr{
// 		IP: net.IPv4(127, 0, 0, 1),
// 	})
// 	if err != nil {
// 		return 0, fmt.Errorf("find unused tcp port: %w", err)
// 	}
// 	port := l.Addr().(*net.TCPAddr).Port
// 	if err := l.Close(); err != nil {
// 		return 0, fmt.Errorf("find unused tcp port: %w", err)
// 	}
// 	return port, nil
// }
