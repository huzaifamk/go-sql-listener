package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

func main() {
	errEnv := godotenv.Load()
	if errEnv != nil {
		logrus.Error("loading env vars", errEnv.Error())
		panic("Failed to load env file")
	}

	username := os.Getenv("STEVE_DB_USERNAME")
	password := os.Getenv("STEVE_DB_PASSWORD")
	databaseName := os.Getenv("STEVE_DB_DATABASE_NAME")
	tableNameOne := os.Getenv("STEVE_DB_TABLE_NAME_ONE")
	tableNameTwo := os.Getenv("STEVE_DB_TABLE_NAME_TWO")
	tableNameThree := os.Getenv("STEVE_DB_TABLE_NAME_THREE")
	host := os.Getenv("STEVE_DB_HOST")
	port := os.Getenv("STEVE_DB_PORT")

	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, databaseName)

	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		log.Fatal("Failed to connect to the database:", err)
	}
	defer db.Close()
	pos, err := getCurrentBinlogPosition(db)
	if err != nil {
		log.Fatal("Failed to retrieve current binlog position:", err)
	}
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%s", host, port)
	cfg.User = username
	cfg.Password = password
	cfg.Dump.ExecutionPath = ""
	cfg.Logger = &logrus.Logger{
		Out:       nil,
		Formatter: nil,
		Hooks:     nil,
		Level:     0,
	}
	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Fatal("Failed to create Canal instance:", err)
	}
	c.SetEventHandler(&MyEventHandler{
		startingPosition: pos,
		targetTableOne:   tableNameOne,
		targetTableTwo:   tableNameTwo,
		targetTableThree: tableNameThree,
	})
	go func() {
		fmt.Println("Starting Canal...")
		err = c.Run()
		if err != nil {
			log.Fatal("Failed to start Canal:", err)
		}
	}()
	waitForTerminationSignal()
}

func getCurrentBinlogPosition(db *sql.DB) (mysql.Position, error) {
	var (
		file string
		pos  uint32
		null interface{}
	)
	err := db.QueryRow("SHOW MASTER STATUS").Scan(&file, &pos, &null, &null, &null)
	if err != nil {
		return mysql.Position{}, err
	}
	return mysql.Position{
		Name: file,
		Pos:  pos,
	}, nil
}

type MyEventHandler struct {
	canal.DummyEventHandler
	startingPosition mysql.Position
	targetTableOne   string
	targetTableTwo   string
	targetTableThree string
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {

	if h.isBeforeStartingPosition(e.Header.LogPos) {
		return nil
	}

	switch e.Table.Name {
	case h.targetTableOne:
		if e.Action == "insert" {
			postgresDB := SetupDatabaseConnection()
			defer CloseDatabaseConnection(postgresDB)
			err := insertRowIntoTransaction(postgresDB, e.Rows)
			if err != nil {
				return err
			}
		}
	case h.targetTableTwo:
		if e.Action == "insert" {
			postgresDB := SetupDatabaseConnection()
			defer CloseDatabaseConnection(postgresDB)
			err := updateRowIntoTransaction(postgresDB, e.Rows)
			if err != nil {
				return err
			}
		}
	case h.targetTableThree:
		if e.Action == "insert" {
			postgresDB := SetupDatabaseConnection()
			defer CloseDatabaseConnection(postgresDB)
			err := updateRowIntoTransactionFailed(postgresDB, e.Rows)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (h *MyEventHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	if h.isBeforeStartingPosition(nextPos.Pos) {
		return nil
	}
	return nil
}

func (h *MyEventHandler) isBeforeStartingPosition(logPos uint32) bool {
	return logPos < h.startingPosition.Pos || strings.Compare(h.startingPosition.Name, "") == 0
}

func waitForTerminationSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
}

func SetupDatabaseConnection() *sql.DB {
	errEnv := godotenv.Load()
	if errEnv != nil {
		logrus.Error("loading env vars", errEnv.Error())
		panic("Failed to load env file")
	}

	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASS")
	dbHost := os.Getenv("DB_HOST")
	dbName := os.Getenv("DB_NAME")
	dbPort := os.Getenv("DB_PORT")
	sslMode := os.Getenv("SSL_MODE")

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", dbHost, dbPort, dbUser, dbPass, dbName, sslMode)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		logrus.Error("error occured:", err.Error())
		panic("Failed to connect to database")
	}
	err = db.Ping()
	if err != nil {
		logrus.Error("error occured:", err.Error())
		panic("Failed to ping database")
	}

	logrus.Info("Successfully connected!")
	return db
}

func CloseDatabaseConnection(db *sql.DB) {
	err := db.Close()
	if err != nil {
		logrus.Error("error occured:", err.Error())
		panic("Failed to close the database connection")
	}
}

func insertRowIntoTransaction(db *sql.DB, rows [][]interface{}) error {
	_, err := db.Exec("INSERT INTO transaction (transaction_pk, event_timestamp, connector_pk, id_tag, start_timestamp, start_value) VALUES ($1, $2, $3, $4, $5, $6)", rows[0][0], rows[0][1], rows[0][2], rows[0][3], rows[0][4], rows[0][5])
	if err != nil {
		return err
	}
	return nil
}

func updateRowIntoTransaction(db *sql.DB, rows [][]interface{}) error {
	_, err := db.Exec("UPDATE transaction SET stop_timestamp = $1, stop_value = $2, stop_reason = $3 WHERE transaction_pk = $4", rows[0][3], rows[0][4], rows[0][5], rows[0][0])
	if err != nil {
		return err
	}
	return nil
}

func updateRowIntoTransactionFailed(db *sql.DB, rows [][]interface{}) error {
	_, err := db.Exec("UPDATE transaction SET stop_timestamp = $1, stop_value = $2, stop_reason = $3, fail_reason = $4 WHERE transaction_pk = $5", rows[0][3], rows[0][4], rows[0][5], rows[0][6], rows[0][0])
	if err != nil {
		return err
	}
	return nil
}
