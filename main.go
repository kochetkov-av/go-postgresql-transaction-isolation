package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "tx-demo"
)

var ctx context.Context

func main() {
	connString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	ctx = context.Background()

	conn1, err := pgx.Connect(ctx, connString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn1.Close(ctx)

	conn2, err := pgx.Connect(ctx, connString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn2.Close(ctx)

	type ReadPhenomena struct {
		name            string
		isolationLevels []string
		testFunction    func(conn1, conn2 *pgx.Conn, isolationLevel string)
	}

	phenomenas := []ReadPhenomena{
		{
			name:            "Dirty read",
			isolationLevels: []string{"READ UNCOMMITTED", "READ COMMITTED"},
			testFunction:    dirtyRead,
		},
		{
			name:            "Nonrepeatable read",
			isolationLevels: []string{"READ COMMITTED", "REPEATABLE READ"},
			testFunction:    nonrepeatableRead,
		},
		{
			name:            "Phantom read",
			isolationLevels: []string{"READ COMMITTED", "REPEATABLE READ"},
			testFunction:    phantomRead,
		},
		{
			name:            "Serialization anomaly",
			isolationLevels: []string{"REPEATABLE READ", "SERIALIZABLE"},
			testFunction:    serializationAnomaly,
		},
	}

	for _, phenomena := range phenomenas {
		fmt.Printf("%s\n", phenomena.name)
		for _, isolationLevel := range phenomena.isolationLevels {
			fmt.Printf("\nIsolation level - %s\n", isolationLevel)
			seedDb(conn1)
			phenomena.testFunction(conn1, conn2, isolationLevel)
			printTable(conn1)
		}
		fmt.Printf("\n---\n\n")
	}
}

func printTable(conn *pgx.Conn) {
	fmt.Printf("Final table state:\n")
	rows, _ := conn.Query(ctx, "SELECT id, name, balance, group_id FROM users ORDER BY id")
	for rows.Next() {
		var name []byte
		var id, balance, group_id int
		rows.Scan(&id, &name, &balance, &group_id)
		fmt.Printf("%2d | %10s | %5d | %d\n", id, name, balance, group_id)
	}
}
