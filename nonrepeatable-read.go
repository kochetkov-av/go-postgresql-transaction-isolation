package main

import (
	"fmt"

	"github.com/jackc/pgx/v4"
)

func nonrepeatableRead(conn1, conn2 *pgx.Conn, isolationLevel string) {
	tx, err := conn1.Begin(ctx)
	if err != nil {
		panic(err)
	}
	tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL "+isolationLevel)

	row := tx.QueryRow(ctx, "SELECT balance FROM users WHERE name='Bob'")
	var balance int
	row.Scan(&balance)
	fmt.Printf("Bob balance at the beginning of transaction: %d\n", balance)

	fmt.Printf("Updating Bob balance to 1000 from connection 2\n")
	_, err = conn2.Exec(ctx, "UPDATE users SET balance = 1000 WHERE name='Bob'")
	if err != nil {
		fmt.Printf("Failed to update Bob balance from conn2  %e", err)
	}

	_, err = tx.Exec(ctx, "UPDATE users SET balance = $1 WHERE name='Bob'", balance+10)
	if err != nil {
		fmt.Printf("Failed to update Bob balance in tx: %v\n", err)
	}

	if err := tx.Commit(ctx); err != nil {
		fmt.Printf("Failed to commit: %v\n", err)
	}
}
