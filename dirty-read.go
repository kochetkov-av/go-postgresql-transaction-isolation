package main

import (
	"fmt"

	"github.com/jackc/pgx/v4"
)

func dirtyRead(conn1, conn2 *pgx.Conn, isolationLevel string) {
	tx, err := conn1.Begin(ctx)
	if err != nil {
		panic(err)
	}
	tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL "+isolationLevel)

	_, err = tx.Exec(ctx, "UPDATE users SET balance = 256 WHERE name='Bob'")
	if err != nil {
		fmt.Printf("Failed to update Bob balance in tx: %v\n", err)
	}

	var balance int
	row := tx.QueryRow(ctx, "SELECT balance FROM users WHERE name='Bob'")
	row.Scan(&balance)
	fmt.Printf("Bob balance from main transaction after update: %d\n", balance)

	row = conn2.QueryRow(ctx, "SELECT balance FROM users WHERE name='Bob'")
	row.Scan(&balance)
	fmt.Printf("Bob balance from concurrent transaction: %d\n", balance)

	if err := tx.Commit(ctx); err != nil {
		fmt.Printf("Failed to commit: %v\n", err)
	}
}
