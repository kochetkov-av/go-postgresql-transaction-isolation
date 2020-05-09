package main

import (
	"fmt"

	"github.com/jackc/pgx/v4"
)

func serializationAnomaly(conn1, conn2 *pgx.Conn, isolationLevel string) {
	tx, err := conn1.Begin(ctx)
	if err != nil {
		panic(err)
	}
	tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL "+isolationLevel)

	tx2, err := conn2.Begin(ctx)
	if err != nil {
		panic(err)
	}
	tx2.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL "+isolationLevel)

	var sum int
	row := tx.QueryRow(ctx, "SELECT SUM(balance) FROM users WHERE group_id = 2")
	row.Scan(&sum)

	tx2.Exec(ctx, "UPDATE users SET group_id = 2 WHERE name='Bob'")
	if err != nil {
		fmt.Printf("Error in tx2: %v\n", err)
	}

	rows, _ := tx.Query(ctx, "SELECT name, balance FROM users WHERE group_id = 2")
	type User struct {
		Name    string
		Balance int
	}
	var users []User
	for rows.Next() {
		var user User
		rows.Scan(&user.Name, &user.Balance)
		users = append(users, user)
	}

	for _, user := range users {
		_, err = tx.Exec(ctx, "UPDATE users SET balance = $1 WHERE name=$2", user.Balance+sum, user.Name)
		if err != nil {
			fmt.Printf("Failed to update in tx: %v\n", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		fmt.Printf("Failed to commit tx: %v\n", err)
	}

	if err := tx2.Commit(ctx); err != nil {
		fmt.Printf("Failed to commit tx2: %v\n", err)
	}
}
