package main

import (
	"fmt"

	"github.com/jackc/pgx/v4"
)

type User struct {
	Name    string
	Balance int
}

func phantomRead(conn1, conn2 *pgx.Conn, isolationLevel string) {
	tx, err := conn1.Begin(ctx)
	if err != nil {
		panic(err)
	}
	tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL "+isolationLevel)

	var users []User
	var user User
	rows, _ := tx.Query(ctx, "SELECT name, balance FROM users WHERE group_id = 2")
	for rows.Next() {
		var user User
		rows.Scan(&user.Name, &user.Balance)
		users = append(users, user)
	}
	fmt.Printf("Users in group 2 at the beginning of transaction:\n%v\n", users)

	fmt.Printf("Cuncurrent transaction moves Bob to group 2\n")
	conn2.Exec(ctx, "UPDATE users SET group_id = 2 WHERE name='Bob'")

	users = []User{}
	rows, _ = tx.Query(ctx, "SELECT name, balance FROM users WHERE group_id = 2")
	for rows.Next() {
		rows.Scan(&user.Name, &user.Balance)
		users = append(users, user)
	}
	fmt.Printf("Users in group 2 after cuncurrent transaction:\n%v\n", users)

	fmt.Printf("Update selected users balances by +15\n")
	for _, user := range users {
		_, err = tx.Exec(ctx, "UPDATE users SET balance = $1 WHERE name=$2", user.Balance+15, user.Name)
		if err != nil {
			fmt.Printf("Failed to update in tx: %v\n", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		fmt.Printf("Failed to commit: %v\n", err)
	}
}
