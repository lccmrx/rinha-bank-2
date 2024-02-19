package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	err    error
	db     *pgxpool.Pool
	dbOnce sync.Once
)

type (
	client struct {
		ID      int `json:"-" db:"id"`
		Balance int `json:"saldo" db:"balance"`
		Limit   int `json:"limite" db:"limit"`
	}

	transaction struct {
		ClientID    int       `json:"cliente_id" db:"client_id"`
		Type        string    `json:"tipo" db:"type"`
		Description string    `json:"descricao" db:"description"`
		Value       int       `json:"valor" db:"value"`
		Timestamp   time.Time `json:"timestamp" db:"timestamp"`
	}

	statement struct {
		Balance          statementBalance        `json:"saldo"`
		LastTransactions []statementTransactions `json:"ultimas_transacoes"`
	}

	statementBalance struct {
		Total         int       `json:"total"`
		Limit         int       `json:"limite"`
		StatementDate time.Time `json:"data_extrato"`
	}
	statementTransactions struct {
		Value       int    `json:"valor"`
		Type        string `json:"tipo"`
		Description string `json:"descricao"`
		Timestamp   string `json:"realizada_em"`
	}
)

func init() {

	dbOnce.Do(func() {
		connStr := fmt.Sprintf("postgresql://%s:%s@%s:%s/postgres?sslmode=disable",
			"postgres",         // user
			"mysecretpassword", // password
			"localhost",        // host
			"5432",             // port
		)

		db, err = pgxpool.New(context.Background(), connStr)
		if err != nil {
			panic(err)
		}
	})
}

func main() {
	r := http.NewServeMux()
	defer db.Close()
	r.Handle("GET /clientes/{id}/extrato", getStatment())
	r.Handle("POST /clientes/{id}/transacoes", saveTransaction())

	http.ListenAndServe(":9999", r)
}

func getStatment() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		rId := uuid.NewString()
		start := time.Now()
		fmt.Println(rId, "start", "statement", start)

		id := r.PathValue("id")

		if id < "1" || id > "5" {
			w.WriteHeader(404)
			return
		}

		fmt.Println(rId, "before-select-balance", time.Since(start))

		var client client
		err = db.QueryRow(ctx, "SELECT balance, \"limit\" FROM client WHERE id = $1", id).Scan(&client.Balance, &client.Limit)
		if err != nil {
			fmt.Println(2, err)
			w.WriteHeader(500)
			return
		}

		fmt.Println(rId, "before-select-last-10-transactions", time.Since(start))
		rows, err := db.Query(ctx, "SELECT client_id, type, value, description, timestamp FROM transaction WHERE client_id = $1 order by timestamp desc limit 10", id)
		if err != nil {
			fmt.Println(3, err)
			w.WriteHeader(500)
			return
		}
		defer rows.Close()

		fmt.Println(rId, "before-transactions-row-scan", time.Since(start))

		statementTxs := make([]statementTransactions, 0)
		for rows.Next() {
			var transaction transaction
			err = rows.Scan(
				&transaction.ClientID,
				&transaction.Type,
				&transaction.Value,
				&transaction.Description,
				&transaction.Timestamp,
			)
			if err != nil {
				fmt.Println(4, err)
				w.WriteHeader(500)
				return
			}

			statementTxs = append(statementTxs, statementTransactions{
				Value:       transaction.Value,
				Type:        transaction.Type,
				Description: transaction.Description,
				Timestamp:   transaction.Timestamp.Format(time.RFC3339Nano),
			})
		}

		statement := statement{
			Balance: statementBalance{
				Total:         client.Balance,
				Limit:         client.Limit,
				StatementDate: time.Now(),
			},
			LastTransactions: statementTxs,
		}

		data, _ := json.Marshal(statement)

		w.WriteHeader(200)
		w.Write(data)
		fmt.Println(rId, "end", time.Since(start))
		// tx.Commit(ctx)
	})
}

func saveTransaction() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		rId := uuid.NewString()
		start := time.Now()
		fmt.Println(rId, "start", "transaction", start)

		id := r.PathValue("id")

		if id < "1" || id > "5" {
			w.WriteHeader(404)
			return
		}

		bodyBytes, _ := io.ReadAll(r.Body)

		var transaction transaction
		err = json.Unmarshal(bodyBytes, &transaction)
		if err != nil {
			w.WriteHeader(422)
			return
		}

		if transaction.Type != "d" && transaction.Type != "c" {
			w.WriteHeader(422)
			return
		}

		if transaction.Value < 1 {
			w.WriteHeader(422)
			return
		}

		if transaction.Description == "" || len(transaction.Description) > 10 {
			w.WriteHeader(422)
			return
		}

		fmt.Println(rId, "before-begin-tx", time.Since(start))

		tx, err := db.Begin(ctx)
		if err != nil {
			w.WriteHeader(500)
			return
		}
		defer tx.Rollback(ctx)

		fmt.Println(rId, "before-select-balance", time.Since(start))

		var client client
		err = tx.QueryRow(ctx, "SELECT balance, \"limit\" FROM client WHERE id = $1 FOR UPDATE", id).Scan(&client.Balance, &client.Limit)
		if err != nil {
			w.WriteHeader(500)
			return
		}

		fmt.Println(rId, "before-update", time.Since(start))
		if transaction.Type == "c" {
			tx.Exec(ctx, "UPDATE client SET balance = balance + $1 WHERE id = $2", transaction.Value, id)
		}

		if transaction.Type == "d" {
			if client.Balance-transaction.Value < client.Limit*-1 {
				w.WriteHeader(422)
				return
			}
			tx.Exec(ctx, "UPDATE client SET balance = balance - $1 WHERE id = $2", transaction.Value, id)
		}
		tx.Exec(ctx, "INSERT INTO transaction (client_id, type, description, value) VALUES ($1, $2, $3, $4)", id, transaction.Type, transaction.Description, transaction.Value)

		data, _ := json.Marshal(client)

		w.WriteHeader(200)
		w.Write(data)
		fmt.Println(rId, "end", time.Since(start))
		tx.Commit(ctx)
	})
}
