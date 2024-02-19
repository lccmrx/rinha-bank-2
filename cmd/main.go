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
	"github.com/lccmrx/rinha-bank/cmd/lock"
)

var (
	ctx       context.Context
	err       error
	db        *pgxpool.Pool
	dbOnce    sync.Once
	clientMem = make(map[string]client)
	mlock     lock.MultipleLock
)

type (
	client struct {
		ID      int `json:"-" db:"id"`
		Limit   int `json:"limite" db:"limit"`
		Balance int `json:"saldo" db:"balance"`
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

	ctx = context.Background()

	dbOnce.Do(func() {
		connStr := fmt.Sprintf("postgresql://%s:%s@%s:%s/postgres?sslmode=disable",
			"postgres",         // user
			"mysecretpassword", // password
			"localhost",        // host
			"5432",             // port
		)

		db, err = pgxpool.New(ctx, connStr)
		if err != nil {
			panic(err)
		}
	})

	rows, err := db.Query(ctx, "SELECT id, \"limit\" FROM client")
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		var client client
		rows.Scan(&client.ID, &client.Limit)
		if err != nil {
			panic(err)
		}

		clientMem[fmt.Sprintf("%d", client.ID)] = client
	}

	mlock = lock.NewMultipleLock()

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

		var balance int
		err = db.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM balance%s ORDER BY timestamp DESC", id)).Scan(&balance)
		if err != nil {
			fmt.Println(2, err)
			w.WriteHeader(500)
			return
		}

		fmt.Println(rId, "before-select-last-10-transactions", time.Since(start))
		rows, err := db.Query(ctx, fmt.Sprintf("SELECT type, value, description, timestamp FROM transaction%s order by timestamp desc limit 10", id))
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
				Total:         balance,
				Limit:         clientMem[id].Limit,
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

		mlock.Lock(any(id))
		defer mlock.Unlock(any(id))
		fmt.Println(rId, "before-select-balance", time.Since(start))

		var balance int
		err = db.QueryRow(ctx, fmt.Sprintf("SELECT balance FROM balance%s ORDER BY timestamp DESC", id)).Scan(&balance)
		if err != nil {
			fmt.Println(2, err)
			w.WriteHeader(500)
			return
		}

		client := clientMem[id]
		if transaction.Type == "c" {
			balance += transaction.Value
		}

		if transaction.Type == "d" {
			if balance-transaction.Value < client.Limit*-1 {
				w.WriteHeader(422)
				return
			}
			balance -= transaction.Value
		}
		fmt.Println(rId, "before-insert-balance-transaction", time.Since(start))
		db.Exec(ctx, fmt.Sprintf("INSERT INTO balance%s (balance) values ($1)", id), balance)
		db.Exec(ctx, fmt.Sprintf("INSERT INTO transaction%s (type, description, value) VALUES ($1, $2, $3)", id), transaction.Type, transaction.Description, transaction.Value)

		client.Balance = balance
		data, _ := json.Marshal(client)

		w.WriteHeader(200)
		w.Write(data)
		fmt.Println(rId, "end", time.Since(start))
	})
}
