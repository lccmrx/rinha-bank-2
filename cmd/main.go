package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ctx       context.Context
	db        *pgxpool.Pool
	clientMem = make(map[string]*client)
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
		Value       uint      `json:"valor" db:"value"`
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
		Value       uint   `json:"valor"`
		Type        string `json:"tipo"`
		Description string `json:"descricao"`
		Timestamp   string `json:"realizada_em"`
	}
)

func init() {
	ctx = context.Background()

	connStr := fmt.Sprintf("postgresql://%s:%s@%s:%s/postgres?sslmode=disable",
		"postgres",         // user
		"mysecretpassword", // password
		"db",               // host
		"5432",             // port
	)

	dbInstance, err := pgxpool.New(ctx, connStr)
	if err != nil {
		panic(err)
	}

	db = dbInstance

	rows, _ := db.Query(ctx, "select id, \"limit\" from clients")
	for rows.Next() {
		var c client
		rows.Scan(&c.ID, &c.Limit)
		clientMem[fmt.Sprintf("%d", c.ID)] = &c
	}
}

func main() {
	r := http.NewServeMux()
	r.Handle("GET /clientes/{id}/extrato", http.HandlerFunc(getStatment))
	r.Handle("POST /clientes/{id}/transacoes", http.HandlerFunc(saveTransaction))

	http.ListenAndServe(fmt.Sprintf(":%s", os.Getenv("port")), r)
}

func getStatment(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	if id < "1" || id > "5" {
		w.WriteHeader(404)
		return
	}

	client := clientMem[id]
	db.QueryRow(ctx, "call get_balance($1)", id).Scan(&client.Balance)

	rows, _ := db.Query(ctx, fmt.Sprintf("SELECT type, value, description, timestamp FROM get_last_transactions(%s)", id))
	defer rows.Close()

	statementTxs := make([]statementTransactions, 0)
	for rows.Next() {
		var transaction transaction
		rows.Scan(
			&transaction.Type,
			&transaction.Value,
			&transaction.Description,
			&transaction.Timestamp,
		)

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
}

var (
	i     int32 = 0
	mutex sync.Mutex
)

func getIter() int32 {
	mutex.Lock()
	defer func() {
		atomic.AddInt32(&i, 1)
		mutex.Unlock()
	}()
	return i
}

func saveTransaction(w http.ResponseWriter, r *http.Request) {
	i := getIter()
	startTime := time.Now()
	rID := uuid.NewString()
	id := r.PathValue("id")
	fmt.Printf("[%d][%s][%s][1] Time started: %v\n", i, rID, id, startTime)
	ctx := r.Context()

	if id < "1" || id > "5" {
		w.WriteHeader(404)
		return
	}

	var transaction *transaction
	json.NewDecoder(r.Body).Decode(&transaction)

	fmt.Printf("[%d][%s][%s][2] Payload: %+v\n", i, rID, id, transaction)

	if transaction.Type != "d" && transaction.Type != "c" {
		w.WriteHeader(422)
		return
	}

	if transaction.Value == 0 {
		w.WriteHeader(422)
		return
	}

	descLen := len(transaction.Description)
	if descLen < 1 || descLen > 10 {
		w.WriteHeader(422)
		return
	}

	compareTime := time.Now()
	var balance *int
	err := db.QueryRow(ctx, "call transact($1, $2, $3, $4)", id, transaction.Value, transaction.Type, transaction.Description).Scan(&balance)
	fmt.Println(err)
	fmt.Printf("[%d][%s][%s][3] Time took to call procedure: %+v\n", i, rID, id, time.Since(compareTime).Microseconds())
	if balance == nil {
		w.WriteHeader(422)
		return
	}
	fmt.Printf("[%d][%s][%s][4] Balance after transacting: %+v\n", i, rID, id, *balance)

	client := clientMem[id]
	client.Balance = *balance
	data, _ := json.Marshal(client)

	fmt.Printf("[%d][%s][%s][5] Time took to process request: %+v\n", i, rID, id, time.Since(startTime).Microseconds())
	w.WriteHeader(200)
	w.Write(data)
}
