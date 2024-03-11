package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lccmrx/rinha-bank/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type (
	server struct {
		pb.UnimplementedTransactionServiceServer
	}
	client struct {
		ID      int64
		Limit   int64
		Balance atomic.Int64
	}
)

var (
	clients = make(map[string]*client)
	ctx     context.Context
	db      *pgxpool.Pool
)

func init() {
	ctx = context.Background()

	connStr := fmt.Sprintf("postgresql://%s:%s@%s:%s/postgres?sslmode=disable",
		"postgres",           // user
		"mysecretpassword",   // password
		os.Getenv("db_host"), // host
		"5432",               // port
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
		clients[fmt.Sprintf("%d", c.ID)] = &c
	}
}

// SendTransaction implements transaction.TransactionService
func (s *server) SendTransaction(ctx context.Context, in *pb.TransactionRequest) (*pb.BalanceResponse, error) {
	client := clients[in.GetUserId()]

	var value int64
	switch in.GetType() {
	case "d":
		if client.Balance.Load()-in.GetAmount() < client.Limit*-1 {
			return &pb.BalanceResponse{Balance: nil}, fmt.Errorf("insufficient funds")
		}
		value = -in.GetAmount()
	case "c":
		value = in.GetAmount()
	}

	newBalance := client.Balance.Add(value)

	go func() {
		db.Exec(context.Background(), "call transact($1, $2, $3, $4)", in.GetUserId(), in.GetAmount(), in.GetType(), in.GetDescription())
	}()

	return &pb.BalanceResponse{Balance: wrapperspb.Int64(newBalance), Limit: client.Limit}, nil
}

func (s *server) GetStatement(ctx context.Context, in *pb.StatementRequest) (*pb.StatementResponse, error) {
	client := clients[in.GetUserId()]

	// client.lock.Lock()
	// defer client.lock.Unlock()

	var transactions []*pb.TransactionRecord
	rows, _ := db.Query(ctx, "select * from get_last_transactions($1)", client.ID)
	var balance int
	for rows.Next() {
		var t struct {
			Type        string
			Amount      int64
			Description string
			Timestamp   time.Time
		}
		rows.Scan(&t.Type, &t.Amount, &t.Description, &t.Timestamp, &balance)
		transactions = append(transactions, &pb.TransactionRecord{
			Type:        t.Type,
			Amount:      t.Amount,
			Description: t.Description,
			Timestamp:   t.Timestamp.Format(time.RFC3339),
		})
	}

	// client.Balance = int64(balance)

	return &pb.StatementResponse{Transactions: transactions, Limit: client.Limit, Balance: client.Balance.Load()}, nil
}

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterTransactionServiceServer(s, &server{})

	fmt.Println("Server running on port 50051")
	if err := s.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
