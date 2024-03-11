package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/lccmrx/rinha-bank/gen/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type (
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
		Value       uint   `json:"valor"`
		Type        string `json:"tipo"`
		Description string `json:"descricao"`
		Timestamp   string `json:"realizada_em"`
	}
)

var (
	gRPCService pb.TransactionServiceClient
)

func init() {

	conn, err := grpc.Dial("orchestrator:50051", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	gRPCService = pb.NewTransactionServiceClient(conn)
}

func main() {
	r := http.NewServeMux()
	r.Handle("GET /clientes/{id}/extrato", http.HandlerFunc(getStatment))
	r.Handle("POST /clientes/{id}/transacoes", http.HandlerFunc(saveTransaction))

	fmt.Println("Server running on port 9999")
	http.ListenAndServe(":9999", r)
}

func saveTransaction(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	reqUUID := uuid.NewString()

	id := r.PathValue("id")

	if id < "1" || id > "5" {
		w.WriteHeader(404)
		return
	}

	var transaction *transaction
	json.NewDecoder(r.Body).Decode(&transaction)

	fmt.Println(reqUUID, id, transaction)
	if transaction.Type != "d" && transaction.Type != "c" {
		w.WriteHeader(422)
		fmt.Println(reqUUID, "tipo invalido")
		return
	}

	if transaction.Value <= 0 {
		fmt.Println(reqUUID, "valor invalido")
		w.WriteHeader(422)
		return
	}

	descLen := len(transaction.Description)
	if 10 < descLen || descLen == 0 {
		fmt.Println(reqUUID, "descricao invalido")
		w.WriteHeader(422)
		return
	}

	result, err := gRPCService.SendTransaction(ctx, &pb.TransactionRequest{UserId: id, Amount: int64(transaction.Value), Type: transaction.Type, Description: transaction.Description})
	if err != nil {
		w.WriteHeader(422)
		fmt.Println(reqUUID, "gRPC call invalido")
		w.Write([]byte(err.Error()))
		return
	}

	s := struct {
		Balance int64 `json:"saldo"`
		Limit   int64 `json:"limite"`
	}{
		Balance: result.GetBalance().GetValue(),
		Limit:   result.GetLimit(),
	}
	sBytes, _ := json.Marshal(s)

	w.Write([]byte(sBytes))
}

func getStatment(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	id := r.PathValue("id")

	if id < "1" || id > "5" {
		w.WriteHeader(404)
		return
	}

	result, _ := gRPCService.GetStatement(ctx, &pb.StatementRequest{UserId: id})

	s := statement{
		Balance: statementBalance{
			Total:         int(result.GetBalance()),
			Limit:         int(result.GetLimit()),
			StatementDate: time.Now(),
		},
		LastTransactions: make([]statementTransactions, 0),
	}

	for _, t := range result.GetTransactions() {
		s.LastTransactions = append(s.LastTransactions, statementTransactions{
			Value:       uint(t.GetAmount()),
			Type:        t.GetType(),
			Description: t.GetDescription(),
			Timestamp:   t.GetTimestamp(),
		})
	}

	sBytes, _ := json.Marshal(s)

	w.Write([]byte(sBytes))
}
