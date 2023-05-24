package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
)

func main() {
	if err := Init(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize: %v\n", err)
		os.Exit(1)
	}

	http.HandleFunc("/", requestHandler)
	http.ListenAndServe(":8080", nil)
}

var (
	dbPool      *pgxpool.Pool
	redisClient *redis.Client
	kafkaReader *kafka.Reader
	grpcConn    *grpc.ClientConn
)

func Init() error {
	// 初始化数据库连接池
	var err error
	dbPool, err = pgxpool.Connect(context.Background(), "postgresql://username:password@localhost:5432/database")
	if err != nil {
		return errors.Wrap(err, "failed to connect to the database")
	}

	// 初始化Redis客户端
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	// 初始化Kafka客户端
	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "topic-name",
		Partition: 0,
	})

	// 初始化gRPC连接
	grpcConn, err = grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		return errors.Wrap(err, "failed to connect to the gRPC server")
	}

	return nil
}

func requestHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := dbPool.Acquire(context.Background())
	if err != nil {
		http.Error(w, "Could not acquire database connection", http.StatusInternalServerError)
		return
	}
	defer conn.Release()

	// 使用数据库连接执行你的操作
	// ...

	fmt.Fprintf(w, "Request processed successfully.")
}
