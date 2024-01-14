package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"

	kafka "github.com/13excite/kafka-svc/pkg/producer"
)

type KafkaServer struct {
	eventsBus   kafka.Producer
	Key         string
	WriteEvents [][]byte
}

var (
	// eventsData is sample data for kafka
	eventsData   = []byte(`{"type":"APP_MYTYPE","timestamp":1705248284,"version":"1.2.3","client":"mobile"}`)
	kafkaTopic   string
	kafkaUrl     string
	workersCount int
	// duration time of test
	durationTime int
	// AvgSleep is a number for use rand.Intn(AvgSleep) for worker sleep
	AvgSleep = 5000
	// randLetters is a letters for random string
	randLetters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

// randSeq generates a random string
func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = randLetters[rand.Intn(len(randLetters))]
	}
	return string(b)
}

// New kafka server instance
func NewKafka(eventsBus kafka.Producer) (*KafkaServer, error) {

	return &KafkaServer{
		eventsBus: eventsBus,
	}, nil
}

func (s *KafkaServer) HealthKafka(ctx context.Context) {
	stats, err := s.eventsBus.Health(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// print kafka health
	log.Print("Kafka health: ", stats)
}

func (s *KafkaServer) Collect(ctx context.Context) {
	err := s.eventsBus.Dump(ctx, kafkaTopic, s.Key, s.WriteEvents...)
	if err != nil {
		log.Print("Dump failed:")
		log.Print(err)
		return
	}
	log.Print("Dump completed")

}

func worker(ctx context.Context, wg *sync.WaitGroup, workerNum int, s *KafkaServer) {
	for {
		select {
		case <-ctx.Done():
			wg.Done()
			return
		default:
			s.Collect(ctx)
			log.Print("Run worker: ", workerNum)
			runtime.Gosched()
		}
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(AvgSleep)))
	}
}

func (s *KafkaServer) PrapareData(ctx context.Context) {
	// prepare data
	var writeEvents = make([][]byte, 0, 6)

	for i := 1; i <= 3; i++ {
		writeEvents = append(writeEvents, eventsData)
	}
	s.Key = "ABCD__" + randSeq(5)
	s.WriteEvents = writeEvents
}

func init() {
	flag.IntVar(&workersCount, "count", 5, "workers count")
	flag.StringVar(&kafkaUrl, "kafka",
		"127.0.0.1:29092", "kafka connection string",
	)
	flag.StringVar(&kafkaTopic, "topic",
		"testtopic", "kafka topic name",
	)
	flag.IntVar(&durationTime, "duration", 5, "duration time")
}

func main() {
	flag.Parse()

	var wg sync.WaitGroup
	wg.Add(1)

	eventsBus, err := kafka.New(
		context.TODO(),
		strings.Split(kafkaUrl, ","),
		func(topic string, err error) {
			log.Print(topic, err)
		},
	)
	if err != nil {
		log.Fatal("could not initialize kafka connection")
	}

	s, err := NewKafka(eventsBus)
	if err != nil {
		log.Fatal("could not initialize server")
	}

	// context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(durationTime)*time.Second)
	defer cancel()

	// check kafka health
	s.HealthKafka(ctx)

	runtime.GOMAXPROCS(0)

	for i := 0; i < workersCount; i++ {
		NewRand := rand.New(rand.NewSource(time.Now().UnixNano()))
		// sleep for seed random
		time.Sleep(time.Millisecond * time.Duration(NewRand.Intn(1000)))
		NewRand.Seed(time.Now().UnixNano())
		// prepares data and writes their to struct fields
		s.PrapareData(ctx)

		go worker(ctx, &wg, i, s)
	}

	wg.Wait()
}
