package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

func tcpListener(producer sarama.SyncProducer, l SocketAddr) {

	tcpAddr, err := net.ResolveTCPAddr(l.proto, l.addr)
	checkError(err)

	listener, err := net.ListenTCP(l.proto, tcpAddr)
	checkError(err)

	log.Println("Created TCP listener on topic ", l.topic)

	var wg sync.WaitGroup

	for {
		//fmt.Println("number of active go routiines is ", runtime.NumGoroutine())
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Println(err)
			conn.Close()
			continue
		}
		wg.Add(1)
		go tcpConnHandler(conn, l.topic, producer, &wg)
	}

	wg.Wait()

}

func tcpConnHandler(conn *net.TCPConn, topic string, producer sarama.SyncProducer, wg *sync.WaitGroup) {

	defer conn.Close()

	addr := conn.RemoteAddr()
	addrString := addr.String()
	//	key := ":::ORIGIN:::" + addrString

	topic = topic[1:]

	reader := bufio.NewReader(conn)

	for {
		buf, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		buf = strings.TrimRight(buf, "\r\n")
		//msg := buf + key
		//log.Println("Got TCP packet: ", msg)

		msg := addrString + ":" + "tcp " + buf

		publish([]byte(msg), topic, producer)

	}
	wg.Done()
}

func udpListener(producer sarama.SyncProducer, l SocketAddr) {

	udpAddr, err := net.ResolveUDPAddr(l.proto, l.addr)
	checkError(err)

	conn, err := net.ListenUDP(l.proto, udpAddr)
	checkError(err)
	log.Println("Created UDP listener on topic ", l.topic)

	checkError(err) // this error needs to be better clarified.

	go udpConnHandler(conn, l.topic, producer)

}

func udpConnHandler(conn *net.UDPConn, topic string, producer sarama.SyncProducer) {

	//defer fmt.Println("Goodbye from UDP handler")
	//	fmt.Println("Got UDP Packet")

	topic = topic[1:]
	var buf [1024]byte
	for {
		n, addr, err := conn.ReadFromUDP(buf[0:])
		if err != nil {
			return
		}

		s := string(buf[:n])
		addrString := addr.String()
		msg := addrString + ":" + "udp " + s
		//log.Println("From ", addr.String(), "got UDP packet ready for topic ", topic, ": ", s)

		publish([]byte(msg), topic, producer)

	}
}

func listenerSocketProcessor(a []SocketAddr, b []SocketAddr) {

	var broker = make([]string, len(a))

	for i, l := range a {
		broker[i] = l.addr
	}

	topicSet := make(map[string]bool)

	for _, l := range b {
		topicSet[l.topic] = true
	}

	initTopics(broker, topicSet)

	producer, err := initProducer(broker)

	checkError(err)

	for _, l := range b {
		if l.proto == "tcp" {
			go tcpListener(producer, l)
		} else {
			go udpListener(producer, l)
		}
	}

	for {
		time.Sleep(100 * time.Second) // this needs to be changed to sync.WaitGroup
	}

}

func initTopics(broker []string, topicSet map[string]bool) {

	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	cluster, err := sarama.NewConsumer(broker, config)
	checkError(err)

	defer func() {
		if err := cluster.Close(); err != nil {
			checkError(err)
		}
	}()

	topics, _ := cluster.Topics()
	var count int
	var s string

	for key, _ := range topicSet {
		s = fmt.Sprintf("%s", key[1:])
		for i, _ := range topics {
			if s == topics[i] {
				count = 0
				break
			} else {
				count++
			}

		}
		if count != 0 {
			log.Println("Creating new topic", s)
			createTopic(s, broker)
			count = 0
		}

	}

}

func createTopic(topic string, broker []string) {

	for i, _ := range broker {

		brokers := sarama.NewBroker(broker[i])

		config := sarama.NewConfig()
		config.Version = sarama.V1_0_0_0
		brokers.Open(config)
		_, err := brokers.Connected()
		checkError(err)

		//need changing later:
		topicDetail := &sarama.TopicDetail{}
		topicDetail.NumPartitions = int32(3)
		topicDetail.ReplicationFactor = int16(3)
		topicDetail.ConfigEntries = make(map[string]*string)

		topicDetails := make(map[string]*sarama.TopicDetail)
		topicDetails[topic] = topicDetail

		request := sarama.CreateTopicsRequest{
			Timeout:      time.Second * 15,
			TopicDetails: topicDetails,
		}

		response, err := brokers.CreateTopics(&request)
		checkError(err)

		t := response.TopicErrors
		for key, val := range t {
			log.Printf("Key is %s", key)
			log.Printf("Value is %#v", val.Err.Error())
		}

		// close connection to broker
		brokers.Close()
	}
}

func initProducer(broker []string) (sarama.SyncProducer, error) {
	// setup sarama log to stdout
	//sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	// producer config
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	//change from sync to async after testing:
	// async producer
	//prd, err := sarama.NewAsyncProducer([]string{kafkaConn}, config)

	prd, err := sarama.NewSyncProducer(broker, config)

	return prd, err
}

func publish(message []byte, topic string, producer sarama.SyncProducer) {
	// publish sync

	msg := &sarama.ProducerMessage{
		Topic: topic,
		//Value: sarama.StringEncoder(message),
		//Key:   "Message",
		Value: sarama.StringEncoder(message),
	}

	p, o, err := producer.SendMessage(msg)
	checkError(err) //this error needs changing, so that publish() can check whether kafka producer is still there

	// publish async
	//producer.Input() <- &sarama.ProducerMessage{

	if debug {
		log.Println("Publishing messages to Partition: ", p, "with offset: ", o)
	}

}
