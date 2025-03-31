package main

import (
    "fmt"
    "time"
    "github.com/IBM/sarama"
)
func main() {
	brokers:=[]string{"localhost:9092"}
	config:=sarama.NewConfig()
	config.Consumer.Return.Errors=true
	consumer, _ := sarama.NewConsumer(brokers,config)
	defer consumer.Close()

	partitionConsumer, _ := consumer.ConsumePartition("myTopic",0,sarama.OffsetNewest)
	defer partitionConsumer.Close()

	timeout := time.After(2 * time.Minute)
	for {
		select {
			case msg := <- partitionConsumer.Messages():
				if msg != nil {
					fmt.Println("Received: %s", string(msg.Value))
				}
			case <-timeout:
				fmt.Println("timeout. exiting")
				return
		}
	}




}
