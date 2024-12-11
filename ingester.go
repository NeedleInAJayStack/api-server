package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type mqttIngester struct {
	recStore          recStore
	currentStore      currentStore
	brokerAddr        string
	connectionTimeout time.Duration
	subscribeTimeout  time.Duration

	// Mutable
	// Stores a list of subject names that have been subscribed to.
	topics     map[string]map[uuid.UUID]bool
	mqttClient mqtt.Client
}

func newMqttIngester(
	recStore recStore,
	currentStore currentStore,
	brokerAddr string,
) mqttIngester {
	return mqttIngester{
		recStore:     recStore,
		currentStore: currentStore,
		brokerAddr:   brokerAddr,
		// TODO: Make configurable
		connectionTimeout: time.Duration(5 * time.Second),
		subscribeTimeout:  time.Duration(5 * time.Second),
	}
}

func (i mqttIngester) start() error {
	err := i.connect()
	if err != nil {
		return err
	}

	// TODO: Abstract around this since it's the only place `recStore` is used
	// TODO: Run periodically so that points may be added/removed
	recs, err := i.recStore.readRecs("mqttSubject")
	if err != nil {
		return fmt.Errorf("error getting mqttSubject points: %s", err)
	}
	i.refreshSubscriptions(recs)

	return nil
}

func (i mqttIngester) stop() {
	for topic, _ := range i.topics {
		i.mqttClient.Unsubscribe(topic)
		i.topics[topic] = nil
		log.Printf("Unsubscribed from %s", topic)
	}
	i.disconnect()
}

// Helper methods

func (i mqttIngester) connect() error {
	i.mqttClient = mqtt.NewClient(mqtt.NewClientOptions().AddBroker(i.brokerAddr))
	connectToken := i.mqttClient.Connect()
	if !connectToken.WaitTimeout(i.connectionTimeout) {
		return fmt.Errorf("unable to connect to %s", i.brokerAddr)
	}
	if connectToken.Error() != nil {
		return connectToken.Error()
	}
	log.Printf("Connected to %s", i.brokerAddr)
	return nil
}

func (i mqttIngester) disconnect() {
	i.mqttClient.Disconnect(1)
	log.Printf("Disconnected from %s", i.brokerAddr)
}

func (i mqttIngester) refreshSubscriptions(recs []rec) {
	// TODO: Block around this. Data races will occur on `topics` if also running the onMessage
	recIDs := map[uuid.UUID]bool{}
	for _, record := range recs {
		recIDs[record.ID] = true
	}

	toSubscribe := []topicAndId{}
	for _, record := range recs {
		topic, ok := record.Tags["mqttSubject"].(string)
		if !ok {
			log.Printf("Error asserting type for mqttSubject")
			continue
		}

		_, present := i.topics[topic]
		if !present {
			toSubscribe = append(toSubscribe, topicAndId{topic: topic, recID: record.ID})
			continue
		}
		_, present = i.topics[topic][record.ID]
		if !present {
			toSubscribe = append(toSubscribe, topicAndId{topic: topic, recID: record.ID})
			continue
		}
	}

	toUnsubscribe := []topicAndId{}
	for topic, subscribedRecIDs := range i.topics {
		for subscribedRecID, _ := range subscribedRecIDs {
			_, present := recIDs[subscribedRecID]
			if !present {
				toUnsubscribe = append(toUnsubscribe, topicAndId{topic: topic, recID: subscribedRecID})
			}
		}
	}

	for _, topicAndId := range toSubscribe {
		i.subscribe(topicAndId.topic, topicAndId.recID)
	}
	for _, topicAndId := range toUnsubscribe {
		i.unsubscribe(topicAndId.topic, topicAndId.recID)
	}
}

// Subscribe to a topic, and associate the rec with the topic
func (i mqttIngester) subscribe(topic string, recID uuid.UUID) {
	subscribeToken := i.mqttClient.Subscribe(
		topic,
		0,
		i.onMessage,
	)
	if !subscribeToken.WaitTimeout(i.subscribeTimeout) {
		log.Printf("Unable to subscribe to %s", i.brokerAddr)
	}
	if subscribeToken.Error() != nil {
		log.Print(subscribeToken.Error())
	}
	log.Printf("Subscribed to %s", topic)

	_, present := i.topics[topic]
	if present {
		i.topics[topic][recID] = true
	} else {
		i.topics[topic] = map[uuid.UUID]bool{recID: true}
	}
}

func (i mqttIngester) unsubscribe(topic string, recID uuid.UUID) {
	if i.topics[topic] == nil {
		return
	} else {
		delete(i.topics[topic], recID)
	}

	if len(i.topics[topic]) == 0 {
		unsubscribeToken := i.mqttClient.Unsubscribe(topic)
		if !unsubscribeToken.WaitTimeout(i.subscribeTimeout) {
			log.Printf("Unable to unsubscribe to %s", i.brokerAddr)
		}
		if unsubscribeToken.Error() != nil {
			log.Print(unsubscribeToken.Error())
		}
		log.Printf("Unsubscribed from %s", topic)
	}
}

// TODO: Only place currentStore is used. This could be abstracted by accepting this as a closure.
func (i mqttIngester) onMessage(c mqtt.Client, m mqtt.Message) {
	var currentItem float64
	err := json.Unmarshal(m.Payload(), &currentItem)
	if err != nil {
		log.Printf("Cannot decode message JSON: %s", err)
		return
	}

	recIDs := i.topics[m.Topic()]
	for recID, _ := range recIDs {
		i.currentStore.setCurrent(recID, currentInput{Value: &currentItem})
	}
}

type topicAndId struct {
	topic string
	recID uuid.UUID
}