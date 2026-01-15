package nats

import (
	"encoding/json"
	"inventory-service/config"
	"inventory-service/models"

	"github.com/nats-io/nats.go"
)

func Connect(config *config.Config) error {
	opts := []nats.Option{
		nats.UserInfo(config.Nats.Username, config.Nats.Password),
	}
	nc, err := nats.Connect(config.Nats.URL, opts...)
	if err != nil {
		config.Logger.Error("NATS: Failed to connect", "error", err)
		return err
	}
	config.Nats.Client = nc
	config.Logger.Info("NATS: Connected successfully")
	return nil
}

func PublishOrderEvent(client *nats.Conn, event models.OrderEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return client.Publish("orders.pending", data)
}

func Close(client *nats.Conn) {
	if client != nil {
		client.Close()
	}
}
