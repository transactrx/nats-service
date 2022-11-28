package client_endpoints

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Client struct {
	ClientId string `json:"clientId"`
	Name     string `json:"name"`
	Comments string `json:"comments"`
	Type     string `json:"type"`
}

func FetchClients(pool *pgxpool.Pool, clientType string) ([]Client, error) {
	sql := `
		select clientid, name, comments, type 
		from clients 
		where type =$1`

	rows, err := pool.Query(context.Background(), sql, clientType)
	if err != nil {
		return nil, err
	}

	var clients []Client

	for rows.Next() {
		client := Client{}

		rows.Scan(&client.ClientId, &client.Name, &client.Comments, &client.Type)

		clients = append(clients, client)
	}

	return clients, nil
}
