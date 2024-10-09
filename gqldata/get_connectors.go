package gqldata

import (
	"context"

	"github.com/machinebox/graphql"
)

type Connector struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

func GetConnectors(ctx context.Context, c *graphql.Client, adminSecret string) ([]Connector, error) {
	req := graphql.NewRequest(`
	query Connectors {
		hub_registry_connector {
			namespace
			name
		}
	}
	`)
	req.Header.Add("x-hasura-admin-secret", adminSecret)

	var response struct {
		Connectors []Connector `json:"hub_registry_connector"`
	}
	err := c.Run(ctx, req, &response)
	if err != nil {
		return nil, err
	}
	return response.Connectors, nil
}
