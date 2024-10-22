package gqldata

import (
	"context"

	"github.com/machinebox/graphql"
)

func GetConnectorVersions(ctx context.Context, c *graphql.Client, adminSecret string) ([]ConnectorVersion, error) {
	req := graphql.NewRequest(`
	query ConnectorVersions {
		hub_registry_connector_version {
			namespace
			name
			version
		}
	}
	`)
	req.Header.Add("x-hasura-admin-secret", adminSecret)

	var response struct {
		ConnectorVersions []ConnectorVersion `json:"hub_registry_connector_version"`
	}
	err := c.Run(ctx, req, &response)
	if err != nil {
		return nil, err
	}
	return response.ConnectorVersions, nil
}

type ConnectorVersion struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Version   string `json:"version"`
}
