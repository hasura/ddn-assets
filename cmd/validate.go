package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate assets",
	Run: func(cmd *cobra.Command, args []string) {
		gqlEndpoint := os.Getenv("HASURA_GRAPHQL_ENDPOINT")
		if len(gqlEndpoint) == 0 {
			fmt.Println("please set HASURA_GRAPHQL_ENDPOINT env var")
			os.Exit(1)
			return
		}
		if !strings.HasSuffix(gqlEndpoint, "/v1/graphql") {
			gqlEndpoint = gqlEndpoint + "/v1/graphql"
		}

		gqlAdminSecret := os.Getenv("HASURA_GRAPHQL_ADMIN_SECRET")
		if len(gqlAdminSecret) == 0 {
			fmt.Println("please set HASURA_GRAPHQL_ADMIN_SECRET env var")
			os.Exit(1)
			return
		}

		// gqlClient := graphql.NewClient(gqlEndpoint)
		// connectorsInDB, err := gqldata.GetConnectors(context.Background(), gqlClient, gqlAdminSecret)
		// if err != nil {
		// 	fmt.Println("error while getting list of onnectors", err)
		// 	os.Exit(1)
		// 	return
		// }
		// connectorVersionsInDB, err := gqldata.GetConnectorVersions(context.Background(), gqlClient, gqlAdminSecret)
		// if err != nil {
		// 	fmt.Println("error while getting list of connector versions", err)
		// 	os.Exit(1)
		// 	return
		// }

		// TODO: enable after fixing neo4j and sendgrid
		// validate index.json: check for presense of all connectors
		// hasValidIndexJSON := true
		// fmt.Printf("total number of connectors: in db = %d, in index.json = %d\n", len(connectorsInDB), index.TotalConnectors)
		// var unpresentConnectorsInHub []string
		// for _, dbc := range connectorsInDB {
		// 	slug := fmt.Sprintf("%s/%s", dbc.Namespace, dbc.Name)
		// 	if _, ok := index.ConnectorVersions[slug]; !ok {
		// 		unpresentConnectorsInHub = append(unpresentConnectorsInHub, slug)
		// 	}
		// }
		// if len(unpresentConnectorsInHub) > 0 {
		// 	fmt.Println("Following connectors are present in DB, but not in ndc-hub:")
		// 	fmt.Println(strings.Join(unpresentConnectorsInHub, "\n"))
		// 	hasValidIndexJSON = false
		// }

		// var unpresentConnectorsInDB []string
		// for _, hubc := range index.Connectors {
		// 	foundInDb := false
		// 	for _, dbc := range connectorsInDB {
		// 		if dbc.Namespace == hubc.Namespace && dbc.Name == hubc.Name {
		// 			foundInDb = true
		// 			break
		// 		}
		// 	}
		// 	if !foundInDb {
		// 		slug := fmt.Sprintf("%s/%s", hubc.Namespace, hubc.Name)
		// 		unpresentConnectorsInDB = append(unpresentConnectorsInDB, slug)
		// 	}
		// }
		// if len(unpresentConnectorsInDB) > 0 {
		// 	fmt.Println("Following connectors are present in ndc-hub, but not in the DB:")
		// 	fmt.Println(strings.Join(unpresentConnectorsInDB, "\n"))
		// 	hasValidIndexJSON = false
		// }

		// if !hasValidIndexJSON {
		// 	os.Exit(1)
		// 	return
		// }

		// TODO: enable after fixing neo4j/neo4j [v0.0.6 v0.0.7 v0.0.10]
		// validate index.json: check for presence of all connector versions
		// unfoundConnectorVersions := make(map[string][]string)
		// for _, dbcv := range connectorVersionsInDB {
		// 	slug := fmt.Sprintf("%s/%s", dbcv.Namespace, dbcv.Name)
		// 	foundVersion := false
		// 	for _, v := range index.ConnectorVersions[slug] {
		// 		if v == dbcv.Version {
		// 			foundVersion = true
		// 			break
		// 		}
		// 	}
		// 	if !foundVersion {
		// 		unfoundConnectorVersions[slug] = append(unfoundConnectorVersions[slug], dbcv.Version)
		// 	}
		// }

		// if len(unfoundConnectorVersions) > 0 {
		// 	fmt.Println("Following connector versions are found in DB but not in the ndc-hub")
		// 	count := 1
		// 	for k, v := range unfoundConnectorVersions {
		// 		fmt.Printf("%d. %s %+v\n", count, k, v)
		// 		count++
		// 	}
		// 	os.Exit(1)
		// 	return
		// }
	},
}
