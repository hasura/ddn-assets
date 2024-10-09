package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/hasura/ddn-assets/gqldata"
	"github.com/machinebox/graphql"
	"golang.org/x/sync/errgroup"
)

const (
	MetadataJSON           = "metadata.json"
	ConnectorPackagingJSON = "connector-packaging.json"
)

func main() {
	ndcHubGitRepoFilePath := os.Getenv("NDC_HUB_GIT_REPO_FILE_PATH")
	if ndcHubGitRepoFilePath == "" {
		fmt.Println("please set a value for NDC_HUB_GIT_REPO_FILE_PATH env var")
		os.Exit(1)
		return
	}

	registryFolder := filepath.Join(ndcHubGitRepoFilePath, "registry")
	_, err := os.Stat(registryFolder)
	if err != nil {
		fmt.Println("error while finding the registry folder", err)
		os.Exit(1)
		return
	}
	if os.IsNotExist(err) {
		fmt.Println("registry folder does not exist")
		os.Exit(1)
		return
	}

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

	gqlClient := graphql.NewClient(gqlEndpoint)
	// connectorsInDB, err := gqldata.GetConnectors(context.Background(), gqlClient, gqlAdminSecret)
	// if err != nil {
	// 	fmt.Println("error while getting list of onnectors", err)
	// 	os.Exit(1)
	// 	return
	// }

	connectorVersionsInDB, err := gqldata.GetConnectorVersions(context.Background(), gqlClient, gqlAdminSecret)
	if err != nil {
		fmt.Println("error while getting list of connector versions", err)
		os.Exit(1)
		return
	}

	var connectorMetadata []Metadata
	var connectorPackaging []ConnectorPackaging
	err = filepath.WalkDir(registryFolder, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if filepath.Base(path) == MetadataJSON {
			metadata, err := getConnectorMetadata(path)
			if err != nil {
				return err
			}
			if metadata != nil {
				connectorMetadata = append(connectorMetadata, *metadata)
			}
		}

		if filepath.Base(path) == ConnectorPackagingJSON {
			cp, err := getConnectorPackaging(path)
			if err != nil {
				return err
			}
			if cp != nil {
				connectorPackaging = append(connectorPackaging, *cp)
			}
		}

		return nil
	})
	if err != nil {
		fmt.Println("error while walking the registry folder", err)
		os.Exit(1)
		return
	}

	connectorVersions := make(map[string][]string)
	for _, cp := range connectorPackaging {
		slug := fmt.Sprintf("%s/%s", cp.Namespace, cp.Name)

		// TODO: remove following block after standardizing in ndc-hub
		version := cp.Version
		if !strings.HasPrefix(cp.Version, "v") {
			version = "v" + cp.Version
		}

		connectorVersions[slug] = append(connectorVersions[slug], version)
	}

	// construct index.json and write it
	index := Index{
		TotalConnectors:   len(connectorMetadata),
		Connectors:        connectorMetadata,
		ConnectorVersions: connectorVersions,
	}
	indexJson, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		fmt.Println("error while marshalling index json")
		os.Exit(1)
		return
	}
	indexJsonPath := "assets/index.json"
	err = os.WriteFile(indexJsonPath, indexJson, 0644)
	if err != nil {
		fmt.Println("error writing", indexJsonPath, err)
		os.Exit(1)
		return
	}
	fmt.Println("successfully wrote: ", indexJsonPath)

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

	// validate index.json: check for presence of all connector versions
	unfoundConnectorVersions := make(map[string][]string)
	for _, dbcv := range connectorVersionsInDB {
		slug := fmt.Sprintf("%s/%s", dbcv.Namespace, dbcv.Name)
		foundVersion := false
		for _, v := range index.ConnectorVersions[slug] {
			if v == dbcv.Version {
				foundVersion = true
				break
			}
		}
		if !foundVersion {
			unfoundConnectorVersions[slug] = append(unfoundConnectorVersions[slug], dbcv.Version)
		}
	}

	if len(unfoundConnectorVersions) > 0 {
		fmt.Println("Following connector versions are found in DB but not in the ndc-hub")
		count := 1
		for k, v := range unfoundConnectorVersions {
			fmt.Printf("%d. %s %+v\n", count, k, v)
			count++
		}
		os.Exit(1)
		return
	}

	var connectorTarball errgroup.Group
	for _, cp := range connectorPackaging {
		versionFolder := fmt.Sprintf("assets/%s/%s/%s", cp.Namespace, cp.Name, cp.Version)
		err = os.MkdirAll(versionFolder, 0777)
		if err != nil {
			fmt.Println("error creating folder:", versionFolder, err)
			os.Exit(1)
		}

		connectorTarball.Go(func() error {
			var err error
			tarballPath := filepath.Join(versionFolder, "connector-definition.tar.gz")

			sha, _ := getSHAIfFileExists(tarballPath)
			if sha == cp.Checksum.Value {
				fmt.Println("checksum matched, so using an existing copy: ", tarballPath)
				return nil
			}

			defer func() {
				if err != nil {
					fmt.Println("error while creating: ", tarballPath)
					return
				}
				sha, _ := getSHAIfFileExists(tarballPath)
				fmt.Printf("successfully wrote: %s (sha256: %s) \n", tarballPath, sha)
			}()

			outFile, err := os.Create(tarballPath)
			if err != nil {
				return err
			}
			defer outFile.Close()

			log.Println("starting download: ", cp.URI)
			resp, err := http.Get(cp.URI)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("error downloading: status code %d", resp.StatusCode)
			}

			_, err = io.Copy(outFile, resp.Body)
			if err != nil {
				return err
			}
			return nil
		})
	}

	err = connectorTarball.Wait()
	if err != nil {
		fmt.Println("error writing connector tarball", err)
		os.Exit(1)
	}
}

func getSHAIfFileExists(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	_, err = io.Copy(hash, file)
	if err != nil {
		return "", err
	}

	checksum := hash.Sum(nil)
	return fmt.Sprintf("%x", checksum), nil
}

type Index struct {
	TotalConnectors   int                 `json:"total_connectors"`
	Connectors        []Metadata          `json:"connectors"`
	ConnectorVersions map[string][]string `json:"connector_versions"`
}

type Metadata struct {
	Namespace     string `json:"namespace"`
	Name          string `json:"name"`
	LatestVersion string `json:"latest_version"`
}

func getConnectorMetadata(path string) (*Metadata, error) {
	if strings.Contains(path, "aliased_connectors") {
		// It should be safe to ignore aliased_connectors
		// as their slug does not in the connector init process
		return nil, nil
	}

	metadataContent, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var metadata struct {
		Overview struct {
			Namespace     string `json:"namespace"`
			LatestVersion string `json:"latest_version"`
		} `json:"overview"`
	}
	err = json.Unmarshal(metadataContent, &metadata)
	if err != nil {
		return nil, err
	}

	return &Metadata{
		Namespace:     metadata.Overview.Namespace,
		Name:          filepath.Base(filepath.Dir(path)),
		LatestVersion: metadata.Overview.LatestVersion,
	}, nil
}

type Checksum struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type Source struct {
	Hash string `json:"hash"`
}

type ConnectorPackaging struct {
	Namespace string `json:"-"`
	Name      string `json:"-"`

	Version  string   `json:"version"`
	URI      string   `json:"uri"`
	Checksum Checksum `json:"checksum"`
	Source   Source   `json:"source"`
}

func getConnectorPackaging(path string) (*ConnectorPackaging, error) {
	if strings.Contains(path, "aliased_connectors") {
		// It should be safe to ignore aliased_connectors
		// as their slug does not in the connector init process
		return nil, nil
	}

	// path looks like this: /some/folder/ndc-hub/registry/hasura/turso/releases/v0.1.0/connector-packaging.json
	versionFolder := filepath.Dir(path)
	releasesFolder := filepath.Dir(versionFolder)
	connectorFolder := filepath.Dir(releasesFolder)
	namespaceFolder := filepath.Dir(connectorFolder)

	connectorPackagingContent, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var connectorPackaging ConnectorPackaging
	err = json.Unmarshal(connectorPackagingContent, &connectorPackaging)
	if err != nil {
		return nil, err
	}

	// TODO: remove following block after standardizing in ndc-hub
	if !strings.HasPrefix(connectorPackaging.Version, "v") {
		connectorPackaging.Version = "v" + connectorPackaging.Version
	}

	connectorPackaging.Namespace = filepath.Base(namespaceFolder)
	connectorPackaging.Name = filepath.Base(connectorFolder)
	return &connectorPackaging, nil
}
