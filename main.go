package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const (
	MetadataJSON = "metadata.json"
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

	var connectorMetadata []Metadata
	err = filepath.WalkDir(registryFolder, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if filepath.Base(path) == MetadataJSON {
			metadata, err := getConnectorMetadata(path)
			if err != nil {
				return err
			}
			connectorMetadata = append(connectorMetadata, *metadata)
		}

		return nil
	})
	if err != nil {
		fmt.Println("error while walking the registry folder", err)
		os.Exit(1)
		return
	}

	indexJson, err := json.MarshalIndent(Index{
		TotalConnectors: len(connectorMetadata),
		Connectors:      connectorMetadata,
	}, "", "  ")
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
}

type Index struct {
	TotalConnectors int        `json:"total_connectors"`
	Connectors      []Metadata `json:"connectors"`
}

type Metadata struct {
	Namespace     string `json:"namespace"`
	Name          string `json:"name"`
	LatestVersion string `json:"latest_version"`
}

func getConnectorMetadata(path string) (*Metadata, error) {
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
