package main

import (
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
			connectorMetadata = append(connectorMetadata, *metadata)
		}

		if filepath.Base(path) == ConnectorPackagingJSON {
			cp, err := getConnectorPackaging(path)
			if err != nil {
				return err
			}
			connectorPackaging = append(connectorPackaging, *cp)
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
	indexJson, err := json.MarshalIndent(Index{
		TotalConnectors:   len(connectorMetadata),
		Connectors:        connectorMetadata,
		ConnectorVersions: connectorVersions,
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
				fmt.Println("successfully wrote: ", tarballPath)
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

	connectorPackaging.Namespace = filepath.Base(namespaceFolder)
	connectorPackaging.Name = filepath.Base(connectorFolder)
	return &connectorPackaging, nil
}
