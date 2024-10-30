package cmd

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/hasura/ddn-assets/internal/asset"
	"github.com/hasura/ddn-assets/internal/ndchub"
	"github.com/spf13/cobra"
)

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate assets",
	Run: func(cmd *cobra.Command, args []string) {
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

		var connectors []asset.Connector
		var connectorPackaging []ndchub.ConnectorPackaging
		err = filepath.WalkDir(registryFolder, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if filepath.Base(path) == ndchub.MetadataJSON {
				metadata, err := getConnectorMetadata(path)
				if err != nil {
					return err
				}
				if metadata != nil {
					connectors = append(connectors, *metadata)
				}
			}

			if filepath.Base(path) == ndchub.ConnectorPackagingJSON {
				cp, err := ndchub.GetConnectorPackaging(path)
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
			connectorVersions[slug] = append(connectorVersions[slug], cp.Version)
		}

		err = asset.WriteIndexJSON(&asset.Index{
			TotalConnectors:   len(connectors),
			Connectors:        connectors,
			ConnectorVersions: connectorVersions,
		})
		if err != nil {
			fmt.Println("error writing index.json", err)
			os.Exit(1)
			return
		}

		if err = ndchub.DownloadConnectorTarballs(connectorPackaging); err != nil {
			fmt.Println("error downloading connector tarball", err)
			os.Exit(1)
		}

		if err = ndchub.ExtractConnectorTarballs(connectorPackaging); err != nil {
			fmt.Println("error extracting connector tarballs", err)
		}
	},
}

func getConnectorMetadata(path string) (*asset.Connector, error) {
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

	return &asset.Connector{
		Namespace:     metadata.Overview.Namespace,
		Name:          filepath.Base(filepath.Dir(path)),
		LatestVersion: metadata.Overview.LatestVersion,
	}, nil
}
