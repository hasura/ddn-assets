package asset

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	AssetFolderPath = "assets"
)

var (
	DownloadsFolderPath = filepath.Join(AssetFolderPath, "downloads")
	ExtractsFolderPath  = filepath.Join(AssetFolderPath, "extracts")
	IndexJsonPath       = filepath.Join(AssetFolderPath, "index.json")
)

func ConnectorVersionFolderForDownload(namespace, name, version string) string {
	return filepath.Join(DownloadsFolderPath, namespace, name, version)
}

func ConnectorTarballDownloadPath(namespace, name, version string) string {
	return filepath.Join(ConnectorVersionFolderForDownload(namespace, name, version), "connector-definition.tar.gz")
}

func ConnectorVersionFolderForExtracting(namespace, name, version string) string {
	return filepath.Join(ExtractsFolderPath, namespace, name, version)
}

type Index struct {
	TotalConnectors   int                 `json:"total_connectors"`
	Connectors        []Connector         `json:"connectors"`
	ConnectorVersions map[string][]string `json:"connector_versions"`
}

type Connector struct {
	Namespace     string `json:"namespace"`
	Name          string `json:"name"`
	LatestVersion string `json:"latest_version"`
}

func WriteIndexJSON(index *Index) error {
	indexJson, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return fmt.Errorf("error while marshalling index json")
	}

	err = os.WriteFile(IndexJsonPath, indexJson, 0644)
	if err != nil {
		return fmt.Errorf("error writing %s: %s", IndexJsonPath, err)
	}

	return nil
}
