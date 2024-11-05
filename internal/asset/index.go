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
	OutputFolderPath    = filepath.Join(AssetFolderPath, "outputs")
	IndexJsonPath       = filepath.Join(AssetFolderPath, "index.json")

	connectorDefinitionTarballName = "connector-definition.tar.gz"
)

func connectorVersionFolderForDownload(namespace, name, version string) string {
	return filepath.Join(DownloadsFolderPath, namespace, name, version)
}

func connectorTarballDownloadPath(namespace, name, version string) string {
	return filepath.Join(connectorVersionFolderForDownload(namespace, name, version), connectorDefinitionTarballName)
}

func extractedConnectorVersionFolder(namespace, name, version string) string {
	return filepath.Join(ExtractsFolderPath, namespace, name, version)
}

func outputConnectorVersionFolder(namespace, name, version string) string {
	return filepath.Join(OutputFolderPath, namespace, name, version)
}

func connectorTarballOutputPath(namespace, name, version string) string {
	return filepath.Join(outputConnectorVersionFolder(namespace, name, version), connectorDefinitionTarballName)
}

func cliPluginFolder(namespace, name, version string) string {
	return filepath.Join(outputConnectorVersionFolder(namespace, name, version), "cli-plugins")
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
