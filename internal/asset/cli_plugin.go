package asset

import (
	"os"

	"github.com/hasura/ddn-assets/internal/ndchub"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

// CLI plugins are packaged according to https://github.com/hasura/ndc-hub/blob/main/rfcs/0011-cli-and-connector-packaging.md

type ConnectorMetadataYAML struct {
	Version   string              `yaml:"version"`
	CLIPlugin CLIPluginDefinition `yaml:"cliPlugin"`
}

func (cmy *ConnectorMetadataYAML) UnmarshalYAML(value *yaml.Node) error {
	var temp struct {
		Version   string `yaml:"version"`
		CLIPlugin struct {
			Type                              CLIPluginType `yaml:"type"`
			DockerCLIPluginDefinition         `yaml:",inline"`
			BinaryInlineCLIPluginDefinition   `yaml:",inline"`
			BinaryExternalCLIPluginDefinition `yaml:",inline"`
		} `yaml:"cliPlugin"`
	}
	if err := value.Decode(&temp); err != nil {
		return err
	}

	cmy.Version = temp.Version
	switch temp.CLIPlugin.Type {
	case Docker:
		cmy.CLIPlugin = &temp.CLIPlugin.DockerCLIPluginDefinition
	case BinaryInline:
		cmy.CLIPlugin = &temp.CLIPlugin.BinaryInlineCLIPluginDefinition
	case Binary:
		fallthrough
	default:
		cmy.CLIPlugin = &temp.CLIPlugin.BinaryExternalCLIPluginDefinition
	}

	return nil
}

type CLIPluginType string

var (
	Binary       CLIPluginType = "Binary"
	BinaryInline CLIPluginType = "BinaryInline"
	Docker       CLIPluginType = "Docker"
)

type CLIPluginDefinition interface {
	GetType() CLIPluginType
}

type BinaryExternalCLIPluginDefinition struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
}

type BinaryInlineCLIPluginDefinition struct {
	Platforms []BinaryCLIPluginPlatform `yaml:"platforms"`
}

type DockerCLIPluginDefinition struct {
	DockerImage string `yaml:"dockerImage"`
}

func (*BinaryExternalCLIPluginDefinition) GetType() CLIPluginType {
	return Binary
}

func (*BinaryInlineCLIPluginDefinition) GetType() CLIPluginType {
	return BinaryInline
}

func (*DockerCLIPluginDefinition) GetType() CLIPluginType {
	return Docker
}

type BinaryCLIPluginPlatform struct {
	Selector string
	URI      string
	SHA256   string
	Bin      string
}

func ApplyCLIPluginTransform(connPkgs []ndchub.ConnectorPackaging) error {
	var transform errgroup.Group
	for _, cp := range connPkgs {
		transform.Go(func() error {
			connMetadataFilePath := connectorVersionFolderForExtracting(cp.Namespace, cp.Name, cp.Version)

			data, err := os.ReadFile(connMetadataFilePath)
			if err != nil {
				return err
			}

			var connMetadata ConnectorMetadataYAML
			err = yaml.Unmarshal(data, &connMetadata)
			if err != nil {
				return err
			}

			if cliPlugin, ok := connMetadata.CLIPlugin.(*BinaryInlineCLIPluginDefinition); ok {
				for range cliPlugin.Platforms {

				}
			}

			return nil
		})
	}
	return transform.Wait()
}
