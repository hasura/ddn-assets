package asset

import (
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

	plgType := temp.CLIPlugin.Type
	if !plgType.Valid() {
		plgType = Binary
	}

	switch plgType {
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

func (c CLIPluginType) Valid() bool {
	switch c {
	case Binary, BinaryInline, Docker:
		return true
	default:
		return false
	}
}

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

func ParseConnectorMetadata(data []byte) (*ConnectorMetadataYAML, error) {
	var connMetadata ConnectorMetadataYAML
	err := yaml.Unmarshal(data, &connMetadata)
	if err != nil {
		return nil, err
	}
	return &connMetadata, nil
}

func TransformCLIPlugin(cmy *ConnectorMetadataYAML) error {
	return nil
}
