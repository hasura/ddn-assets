package asset

import "gopkg.in/yaml.v3"

// CLI plugins are packaged according to https://github.com/hasura/ndc-hub/blob/main/rfcs/0011-cli-and-connector-packaging.md

type ConnectorMetadataYAML struct {
	Version string `yaml:"version"`
}

type BinaryCliPluginDefinition struct {
}

type BinaryInlineCliPluginDefinition struct {
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
