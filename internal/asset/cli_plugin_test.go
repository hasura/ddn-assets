package asset

import "testing"

func TestParseConnectorMetadata(t *testing.T) {
	tt := []struct {
		Name     string
		RawYAML  []byte
		Expected *ConnectorMetadataYAML
	}{
		{
			Name: "Binary CLI plugin",
			RawYAML: []byte(
				`
version: v2
cliPlugin:
  name: ndc-postgres
  version: v1.2.0
`),
			Expected: &ConnectorMetadataYAML{
				Version: "v2",
				CLIPlugin: &BinaryExternalCLIPluginDefinition{
					Name:    "ndc-postgres",
					Version: "v1.2.0",
				},
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			connMetadata, err := ParseConnectorMetadata(tc.RawYAML)
			if err != nil {
				t.Error(err)
				return
			}

			if tc.Expected == nil && connMetadata != nil {
				t.Error("expected connector metadata to be nil")
				return
			}

			if connMetadata.Version != tc.Expected.Version {
				t.Errorf("expected version %s, got %s", tc.Expected.Version, connMetadata.Version)
				return
			}

			if connMetadata.CLIPlugin.GetType() != tc.Expected.CLIPlugin.GetType() {
				t.Errorf("expected cli plugin type %s, got %s", tc.Expected.CLIPlugin.GetType(), connMetadata.CLIPlugin.GetType())
			}

			switch expected := tc.Expected.CLIPlugin.(type) {
			case *BinaryExternalCLIPluginDefinition:
				got, ok := connMetadata.CLIPlugin.(*BinaryExternalCLIPluginDefinition)
				if !ok {
					t.Error("expected to infer plugin type to be external binary cli plugin")
					return
				}
				if got.Name != expected.Name {
					t.Errorf("expected name %s, got %s", expected.Name, got.Name)
					return
				}
				if got.Version != expected.Version {
					t.Errorf("expected version %s, got %s", expected.Version, got.Version)
					return
				}
			case *BinaryInlineCLIPluginDefinition:
			case *DockerCLIPluginDefinition:
			}
		})
	}
}
