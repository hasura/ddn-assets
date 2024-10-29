package asset

import "testing"

func TestParseConnectorMetadata(t *testing.T) {
	tt := []struct {
		RawYAML  []byte
		Expected *ConnectorMetadataYAML
	}{
		{
			RawYAML: []byte(`version: v1`),
			Expected: &ConnectorMetadataYAML{
				Version: "v1",
			},
		},
	}

	for _, tc := range tt {
		connMetadata, err := ParseConnectorMetadata(tc.RawYAML)
		if err != nil {
			t.Error(err)
			continue
		}
		if *connMetadata != *tc.Expected {
			t.Errorf("expected %+v, got %+v", connMetadata, tc.Expected)
			continue
		}
	}
}
