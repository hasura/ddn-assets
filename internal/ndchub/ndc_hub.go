package ndchub

const (
	MetadataJSON           = "metadata.json"
	ConnectorPackagingJSON = "connector-packaging.json"
)

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
