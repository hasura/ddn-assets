package ndchub

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/hasura/ddn-assets/internal/asset"
	"golang.org/x/sync/errgroup"
)

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

func GetConnectorPackaging(path string) (*ConnectorPackaging, error) {
	if strings.Contains(path, "aliased_connectors") {
		// It should be safe to ignore aliased_connectors
		// as their slug does not in the connector init process
		return nil, nil
	}

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

func DownloadConnectorTarballs(connPkgs []ConnectorPackaging) error {
	var connectorTarball errgroup.Group
	for _, cp := range connPkgs {
		versionFolder := asset.ConnectorVersionFolderForDownload(cp.Namespace, cp.Name, cp.Version)
		err := os.MkdirAll(versionFolder, 0777)
		if err != nil {
			return fmt.Errorf("error creating folder: %s %w", versionFolder, err)
		}

		connectorTarball.Go(func() error {
			var err error
			tarballPath := asset.ConnectorTarballDownloadPath(cp.Namespace, cp.Name, cp.Version)

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
				sha, _ := getSHAIfFileExists(tarballPath)
				fmt.Printf("successfully wrote: %s (sha256: %s) \n", tarballPath, sha)
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

	return connectorTarball.Wait()
}

func ExtractConnectorTarballs(connPkgs []ConnectorPackaging) error {
	var extract errgroup.Group
	for _, cp := range connPkgs {
		extract.Go(func() error {
			srcTarball := asset.ConnectorTarballDownloadPath(cp.Namespace, cp.Name, cp.Version)

			file, err := os.Open(srcTarball)
			if err != nil {
				return fmt.Errorf("could not open file: %v", err)
			}
			defer file.Close()

			gzReader, err := gzip.NewReader(file)
			if err != nil {
				return fmt.Errorf("could not create gzip reader: %v", err)
			}
			defer gzReader.Close()

			tarReader := tar.NewReader(gzReader)

			for {
				header, err := tarReader.Next()
				if err == io.EOF {
					break // end of archive
				}
				if err != nil {
					return fmt.Errorf("could not read tar header: %v", err)
				}

				// TODO: make use of the header
				fmt.Println(header)
			}

			return nil
		})
	}
	return extract.Wait()
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
