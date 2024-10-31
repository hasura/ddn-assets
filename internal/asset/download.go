package asset

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/hasura/ddn-assets/internal/ndchub"
	"golang.org/x/sync/errgroup"
)

func DownloadConnectorTarballs(connPkgs []ndchub.ConnectorPackaging) error {
	var connectorTarball errgroup.Group
	for _, cp := range connPkgs {
		versionFolder := connectorVersionFolderForDownload(cp.Namespace, cp.Name, cp.Version)
		err := os.MkdirAll(versionFolder, 0777)
		if err != nil {
			return fmt.Errorf("error creating folder: %s %w", versionFolder, err)
		}

		connectorTarball.Go(func() error {
			var err error
			tarballPath := connectorTarballDownloadPath(cp.Namespace, cp.Name, cp.Version)

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
