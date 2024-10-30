package ndchub

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hasura/ddn-assets/internal/asset"
	"golang.org/x/sync/errgroup"
)

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

			destFolder := asset.ConnectorVersionFolderForExtracting(cp.Namespace, cp.Name, cp.Version)
			err = os.MkdirAll(destFolder, 0777)
			if err != nil {
				return fmt.Errorf("error creating folder: %s %w", destFolder, err)
			}

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

				outPath := filepath.Join(destFolder, header.Name)
				switch header.Typeflag {
				case tar.TypeDir:
					// Create the directory
					if err := os.MkdirAll(outPath, os.FileMode(header.Mode)); err != nil {
						return fmt.Errorf("could not create directory: %v", err)
					}
				case tar.TypeReg:
					// Create the file
					outFile, err := os.Create(outPath)
					if err != nil {
						return fmt.Errorf("could not create file: %v", err)
					}
					defer outFile.Close()

					// Copy the file content
					if _, err := io.Copy(outFile, tarReader); err != nil {
						return fmt.Errorf("could not write file content: %v", err)
					}

					// Set file permissions
					if err := os.Chmod(outPath, os.FileMode(header.Mode)); err != nil {
						return fmt.Errorf("could not set file permissions: %v", err)
					}
				default:
					// Handle other types if needed
					fmt.Printf("Skipping unsupported file type: %c in %s\n", header.Typeflag, header.Name)
				}
			}

			return nil
		})
	}
	return extract.Wait()
}
