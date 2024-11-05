package asset

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hasura/ddn-assets/internal/ndchub"
	"golang.org/x/sync/errgroup"
)

func OutputConnectorTarballs(connPkgs []ndchub.ConnectorPackaging) error {
	var targz errgroup.Group
	for _, cp := range connPkgs {
		targz.Go(func() error {
			destFolder := outputConnectorVersionFolder(cp.Namespace, cp.Name, cp.Version)
			err := os.MkdirAll(destFolder, 0777)
			if err != nil {
				return fmt.Errorf("error creating folder: %s %w", destFolder, err)
			}

			return tarGzFolder(
				extractedConnectorVersionFolder(cp.Namespace, cp.Name, cp.Version),
				connectorTarballOutputPath(cp.Namespace, cp.Name, cp.Version),
			)
		})
	}
	return targz.Wait()
}

// tarGzFolder takes a source directory and creates a .tar.gz file at the destination path,
// with files and folders at the root of the archive.
func tarGzFolder(sourceDir, destFile string) error {
	outFile, err := os.Create(destFile)
	if err != nil {
		return fmt.Errorf("could not create tar.gz file: %v", err)
	}
	defer outFile.Close()

	gzWriter := gzip.NewWriter(outFile)
	defer gzWriter.Close()
	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	err = filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return err
		}

		// Adjust the header name to ensure relative paths within the archive
		header.Name, err = filepath.Rel(filepath.Dir(sourceDir+"/"), path)
		if err != nil {
			return err
		}

		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		// If it's a directory, no need to copy content
		if info.IsDir() {
			return nil
		}

		// Open the file and copy its contents into the archive
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		if _, err := io.Copy(tarWriter, file); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("error walking source directory: %v", err)
	}

	return nil
}
