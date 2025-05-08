package utils

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Modified from: https://github.com/golang/build/blob/master/internal/untar/untar.go

func Untar(r io.Reader, dir string) (err error) {
	// Create tar reader
	tr := tar.NewReader(r)

	// Extract each file
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break // end of tar archive
		}
		if err != nil {
			return err
		}

		target := filepath.Join(dir, header.Name)

		// Check the file type
		switch header.Typeflag {
		case tar.TypeDir:
			// Create directory if it doesn’t exist
			if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
				return err
			}
		case tar.TypeReg:

			// check if parent dirs exist
			parentDir := filepath.Dir(target)
			err := os.MkdirAll(parentDir, os.ModePerm) // os.ModePerm = 0777
			if err != nil {
				return fmt.Errorf("failed to create parent directories: %w", err)
			}

			// Create file
			file, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			defer file.Close()

			// Copy file contents
			if _, err := io.Copy(file, tr); err != nil {
				return err
			}
		}
	}
	return nil
}
