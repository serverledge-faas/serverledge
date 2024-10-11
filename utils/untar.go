package utils

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
	"strings"
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

		// Strip the first component from the header name
		components := strings.SplitN(header.Name, "/", 2)
		var target string

		if len(components) > 1 {
			target = filepath.Join(dir, components[1]) // Skip the first component
		} else {
			target = filepath.Join(dir, header.Name) // No components to strip
		}

		// Check the file type
		switch header.Typeflag {
		case tar.TypeDir:
			// Create directory if it doesn’t exist
			if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
				return err
			}
		case tar.TypeReg:
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
