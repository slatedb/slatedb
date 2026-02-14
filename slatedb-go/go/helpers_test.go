package slatedb_test

import (
	"os"
	"path/filepath"
)

func createEnvFile(dirPath string) (string, error) {
	envFile := filepath.Join(dirPath, ".env")
	fp, err := os.OpenFile(envFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return "", err
	}
	if _, err = fp.WriteString("CLOUD_PROVIDER=local\nLOCAL_PATH=/\n"); err != nil {
		return "", err
	}
	if err = fp.Close(); err != nil {
		return "", err
	}
	return envFile, nil
}

func cleanupEnvVariables() error {
	for _, name := range []string{
		"CLOUD_PROVIDER",
		"LOCAL_PATH",
		"SLATEDB_OBJECT_STORE_URL",
		"OBJECT_STORE_URL",
	} {
		if err := os.Unsetenv(name); err != nil {
			return err
		}
	}
	return nil
}
