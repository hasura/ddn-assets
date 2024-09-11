package main

import (
	"fmt"
	"os"
)

func main() {
	ndcHubGitRepoFilePath := os.Getenv("NDC_HUB_GIT_REPO_FILE_PATH")
	if ndcHubGitRepoFilePath == "" {
		fmt.Println("please set a value for NDC_HUB_GIT_REPO_FILE_PATH env var")
		os.Exit(1)
		return
	}
}
