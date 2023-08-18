package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	// "os"
	"path/filepath"
	"strings"
)

const (
	port int = 4000
)

func main() {
	http.HandleFunc("/name/", fileProvider)

	addr := fmt.Sprintf(":%d", port)

	fmt.Printf("Server listening on port %d...\n", port)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		fmt.Println("Error:", err)
	}

}

func fileProvider(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/name/")

	fmt.Println("the name: ", name)

	// Get the path of the executable
	// exePath, err := os.Executable()
	// if err != nil {
	// 	http.Error(w, "Error determining executable path", http.StatusInternalServerError)
	// 	return
	// }

	// Construct the full file path using the executable path
	// dirPath := filepath.Dir(exePath)

	if name == "person" {
		// Read the content of the file at "mypath/myfile.txt"
		filePath := filepath.Join("./files", "Person.pb.go")

		fmt.Println("the path: ", filePath)

		fileContent, err := ioutil.ReadFile(filePath)
		if err != nil {
			http.Error(w, "Error reading file", http.StatusInternalServerError)
			return
		}

		// Respond with the content of the file
		fmt.Fprintf(w, "%s", fileContent)
	}

}
