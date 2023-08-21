package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const (
	port     int = 4000
	pathToPb     = "api"

	// name package
	v1Name = "/v1/name/"
)

var currentDir string

func main() {
	// name is the serviceName,
	http.HandleFunc(v1Name, v1NameProtobuf)

	addr := fmt.Sprintf(":%d", port)

	// Get the current working directory
	var err error
	currentDir, err = os.Getwd()
	if err != nil {
		log.Fatal("Can not find the current directory: ", err)
	}

	fmt.Printf("Server listening on port %d...\n", port)
	err = http.ListenAndServe(addr, nil)
	if err != nil {
		fmt.Println("Error:", err)
	}

}

func setPathToReadFrom(versionPackageName, pbGo string) (string, error) {
	pathToReadFrom := filepath.Join(currentDir, pathToPb, versionPackageName, pbGo)
	return pathToReadFrom, nil
}

func readAndReturnProtobufGO(w http.ResponseWriter, pathToReadFrom string) {
	log.Println("The file to read from: ", pathToReadFrom)
	file, err := os.Open(pathToReadFrom)
	if err != nil {
		http.Error(w, "Error reading file", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// Get the file size
	fileInfo, err := file.Stat()
	if err != nil {
		http.Error(w, "Error reading file", http.StatusInternalServerError)
		return
	}
	fileSize := fileInfo.Size()

	// Create a buffer to read the file contents into
	bufferSize := int(fileSize) // Use the file size as the buffer size
	buffer := make([]byte, bufferSize)
	for {
		n, err := file.Read(buffer)
		if err != nil && err.Error() != "EOF" {
			http.Error(w, "Error reading file", http.StatusInternalServerError)
			return
		}
		if n == 0 {
			break
		}
		w.Write(buffer[:n])
	}

	fmt.Fprintln(w, "")

	// fileContent, err := ioutil.ReadFile(pathToReadFrom)
	// if err != nil {
	// 	http.Error(w, "Error reading file", http.StatusInternalServerError)
	// 	return
	// }

	// // Respond with the content of the file
	// fmt.Fprintf(w, "%s", fileContent)
}

func capitalize(s string) string {
	if len(s) == 0 {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func v1NameProtobuf(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, v1Name)

	fmt.Println("the name: ", name)

	switch name {
	case "person", "address":
		name = fmt.Sprintf("%s.pb.go", capitalize(name))
		pathToReadFrom, err := setPathToReadFrom(v1Name, name)
		if err != nil {
			http.Error(w, "Error to find path to read from", http.StatusInternalServerError)
		}

		fmt.Println("the path: ", pathToReadFrom)

		readAndReturnProtobufGO(w, pathToReadFrom)

	default:
		http.Error(w, "Invalid package", http.StatusNotFound)
		return
	}
}
