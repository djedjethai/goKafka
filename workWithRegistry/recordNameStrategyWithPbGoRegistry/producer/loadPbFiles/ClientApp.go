package main

import (

	// "fmt"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

const (
	// PersonPbGo = "../producerRun/api/v1/name/Person.pb.go"
	pathToSave  = "api/v1/name"
	personPbGo  = "Person.pb.go"
	addressPbGo = "Address.pb.go"

	serviceToSave  = "producerRun"
	schemaRegistry = "http://localhost:4000"
	version        = "v1"
	packageName    = "name"
	schemaPerson   = "person"
	schemaAddress  = "address"
)

func writePbGO(r *http.Response, pathToWrite string) {
	file, err := os.Create(pathToWrite)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	_, err = io.Copy(file, r.Body)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("pb.go installed")
}

func main() {
	// Get the current working directory
	currentDir, err := os.Getwd()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	personPathToWrite := filepath.Join(currentDir, "..", serviceToSave, pathToSave, personPbGo)

	log.Println("see the path to write: ", personPathToWrite)

	response, err := http.Get(fmt.Sprintf("%s/%s/%s/%s", schemaRegistry, version, packageName, schemaPerson))
	if err != nil {
		log.Fatal(err)
	}
	defer response.Body.Close()

	writePbGO(response, personPathToWrite)

	// content, err := ioutil.ReadAll(response.Body)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// // err = ioutil.WriteFile(PersonPbGo, content, 0644)
	// err = ioutil.WriteFile(pathToWrite, content, 0644)
	// if err != nil {
	// 	log.Fatal(err)
	// }
}
