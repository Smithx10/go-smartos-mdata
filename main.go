package main

import (
	"fmt"
	"smartos-mdata/mdata"
)

func main() {
	config := mdata.DefaultClientConfig()
	client, err := mdata.NewMetadataClient(config)
	if err != nil {
		fmt.Println("Error creating client:", err)
		return
	}
	defer client.Close()

	response, err := client.Get("sdc:uuid")
	if err != nil {
		fmt.Println("Get error:", err)
		return
	}
	fmt.Println(response)

	s, err := client.Keys()
	fmt.Println(s, err)

	s, err = client.Put("foo", "bar")
	fmt.Println(s, err)

	s, err = client.Get("foo")
	fmt.Println(s, err)

	s, err = client.Keys()
	fmt.Println(s, err)

	s, err = client.Delete("foo")
	fmt.Println(s, err)

	s, err = client.Keys()
	fmt.Println(s, err)
}
