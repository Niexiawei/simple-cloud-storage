package cloudstorage

import "fmt"

func ExampleNewClient() {
	// Supply an initialized Provider. Here a test provider supplies the SPI.
	client, err := NewClient(newMockProvider(), Config{
		Prefix:      " /apps/demo/ ",
		Concurrency: 1,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(client.Prefix())
	// Output: apps/demo
}
