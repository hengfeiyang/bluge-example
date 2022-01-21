package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/safeie/bluge-example/aggregation"
)

func main() {
	config := bluge.DefaultConfig("./data/myindex")
	writer, err := bluge.OpenWriter(config)
	if err != nil {
		log.Fatalln(err)
	}
	defer writer.Close()

	// err = document.Creates(writer)
	// if err != nil {
	// 	log.Println(err)
	// }

	err = aggregation.Terms(writer)
	if err != nil {
		log.Println(err)
	}

	// test date

	s := `{"key":"xxx","time":"2022-01-21T09:22:50.604Z","int":1233,"float":123.111,"bool": false}`
	xx := make(map[string]interface{})
	err = json.Unmarshal([]byte(s), &xx)
	fmt.Println(err, xx)
	for k, v := range xx {
		fmt.Print(k, v)
		switch v.(type) {
		case string:
			fmt.Print("string")
		case int:
			fmt.Print("int")
		case int64:
			fmt.Print("int64")
		case float64:
			fmt.Print("float64")
		case time.Time:
			fmt.Print("time")
		case bool:
			fmt.Print("boolean")
		}
		fmt.Println("")
	}

}
