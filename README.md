Bitcask
----

Bitcask is a log-structured fast KV store.

This project is an implementation of Bitcask written in Go.

[Bitcask intro here](https://riak.com/assets/bitcask-intro.pdf)

## Example

```go
package main

import (
	"github.com/qichengzx/bitcask"
	"log"
)

func main() {
	d, err := bitcask.New("your/path/here")
	if err != nil {
		log.Fatal(err)
	}
	defer d.Close()

	d.Put([]byte("bitcask"), []byte("bitcask is a log-structured fast KV store"))
	v, _ := d.Get([]byte("bitcask"))
	log.Println(string(v))
}
```