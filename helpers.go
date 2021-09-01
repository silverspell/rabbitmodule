package rabbitmodule

import "fmt"

func failOnError(err error, msg string) {
	if err != nil {
		s := fmt.Errorf("%s: %s", msg, err)
		fmt.Printf("%s\n", s)
		panic(s)
	}
}
