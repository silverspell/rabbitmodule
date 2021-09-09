package rabbitmodule

func failOnError(err error, msg string) {
	if err != nil {
		Sugar.Infof("%s: %s", msg, err)
		panic(err)
	}
}
