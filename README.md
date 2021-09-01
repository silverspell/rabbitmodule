# Rabbit Module

## Gereksinimler
- go get github.com/silverspell/rabbitmodule@v1.0.6
- AMQP_HOST isimli env değişkeni (amqp://admin:admin@localhost:5672/)

## Fanout tipi ile çalışmak 

Fanout tipi Publish edilen bir mesajın İlgili Exchange üzerinde Subscribe olmuş TÜM nodelara dağıtılmasıdır. Bir exchange üzerinde aynı mesajı farklı farklı uygulamalarınız dinliyorsa bunu tercih edin.

```go
func writer() {
    sendChan := make(chan string)
	go rabbitmodule.ConnectPublisher(sendChan, "myfanout")

	for i := 0; i < 5; i++ {
		sendChan <- "Hello"
	}
}

func reader() {
    receiveChan := make(chan string)
    go rabbitmodule.ConnectSubscriber(receiveChan, "myfanout")
	for {
		msg := <-receiveChan
		fmt.Printf("Received: %s\n", msg)
	}
}

```

## Direct tipi ile çalışmak

Direct tip bir exchange üzerinde farklı routing keyler ile mesajın yönlendirilmesidir. Severity parametresine göre log mesajlarını farklı şekilde işlemek istiyorsanız bunu tercih edin.

```go
func readerEvenDirect(who int) {
	listen := make(chan string)
	go rabbitmodule.ConnectSubscriberDirect(listen, "mydirecttopic", "numbers.even")
	for {
		msg := <-listen
		fmt.Printf("%d Received: %s\n", who, msg)
	}
}

func readerOddDirect(who int) {
	listen := make(chan string)
	go rabbitmodule.ConnectSubscriberDirect(listen, "mydirecttopic", "numbers.odd")
	for {
		msg := <-listen
		fmt.Printf("%d Received: %s\n", who, msg)
	}
}

func writerDirect() {
    sendEvenChan, sendOddChan := make(chan string), make(chan string)
    go rabbitmodule.ConnectPublisherDirect(sendChan, "mydirecttopic", "numbers.even")
    go rabbitmodule.ConnectPublisherDirect(sendChan, "mydirecttopic", "numbers.odd")
	for i := 0; i < 10; i++ {
	    if i % 2 == 0 {
		    sendEvenChan <- strconv.Itoa(i)
		} else {
		    sendOddChan <- strconv.Itoa(i)
		}
	}
}

```


## Task Queue ile çalışmak

Aynı tipte mesajları birden çok subscriber'a round-robin (sırayla) dağıtmak istiyorsanız bunu tercih edin.

```go
// Bu taskı minimum 2 instance kaldırın.
func readerTaskWorker(who int) {
	myChan := make(chan string)
	go rabbitmodule.ConnectSubscriberTaskQueue(myChan, "myqueue")
	for {
		msg := <-myChan
		fmt.Printf("%d Received: %s\n", who, msg)
	}
}

func writerTaskWorker() {
	myChan := make(chan string)
	go rabbitmodule.ConnectPublisherTaskQueue(myChan, "myqueue")
	for i := 0; i < 10; i++ {
		myChan <- strconv.Itoa(i)
	}
}
```
