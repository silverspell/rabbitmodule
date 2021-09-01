package rabbitmodule

type QueuePacket struct {
	To  string `json:"to"`
	Cmd string `json:"cmd"`
	Val string `json:"val"`
}
