package producer

type Producer interface {
	Connect(interface{}) error
	Send(interface{}) error
}
