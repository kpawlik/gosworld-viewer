package gosworldviewer

// Acp holds I/O buffer to communicate with Magik ACP
type IAcp interface {
	Flush()
	Write([]byte)
	PutBool(bool)
	PutUbyte(uint8)
	PutByte(int8)
	PutUshort(uint16)
	PutShort(int16)
	PutUint(uint32)
	PutInt(int32)
	PutUlong(uint64)
	PutLong(int64)
	PutShortFloat(float32)
	PutFloat(float64)
	PutString(string)
	ReadNumber(interface{})
	GetBool() bool
	GetUbyte() int
	GetByte() int
	GetUshort() int
	GetShort() int
	GetUint() int
	GetInt() int
	GetUlong() uint64
	GetLong() int64
	GetShortFloat() float32
	GetFloat() float64
	GetString() string
	GetCoord() [2]float64
	VerifyConnection(string) bool
	EstablishProtocol(int, int) bool
	Connect(string, int, int) error
	Put(string, interface{}) error
	Get(string) (interface{}, *AcpErr)
	GetType() string
}
