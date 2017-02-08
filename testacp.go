package gosworldviewer

import (
	"fmt"
	"log"
	"math/rand"
)

// Acp holds I/O buffer to communicate with Magik ACP
type TestAcp struct {
	Name string
}

// NewAcp creates and init new Acp with name
func NewTestAcp(name string) *TestAcp {
	return &TestAcp{Name: name}
}

// Flush send buffer data
func (a *TestAcp) Flush() {

}

// Write writes buffer to Acp output
func (a *TestAcp) Write(buf []byte) {

}

// PutBool sends boolean value to Acp output
func (a *TestAcp) PutBool(b bool) {

}

// PutUbyte sends unsigned byte value to Acp output
func (a *TestAcp) PutUbyte(value uint8) {
}

// PutByte sends  byte value to Acp output
func (a *TestAcp) PutByte(value int8) {
}

// PutUshort sends unsigned short value to Acp output
func (a *TestAcp) PutUshort(value uint16) {
}

// PutShort sends short value to Acp output
func (a *TestAcp) PutShort(value int16) {
}

// PutUint sends int value to Acp output
func (a *TestAcp) PutUint(value uint32) {
}

// PutInt sends int value to Acp output
func (a *TestAcp) PutInt(value int32) {
}

// PutUlong sends unsigned long value to Acp output
func (a *TestAcp) PutUlong(value uint64) {

}

// PutLong sends long value to Acp output
func (a *TestAcp) PutLong(value int64) {
}

// PutShortFloat sends short float value to Acp output
func (a *TestAcp) PutShortFloat(value float32) {
}

// PutFloat sends float value to Acp output
func (a *TestAcp) PutFloat(value float64) {
}

// PutString sends string value to Acp output
func (a *TestAcp) PutString(s string) {
	log.Printf("Put string %s", s)
}

// ReadNumber reads number from Acp input
func (a *TestAcp) ReadNumber(data interface{}) {
	data = rand.Int()
}

// GetBool reads boolean value from Acp input
func (a *TestAcp) GetBool() bool {
	return true
}

// GetUbyte reads unsigned byte from Acp input
func (a *TestAcp) GetUbyte() int {
	return 0
}

// GetByte reads byte from Acp input
func (a *TestAcp) GetByte() int {
	return rand.Intn(10)
}

// GetUshort reads unsigned short from Acp input
func (a *TestAcp) GetUshort() int {
	var res uint16
	a.ReadNumber(&res)
	return int(res)
}

// GetShort reads short from Acp input
func (a *TestAcp) GetShort() int {
	var res = rand.Int31n(100)
	return int(res)
}

// GetUint reads unsigned int from Acp input
func (a *TestAcp) GetUint() int {
	return rand.Intn(100)
}

// GetInt reads unsigned int from Acp input
func (a *TestAcp) GetInt() int {
	var res int32
	a.ReadNumber(&res)
	return int(res)
}

// GetUlong reads unsigned long from Acp input
func (a *TestAcp) GetUlong() uint64 {
	var res uint64
	a.ReadNumber(&res)
	return res
}

// GetLong reads long from Acp input
func (a *TestAcp) GetLong() int64 {
	var res int64
	a.ReadNumber(&res)
	return res
}

// GetShortFloat read float32 from Acp input
func (a *TestAcp) GetShortFloat() float32 {
	var res float32
	a.ReadNumber(&res)
	return res
}

// GetFloat read float64 from Acp input
func (a *TestAcp) GetFloat() float64 {
	return rand.Float64() * 10
}

// GetString reads string from Acp input
func (a *TestAcp) GetString() string {

	return "test acp"
}

func (a *TestAcp) GetCoord() [2]float64 {
	var (
		res [2]float64
	)
	res[0] = a.GetFloat()
	res[1] = a.GetFloat()
	return res

}

// VerifyConnection verify Acp process name
func (a *TestAcp) VerifyConnection(name string) bool {
	return true
}

// EstablishProtocol checks Acp protocol
func (a *TestAcp) EstablishProtocol(minProtocol, maxProtocol int) bool {
	return true
}

// Connect verify connection and protocol to Acp
func (a *TestAcp) Connect(processName string, protocolMin, protocolMax int) (err error) {
	log.Printf("test acp connect - sucess")
	return
}

// Put convert value to dataType and send this value to ACP
func (a *TestAcp) Put(dataType string, value interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
			return
		}
	}()
	switch dataType {
	case "boolean":
		ival := value.(bool)
		a.PutBool(ival)
	case "unsigned_byte":
		ival := value.(uint8)
		a.PutUbyte(ival)
	case "signed_byte":
		ival := value.(int8)
		a.PutByte(ival)
	case "unsigned_short":
		ival := value.(uint16)
		a.PutUshort(ival)
	case "signed_short":
		ival := value.(int16)
		a.PutShort(ival)
	case "unsigned_int":
		ival := value.(uint32)
		a.PutUint(ival)
	case "signed_int":
		ival := value.(int32)
		a.PutInt(ival)
	case "unsigned_long":
		ival := value.(uint64)
		a.PutUlong(ival)
	case "signed_long":
		ival := value.(int64)
		a.PutLong(ival)
	case "short_float":
		ival := value.(float32)
		a.PutShortFloat(ival)
	case "float":
		ival := value.(float64)
		a.PutFloat(ival)
	case "chars":
		ival := value.(string)
		a.PutString(ival)
	default:
		return fmt.Errorf("Unsuported data type '%s' in Put method", dataType)
	}
	return nil

}

// Get method reads dataType value from ACP
func (a *TestAcp) Get(dataType string) (value interface{}, err *AcpErr) {
	defer func() {
		if r := recover(); r != nil {
			err = NewAcpErr(fmt.Sprint(r.(error)))
			return
		}
	}()
	switch dataType {
	case "boolean":
		return a.GetBool(), nil
	case "unsigned_byte":
		return a.GetUbyte(), nil
	case "signed_byte":
		return a.GetByte(), nil
	case "unsigned_short":
		return a.GetUshort(), nil
	case "signed_short":
		return a.GetShort(), nil
	case "unsigned_int":
		return a.GetUint(), nil
	case "signed_int":
		return a.GetInt(), nil
	case "unsigned_long":
		return a.GetUlong(), nil
	case "signed_long":
		return a.GetLong(), nil
	case "short_float":
		return a.GetShortFloat(), nil
	case "float":
		return a.GetFloat(), nil
	case "chars":
		return a.GetString(), nil
	default:
		return nil, NewAcpErr(fmt.Sprintf("Unsuported data type '%s' in Get method", dataType))
	}
}

func (a *TestAcp) GetType() string {
	geomType := 13
	return incomeTypes[geomType]
}
