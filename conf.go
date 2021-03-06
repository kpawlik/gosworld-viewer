package gosworldviewer

import (
	"encoding/json"
	"io/ioutil"
)

// WorkMode - enumerator with serwer mode types
type WorkMode int

const (
	// Version number
	Version = "0.9"
	// UnknownMode is unrecognized mode
	UnknownMode WorkMode = iota
	// NormalMode is production mode
	NormalMode
	// TestMode  to test communication between Acp and worker
	TestMode
)

var (
	modes = map[WorkMode]string{NormalMode: "normal", TestMode: "test"}

	incomeTypes = map[int]string{
		1:  "boolean",
		2:  "unsigned_byte",
		3:  "signed_byte",
		4:  "unsigned_short",
		5:  "signed_short",
		6:  "unsigned_int",
		7:  "signed_int",
		8:  "unsigned_long",
		9:  "signed_long",
		10: "short_float",
		11: "float",
		12: "chars",
		13: "point",
		14: "line",
		15: "polygon",
	}
)

func (w WorkMode) String() string {
	return modes[w]
}

//WorkModeFromString converts string value to enumerator if not found then UnknownMode returns
func WorkModeFromString(mode string) WorkMode {
	for val, m := range modes {
		if m == mode {
			return val
		}
	}
	return UnknownMode
}

// ServerConf server configuration
type ServerConf struct {
	Port      int
	Protocols []*ProtocolConf
}

// WorkerConf wrkers configuration
type WorkerConf struct {
	Host string
	Name string
	Port int
}

// ParameterConf is parameter name and type definition. Type could take values string, unsigned_int, signed_int, etc
type ParameterConf struct {
	Name string
	Type string
}

// ProtocolConf is a definition of protocol. Contains name, list of entry parameters and list of results fields
type ProtocolConf struct {
	Name    string
	Enabled bool
	Params  []*ParameterConf
	Results []*ParameterConf
}

// Config application configuration structure
type Config struct {
	Server  ServerConf
	Workers []*WorkerConf
}

// GetProtocolDef returns Protocol definition of nil if not found
func (c Config) GetProtocolDef(name string) *ProtocolConf {
	for _, prot := range c.Server.Protocols {
		if prot.Name == name {
			return prot
		}
	}
	return nil
}

// GetWorkerDef returns worker connection definition of nil if not found
func (c Config) GetWorkerDef(name string) *WorkerConf {
	for _, worker := range c.Workers {
		if worker.Name == name {
			return worker
		}
	}
	return nil
}

// ReadConf reads and decodes JSON from file
func ReadConf(filePath string) (conf *Config, err error) {
	var data []byte
	if data, err = ioutil.ReadFile(filePath); err == nil {
		conf, err = unmarshal(data)
	}
	return
}

func unmarshal(data []byte) (conf *Config, err error) {
	err = json.Unmarshal(data, &conf)
	return
}
