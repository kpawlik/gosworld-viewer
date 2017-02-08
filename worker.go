package gosworldviewer

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

var (
	acp IAcp
)

const (
	// SucessStatus value which should be returned from ACP if no error ocure
	SucessStatus = 0
)

// StartWorker start register structs and start RPC server
func StartWorker(config *Config, name string, mode WorkMode) {
	defer func() {
		if err := recover(); err != nil {
			log.Panic(err)
		}
	}()
	workerDef := config.GetWorkerDef(name)
	if workerDef == nil {
		log.Fatalf("Error starting worker. No definition in config for name : %s\n", name)
	}
	switch mode {
	case NormalMode:
		acp = NewAcp(name)
	case TestMode:
		acp = NewTestAcp(name)
	default:
		return
	}

	if err := acp.Connect(name, 0, 1); err != nil {
		log.Panicf("ACP Connection error: %v\n", err)
	}

	// register worker for RPC server
	worker := &Worker{
		Port:       workerDef.Port,
		WorkerName: workerDef.Name}
	rpc.Register(worker)
	rpc.HandleHTTP()
	// start listening for requests from HTTP server
	if listener, err := net.Listen("tcp", portNo(worker.Port)); err != nil {
		log.Panicf("Start worker error on port %d. Error: %v\n", worker.Port, err)
	} else {
		log.Printf("Worker started at port: %d\n", workerDef.Port)
		log.Fatalf("RPC SERVER ERROR! %s\n", http.Serve(listener, nil))
	}
}

// Worker type to wrap RPC communication
type Worker struct {
	Port       int
	WorkerName string
	Protocol   *ProtocolConf
}

// chackAcpStatus checks if ACP returns valid sucess status. If not then read error message and create error object
func (w *Worker) checkAcpStatus() *AcpErr {
	status := acp.GetUbyte()
	if status != SucessStatus {
		return NewAcpErr(fmt.Sprintf("Status error from ACP: Code %d. Message: %s", status, acp.GetString()))
	}
	return nil
}

//sendParameters sends list of parameters from request to ACP
func (w *Worker) sendParameters(protocol *ProtocolConf, pathParams []string) (err *AcpErr) {
	for i, paramDef := range protocol.Params {
		if err = w.sendParameter(paramDef, pathParams[i]); err != nil {
			return
		}
	}
	return
}

//sendParameter convert and send string parameter to ACP
func (w *Worker) sendParameter(paramDef *ParameterConf, value string) (acpErr *AcpErr) {
	var (
		param interface{}
		err   error
	)
	if param, err = ParseStringParam(value, paramDef.Type); err != nil {
		acpErr = NewAcpErr(fmt.Sprint(err))
		return
	}
	if err = acp.Put(paramDef.Type, param); err != nil {
		acpErr = NewAcpErr(fmt.Sprint(err))
		return
	}
	return
}

// getFields reads list of fields from ACP to map
func (w *Worker) getFields(resultFieldsDef []*ParameterConf) (bodyElem BodyElement, acpErr *AcpErr) {
	var (
		value interface{}
	)
	bodyElem = make(BodyElement, len(resultFieldsDef))
	for _, fieldDef := range resultFieldsDef {
		if value, acpErr = acp.Get(fieldDef.Type); acpErr != nil {
			return
		}
		bodyElem[fieldDef.Name] = value
	}
	return
}
