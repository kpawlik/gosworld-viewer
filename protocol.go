package goworld

import (
	"log"
	"strings"
)

// BodyElement is a type which is a part of JSON reposnse
type BodyElement map[string]interface{}

// Body list of BodyElemnts. JSON response object.
type Body []BodyElement

// Response struct
// Body - result map (field, value) to json
type StandardResponse struct {
	Body  Body
	Error error
}

func (r *StandardResponse) GetError() error {
	return r.Error
}

func (r *StandardResponse) GetBody() interface{} {
	return r.Body
}

//
// Request struct
type StandardRequest struct {
	Path     string
	Protocol *ProtocolConf
}

// ListObjectsFields returns response object from worker
// Demo protocol method. Returns list of fields from objects.
// All data are converted to strings
func (w *Worker) ListObjectsFields(request *StandardRequest, resp *StandardResponse) error {
	var (
		bodyElem BodyElement
	)
	defer func() {
		if r := recover(); r != nil {
			log.Panicf("PANIC in method ListObjectsFields: %v\n", r.(error))
		}
	}()
	// send protocol name to ACP
	acp.PutString(request.Protocol.Name)
	// send path
	acp.PutString(request.Path)
	// get status
	if err := w.checkAcpStatus(); err != nil {
		resp.Error = err
		return nil
	}
	noOfRecs := acp.GetUint()
	noOfFields := acp.GetUint()
	body := make(Body, 0, noOfRecs)
	for i := 0; i < noOfRecs; i++ {
		bodyElem = make(BodyElement)
		for j := 0; j < noOfFields; j++ {
			fieldName := acp.GetString()
			fieldValue := acp.GetString()
			bodyElem[fieldName] = fieldValue
		}
		body = append(body, bodyElem)
	}
	resp.Body = body
	return nil
}

// Custom handles communication defined by custom protocol in config file
func (w *Worker) Custom(request *StandardRequest, resp *StandardResponse) (err error) {
	var (
		bodyElem BodyElement
		acpErr   *AcpErr
	)
	protocol := request.Protocol
	pathParams := strings.Split(request.Path, "/")
	if len(pathParams) != len(protocol.Params) {
		resp.Error = NewAcpErr("Wrong number of parameters.")
		return nil
	}
	// send protocol name to ACP
	acp.PutString(protocol.Name)
	// Send all param name and value to ACP
	if acpErr = w.sendParameters(protocol, pathParams); acpErr != nil {
		resp.Error = acpErr
		return
	}
	// get status
	if acpErr = w.checkAcpStatus(); acpErr != nil {
		resp.Error = acpErr
		return
	}
	// Get Recods
	noOfRecs := acp.GetUint()
	body := make(Body, 0, noOfRecs)

	resultFieldsDef := protocol.Results
	for i := 0; i < noOfRecs; i++ {
		if bodyElem, acpErr = w.getFields(resultFieldsDef); acpErr != nil {
			resp.Error = acpErr
			return
		}
		body = append(body, bodyElem)
	}
	resp.Body = body
	return
}
