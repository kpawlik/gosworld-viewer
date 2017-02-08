package gosworldviewer

import (
	//	"fmt"
	"github.com/kpawlik/geojson"
	"log"
	"net/url"
	"strings"
)

// Response struct
// Body - result map (field, value) to json
type FeaturesResponse struct {
	Body  *geojson.FeatureCollection
	Error error
}

func (r *FeaturesResponse) GetError() error {
	return r.Error
}
func (r *FeaturesResponse) GetBody() interface{} {
	return r.Body
}

func (r *FeaturesResponse) SetError(err error) {
	r.Error = err
}
func (r *FeaturesResponse) SetBody(body interface{}) {
	r.Body = body.(*geojson.FeatureCollection)
}

//
// Request struct
type FeaturesRequest struct {
	Dataset    string
	Collection string
	Values     url.Values
}

func NewFeatureReques(reqUrl *url.URL) *FeaturesRequest {
	var (
		path                string
		dataset, collection string
		err                 error
	)
	if path, err = url.QueryUnescape(reqUrl.Path[1:]); err != nil {
		log.Fatalf("Error unescape path '%s': %v\n", path, err)
	}

	if res := strings.Split(path, "/"); len(res) == 3 {
		dataset = res[1]
		collection = res[2]
	}
	values := reqUrl.Query()
	return &FeaturesRequest{Dataset: dataset,
		Collection: collection,
		Values:     values,
	}
}

func (w *Worker) Features(request *FeaturesRequest, resp *FeaturesResponse) (respErr error) {
	var (
		bb    [4]float64
		err   error
		coord geojson.Coordinate
		geom  geojson.Geometry
		ok    bool
	)
	defer func() {
		if r := recover(); r != nil {
			log.Panicf("PANIC in method Worker.Features: %v\n", r.(error))
		}
	}()
	if _, ok = request.Values["bb"]; ok {
		if bb, err = parseBB(request.Values["bb"][0]); err != nil {
			resp.SetError(NewAcpErrf("%v", err))
			return
		}
	}
	_ = bb

	// send protocol name to ACP
	log.Printf("Dataset? %v", request.Dataset)
	acp.PutString(request.Dataset)
	// send path
	acp.PutString(request.Collection)
	// get status

	if err := w.checkAcpStatus(); err != nil {
		resp.SetError(err)
		return nil
	}
	noOfRecs := acp.GetUint()
	//	noOfFields := acp.GetUint()
	// TODO : implement PROTOCOL
	//	body := make(Body, 0, noOfRecs)
	fc := geojson.NewFeatureCollection(nil)
	for i := 0; i < noOfRecs; i++ {
		switch acp.GetType() {
		case "point":
			coord = toCoordinate(acp.GetCoord())
			geom = geojson.NewPoint(coord)
		case "polygon":
			noOfCoords := acp.GetUint()
			poly := geojson.NewPolygon(nil)
			for j := 0; j < noOfCoords; j++ {
				poly.AddCoordinates(toCoordinates(acp.GetCoord()))
			}
			geom = poly
		case "line":
			noOfCoords := acp.GetUint()
			line := geojson.NewLineString(nil)
			for j := 0; j < noOfCoords; j++ {
				line.AddCoordinates(toCoordinate(acp.GetCoord()))
			}
			geom = line
		}
		noOfFields := acp.GetUint()
		fields := make(map[string]interface{})
		for j := 0; j < noOfFields; j++ {
			name := acp.GetString()
			value := acp.GetString()
			fields[name] = value
		}
		f := geojson.NewFeature(geom, fields, nil)
		fc.AddFeatures(f)
	}
	resp.SetBody(fc)
	return nil
}
