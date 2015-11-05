package goworld

import (
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

//
// Request struct
type FeaturesRequest struct {
	Dataset    string
	Collection string
	Values     url.Values
}

func NewFeatureReques(path string) *FeaturesRequest {
	var (
		dataset, collection string
		res                 []string
		err                 error
		u                   *url.URL
		ok                  bool
	)
	if u, err = url.Parse(path); err != nil {
		log.Fatalf("Wrong path '%s': %v\n", path, err)
	}
	if path, err = url.QueryUnescape(u.Path); err != nil {
		log.Fatalf("Error unescape path '%s': %v\n", u.Path, err)
	}
	res = strings.Split(path, "/")
	if ok = len(res) == 2; ok {
		dataset = res[0]
		collection = res[1]
	}
	values := u.Query()

	return &FeaturesRequest{Dataset: dataset,
		Collection: collection,
		Values:     values,
	}
}

func (w *Worker) Features(request *FeaturesRequest, resp *FeaturesResponse) error {
	var (
		bb       [4]float64
		err      error
		geomType int
		coord    geojson.Coordinate
		geom     geojson.Geometry
	)
	defer func() {
		if r := recover(); r != nil {
			log.Panicf("PANIC in method ListObjectsFields: %v\n", r.(error))
		}
	}()
	if bb, err = parseBB(request.Values["bb"][0]); err != nil {
		resp.Error = err
		return nil
	}
	_ = bb
	// send protocol name to ACP
	acp.PutString(request.Dataset)
	// send path
	acp.PutString(request.Collection)
	// get status

	if err := w.checkAcpStatus(); err != nil {
		resp.Error = err
		return nil
	}
	noOfRecs := acp.GetUint()
	//	noOfFields := acp.GetUint()
	// TODO : implement PROTOCOL
	//	body := make(Body, 0, noOfRecs)
	fc := geojson.NewFeatureCollection(nil)
	for i := 0; i < noOfRecs; i++ {
		geomType = acp.GetUbyte()
		switch incomeTypes[geomType] {
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

		f := geojson.NewFeature(geom, nil, nil)
		fc.AddFeatures(f)
	}
	resp.Body = fc
	return nil
}
