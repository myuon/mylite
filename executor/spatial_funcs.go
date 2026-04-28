package executor

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	sfgeom "github.com/peterstace/simplefeatures/geom"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Geometry values are stored as strings. When a geometry has a non-zero SRID,
// it is encoded in Extended WKT (EWKT) format: "SRID=N;WKT_GEOMETRY".
// When SRID is 0 or absent, plain WKT is used.

// geomSetSRID sets the SRID on a geometry string, returning EWKT if srid != 0 or plain WKT if srid == 0.
func geomSetSRID(geom string, srid uint32) string {
	// Strip existing SRID prefix if present
	wkt := geomStripSRID(geom)
	if srid == 0 {
		return wkt
	}
	return "SRID=" + strconv.FormatUint(uint64(srid), 10) + ";" + wkt
}

// geomGetSRID returns the SRID encoded in a geometry string (0 if absent).
func geomGetSRID(geom string) uint32 {
	upper := geom
	if len(geom) > 8 && (geom[0] == 'S' || geom[0] == 's') {
		upper = strings.ToUpper(geom[:5])
	}
	if !strings.HasPrefix(upper, "SRID=") {
		return 0
	}
	semi := strings.IndexByte(geom, ';')
	if semi < 0 {
		return 0
	}
	sridStr := geom[5:semi]
	v, err := strconv.ParseUint(strings.TrimSpace(sridStr), 10, 32)
	if err != nil {
		return 0
	}
	return uint32(v)
}

// geomStripSRID strips the EWKT SRID prefix from a geometry, returning plain WKT.
func geomStripSRID(geom string) string {
	if len(geom) > 5 {
		upper := geom
		if geom[0] == 'S' || geom[0] == 's' {
			upper = strings.ToUpper(geom[:5])
		}
		if strings.HasPrefix(upper, "SRID=") {
			semi := strings.IndexByte(geom, ';')
			if semi >= 0 {
				return geom[semi+1:]
			}
		}
	}
	return geom
}

// parseSpatialPointCoords extracts X,Y from a WKT POINT string like "POINT(1 2)" or "POINT (1 2)".
// Returns [x, y] or nil if parsing fails.
func parseSpatialPointCoords(wkt string) []float64 {
	wkt = geomStripSRID(strings.TrimSpace(wkt))
	upper := strings.ToUpper(wkt)
	if !strings.HasPrefix(upper, "POINT") {
		return nil
	}
	// Find the parenthesized part
	idx := strings.Index(wkt, "(")
	if idx < 0 {
		return nil
	}
	end := strings.LastIndex(wkt, ")")
	if end < 0 || end <= idx {
		return nil
	}
	inner := strings.TrimSpace(wkt[idx+1 : end])
	parts := strings.Fields(inner)
	if len(parts) < 2 {
		return nil
	}
	x, err1 := strconv.ParseFloat(parts[0], 64)
	y, err2 := strconv.ParseFloat(parts[1], 64)
	if err1 != nil || err2 != nil {
		return nil
	}
	return []float64{x, y}
}

// extractSpatialPointCoord extracts a coordinate from a WKT POINT.
func extractSpatialPointCoord(wkt string, prop sqlparser.PointPropertyType) (interface{}, error) {
	coords := parseSpatialPointCoords(wkt)
	if coords == nil {
		return nil, nil
	}
	switch prop {
	case sqlparser.XCordinate, sqlparser.Longitude:
		return coords[0], nil
	case sqlparser.YCordinate, sqlparser.Latitude:
		return coords[1], nil
	}
	return nil, nil
}

// setSpatialPointCoord sets a coordinate on a WKT POINT and returns the modified WKT.
// Preserves EWKT SRID prefix if present.
func setSpatialPointCoord(wkt string, prop sqlparser.PointPropertyType, newVal interface{}) (interface{}, error) {
	srid := geomGetSRID(wkt)
	coords := parseSpatialPointCoords(wkt) // parseSpatialPointCoords strips SRID internally
	if coords == nil {
		return nil, nil
	}
	nv := toFloat(newVal)
	switch prop {
	case sqlparser.XCordinate, sqlparser.Longitude:
		coords[0] = nv
	case sqlparser.YCordinate, sqlparser.Latitude:
		coords[1] = nv
	}
	result := fmt.Sprintf("POINT(%s %s)", formatSpatialFloat(coords[0]), formatSpatialFloat(coords[1]))
	return geomSetSRID(result, srid), nil
}

// formatSpatialFloat formats a float for WKT output matching MySQL's ST_AsText format.
// MySQL uses decimal notation for values in [1e-6, 1e15) range, scientific notation otherwise.
// No '+' in exponent, and negative zero is preserved as "-0".
func formatSpatialFloat(f float64) string {
	// Handle negative zero specially
	if f == 0 {
		if math.Signbit(f) {
			return "-0"
		}
		return "0"
	}
	abs := math.Abs(f)
	// Integer values in safe range
	if f == float64(int64(f)) && !math.IsInf(f, 0) && abs < 1e15 {
		return strconv.FormatInt(int64(f), 10)
	}
	// Use fixed notation for values between 1e-6 and 1e15 (MySQL behavior)
	if abs >= 1e-6 && abs < 1e15 {
		s := strconv.FormatFloat(f, 'f', -1, 64)
		// Strip trailing zeros after decimal point but keep at least one digit
		if strings.Contains(s, ".") {
			s = strings.TrimRight(s, "0")
			s = strings.TrimRight(s, ".")
		}
		return s
	}
	// Scientific notation for very large or very small values
	s := strconv.FormatFloat(f, 'e', -1, 64)
	// Remove '+' from exponent and leading zeros in exponent
	// e.g., "1e+308" -> "1e308", "1e-06" -> "1e-06" (keep negative)
	s = strings.ReplaceAll(s, "e+0", "e")
	s = strings.ReplaceAll(s, "e+", "e")
	// For negative exponents: remove leading zeros (e-06 -> e-06 stays, but e-006 -> e-06)
	// Go doesn't produce leading zeros in exponent for 'e' format with -1 precision
	return s
}

// evalGeomProperty evaluates geometry property functions.
func evalGeomProperty(wkt string, prop sqlparser.GeomPropertyType) (interface{}, error) {
	// Strip EWKT SRID prefix before processing
	plainWKT := geomStripSRID(strings.TrimSpace(wkt))
	upper := strings.ToUpper(plainWKT)
	switch prop {
	case sqlparser.IsSimple:
		return int64(1), nil
	case sqlparser.IsEmpty:
		if upper == "" {
			return int64(1), nil
		}
		return int64(0), nil
	case sqlparser.Dimension:
		if strings.HasPrefix(upper, "POINT") {
			return int64(0), nil
		} else if strings.HasPrefix(upper, "LINESTRING") || strings.HasPrefix(upper, "MULTILINESTRING") {
			return int64(1), nil
		} else if strings.HasPrefix(upper, "POLYGON") || strings.HasPrefix(upper, "MULTIPOLYGON") {
			return int64(2), nil
		}
		return int64(0), nil
	case sqlparser.GeometryType:
		return detectGeometryType(upper), nil
	case sqlparser.Envelope:
		return computeEnvelope(plainWKT), nil
	}
	return nil, nil
}

// computeEnvelope computes the MBR (Minimum Bounding Rectangle) for a WKT geometry
// and returns the result as MySQL ST_Envelope would:
//   - POINT if the MBR collapses to a single point
//   - LINESTRING if the MBR collapses to a line (one dimension zero)
//   - POLYGON with 5 points for a normal 2D bounding box
//   - GEOMETRYCOLLECTION EMPTY for an empty geometry collection
func computeEnvelope(wkt string) interface{} {
	upper := strings.TrimSpace(strings.ToUpper(wkt))

	// Handle empty geometry collections
	if strings.HasPrefix(upper, "GEOMETRYCOLLECTION") || strings.HasPrefix(upper, "GEOMCOLLECTION") {
		pts := collectAllPoints(wkt)
		if len(pts) == 0 {
			return "GEOMETRYCOLLECTION EMPTY"
		}
		return mbrFromPoints(pts)
	}

	pts := collectAllPoints(wkt)
	if len(pts) == 0 {
		return wkt
	}
	return mbrFromPoints(pts)
}

// normalizeMBRFloat normalizes -0 to +0 for MBR coordinate output.
// MySQL's ST_Envelope treats -0 as 0 in the output coordinates.
func normalizeMBRFloat(f float64) float64 {
	if f == 0 {
		return 0 // converts -0 to +0
	}
	return f
}

// mbrFromPoints computes the MBR polygon/linestring/point from a set of points.
func mbrFromPoints(pts [][]float64) interface{} {
	if len(pts) == 0 {
		return nil
	}
	minX, minY := pts[0][0], pts[0][1]
	maxX, maxY := minX, minY
	for _, p := range pts[1:] {
		if p[0] < minX {
			minX = p[0]
		}
		if p[1] < minY {
			minY = p[1]
		}
		if p[0] > maxX {
			maxX = p[0]
		}
		if p[1] > maxY {
			maxY = p[1]
		}
	}

	// Check for single-point degenerate case before normalization,
	// so we can preserve negative zero in the output (MySQL behavior).
	if minX == maxX && minY == maxY {
		// Single point: preserve original coordinate values including -0
		return fmt.Sprintf("POINT(%s %s)", formatSpatialFloat(minX), formatSpatialFloat(minY))
	}

	// Normalize -0 to +0 for non-degenerate MBR output (MySQL behavior)
	minX = normalizeMBRFloat(minX)
	minY = normalizeMBRFloat(minY)
	maxX = normalizeMBRFloat(maxX)
	maxY = normalizeMBRFloat(maxY)

	if minX == maxX || minY == maxY {
		// Collinear: return a LINESTRING
		return fmt.Sprintf("LINESTRING(%s %s,%s %s)",
			formatSpatialFloat(minX), formatSpatialFloat(minY),
			formatSpatialFloat(maxX), formatSpatialFloat(maxY))
	}
	// Normal bounding rectangle
	return fmt.Sprintf("POLYGON((%s %s,%s %s,%s %s,%s %s,%s %s))",
		formatSpatialFloat(minX), formatSpatialFloat(minY),
		formatSpatialFloat(maxX), formatSpatialFloat(minY),
		formatSpatialFloat(maxX), formatSpatialFloat(maxY),
		formatSpatialFloat(minX), formatSpatialFloat(maxY),
		formatSpatialFloat(minX), formatSpatialFloat(minY))
}

// collectAllPoints recursively collects all coordinate points from any WKT geometry.
func collectAllPoints(wkt string) [][]float64 {
	wkt = strings.TrimSpace(wkt)
	upper := strings.ToUpper(wkt)

	switch {
	case strings.HasPrefix(upper, "POINT"):
		coords := parseSpatialPointCoords(wkt)
		if coords == nil {
			return nil
		}
		return [][]float64{coords}
	case strings.HasPrefix(upper, "LINESTRING"):
		return parseLineStringPoints(wkt)
	case strings.HasPrefix(upper, "MULTILINESTRING"):
		return collectPointsFromMultiLineString(wkt)
	case strings.HasPrefix(upper, "MULTIPOLYGON"):
		rings := parsePolygonRings(wkt)
		var pts [][]float64
		for _, ring := range rings {
			pts = append(pts, ring...)
		}
		return pts
	case strings.HasPrefix(upper, "POLYGON"):
		rings := parsePolygonRings(wkt)
		var pts [][]float64
		for _, ring := range rings {
			pts = append(pts, ring...)
		}
		return pts
	case strings.HasPrefix(upper, "MULTIPOINT"):
		rings := parsePolygonRings(wkt)
		var pts [][]float64
		for _, ring := range rings {
			pts = append(pts, ring...)
		}
		return pts
	case strings.HasPrefix(upper, "GEOMETRYCOLLECTION"), strings.HasPrefix(upper, "GEOMCOLLECTION"):
		geoms := parseGeomCollection(wkt)
		var pts [][]float64
		for _, g := range geoms {
			pts = append(pts, collectAllPoints(g)...)
		}
		return pts
	}
	return nil
}

// collectPointsFromMultiLineString extracts all points from a MULTILINESTRING.
func collectPointsFromMultiLineString(wkt string) [][]float64 {
	idx := strings.Index(wkt, "(")
	end := strings.LastIndex(wkt, ")")
	if idx < 0 || end <= idx {
		return nil
	}
	inner := strings.TrimSpace(wkt[idx+1 : end])
	// Each component is a linestring ring: (x1 y1, x2 y2, ...)
	var pts [][]float64
	depth := 0
	start := -1
	for i, ch := range inner {
		switch ch {
		case '(':
			if depth == 0 {
				start = i + 1
			}
			depth++
		case ')':
			depth--
			if depth == 0 && start >= 0 {
				ringStr := inner[start:i]
				for _, part := range strings.Split(ringStr, ",") {
					fields := strings.Fields(strings.TrimSpace(part))
					if len(fields) >= 2 {
						x, err1 := strconv.ParseFloat(fields[0], 64)
						y, err2 := strconv.ParseFloat(fields[1], 64)
						if err1 == nil && err2 == nil {
							pts = append(pts, []float64{x, y})
						}
					}
				}
				start = -1
			}
		}
	}
	return pts
}

// detectGeometryType returns the MySQL geometry type name.
func detectGeometryType(upper string) string {
	switch {
	case strings.HasPrefix(upper, "MULTIPOLYGON"):
		return "MULTIPOLYGON"
	case strings.HasPrefix(upper, "MULTILINESTRING"):
		return "MULTILINESTRING"
	case strings.HasPrefix(upper, "MULTIPOINT"):
		return "MULTIPOINT"
	case strings.HasPrefix(upper, "GEOMETRYCOLLECTION"), strings.HasPrefix(upper, "GEOMCOLLECTION"):
		// MySQL 8.0 uses "GEOMCOLLECTION" as the canonical type name
		return "GEOMCOLLECTION"
	case strings.HasPrefix(upper, "POLYGON"):
		return "POLYGON"
	case strings.HasPrefix(upper, "LINESTRING"):
		return "LINESTRING"
	case strings.HasPrefix(upper, "POINT"):
		return "POINT"
	default:
		return "GEOMETRY"
	}
}

// parseLineStringPoints extracts coordinate pairs from a WKT LINESTRING.
func parseLineStringPoints(wkt string) [][]float64 {
	upper := strings.TrimSpace(strings.ToUpper(wkt))
	if !strings.HasPrefix(upper, "LINESTRING") {
		return nil
	}
	idx := strings.Index(wkt, "(")
	end := strings.LastIndex(wkt, ")")
	if idx < 0 || end <= idx {
		return nil
	}
	inner := strings.TrimSpace(wkt[idx+1 : end])
	var points [][]float64
	for _, part := range strings.Split(inner, ",") {
		fields := strings.Fields(strings.TrimSpace(part))
		if len(fields) >= 2 {
			x, _ := strconv.ParseFloat(fields[0], 64)
			y, _ := strconv.ParseFloat(fields[1], 64)
			points = append(points, []float64{x, y})
		}
	}
	return points
}

// evalLinestrProperty evaluates linestring property functions.
func evalLinestrProperty(wkt string, prop sqlparser.LinestrPropType, arg interface{}) (interface{}, error) {
	points := parseLineStringPoints(wkt)
	switch prop {
	case sqlparser.StartPoint:
		if len(points) == 0 {
			return nil, nil
		}
		return fmt.Sprintf("POINT(%s %s)", formatSpatialFloat(points[0][0]), formatSpatialFloat(points[0][1])), nil
	case sqlparser.EndPoint:
		if len(points) == 0 {
			return nil, nil
		}
		last := points[len(points)-1]
		return fmt.Sprintf("POINT(%s %s)", formatSpatialFloat(last[0]), formatSpatialFloat(last[1])), nil
	case sqlparser.NumPoints:
		return int64(len(points)), nil
	case sqlparser.PointN:
		if arg == nil {
			return nil, nil
		}
		n := int(toInt64(arg))
		if n < 1 || n > len(points) {
			return nil, nil
		}
		p := points[n-1]
		return fmt.Sprintf("POINT(%s %s)", formatSpatialFloat(p[0]), formatSpatialFloat(p[1])), nil
	case sqlparser.IsClosed:
		if len(points) < 2 {
			return int64(0), nil
		}
		if points[0][0] == points[len(points)-1][0] && points[0][1] == points[len(points)-1][1] {
			return int64(1), nil
		}
		return int64(0), nil
	case sqlparser.Length:
		total := 0.0
		for i := 1; i < len(points); i++ {
			dx := points[i][0] - points[i-1][0]
			dy := points[i][1] - points[i-1][1]
			total += math.Sqrt(dx*dx + dy*dy)
		}
		return total, nil
	}
	return nil, nil
}

// evalPolygonProperty evaluates polygon property functions.
func evalPolygonProperty(wkt string, prop sqlparser.PolygonPropType, arg interface{}) (interface{}, error) {
	switch prop {
	case sqlparser.Area:
		return computePolygonArea(wkt), nil
	case sqlparser.Centroid:
		return computeCentroid(wkt), nil
	case sqlparser.ExteriorRing:
		return extractExteriorRing(wkt), nil
	case sqlparser.NumInteriorRings:
		return countInteriorRings(wkt), nil
	case sqlparser.InteriorRingN:
		if arg == nil {
			return nil, nil
		}
		return extractInteriorRing(wkt, int(toInt64(arg))), nil
	}
	return nil, nil
}

// computePolygonArea computes the area of a polygon using the shoelace formula.
func computePolygonArea(wkt string) float64 {
	rings := parsePolygonRings(wkt)
	if len(rings) == 0 {
		return 0.0
	}
	area := ringArea(rings[0])
	for i := 1; i < len(rings); i++ {
		area -= math.Abs(ringArea(rings[i]))
	}
	return math.Abs(area)
}

func ringArea(pts [][]float64) float64 {
	n := len(pts)
	if n < 3 {
		return 0.0
	}
	area := 0.0
	for i := 0; i < n; i++ {
		j := (i + 1) % n
		area += pts[i][0] * pts[j][1]
		area -= pts[j][0] * pts[i][1]
	}
	return area / 2.0
}

// parsePolygonRings extracts rings from a WKT POLYGON or MULTIPOINT etc.
func parsePolygonRings(wkt string) [][][]float64 {
	upper := strings.TrimSpace(strings.ToUpper(wkt))
	// Handle both POLYGON and non-polygon types that are passed to polygon funcs
	prefix := ""
	if strings.HasPrefix(upper, "POLYGON") {
		prefix = "POLYGON"
	} else if strings.HasPrefix(upper, "MULTIPOINT") {
		// For MULTIPOINT used with centroid, parse all points
		prefix = "MULTIPOINT"
	}
	_ = prefix
	// Find the outermost parens
	idx := strings.Index(wkt, "(")
	end := strings.LastIndex(wkt, ")")
	if idx < 0 || end <= idx {
		return nil
	}
	inner := wkt[idx+1 : end]
	// Split by ring: each ring is (...)
	var rings [][][]float64
	depth := 0
	start := -1
	for i, ch := range inner {
		switch ch {
		case '(':
			if depth == 0 {
				start = i + 1
			}
			depth++
		case ')':
			depth--
			if depth == 0 && start >= 0 {
				ringStr := inner[start:i]
				var pts [][]float64
				for _, part := range strings.Split(ringStr, ",") {
					fields := strings.Fields(strings.TrimSpace(part))
					if len(fields) >= 2 {
						x, _ := strconv.ParseFloat(fields[0], 64)
						y, _ := strconv.ParseFloat(fields[1], 64)
						pts = append(pts, []float64{x, y})
					}
				}
				if len(pts) > 0 {
					rings = append(rings, pts)
				}
				start = -1
			}
		}
	}
	// If no nested parens found, try parsing as flat coordinate list (for MULTIPOINT(0 0, 10 10) etc.)
	if len(rings) == 0 {
		inner2 := strings.TrimSpace(wkt[idx+1 : end])
		var pts [][]float64
		for _, part := range strings.Split(inner2, ",") {
			fields := strings.Fields(strings.TrimSpace(part))
			if len(fields) >= 2 {
				x, _ := strconv.ParseFloat(fields[0], 64)
				y, _ := strconv.ParseFloat(fields[1], 64)
				pts = append(pts, []float64{x, y})
			}
		}
		if len(pts) > 0 {
			rings = append(rings, pts)
		}
	}
	return rings
}

func computeCentroid(wkt string) interface{} {
	rings := parsePolygonRings(wkt)
	if len(rings) == 0 {
		return nil
	}
	// Use centroid of exterior ring (or all points)
	pts := rings[0]
	if len(pts) == 0 {
		return nil
	}
	sx, sy := 0.0, 0.0
	for _, p := range pts {
		sx += p[0]
		sy += p[1]
	}
	n := float64(len(pts))
	return fmt.Sprintf("POINT(%s %s)", formatSpatialFloat(sx/n), formatSpatialFloat(sy/n))
}

func extractExteriorRing(wkt string) interface{} {
	rings := parsePolygonRings(wkt)
	if len(rings) == 0 {
		return nil
	}
	pts := rings[0]
	var parts []string
	for _, p := range pts {
		parts = append(parts, fmt.Sprintf("%s %s", formatSpatialFloat(p[0]), formatSpatialFloat(p[1])))
	}
	return fmt.Sprintf("LINESTRING(%s)", strings.Join(parts, ","))
}

func countInteriorRings(wkt string) int64 {
	rings := parsePolygonRings(wkt)
	if len(rings) <= 1 {
		return 0
	}
	return int64(len(rings) - 1)
}

func extractInteriorRing(wkt string, n int) interface{} {
	rings := parsePolygonRings(wkt)
	if n < 1 || n >= len(rings) {
		return nil
	}
	pts := rings[n]
	var parts []string
	for _, p := range pts {
		parts = append(parts, fmt.Sprintf("%s %s", formatSpatialFloat(p[0]), formatSpatialFloat(p[1])))
	}
	return fmt.Sprintf("LINESTRING(%s)", strings.Join(parts, ","))
}

// evalGeomCollProperty evaluates geometry collection property functions.
func evalGeomCollProperty(wkt string, prop sqlparser.GeomCollPropType, arg interface{}) (interface{}, error) {
	geoms := parseGeomCollection(wkt)
	switch prop {
	case sqlparser.NumGeometries:
		return int64(len(geoms)), nil
	case sqlparser.GeometryN:
		if arg == nil {
			return nil, nil
		}
		n := int(toInt64(arg))
		if n < 1 || n > len(geoms) {
			return nil, nil
		}
		return geoms[n-1], nil
	}
	return nil, nil
}

// parseGeomCollection splits a GEOMETRYCOLLECTION WKT into sub-geometries.
func parseGeomCollection(wkt string) []string {
	upper := strings.TrimSpace(strings.ToUpper(wkt))
	if !strings.HasPrefix(upper, "GEOMETRYCOLLECTION") {
		// For non-collections, treat the geometry itself as a single-element collection
		return []string{wkt}
	}
	idx := strings.Index(wkt, "(")
	end := strings.LastIndex(wkt, ")")
	if idx < 0 || end <= idx {
		return nil
	}
	inner := strings.TrimSpace(wkt[idx+1 : end])
	if inner == "" {
		return nil
	}
	// Split by commas at depth 0
	var result []string
	depth := 0
	start := 0
	for i, ch := range inner {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				result = append(result, strings.TrimSpace(inner[start:i]))
				start = i + 1
			}
		}
	}
	result = append(result, strings.TrimSpace(inner[start:]))
	return result
}

// wktToGeoJSON converts a WKT geometry to a GeoJSON string.
func wktToGeoJSON(wkt string) (interface{}, error) {
	upper := strings.TrimSpace(strings.ToUpper(wkt))
	result := make(map[string]interface{})

	switch {
	case strings.HasPrefix(upper, "POINT"):
		coords := parseSpatialPointCoords(wkt)
		if coords == nil {
			return nil, nil
		}
		result["type"] = "Point"
		result["coordinates"] = coords
	case strings.HasPrefix(upper, "LINESTRING"):
		pts := parseLineStringPoints(wkt)
		if pts == nil {
			return nil, nil
		}
		result["type"] = "LineString"
		result["coordinates"] = pts
	case strings.HasPrefix(upper, "POLYGON"):
		rings := parsePolygonRings(wkt)
		if rings == nil {
			return nil, nil
		}
		result["type"] = "Polygon"
		result["coordinates"] = rings
	case strings.HasPrefix(upper, "MULTIPOINT"):
		// Parse as flat coordinate list
		idx := strings.Index(wkt, "(")
		end := strings.LastIndex(wkt, ")")
		if idx < 0 || end <= idx {
			return nil, nil
		}
		inner := strings.TrimSpace(wkt[idx+1 : end])
		var pts [][]float64
		for _, part := range strings.Split(inner, ",") {
			// Remove optional parentheses around each point
			part = strings.Trim(strings.TrimSpace(part), "()")
			fields := strings.Fields(part)
			if len(fields) >= 2 {
				x, _ := strconv.ParseFloat(fields[0], 64)
				y, _ := strconv.ParseFloat(fields[1], 64)
				pts = append(pts, []float64{x, y})
			}
		}
		result["type"] = "MultiPoint"
		result["coordinates"] = pts
	default:
		// Fallback: return the WKT as type unknown
		result["type"] = "GeometryCollection"
		result["geometries"] = []interface{}{}
	}

	data, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

// geoJSONToWkt converts a GeoJSON string to WKT.
func geoJSONToWkt(jsonStr string) (interface{}, error) {
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &obj); err != nil {
		return nil, fmt.Errorf("invalid GeoJSON: %v", err)
	}
	geoType, _ := obj["type"].(string)
	coords := obj["coordinates"]

	switch strings.ToUpper(geoType) {
	case "POINT":
		arr, ok := coords.([]interface{})
		if !ok || len(arr) < 2 {
			return nil, nil
		}
		x := toFloat(arr[0])
		y := toFloat(arr[1])
		return fmt.Sprintf("POINT(%s %s)", formatSpatialFloat(x), formatSpatialFloat(y)), nil
	case "LINESTRING":
		arr, ok := coords.([]interface{})
		if !ok {
			return nil, nil
		}
		var parts []string
		for _, p := range arr {
			pt, ok := p.([]interface{})
			if !ok || len(pt) < 2 {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s %s", formatSpatialFloat(toFloat(pt[0])), formatSpatialFloat(toFloat(pt[1]))))
		}
		return fmt.Sprintf("LINESTRING(%s)", strings.Join(parts, ",")), nil
	case "POLYGON":
		arr, ok := coords.([]interface{})
		if !ok {
			return nil, nil
		}
		var rings []string
		for _, ring := range arr {
			rarr, ok := ring.([]interface{})
			if !ok {
				continue
			}
			var pts []string
			for _, p := range rarr {
				pt, ok := p.([]interface{})
				if !ok || len(pt) < 2 {
					continue
				}
				pts = append(pts, fmt.Sprintf("%s %s", formatSpatialFloat(toFloat(pt[0])), formatSpatialFloat(toFloat(pt[1]))))
			}
			rings = append(rings, "("+strings.Join(pts, ",")+")")
		}
		return fmt.Sprintf("POLYGON(%s)", strings.Join(rings, ",")), nil
	case "MULTIPOINT":
		arr, ok := coords.([]interface{})
		if !ok {
			return nil, nil
		}
		var parts []string
		for _, p := range arr {
			pt, ok := p.([]interface{})
			if !ok || len(pt) < 2 {
				continue
			}
			// MySQL wraps each point in parentheses: MULTIPOINT((x1 y1),(x2 y2),...)
			parts = append(parts, fmt.Sprintf("(%s %s)", formatSpatialFloat(toFloat(pt[0])), formatSpatialFloat(toFloat(pt[1]))))
		}
		return fmt.Sprintf("MULTIPOINT(%s)", strings.Join(parts, ",")), nil
	case "GEOMETRYCOLLECTION":
		geoms, _ := obj["geometries"].([]interface{})
		if len(geoms) == 0 {
			return "GEOMETRYCOLLECTION EMPTY", nil
		}
		var parts []string
		for _, g := range geoms {
			gJSON, _ := json.Marshal(g)
			wkt, err := geoJSONToWkt(string(gJSON))
			if err != nil || wkt == nil {
				continue
			}
			parts = append(parts, toString(wkt))
		}
		return fmt.Sprintf("GEOMETRYCOLLECTION(%s)", strings.Join(parts, ",")), nil
	}
	return nil, nil
}

// evalGeomFromGeoHash evaluates ST_LatFromGeoHash, ST_LongFromGeoHash, ST_PointFromGeoHash.
func evalGeomFromGeoHash(hash string, geomType sqlparser.GeomFromHashType) (interface{}, error) {
	lat, lon := decodeGeoHash(hash)
	switch geomType {
	case sqlparser.LatitudeFromHash:
		return lat, nil
	case sqlparser.LongitudeFromHash:
		return lon, nil
	case sqlparser.PointFromHash:
		return fmt.Sprintf("POINT(%s %s)", formatSpatialFloat(lon), formatSpatialFloat(lat)), nil
	}
	return nil, nil
}

// base32 alphabet for geohash
const geoHashBase32 = "0123456789bcdefghjkmnpqrstuvwxyz"

// decodeGeoHash decodes a geohash string to lat, lon.
func decodeGeoHash(hash string) (float64, float64) {
	hash = strings.ToLower(strings.TrimSpace(hash))
	minLat, maxLat := -90.0, 90.0
	minLon, maxLon := -180.0, 180.0
	isLon := true
	for _, c := range hash {
		idx := strings.IndexRune(geoHashBase32, c)
		if idx < 0 {
			break
		}
		for bit := 4; bit >= 0; bit-- {
			mid := 0.0
			if isLon {
				mid = (minLon + maxLon) / 2
				if (idx>>bit)&1 == 1 {
					minLon = mid
				} else {
					maxLon = mid
				}
			} else {
				mid = (minLat + maxLat) / 2
				if (idx>>bit)&1 == 1 {
					minLat = mid
				} else {
					maxLat = mid
				}
			}
			isLon = !isLon
		}
	}
	return (minLat + maxLat) / 2, (minLon + maxLon) / 2
}

// evalGeoHash encodes lat, lon into a geohash string of given length.
func evalGeoHash(lat, lon float64, maxLen int) (interface{}, error) {
	if maxLen <= 0 {
		maxLen = 1
	}
	if maxLen > 100 {
		maxLen = 100
	}
	minLat, maxLat := -90.0, 90.0
	minLon, maxLon := -180.0, 180.0
	isLon := true
	var result strings.Builder
	bits := 0
	ch := 0
	for result.Len() < maxLen {
		if isLon {
			mid := (minLon + maxLon) / 2
			if lon >= mid {
				ch = ch*2 + 1
				minLon = mid
			} else {
				ch = ch * 2
				maxLon = mid
			}
		} else {
			mid := (minLat + maxLat) / 2
			if lat >= mid {
				ch = ch*2 + 1
				minLat = mid
			} else {
				ch = ch * 2
				maxLat = mid
			}
		}
		isLon = !isLon
		bits++
		if bits == 5 {
			result.WriteByte(geoHashBase32[ch])
			bits = 0
			ch = 0
		}
	}
	return result.String(), nil
}

// extractPointCoords extracts coordinates from POINT(x y) as "x y" string.
func extractPointCoords(wkt string) string {
	coords := parseSpatialPointCoords(wkt)
	if coords != nil {
		return fmt.Sprintf("%s %s", formatSpatialFloat(coords[0]), formatSpatialFloat(coords[1]))
	}
	// Fallback: return as-is
	return strings.TrimSpace(wkt)
}

// extractRingCoords extracts coordinates from LINESTRING(x1 y1, x2 y2, ...) as "(x1 y1,x2 y2,...)" string.
func extractRingCoords(wkt string) string {
	upper := strings.TrimSpace(strings.ToUpper(wkt))
	if strings.HasPrefix(upper, "LINESTRING") {
		idx := strings.Index(wkt, "(")
		end := strings.LastIndex(wkt, ")")
		if idx >= 0 && end > idx {
			return "(" + strings.TrimSpace(wkt[idx+1:end]) + ")"
		}
	}
	// If already bare coords, wrap in parens
	return "(" + strings.TrimSpace(wkt) + ")"
}

// extractPolygonCoords extracts coordinates from POLYGON((...),(...)) as "((...),(...))" string.
func extractPolygonCoords(wkt string) string {
	upper := strings.TrimSpace(strings.ToUpper(wkt))
	if strings.HasPrefix(upper, "POLYGON") {
		idx := strings.Index(wkt, "(")
		end := strings.LastIndex(wkt, ")")
		if idx >= 0 && end > idx {
			return "(" + strings.TrimSpace(wkt[idx+1:end]) + ")"
		}
	}
	return "(" + strings.TrimSpace(wkt) + ")"
}

// normalizeWKT normalizes a WKT geometry string to MySQL's canonical display format.
// Currently handles MULTIPOINT(x y, ...) -> MULTIPOINT((x y), ...) normalization.
// EWKT SRID prefix (SRID=N;) is preserved if present.
func normalizeWKT(wkt string) string {
	// Preserve SRID prefix if present
	srid := geomGetSRID(wkt)
	plainWKT := geomStripSRID(wkt)
	upper := strings.ToUpper(strings.TrimSpace(plainWKT))
	if !strings.HasPrefix(upper, "MULTIPOINT") {
		return wkt
	}
	// Find the content inside outermost parentheses (work on plainWKT)
	idx := strings.Index(plainWKT, "(")
	if idx < 0 {
		return wkt
	}
	end := strings.LastIndex(plainWKT, ")")
	if end <= idx {
		return wkt
	}
	inner := strings.TrimSpace(plainWKT[idx+1 : end])
	// Check if already has nested parens (e.g., "(0 0),(10 10)") - already normalized
	if strings.HasPrefix(inner, "(") {
		return wkt
	}
	// Parse as flat coordinate list: "x1 y1, x2 y2, ..."
	parts := strings.Split(inner, ",")
	var normalized []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			normalized = append(normalized, "("+p+")")
		}
	}
	prefix := plainWKT[:idx+1]
	normalizedWKT := prefix + strings.Join(normalized, ",") + ")"
	return geomSetSRID(normalizedWKT, srid)
}

// swapWKTCoords swaps X and Y coordinates in all coordinate pairs of a WKT geometry.
// This is needed when converting from Cartesian to geographic SRS (e.g., SRID=4326),
// where MySQL stores coordinates as (latitude, longitude) = (Y, X).
// The EWKT SRID prefix (SRID=N;) is preserved if present.
func swapWKTCoords(wkt string) string {
	srid := geomGetSRID(wkt)
	plain := geomStripSRID(wkt)
	swapped := swapWKTCoordsPlain(plain)
	return geomSetSRID(swapped, srid)
}

// swapWKTCoordsPlain swaps X↔Y in all coordinate pairs of a plain WKT string (no SRID prefix).
func swapWKTCoordsPlain(wkt string) string {
	wkt = strings.TrimSpace(wkt)
	upper := strings.ToUpper(wkt)

	// Determine geometry keyword and the rest of the string
	// Find the first '(' which delimits the keyword from coordinates
	paren := strings.Index(wkt, "(")
	if paren < 0 {
		return wkt
	}
	keyword := wkt[:paren]
	rest := wkt[paren:]

	upperKw := strings.ToUpper(strings.TrimSpace(keyword))
	_ = upper

	switch {
	case upperKw == "POINT":
		// POINT(x y)
		return keyword + swapCoordPairsInRing(rest)
	case upperKw == "LINESTRING":
		// LINESTRING(x1 y1,x2 y2,...)
		return keyword + swapCoordPairsInRing(rest)
	case upperKw == "POLYGON":
		// POLYGON((x1 y1,...),(x1 y1,...))
		return keyword + swapCoordPairsInNestedRings(rest)
	case upperKw == "MULTIPOINT":
		// MULTIPOINT((x1 y1),(x2 y2),...) or MULTIPOINT(x1 y1,x2 y2,...)
		return keyword + swapCoordPairsInNestedOrFlat(rest)
	case upperKw == "MULTILINESTRING":
		// MULTILINESTRING((x1 y1,...),(x1 y1,...))
		return keyword + swapCoordPairsInNestedRings(rest)
	case upperKw == "MULTIPOLYGON":
		// MULTIPOLYGON(((x1 y1,...)),...) — deeper nesting
		return keyword + swapCoordPairsInMultiPolygon(rest)
	case upperKw == "GEOMETRYCOLLECTION" || upperKw == "GEOMCOLLECTION":
		return swapGeomCollection(wkt)
	}
	return wkt
}

// swapCoordPair swaps two space-separated floats: "x y" → "y x"
func swapCoordPair(pair string) string {
	pair = strings.TrimSpace(pair)
	fields := strings.Fields(pair)
	if len(fields) < 2 {
		return pair
	}
	// Swap first two fields, keep any extra (e.g., Z coordinate)
	result := fields[1] + " " + fields[0]
	for _, f := range fields[2:] {
		result += " " + f
	}
	return result
}

// swapCoordPairsInRing swaps all coord pairs inside a single ring like "(x1 y1,x2 y2,...)"
func swapCoordPairsInRing(s string) string {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return s
	}
	inner := s[1 : len(s)-1]
	pairs := strings.Split(inner, ",")
	var result []string
	for _, p := range pairs {
		result = append(result, swapCoordPair(p))
	}
	return "(" + strings.Join(result, ",") + ")"
}

// swapCoordPairsInNestedRings handles strings like "((r1),(r2),...)" where each ring is "(x y,...)"
func swapCoordPairsInNestedRings(s string) string {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return s
	}
	inner := s[1 : len(s)-1]
	// Split by commas at depth 0
	parts := splitAtDepth0(inner)
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		result = append(result, swapCoordPairsInRing(p))
	}
	return "(" + strings.Join(result, ",") + ")"
}

// swapCoordPairsInNestedOrFlat handles MULTIPOINT which may be nested or flat
func swapCoordPairsInNestedOrFlat(s string) string {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return s
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	// Check if nested (starts with '(')
	if strings.HasPrefix(inner, "(") {
		return swapCoordPairsInNestedRings(s)
	}
	// Flat: "x1 y1,x2 y2,..."
	return swapCoordPairsInRing(s)
}

// swapCoordPairsInMultiPolygon handles MULTIPOLYGON structure: "(((r1),(r2)),((r3)),...)"
func swapCoordPairsInMultiPolygon(s string) string {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return s
	}
	inner := s[1 : len(s)-1]
	// Split polygons at depth 0
	parts := splitAtDepth0(inner)
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		// Each part is "((rings))"
		result = append(result, swapCoordPairsInNestedRings(p))
	}
	return "(" + strings.Join(result, ",") + ")"
}

// swapGeomCollection handles GEOMETRYCOLLECTION/GEOMCOLLECTION
func swapGeomCollection(wkt string) string {
	wkt = strings.TrimSpace(wkt)
	paren := strings.Index(wkt, "(")
	if paren < 0 {
		return wkt
	}
	keyword := wkt[:paren]
	rest := strings.TrimSpace(wkt[paren:])
	if len(rest) < 2 || rest[0] != '(' || rest[len(rest)-1] != ')' {
		return wkt
	}
	inner := rest[1 : len(rest)-1]
	parts := splitAtDepth0(inner)
	var result []string
	for _, p := range parts {
		result = append(result, swapWKTCoordsPlain(strings.TrimSpace(p)))
	}
	return keyword + "(" + strings.Join(result, ",") + ")"
}

// splitAtDepth0 splits a string by commas at depth 0 (not inside parentheses).
func splitAtDepth0(s string) []string {
	var result []string
	depth := 0
	start := 0
	for i, ch := range s {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				result = append(result, s[start:i])
				start = i + 1
			}
		}
	}
	result = append(result, s[start:])
	return result
}

// parseSRIDValue parses a SRID value (like an integer or string) and returns:
// - the int64 value (0 if non-numeric)
// - whether a truncation happened (non-numeric string)
// - the original string representation (for warning messages)
func parseSRIDValue(v interface{}) (int64, bool, string) {
	switch n := v.(type) {
	case int64:
		return n, false, ""
	case uint64:
		return int64(n), false, ""
	case float64:
		return int64(n), false, ""
	case string:
		// Try parsing as integer
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return i, false, ""
		}
		// Try as float
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return int64(f), false, ""
		}
		// Non-numeric: truncate to 0 with warning
		return 0, true, n
	}
	return toInt64(v), false, ""
}

// Spatial function helpers for evalFuncExpr — these handle functions called by name
// (as opposed to AST node types).

var spatialFuncRe = regexp.MustCompile(`(?i)^(st_|mbr)`)

// evalSpatialFunc handles spatial functions that appear as regular function calls.
// Returns (result, handled, error).
func evalSpatialFunc(e *Executor, name string, exprs []sqlparser.Expr) (interface{}, bool, error) {
	lower := strings.ToLower(name)
	switch lower {
	case "st_srid":
		// ST_SRID requires exactly 1 or 2 arguments.
		if len(exprs) == 0 || len(exprs) > 2 {
			return nil, true, mysqlError(1582, "42000", "Incorrect parameter count in the call to native function 'ST_SRID'")
		}
		val, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		if len(exprs) == 2 {
			// Setter form: ST_SRID(geom, srid)
			sridVal, err2 := e.evalExpr(exprs[1])
			if err2 != nil {
				return nil, true, err2
			}
			if val == nil || sridVal == nil {
				return nil, true, nil
			}
			// Get geometry as WKT string (validate it is a non-empty geometry)
			var geomStr string
			if b, ok := val.([]byte); ok {
				// Binary input: convert to WKT
				geomStr = wkbToWKT(b)
				if geomStr == "" {
					return nil, true, mysqlError(3516, "22023", "Invalid GIS data provided to function st_srid.")
				}
			} else {
				geomStr = toString(val)
				if geomStr == "" {
					return nil, true, mysqlError(3516, "22023", "Invalid GIS data provided to function st_srid.")
				}
			}
			// Parse SRID value, emit warning for non-integer strings
			sridInt, sridTruncated, sridOrigStr := parseSRIDValue(sridVal)
			if sridTruncated {
				e.addWarning("Warning", 1292, fmt.Sprintf("Truncated incorrect INTEGER value: '%s'", sridOrigStr))
			}
			// Validate SRID range: must be in [0, 2^32-1]
			if sridInt < 0 || sridInt > 4294967295 {
				return nil, true, mysqlError(3037, "22003", "SRID value is out of range in 'st_srid'")
			}
			srid := uint32(sridInt)
			// Check known SRIDs: only known SRIDs are supported.
			// All other valid-range SRIDs return ER_SRS_NOT_FOUND.
			if !e.sridIsKnown(srid) {
				return nil, true, mysqlError(3548, "SR001", fmt.Sprintf("There's no spatial reference system with SRID %d.", srid))
			}
			// When converting to/from a geographic SRS, swap X↔Y coordinates.
			// MySQL stores geographic coordinates as (latitude=Y, longitude=X), so
			// switching between Cartesian (SRID=0 or projected) and geographic (e.g., 4326)
			// requires a coordinate axis swap.
			oldSRID := geomGetSRID(geomStr)
			oldIsGeo := isGeographicSRID(oldSRID)
			newIsGeo := isGeographicSRID(srid)
			if oldIsGeo != newIsGeo {
				// Swap coordinates when axis order changes between Cartesian and geographic
				geomStr = swapWKTCoords(geomStr)
			}
			// Return geometry with updated SRID as EWKT string
			return geomSetSRID(geomStr, srid), true, nil
		}
		// Getter form: ST_SRID(geom)
		if val == nil {
			return nil, true, nil
		}
		geomStr := toString(val)
		// Getter form: ST_SRID(geom) — return the SRID
		return int64(geomGetSRID(geomStr)), true, nil
	case "st_isvalid":
		if len(exprs) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		return int64(1), true, nil
	case "st_validate":
		if len(exprs) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		return val, true, nil
	case "st_distance":
		if len(exprs) < 2 {
			return nil, true, fmt.Errorf("ST_DISTANCE requires 2 arguments")
		}
		a, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		b, err := e.evalExpr(exprs[1])
		if err != nil {
			return nil, true, err
		}
		if a == nil || b == nil {
			return nil, true, nil
		}
		return computeSpatialDistance(toString(a), toString(b)), true, nil
	case "st_distance_sphere":
		if len(exprs) < 2 {
			return nil, true, nil
		}
		a, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		b, err := e.evalExpr(exprs[1])
		if err != nil {
			return nil, true, err
		}
		if a == nil || b == nil {
			return nil, true, nil
		}
		return computeDistanceSphere(toString(a), toString(b)), true, nil
	case "st_contains":
		return evalSpatialRelationDE9IM(e, exprs, "contains")
	case "mbrcontains":
		return evalSpatialRelation(e, exprs, "contains")
	case "st_within":
		return evalSpatialRelationDE9IM(e, exprs, "within")
	case "mbrwithin":
		return evalSpatialRelation(e, exprs, "within")
	case "st_intersects":
		return evalSpatialRelationDE9IM(e, exprs, "intersects")
	case "mbrintersects":
		return evalSpatialRelation(e, exprs, "intersects")
	case "st_disjoint":
		return evalSpatialRelationDE9IM(e, exprs, "disjoint")
	case "mbrdisjoint":
		return evalSpatialRelation(e, exprs, "disjoint")
	case "st_touches":
		return evalSpatialRelationDE9IM(e, exprs, "touches")
	case "mbrtouches":
		return evalSpatialRelation(e, exprs, "touches")
	case "st_overlaps":
		return evalSpatialRelationDE9IM(e, exprs, "overlaps")
	case "mbroverlaps":
		return evalSpatialRelation(e, exprs, "overlaps")
	case "st_crosses":
		return evalSpatialRelationDE9IM(e, exprs, "crosses")
	case "mbrequals":
		return evalSpatialRelation(e, exprs, "equals")
	case "mbrcovers":
		return evalSpatialRelation(e, exprs, "covers")
	case "mbrcoveredby":
		return evalSpatialRelation(e, exprs, "coveredby")
	case "st_union", "st_intersection", "st_difference", "st_symdifference":
		if len(exprs) < 2 {
			return nil, true, nil
		}
		aVal, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		bVal, err := e.evalExpr(exprs[1])
		if err != nil {
			return nil, true, err
		}
		if aVal == nil || bVal == nil {
			return nil, true, nil
		}
		aStr := toString(aVal)
		bStr := toString(bVal)
		result, err := evalSpatialSetOp(lower, aStr, bStr)
		if err != nil {
			return nil, true, err
		}
		return result, true, nil
	case "st_buffer_strategy":
		// Stub: return strategy name as-is
		if len(exprs) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		return toString(val), true, nil
	case "st_buffer":
		if len(exprs) < 2 {
			return nil, true, nil
		}
		val, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		distVal, err := e.evalExpr(exprs[1])
		if err != nil {
			return nil, true, err
		}
		if val == nil || distVal == nil {
			return nil, true, nil
		}
		wktStr := toString(val)
		distStr := strings.TrimSpace(fmt.Sprintf("%v", distVal))
		if distStr == "" {
			distStr = "0"
		}
		dist, parseErr := strconv.ParseFloat(distStr, 64)
		if parseErr != nil {
			// MySQL treats non-numeric strings as 0 (implicit conversion)
			dist = 0
		}
		result, bufErr := evalSpatialBuffer(wktStr, dist)
		if bufErr != nil {
			return nil, true, bufErr
		}
		return result, true, nil
	case "st_convexhull":
		if len(exprs) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		wktStr := toString(val)
		result, convexErr := evalSpatialConvexHull(wktStr)
		if convexErr != nil {
			return nil, true, convexErr
		}
		return result, true, nil
	case "st_simplify":
		if len(exprs) < 2 {
			return nil, true, nil
		}
		val, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		threshVal, err := e.evalExpr(exprs[1])
		if err != nil {
			return nil, true, err
		}
		if val == nil || threshVal == nil {
			return nil, true, nil
		}
		wktStr := toString(val)
		threshold, parseErr := strconv.ParseFloat(fmt.Sprintf("%v", threshVal), 64)
		if parseErr != nil {
			return nil, true, fmt.Errorf("st_simplify: invalid threshold: %v", threshVal)
		}
		result, simplifyErr := evalSpatialSimplify(wktStr, threshold)
		if simplifyErr != nil {
			return nil, true, simplifyErr
		}
		return result, true, nil
	case "st_transform":
		if len(exprs) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		return val, true, nil
	case "st_swapxy":
		if len(exprs) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		if val == nil {
			return nil, true, nil
		}
		return swapXY(toString(val)), true, nil
	case "geometrycollection", "st_geomcollection":
		// GEOMETRYCOLLECTION(geom, geom, ...)
		var parts []string
		for _, expr := range exprs {
			val, err := e.evalExpr(expr)
			if err != nil {
				return nil, true, err
			}
			if val != nil {
				parts = append(parts, toString(val))
			}
		}
		if len(parts) == 0 {
			return "GEOMETRYCOLLECTION EMPTY", true, nil
		}
		return fmt.Sprintf("GEOMETRYCOLLECTION(%s)", strings.Join(parts, ",")), true, nil
	case "st_makeenvelope":
		if len(exprs) != 2 {
			return nil, true, mysqlError(1582, "42000", "Incorrect parameter count in the call to native function 'ST_MAKEENVELOPE'")
		}
		a, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		b, err := e.evalExpr(exprs[1])
		if err != nil {
			return nil, true, err
		}
		if a == nil || b == nil {
			return nil, true, nil
		}
		return makeSpatialEnvelope(toString(a), toString(b)), true, nil
	}
	return nil, false, nil
}

// computeSpatialDistance computes Euclidean distance between two geometries (points).
func computeSpatialDistance(a, b string) interface{} {
	ca := parseSpatialPointCoords(a)
	cb := parseSpatialPointCoords(b)
	if ca == nil || cb == nil {
		return nil
	}
	dx := ca[0] - cb[0]
	dy := ca[1] - cb[1]
	return math.Sqrt(dx*dx + dy*dy)
}

// computeDistanceSphere computes the great-circle distance in meters.
func computeDistanceSphere(a, b string) interface{} {
	ca := parseSpatialPointCoords(a)
	cb := parseSpatialPointCoords(b)
	if ca == nil || cb == nil {
		return nil
	}
	// Haversine formula; Earth radius = 6370986.0 (MySQL default)
	const R = 6370986.0
	lat1 := ca[1] * math.Pi / 180
	lat2 := cb[1] * math.Pi / 180
	dLat := (cb[1] - ca[1]) * math.Pi / 180
	dLon := (cb[0] - ca[0]) * math.Pi / 180
	a2 := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a2), math.Sqrt(1-a2))
	return R * c
}

// relToFuncName maps internal relation names to MySQL function names for error messages.
func relToFuncName(rel string, mbr bool) string {
	prefix := "st_"
	if mbr {
		prefix = "mbr"
	}
	switch rel {
	case "contains":
		return prefix + "contains"
	case "within":
		return prefix + "within"
	case "intersects":
		return prefix + "intersects"
	case "disjoint":
		return prefix + "disjoint"
	case "touches":
		return prefix + "touches"
	case "overlaps":
		return prefix + "overlaps"
	case "crosses":
		return prefix + "crosses"
	case "equals":
		return prefix + "equals"
	case "covers":
		return prefix + "covers"
	case "coveredby":
		return prefix + "coveredby"
	}
	return prefix + rel
}

// evalSpatialRelation evaluates spatial predicate functions (contains, within, etc.).
func evalSpatialRelation(e *Executor, exprs []sqlparser.Expr, rel string) (interface{}, bool, error) {
	if len(exprs) < 2 {
		return nil, true, nil
	}
	a, err := e.evalExpr(exprs[0])
	if err != nil {
		return nil, true, err
	}
	b, err := e.evalExpr(exprs[1])
	if err != nil {
		return nil, true, err
	}
	if a == nil || b == nil {
		return nil, true, nil
	}
	// Simple stub: equals returns 1 for identical geometries, 0 otherwise.
	// For other relations, use MBR-based approximation.
	// Convert binary WKB inputs to EWKT strings so SRID can be extracted properly.
	sa, sridA := geomValueToEWKT(a)
	sb, sridB := geomValueToEWKT(b)

	// Validate SRID mismatch (MySQL returns HY000 error for mismatched SRIDs).
	if sridA != sridB {
		fnName := relToFuncName(rel, true)
		return nil, true, mysqlError(3572, "HY000", fmt.Sprintf("Binary geometry function %s given two geometries of different srids: %d and %d, which should have been identical.", fnName, sridA, sridB))
	}

	// For geographic SRIDs (e.g. 4326), validate coordinate bounds before computing relation.
	if isGeographicSRID(sridA) {
		mbrFnName := relToFuncName(rel, true)
		isLatLong := e.sridIsLatLongOrdered(sridA)
		if valErr := validateGeogCoordsForSpatialPredicate(sa, isLatLong, mbrFnName); valErr != nil {
			return nil, true, valErr
		}
		if valErr := validateGeogCoordsForSpatialPredicate(sb, isLatLong, mbrFnName); valErr != nil {
			return nil, true, valErr
		}
	}

	// Validate that geometry arguments are actually WKT geometries (not e.g. numeric columns).
	if !isLikelyWKT(sa) {
		fnName := relToFuncName(rel, true)
		return nil, true, mysqlError(3516, "22023", fmt.Sprintf("Invalid GIS data provided to function %s.", fnName))
	}
	if !isLikelyWKT(sb) {
		fnName := relToFuncName(rel, true)
		return nil, true, mysqlError(3516, "22023", fmt.Sprintf("Invalid GIS data provided to function %s.", fnName))
	}
	switch rel {
	case "equals":
		if strings.EqualFold(strings.TrimSpace(sa), strings.TrimSpace(sb)) {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "disjoint":
		// Simple approximation: disjoint if MBRs don't overlap
		if mbrOverlap(sa, sb) {
			return int64(0), true, nil
		}
		return int64(1), true, nil
	case "contains", "covers":
		if mbrContains(sa, sb) {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "within", "coveredby":
		if mbrContains(sb, sa) {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "intersects":
		if mbrOverlap(sa, sb) {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "touches":
		return int64(0), true, nil
	case "overlaps":
		if mbrOverlap(sa, sb) {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	case "crosses":
		return int64(0), true, nil
	}
	return int64(0), true, nil
}

// getMBR returns minX, minY, maxX, maxY for a WKT geometry.
func getMBR(wkt string) (float64, float64, float64, float64) {
	// Extract all numbers from the WKT
	wkt = strings.TrimSpace(wkt)
	// Find all coordinate pairs
	idx := strings.Index(wkt, "(")
	if idx < 0 {
		return 0, 0, 0, 0
	}
	inner := wkt[idx:]
	// Remove all parens
	inner = strings.NewReplacer("(", " ", ")", " ").Replace(inner)
	parts := strings.Split(inner, ",")
	minX, minY := math.MaxFloat64, math.MaxFloat64
	maxX, maxY := -math.MaxFloat64, -math.MaxFloat64
	count := 0
	for _, part := range parts {
		fields := strings.Fields(strings.TrimSpace(part))
		if len(fields) >= 2 {
			x, err1 := strconv.ParseFloat(fields[0], 64)
			y, err2 := strconv.ParseFloat(fields[1], 64)
			if err1 == nil && err2 == nil {
				if x < minX {
					minX = x
				}
				if x > maxX {
					maxX = x
				}
				if y < minY {
					minY = y
				}
				if y > maxY {
					maxY = y
				}
				count++
			}
		}
	}
	if count == 0 {
		return 0, 0, 0, 0
	}
	return minX, minY, maxX, maxY
}

func mbrOverlap(a, b string) bool {
	ax1, ay1, ax2, ay2 := getMBR(a)
	bx1, by1, bx2, by2 := getMBR(b)
	return ax1 <= bx2 && ax2 >= bx1 && ay1 <= by2 && ay2 >= by1
}

// mbrDimContains checks MySQL 5.7 MBRCONTAINS semantics for a single dimension.
// For non-degenerate outer (oMin < oMax): inner range must strictly intersect outer's open interior.
// For degenerate outer (oMin == oMax): inner range must equal that single value.
func mbrDimContains(oMin, oMax, iMin, iMax float64) bool {
	if oMin == oMax {
		// Degenerate outer in this dimension: inner must be exactly at this value.
		return iMin == oMin && iMax == oMin
	}
	// Non-degenerate outer: inner range must strictly intersect open interval (oMin, oMax).
	// (oMin, oMax) ∩ [iMin, iMax] ≠ ∅  iff  iMin < oMax AND iMax > oMin
	return iMin < oMax && iMax > oMin
}

// mbrContainsBoxes returns true if outer MBR box [ox1,oy1,ox2,oy2] contains inner
// MBR box [ix1,iy1,ix2,iy2] according to MySQL 5.7 MBRCONTAINS semantics:
//   - Exact equality → true (handles degenerate equal MBRs like POINT=POINT)
//   - Inner weakly contained in outer (all coordinates), AND for each dimension,
//     inner must strictly intersect the open interior of outer in that dimension
//     (or exactly equal a degenerate outer dimension).
func mbrContainsBoxes(outer, inner []float64) bool {
	ox1, oy1, ox2, oy2 := outer[0], outer[1], outer[2], outer[3]
	ix1, iy1, ix2, iy2 := inner[0], inner[1], inner[2], inner[3]
	// Weak containment
	if !(ox1 <= ix1 && oy1 <= iy1 && ox2 >= ix2 && oy2 >= iy2) {
		return false
	}
	// Exact equality
	if ox1 == ix1 && oy1 == iy1 && ox2 == ix2 && oy2 == iy2 {
		return true
	}
	// Per-dimension containment check
	return mbrDimContains(ox1, ox2, ix1, ix2) && mbrDimContains(oy1, oy2, iy1, iy2)
}

// mbrContains returns true if outer geometry's MBR contains inner geometry's MBR
// according to MySQL 5.7 MBRCONTAINS semantics.
func mbrContains(outer, inner string) bool {
	ox1, oy1, ox2, oy2 := getMBR(outer)
	ix1, iy1, ix2, iy2 := getMBR(inner)
	outerBox := []float64{ox1, oy1, ox2, oy2}
	innerBox := []float64{ix1, iy1, ix2, iy2}
	return mbrContainsBoxes(outerBox, innerBox)
}

func swapXY(wkt string) interface{} {
	coords := parseSpatialPointCoords(wkt)
	if coords != nil {
		return fmt.Sprintf("POINT(%s %s)", formatSpatialFloat(coords[1]), formatSpatialFloat(coords[0]))
	}
	return wkt
}

func makeSpatialEnvelope(a, b string) interface{} {
	ca := parseSpatialPointCoords(a)
	cb := parseSpatialPointCoords(b)
	if ca == nil || cb == nil {
		return nil
	}
	return mbrFromPoints([][]float64{ca, cb})
}

// wktBoundingBox extracts the bounding box (MBR) from a WKT geometry string.
// Returns [minX, minY, maxX, maxY] or nil if the geometry can't be parsed or is empty.
// Handles all geometry types: POINT, LINESTRING, POLYGON, MULTILINESTRING,
// MULTIPOLYGON, MULTIPOINT, GEOMETRYCOLLECTION, and GEOMCOLLECTION.
func wktBoundingBox(wkt string) []float64 {
	wkt = strings.TrimSpace(wkt)
	upper := strings.ToUpper(wkt)

	// Handle GEOMETRYCOLLECTION / GEOMCOLLECTION recursively.
	if strings.HasPrefix(upper, "GEOMETRYCOLLECTION") || strings.HasPrefix(upper, "GEOMCOLLECTION") {
		subGeoms := parseGeomCollection(wkt)
		if len(subGeoms) == 0 {
			return nil
		}
		var minX, minY, maxX, maxY float64
		first := true
		for _, sg := range subGeoms {
			bb := wktBoundingBox(sg)
			if bb == nil {
				continue
			}
			if first {
				minX, minY, maxX, maxY = bb[0], bb[1], bb[2], bb[3]
				first = false
			} else {
				if bb[0] < minX {
					minX = bb[0]
				}
				if bb[1] < minY {
					minY = bb[1]
				}
				if bb[2] > maxX {
					maxX = bb[2]
				}
				if bb[3] > maxY {
					maxY = bb[3]
				}
			}
		}
		if first {
			return nil
		}
		return []float64{minX, minY, maxX, maxY}
	}

	// For all other geometry types, use getMBR which strips all parentheses and
	// splits on commas to extract coordinate pairs — works for POINT, LINESTRING,
	// POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON.
	if !strings.Contains(wkt, "(") {
		return nil
	}
	minX, minY, maxX, maxY := getMBR(wkt)
	// getMBR returns 0,0,0,0 when no coordinates were found (count == 0).
	// We must disambiguate "no data" from "all-zero coordinates" by re-checking.
	if minX == 0 && minY == 0 && maxX == 0 && maxY == 0 {
		inner := wkt[strings.Index(wkt, "("):]
		inner = strings.NewReplacer("(", " ", ")", " ").Replace(inner)
		found := false
		for _, part := range strings.Split(inner, ",") {
			fields := strings.Fields(strings.TrimSpace(part))
			if len(fields) >= 2 {
				_, err1 := strconv.ParseFloat(fields[0], 64)
				_, err2 := strconv.ParseFloat(fields[1], 64)
				if err1 == nil && err2 == nil {
					found = true
					break
				}
			}
		}
		if !found {
			return nil
		}
	}
	return []float64{minX, minY, maxX, maxY}
}

// WktToWKB is the exported form of wktToWKB for use from other packages (e.g. server).
func WktToWKB(wkt string) []byte {
	return wktToWKB(wkt)
}

// wkbToWKTWithSwap converts WKB bytes to WKT, optionally swapping X and Y coordinates.
// This is used for WKB functions that support axis-order option (axis-order=lat-long swaps coords).
func wkbToWKTWithSwap(data []byte, needsSwap bool) string {
	if !needsSwap {
		return wkbToWKT(data)
	}
	if len(data) == 0 {
		return ""
	}
	// Handle MySQL-internal format [srid:4 LE][WKB] or pure ISO WKB.
	if data[0] != 0x00 && data[0] != 0x01 {
		// SRID-prefixed
		if len(data) < 9 {
			return ""
		}
		offset := 4
		wkt, ok := parseWKBSwap(data, &offset, true)
		if !ok {
			return ""
		}
		return wkt
	}
	// Try pure WKB first
	offset0 := 0
	wkt0, ok0 := parseWKBSwap(data, &offset0, true)
	if ok0 && offset0 == len(data) {
		return wkt0
	}
	// Try as SRID-prefixed
	if len(data) >= 9 && (data[4] == 0x00 || data[4] == 0x01) {
		offset4 := 4
		wkt4, ok4 := parseWKBSwap(data, &offset4, true)
		if ok4 {
			return wkt4
		}
	}
	if ok0 {
		return wkt0
	}
	return ""
}

// swapXYString swaps X and Y coordinates in a WKT geometry string.
// Used for axis-order=lat-long handling in WKB functions.
func swapXYString(wkt string) string {
	plain := geomStripSRID(strings.TrimSpace(wkt))
	upper := strings.ToUpper(plain)
	switch {
	case strings.HasPrefix(upper, "POINT"):
		coords := parseSpatialPointCoords(plain)
		if coords != nil {
			return fmt.Sprintf("POINT(%s %s)", formatSpatialFloat(coords[1]), formatSpatialFloat(coords[0]))
		}
	case strings.HasPrefix(upper, "LINESTRING"):
		pts := parseLineStringPoints(plain)
		if pts != nil {
			var parts []string
			for _, p := range pts {
				parts = append(parts, fmt.Sprintf("%s %s", formatSpatialFloat(p[1]), formatSpatialFloat(p[0])))
			}
			return "LINESTRING(" + strings.Join(parts, ",") + ")"
		}
	case strings.HasPrefix(upper, "POLYGON"):
		rings := parsePolygonRings(plain)
		if rings != nil {
			var ringParts []string
			for _, ring := range rings {
				var pts []string
				for _, p := range ring {
					pts = append(pts, fmt.Sprintf("%s %s", formatSpatialFloat(p[1]), formatSpatialFloat(p[0])))
				}
				ringParts = append(ringParts, "("+strings.Join(pts, ",")+")")
			}
			return "POLYGON(" + strings.Join(ringParts, ",") + ")"
		}
	}
	return wkt
}

// swapAllXYInWKT swaps all X and Y coordinates in a WKT geometry string.
// This handles all geometry types (POINT, LINESTRING, POLYGON, MULTI*, GEOMETRYCOLLECTION).
// Each coordinate pair "X Y" is replaced with "Y X".
func swapAllXYInWKT(wkt string) string {
	// Strategy: find all coordinate pairs in the WKT and swap them.
	// Coordinate pairs appear as "N1 N2" where N1 and N2 are floating-point numbers.
	// They appear within parentheses, separated by commas or adjacent to other parens.
	// We scan the string and whenever we find a number, check if it's followed by
	// a space and another number (forming a coord pair), then swap them.
	plain := geomStripSRID(strings.TrimSpace(wkt))
	if len(plain) == 0 {
		return wkt
	}
	// Find the geometry type prefix (letters before first '(')
	parenIdx := strings.IndexByte(plain, '(')
	if parenIdx < 0 {
		return plain
	}
	prefix := plain[:parenIdx]
	body := plain[parenIdx:]
	// Swap coordinate pairs in the body
	result := swapCoordPairsInBody(body)
	return prefix + result
}

// swapCoordPairsInBody scans a WKT body (starting with '(' ) and swaps all coordinate pairs.
func swapCoordPairsInBody(s string) string {
	var buf strings.Builder
	i := 0
	n := len(s)
	for i < n {
		ch := s[i]
		if ch == '(' || ch == ')' || ch == ',' {
			buf.WriteByte(ch)
			i++
			continue
		}
		if ch == ' ' {
			// Skip spaces between tokens (but not within coord pairs - we handle those below)
			buf.WriteByte(ch)
			i++
			continue
		}
		// Try to parse a floating-point number
		if ch == '-' || (ch >= '0' && ch <= '9') {
			x, xLen := parseFloatFromWKT(s[i:])
			if xLen > 0 {
				j := i + xLen
				// Check for a space followed by another number (coord pair)
				if j < n && s[j] == ' ' {
					k := j + 1
					if k < n && (s[k] == '-' || (s[k] >= '0' && s[k] <= '9')) {
						y, yLen := parseFloatFromWKT(s[k:])
						if yLen > 0 {
							// Write swapped pair: Y X
							buf.WriteString(formatSpatialFloat(y))
							buf.WriteByte(' ')
							buf.WriteString(formatSpatialFloat(x))
							i = k + yLen
							continue
						}
					}
				}
				// Just a single number (shouldn't happen in well-formed WKT, but handle gracefully)
				buf.WriteString(s[i : i+xLen])
				i += xLen
				continue
			}
		}
		buf.WriteByte(ch)
		i++
	}
	return buf.String()
}

// wkbFuncName returns the canonical MySQL function name for a GeomFromWkbType.
func wkbFuncName(t sqlparser.GeomFromWkbType) string {
	switch t {
	case sqlparser.GeometryFromWKB:
		return "st_geomfromwkb"
	case sqlparser.GeometryCollectionFromWKB:
		return "st_geomcollfromwkb"
	case sqlparser.PointFromWKB:
		return "st_pointfromwkb"
	case sqlparser.LineStringFromWKB:
		return "st_linefromwkb"
	case sqlparser.PolygonFromWKB:
		return "st_polyfromwkb"
	case sqlparser.MultiPointFromWKB:
		return "st_mpointfromwkb"
	case sqlparser.MultiPolygonFromWKB:
		return "st_mpolyfromwkb"
	case sqlparser.MultiLinestringFromWKB:
		return "st_mlinefromwkb"
	}
	return "st_geomfromwkb"
}

// toSpatialOptionStr converts a value to a string suitable for use as a spatial function option.
// For HexBytes (x'...' hex literals), the hex digits are decoded to actual binary bytes.
// This allows null bytes and other binary content to be properly handled during option parsing.
// For other types, behaves like toString.
func toSpatialOptionStr(v interface{}) string {
	if hb, ok := v.(HexBytes); ok {
		// Decode hex digits to actual binary bytes.
		decoded, err := hex.DecodeString(string(hb))
		if err != nil {
			// If decode fails, fall through to toString behavior.
			return toString(v)
		}
		return string(decoded)
	}
	return toString(v)
}

// truncateAtNull returns the string up to (but not including) the first null byte.
// Used for display in error messages where MySQL uses C-string display behavior.
func truncateAtNull(s string) string {
	if idx := strings.IndexByte(s, 0); idx >= 0 {
		return s[:idx]
	}
	return s
}

// parseWKBAxisOrderOption parses the axis-order option string from WKB functions.
// Returns (needsSwap bool, err error).
// The option string format is a comma-separated list of "key=value" pairs (whitespace trimmed).
// MySQL supports only one option key: "axis-order" with values "long-lat", "lat-long", "srid-defined".
// An empty or whitespace-only option string is valid and means "no axis-order option" (no swap).
func parseWKBAxisOrderOption(optStr string, sridIsLatLong bool, fnName string) (bool, error) {
	trimmed := strings.TrimSpace(optStr)
	// Empty or whitespace-only → valid, no axis-order (default long-lat, no swap)
	if trimmed == "" {
		return false, nil
	}
	// Check for invalid start character '='
	if strings.HasPrefix(trimmed, "=") {
		return false, mysqlError(3516, "22023", fmt.Sprintf("The options argument in function %s starts with the invalid character '='.", fnName))
	}
	// Check for double-equals '==' (invalid character sequence)
	if strings.Contains(trimmed, "==") {
		return false, mysqlError(3516, "22023", fmt.Sprintf("The options argument in function %s contains the invalid character sequence '=='.", fnName))
	}
	// Check for invalid end character '='
	if strings.HasSuffix(trimmed, "=") {
		return false, mysqlError(3516, "22023", fmt.Sprintf("The options argument in function %s ends with the invalid character '='.", fnName))
	}
	// Split by comma for multiple key-value pairs
	parts := strings.Split(trimmed, ",")
	axisOrderSeen := false
	needsSwap := false
	for _, part := range parts {
		kv := strings.TrimSpace(part)
		if kv == "" {
			continue
		}
		// Must contain exactly one '=' to be a valid key=value pair
		eqIdx := strings.Index(kv, "=")
		if eqIdx < 0 {
			return false, mysqlError(3516, "22023", fmt.Sprintf("The string '%s' is not a valid key = value pair in function %s.", truncateAtNull(kv), fnName))
		}
		key := strings.TrimSpace(kv[:eqIdx])
		value := strings.TrimSpace(kv[eqIdx+1:])
		keyLower := strings.ToLower(key)
		// Validate key name
		if keyLower != "axis-order" {
			return false, mysqlError(3516, "22023", fmt.Sprintf("Invalid option key '%s' in function %s.", key, fnName))
		}
		// Check for duplicate axis-order key
		if axisOrderSeen {
			// MySQL uses "funtion" (typo) for duplicate key error
			return false, mysqlError(3516, "22023", fmt.Sprintf("Duplicate option key 'axis-order' in funtion '%s'.", fnName))
		}
		axisOrderSeen = true
		// Validate value and compute needsSwap.
		// The formula is: needsSwap = axisIsLatLong XOR sridIsLatLong
		// This accounts for both the WKB input axis-order and the SRS's native output axis-order.
		// For geographic SRS with lat-long ordering, the output is in (lat, long) order,
		// so if the WKB is also in lat-long order, no swap is needed (XOR = false).
		valueLower := strings.ToLower(value)
		switch valueLower {
		case "long-lat":
			// axisIsLatLong = false; needsSwap = false XOR sridIsLatLong = sridIsLatLong
			needsSwap = sridIsLatLong
		case "lat-long":
			// axisIsLatLong = true; needsSwap = true XOR sridIsLatLong = !sridIsLatLong
			needsSwap = !sridIsLatLong
		case "srid-defined":
			// axisIsLatLong = sridIsLatLong; needsSwap = sridIsLatLong XOR sridIsLatLong = false
			needsSwap = false
		default:
			return false, mysqlError(3516, "22023", fmt.Sprintf("Invalid value '%s' for option 'axis-order' in function '%s'.", value, fnName))
		}
	}
	return needsSwap, nil
}

// axisOrderEnum represents the axis-order option value.
type axisOrderEnum int8

const (
	axisOrderLongLat    axisOrderEnum = 0 // long-lat (default/LONG-LAT)
	axisOrderLatLong    axisOrderEnum = 1 // lat-long
	axisOrderSridDefined axisOrderEnum = 2 // srid-defined
)

// parseAxisOrderStr parses an axis-order option string using the same format validation
// as parseWKBAxisOrderOption but returns an axisOrderEnum instead of a bool.
// This is used for ST_ASWKB which has different swap semantics.
// An empty or whitespace-only string returns (axisOrderSridDefined, nil) (default behavior).
func parseAxisOrderStr(optStr string, fnName string) (axisOrderEnum, error) {
	trimmed := strings.TrimSpace(optStr)
	if trimmed == "" {
		return axisOrderSridDefined, nil
	}
	// Check for invalid leading/trailing/double characters at the full-string level.
	if strings.HasPrefix(trimmed, "=") {
		return 0, mysqlError(3516, "22023", fmt.Sprintf("The options argument in function %s starts with the invalid character '='.", fnName))
	}
	if strings.Contains(trimmed, "==") {
		return 0, mysqlError(3516, "22023", fmt.Sprintf("The options argument in function %s contains the invalid character sequence '=='.", fnName))
	}
	if strings.HasSuffix(trimmed, "=") {
		return 0, mysqlError(3516, "22023", fmt.Sprintf("The options argument in function %s ends with the invalid character '='.", fnName))
	}
	if strings.HasSuffix(trimmed, ",") {
		return 0, mysqlError(3516, "22023", fmt.Sprintf("The options argument in function %s ends with the invalid character ','.", fnName))
	}
	if strings.HasPrefix(trimmed, ",") {
		return 0, mysqlError(3516, "22023", fmt.Sprintf("The options argument in function %s starts with the invalid character ','.", fnName))
	}
	if strings.Contains(trimmed, ",,") {
		return 0, mysqlError(3516, "22023", fmt.Sprintf("The options argument in function %s contains the invalid character sequence ',,'.", fnName))
	}
	parts := strings.Split(trimmed, ",")
	// Pass 1: structural validation of each segment (leading/trailing '=', no '=' at all,
	// space in value, embedded '=' in value). These errors take priority over semantic errors.
	for _, part := range parts {
		kv := strings.TrimSpace(part)
		if kv == "" {
			continue
		}
		if strings.HasPrefix(kv, "=") {
			return 0, mysqlError(3516, "22023", fmt.Sprintf("The options argument in function %s starts with the invalid character '='.", fnName))
		}
		if strings.HasSuffix(kv, "=") {
			return 0, mysqlError(3516, "22023", fmt.Sprintf("The options argument in function %s ends with the invalid character '='.", fnName))
		}
		eqIdx := strings.Index(kv, "=")
		if eqIdx < 0 {
			return 0, mysqlError(3516, "22023", fmt.Sprintf("The string '%s' is not a valid key = value pair in function %s.", truncateAtNull(kv), fnName))
		}
		value := strings.TrimSpace(kv[eqIdx+1:])
		// If the value contains spaces or embedded '=', the segment is not a valid key=value pair.
		if strings.ContainsAny(value, " \t") || strings.Contains(value, "=") {
			return 0, mysqlError(3516, "22023", fmt.Sprintf("The string '%s' is not a valid key = value pair in function %s.", truncateAtNull(kv), fnName))
		}
	}
	// Pass 2: semantic validation (key name, value, duplicates).
	axisOrderSeen := false
	result := axisOrderSridDefined
	for _, part := range parts {
		kv := strings.TrimSpace(part)
		if kv == "" {
			continue
		}
		eqIdx := strings.Index(kv, "=")
		key := strings.TrimSpace(kv[:eqIdx])
		value := strings.TrimSpace(kv[eqIdx+1:])
		keyLower := strings.ToLower(key)
		if keyLower != "axis-order" {
			return 0, mysqlError(3516, "22023", fmt.Sprintf("Invalid option key '%s' in function %s.", key, fnName))
		}
		if axisOrderSeen {
			return 0, mysqlError(3516, "22023", fmt.Sprintf("Duplicate option key 'axis-order' in funtion '%s'.", fnName))
		}
		axisOrderSeen = true
		valueLower := strings.ToLower(value)
		switch valueLower {
		case "long-lat":
			result = axisOrderLongLat
		case "lat-long":
			result = axisOrderLatLong
		case "srid-defined":
			result = axisOrderSridDefined
		default:
			return 0, mysqlError(3516, "22023", fmt.Sprintf("Invalid value '%s' for option 'axis-order' in function '%s'.", value, fnName))
		}
	}
	return result, nil
}

// validateWKTCoordsForGeographicSRS checks that all coordinate pairs in a WKT geometry
// are within valid geographic bounds for the given SRS.
// For LONG-LAT SRS: X is longitude in [-180, 180], Y is latitude in [-90, 90].
// For LAT-LONG SRS: X is latitude in [-90, 90], Y is longitude in [-180, 180].
// Returns a MySQL error if any coordinate is out of range.
func validateWKTCoordsForGeographicSRS(wkt string, isLatLongSRS bool, fnName string) error {
	// Parse all coordinate pairs from the WKT.
	// Each coordinate pair is "X Y" where X and Y are float64 numbers.
	// We scan through the WKT looking for number pairs inside parentheses.
	plain := strings.TrimSpace(geomStripSRID(wkt))
	// Remove geometry type prefix (e.g., "POINT", "LINESTRING", etc.)
	// Focus on extracting all (x y) pairs.
	return validateCoordPairsInWKT(plain, isLatLongSRS, fnName)
}

// validateCoordPairsInWKT recursively validates coordinate pairs in a WKT geometry string.
func validateCoordPairsInWKT(wkt string, isLatLongSRS bool, fnName string) error {
	// Find all number pairs separated by spaces within parentheses.
	// Strategy: scan through the string looking for digits/minus signs followed by space + digit/minus.
	// Simple approach: find the innermost parenthetical content and scan for coordinate pairs.
	i := 0
	n := len(wkt)
	for i < n {
		// Skip until we find a digit or minus sign that could start a coordinate
		if wkt[i] == '(' || wkt[i] == ',' {
			i++
			continue
		}
		if wkt[i] == ')' {
			i++
			continue
		}
		// Try to read a coordinate pair: X Y
		if wkt[i] == '-' || (wkt[i] >= '0' && wkt[i] <= '9') {
			// Read first number (X)
			x, xLen := parseFloatFromWKT(wkt[i:])
			if xLen <= 0 {
				i++
				continue
			}
			j := i + xLen
			// Skip spaces
			for j < n && wkt[j] == ' ' {
				j++
			}
			if j >= n {
				break
			}
			// Read second number (Y)
			if wkt[j] == '-' || (wkt[j] >= '0' && wkt[j] <= '9') {
				y, yLen := parseFloatFromWKT(wkt[j:])
				if yLen > 0 {
					// Validate the coordinate pair
					var lon, lat float64
					if isLatLongSRS {
						// X is latitude, Y is longitude
						lat = x
						lon = y
					} else {
						// X is longitude, Y is latitude (long-lat ordering)
						lon = x
						lat = y
					}
					if lon < -180.0 || lon > 180.0 {
						return mysqlError(3617, "22S02", fmt.Sprintf(
							"Longitude %f is out of range in function %s. It must be within (-180.000000, 180.000000].",
							lon, fnName))
					}
					if lat < -90.0 || lat > 90.0 {
						return mysqlError(3616, "22S03", fmt.Sprintf(
							"Latitude %f is out of range in function %s. It must be within [-90.000000, 90.000000].",
							lat, fnName))
					}
					i = j + yLen
					continue
				}
			}
			i = j
			continue
		}
		i++
	}
	return nil
}

// validateGeogCoordsForSpatialPredicate checks that all coordinate pairs in a WKT geometry
// are within valid geographic bounds for a geographic SRS, using the MySQL error message format
// for spatial predicate functions (ST_Contains, MBRContains, etc.).
// For LAT-LONG SRS (isLatLongSRS=true): X is latitude in [-90,90], Y is longitude in (-180,180].
// For LONG-LAT SRS (isLatLongSRS=false): X is longitude in (-180,180], Y is latitude in [-90,90].
// fnName should be the lowercase function name (e.g. "st_contains").
func validateGeogCoordsForSpatialPredicate(wkt string, isLatLongSRS bool, fnName string) error {
	plain := strings.TrimSpace(geomStripSRID(wkt))
	i := 0
	n := len(plain)
	for i < n {
		if plain[i] == '(' || plain[i] == ',' || plain[i] == ')' {
			i++
			continue
		}
		if plain[i] == '-' || (plain[i] >= '0' && plain[i] <= '9') {
			x, xLen := parseFloatFromWKT(plain[i:])
			if xLen <= 0 {
				i++
				continue
			}
			j := i + xLen
			for j < n && plain[j] == ' ' {
				j++
			}
			if j < n && (plain[j] == '-' || (plain[j] >= '0' && plain[j] <= '9')) {
				y, yLen := parseFloatFromWKT(plain[j:])
				if yLen > 0 {
					var lon, lat float64
					if isLatLongSRS {
						lat = x
						lon = y
					} else {
						lon = x
						lat = y
					}
					if lat < -90.0 || lat > 90.0 {
						return mysqlError(3616, "22S03", fmt.Sprintf(
							"A parameter of function %s contains a geometry with latitude %f, which is out of range. It must be within [-90.000000, 90.000000].",
							fnName, lat))
					}
					if lon < -180.0 || lon > 180.0 {
						return mysqlError(3617, "22S02", fmt.Sprintf(
							"A parameter of function %s contains a geometry with longitude %f, which is out of range. It must be within (-180.000000, 180.000000].",
							fnName, lon))
					}
					i = j + yLen
					continue
				}
			}
			i = j
			continue
		}
		i++
	}
	return nil
}

// geomValueToEWKT converts a geometry value (either []byte/HexBytes WKB or string WKT) to an EWKT string.
// For binary WKB input ([]byte or HexBytes hex string), extracts the SRID and WKT using wkbToWKTAndSRID.
// Returns the EWKT string and the parsed SRID.
func geomValueToEWKT(v interface{}) (string, uint32) {
	var binData []byte
	switch val := v.(type) {
	case []byte:
		binData = val
	case HexBytes:
		// HexBytes stores raw hex digits (e.g. "E6100000..."); decode to actual bytes.
		if decoded, err := hex.DecodeString(string(val)); err == nil {
			binData = decoded
		}
	}
	if binData != nil {
		wkt, srid := wkbToWKTAndSRID(binData)
		if wkt == "" {
			return toString(v), 0
		}
		if srid == 0 {
			return wkt, 0
		}
		return "SRID=" + strconv.FormatUint(uint64(srid), 10) + ";" + wkt, srid
	}
	s := toString(v)
	return s, geomGetSRID(s)
}

// parseFloatFromWKT parses a floating-point number from the start of s.
// Returns the parsed float64 and the number of characters consumed.
// Returns (0, 0) if no number could be parsed.
func parseFloatFromWKT(s string) (float64, int) {
	i := 0
	n := len(s)
	if i < n && s[i] == '-' {
		i++
	}
	hasDigit := false
	for i < n && s[i] >= '0' && s[i] <= '9' {
		hasDigit = true
		i++
	}
	if i < n && s[i] == '.' {
		i++
		for i < n && s[i] >= '0' && s[i] <= '9' {
			hasDigit = true
			i++
		}
	}
	if i < n && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < n && (s[i] == '+' || s[i] == '-') {
			i++
		}
		for i < n && s[i] >= '0' && s[i] <= '9' {
			i++
		}
	}
	if !hasDigit {
		return 0, 0
	}
	f, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, 0
	}
	return f, i
}

// wktToWKB converts a WKT geometry string (e.g. "POINT(1 2)") to MySQL's internal
// binary geometry format: 4-byte SRID (little-endian) + WKB (ISO).
// Returns nil if parsing fails. Handles EWKT SRID prefix.
func wktToWKB(wkt string) []byte {
	// Extract SRID from EWKT prefix if present, then delegate to wktToWKBWithSRID
	srid := geomGetSRID(wkt)
	return wktToWKBWithSRID(wkt, srid)
}

// wktToWKBWithSRID converts a WKT geometry string to MySQL's internal binary
// format: [srid:4 bytes LE][WKB body]. Returns nil if parsing fails.
// The srid parameter is used as the SRID prefix; any EWKT SRID in wkt is stripped by wktToWKBBody.
func wktToWKBWithSRID(wkt string, srid uint32) []byte {
	wkt = strings.TrimSpace(wkt)
	wkbBody := wktToWKBBody(wkt) // strips EWKT SRID prefix internally
	if wkbBody == nil {
		return nil
	}
	// Prepend 4-byte SRID (little-endian)
	buf := make([]byte, 4+len(wkbBody))
	binary.LittleEndian.PutUint32(buf[0:4], srid)
	copy(buf[4:], wkbBody)
	return buf
}

// readSRIDFromBinary reads the 4-byte little-endian SRID prefix from binary geometry data.
// Returns (srid, hasSRID) where hasSRID is true if the data appears to have a valid SRID prefix.
// MySQL geometry binary format: [srid:4 LE][wkb_body]
// WKB body starts with a byte order marker (0x00 or 0x01).
func readSRIDFromBinary(data []byte) (uint32, bool) {
	if len(data) < 5 {
		return 0, false
	}
	// Check if byte 4 is a valid WKB byte order marker (0x00=big-endian, 0x01=little-endian)
	if data[4] == 0x00 || data[4] == 0x01 {
		srid := binary.LittleEndian.Uint32(data[0:4])
		return srid, true
	}
	return 0, false
}

// replaceSRIDInBinary replaces the 4-byte SRID prefix in binary geometry data.
// Returns nil if data is too short or malformed.
func replaceSRIDInBinary(data []byte, newSRID uint32) []byte {
	if len(data) < 5 {
		return nil
	}
	// Verify byte 4 is a valid WKB byte order marker
	if data[4] != 0x00 && data[4] != 0x01 {
		return nil
	}
	// Must have at least SRID(4) + byteOrder(1) + geomType(4) = 9 bytes minimum
	if len(data) < 9 {
		return nil
	}
	result := make([]byte, len(data))
	copy(result, data)
	binary.LittleEndian.PutUint32(result[0:4], newSRID)
	return result
}

// isGeographicSRID returns true if the given SRID uses geographic coordinates
// (latitude/longitude) where ST_AsText should swap X/Y for display.
// MySQL only supports SRID 4326 (WGS84) as a built-in geographic SRS.
func isGeographicSRID(srid uint32) bool {
	return srid == 4326
}

// wktToWKBBody converts a WKT geometry to pure WKB (ISO WKB, no SRID prefix).
// This is used both for the top-level conversion and for sub-geometries in collections.
// Returns nil if parsing fails. Strips EWKT SRID prefix if present.
func wktToWKBBody(wkt string) []byte {
	wkt = geomStripSRID(strings.TrimSpace(wkt))
	upper := strings.ToUpper(wkt)
	switch {
	case strings.HasPrefix(upper, "MULTIPOLYGON"):
		return wktMultiPolygonToWKB(wkt)
	case strings.HasPrefix(upper, "MULTILINESTRING"):
		return wktMultiLineStringToWKB(wkt)
	case strings.HasPrefix(upper, "MULTIPOINT"):
		return wktMultiPointToWKB(wkt)
	case strings.HasPrefix(upper, "GEOMETRYCOLLECTION"), strings.HasPrefix(upper, "GEOMCOLLECTION"):
		return wktGeomCollToWKB(wkt)
	case strings.HasPrefix(upper, "POLYGON"):
		return wktPolygonToWKB(wkt)
	case strings.HasPrefix(upper, "LINESTRING"):
		return wktLineStringToWKB(wkt)
	case strings.HasPrefix(upper, "POINT"):
		return wktPointToWKB(wkt)
	}
	return nil
}

// wktPointToWKB encodes a WKT POINT to WKB bytes (no SRID).
func wktPointToWKB(wkt string) []byte {
	coords := parseSpatialPointCoords(wkt)
	if len(coords) < 2 {
		return nil
	}
	buf := make([]byte, 1+4+8+8) // byteOrder + type + X + Y
	buf[0] = 1                   // little-endian
	binary.LittleEndian.PutUint32(buf[1:5], 1) // type = POINT
	binary.LittleEndian.PutUint64(buf[5:13], math.Float64bits(coords[0]))
	binary.LittleEndian.PutUint64(buf[13:21], math.Float64bits(coords[1]))
	return buf
}

// wktLineStringToWKB encodes a WKT LINESTRING to WKB bytes (no SRID).
func wktLineStringToWKB(wkt string) []byte {
	pts := parseLineStringPoints(wkt)
	if pts == nil {
		return nil
	}
	n := len(pts)
	buf := make([]byte, 1+4+4+n*16)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], 2) // type = LINESTRING
	binary.LittleEndian.PutUint32(buf[5:9], uint32(n))
	for i, p := range pts {
		off := 9 + i*16
		binary.LittleEndian.PutUint64(buf[off:off+8], math.Float64bits(p[0]))
		binary.LittleEndian.PutUint64(buf[off+8:off+16], math.Float64bits(p[1]))
	}
	return buf
}

// wktPolygonToWKB encodes a WKT POLYGON to WKB bytes (no SRID).
func wktPolygonToWKB(wkt string) []byte {
	rings := parsePolygonRings(wkt)
	if rings == nil {
		return nil
	}
	// Calculate total size
	size := 1 + 4 + 4 // byteOrder + type + numRings
	for _, ring := range rings {
		size += 4 + len(ring)*16 // numPoints + points
	}
	buf := make([]byte, size)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], 3) // type = POLYGON
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(rings)))
	off := 9
	for _, ring := range rings {
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(ring)))
		off += 4
		for _, p := range ring {
			binary.LittleEndian.PutUint64(buf[off:off+8], math.Float64bits(p[0]))
			binary.LittleEndian.PutUint64(buf[off+8:off+16], math.Float64bits(p[1]))
			off += 16
		}
	}
	return buf
}

// wktMultiPointToWKB encodes a WKT MULTIPOINT to WKB bytes (no SRID).
func wktMultiPointToWKB(wkt string) []byte {
	// Parse points from MULTIPOINT
	upper := strings.TrimSpace(strings.ToUpper(wkt))
	_ = upper
	idx := strings.Index(wkt, "(")
	end := strings.LastIndex(wkt, ")")
	if idx < 0 || end <= idx {
		return nil
	}
	inner := strings.TrimSpace(wkt[idx+1 : end])

	// Parse comma-separated points (possibly wrapped in parens)
	var points [][]float64
	depth := 0
	start := 0
	for i, ch := range inner {
		switch ch {
		case '(':
			if depth == 0 {
				start = i + 1
			}
			depth++
		case ')':
			depth--
			if depth == 0 {
				// Extract point inside parens
				ptStr := strings.TrimSpace(inner[start:i])
				fields := strings.Fields(ptStr)
				if len(fields) >= 2 {
					x, _ := strconv.ParseFloat(fields[0], 64)
					y, _ := strconv.ParseFloat(fields[1], 64)
					points = append(points, []float64{x, y})
				}
			}
		}
	}
	// If no nested parens, parse as flat coordinate list
	if len(points) == 0 {
		for _, part := range strings.Split(inner, ",") {
			fields := strings.Fields(strings.TrimSpace(part))
			if len(fields) >= 2 {
				x, _ := strconv.ParseFloat(fields[0], 64)
				y, _ := strconv.ParseFloat(fields[1], 64)
				points = append(points, []float64{x, y})
			}
		}
	}

	n := len(points)
	// Each sub-geometry is a WKB POINT: 1+4+8+8 = 21 bytes
	buf := make([]byte, 1+4+4+n*21)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], 4) // type = MULTIPOINT
	binary.LittleEndian.PutUint32(buf[5:9], uint32(n))
	off := 9
	for _, p := range points {
		buf[off] = 1 // byte order
		binary.LittleEndian.PutUint32(buf[off+1:off+5], 1) // POINT
		binary.LittleEndian.PutUint64(buf[off+5:off+13], math.Float64bits(p[0]))
		binary.LittleEndian.PutUint64(buf[off+13:off+21], math.Float64bits(p[1]))
		off += 21
	}
	return buf
}

// wktMultiLineStringToWKB encodes a WKT MULTILINESTRING to WKB bytes (no SRID).
func wktMultiLineStringToWKB(wkt string) []byte {
	linestrings := parseMultiSubGeometries(wkt, "LINESTRING")
	if linestrings == nil {
		return nil
	}
	var subWKBs [][]byte
	for _, ls := range linestrings {
		sub := wktLineStringToWKB(ls)
		if sub == nil {
			return nil
		}
		subWKBs = append(subWKBs, sub)
	}
	return buildMultiWKB(5, subWKBs) // type = MULTILINESTRING
}

// wktMultiPolygonToWKB encodes a WKT MULTIPOLYGON to WKB bytes (no SRID).
func wktMultiPolygonToWKB(wkt string) []byte {
	polygons := parseMultiPolygonSubs(wkt)
	if polygons == nil {
		return nil
	}
	var subWKBs [][]byte
	for _, poly := range polygons {
		sub := wktPolygonToWKB(poly)
		if sub == nil {
			return nil
		}
		subWKBs = append(subWKBs, sub)
	}
	return buildMultiWKB(6, subWKBs) // type = MULTIPOLYGON
}

// wktGeomCollToWKB encodes a WKT GEOMETRYCOLLECTION to WKB bytes (no SRID).
func wktGeomCollToWKB(wkt string) []byte {
	geoms := parseGeomCollection(wkt)
	var subWKBs [][]byte
	for _, g := range geoms {
		sub := wktToWKBBody(g)
		if sub == nil {
			return nil
		}
		subWKBs = append(subWKBs, sub)
	}
	return buildMultiWKB(7, subWKBs) // type = GEOMETRYCOLLECTION
}

// buildMultiWKB builds a WKB multi-geometry from sub-geometry WKB bodies.
// typeCode is the WKB type for the multi-geometry.
func buildMultiWKB(typeCode uint32, subWKBs [][]byte) []byte {
	totalSize := 1 + 4 + 4 // byteOrder + type + numGeometries
	for _, sub := range subWKBs {
		totalSize += len(sub)
	}
	buf := make([]byte, totalSize)
	buf[0] = 1
	binary.LittleEndian.PutUint32(buf[1:5], typeCode)
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(subWKBs)))
	off := 9
	for _, sub := range subWKBs {
		copy(buf[off:], sub)
		off += len(sub)
	}
	return buf
}

// parseMultiSubGeometries parses sub-geometries from MULTILINESTRING WKT.
// Returns strings like "LINESTRING(0 1,2 3)" for each sub-geometry.
func parseMultiSubGeometries(wkt string, prefix string) []string {
	idx := strings.Index(wkt, "(")
	end := strings.LastIndex(wkt, ")")
	if idx < 0 || end <= idx {
		return nil
	}
	inner := strings.TrimSpace(wkt[idx+1 : end])
	// Split at depth 0 commas, each part is a ring "(...)"
	var result []string
	depth := 0
	start := 0
	for i, ch := range inner {
		switch ch {
		case '(':
			if depth == 0 {
				start = i
			}
			depth++
		case ')':
			depth--
			if depth == 0 {
				ring := strings.TrimSpace(inner[start : i+1])
				result = append(result, prefix+ring)
			}
		}
	}
	return result
}

// parseMultiPolygonSubs parses sub-polygons from MULTIPOLYGON WKT.
// Returns strings like "POLYGON((0 1,2 3,...))" for each sub-geometry.
func parseMultiPolygonSubs(wkt string) []string {
	idx := strings.Index(wkt, "(")
	end := strings.LastIndex(wkt, ")")
	if idx < 0 || end <= idx {
		return nil
	}
	inner := strings.TrimSpace(wkt[idx+1 : end])
	// Each sub-polygon starts with "(("
	var result []string
	depth := 0
	start := -1
	for i, ch := range inner {
		switch ch {
		case '(':
			if depth == 0 {
				start = i
			}
			depth++
		case ')':
			depth--
			if depth == 0 && start >= 0 {
				poly := strings.TrimSpace(inner[start : i+1])
				result = append(result, "POLYGON"+poly)
				start = -1
			}
		}
	}
	return result
}

// wkbToWKT converts WKB bytes to a WKT string.
// Input may optionally have a 4-byte SRID prefix (MySQL internal format).
// Returns "" if parsing fails.
// For geographic SRIDs (e.g. 4326), X and Y coordinates are swapped in the output
// to match MySQL's ST_AsText behavior (latitude, longitude order).
func wkbToWKT(data []byte) string {
	wkt, _ := wkbToWKTAndSRID(data)
	return wkt
}

// wkbToWKTAndSRID converts WKB bytes to a WKT string, also returning the SRID.
// Input may optionally have a 4-byte SRID prefix (MySQL internal format).
// Returns ("", 0) if parsing fails.
// For geographic SRIDs (e.g. 4326), X and Y coordinates are swapped in the output.
func wkbToWKTAndSRID(data []byte) (string, uint32) {
	if len(data) == 0 {
		return "", 0
	}
	// Detect whether the input is MySQL-internal format [srid:4 LE][WKB] or pure ISO WKB.
	//
	// Pure WKB always starts with a byte-order marker: 0x00 (big-endian) or 0x01 (little-endian).
	// MySQL-internal format has a 4-byte SRID prefix followed by the byte-order marker at offset 4.
	//
	// Ambiguous case: SRID=0 prefix is 00 00 00 00, so data[0]=0x00, which is also a valid WKB
	// byte-order marker. In this case we must use another heuristic to decide.
	//
	// Strategy:
	//   1. If data[0] is NOT a valid byte-order marker (not 0x00/0x01): must be SRID-prefixed.
	//   2. If data[0] IS a valid byte-order marker:
	//      a. Try parsing as pure WKB from offset 0. If it consumes exactly all bytes, it's pure WKB.
	//      b. Otherwise, try parsing as SRID-prefixed (offset 4).

	srid := uint32(0)

	if data[0] != 0x00 && data[0] != 0x01 {
		// Definitely SRID-prefixed: data[0] is not a WKB byte-order marker.
		if len(data) < 9 {
			return "", 0
		}
		srid = binary.LittleEndian.Uint32(data[0:4])
		offset := 4
		wkt, ok := parseWKBWithSRID(data, &offset, srid)
		if !ok {
			return "", 0
		}
		return wkt, srid
	}

	// data[0] is 0x00 or 0x01 — could be pure WKB or SRID=0 prefix.
	// Try parsing as pure WKB first: if it exactly consumes all bytes, treat as pure WKB.
	offset0 := 0
	wkt0, ok0 := parseWKB(data, &offset0)
	if ok0 && offset0 == len(data) {
		// Successfully parsed all bytes as pure WKB.
		return wkt0, 0
	}

	// Pure WKB parse failed or didn't consume all bytes. Try as SRID-prefixed.
	if len(data) >= 9 && (data[4] == 0x00 || data[4] == 0x01) {
		srid = binary.LittleEndian.Uint32(data[0:4])
		offset4 := 4
		wkt4, ok4 := parseWKBWithSRID(data, &offset4, srid)
		if ok4 {
			return wkt4, srid
		}
	}

	// Both attempts failed. Try the original pure WKB parse result even if incomplete
	// (e.g. collections with trailing data).
	if ok0 {
		return wkt0, 0
	}

	return "", 0
}

// parseWKBWithSRID parses WKB at data[*offset] and advances offset.
// If the SRID indicates a geographic SRS, X and Y coordinates are swapped in output.
// Returns the WKT string and true if successful.
func parseWKBWithSRID(data []byte, offset *int, srid uint32) (string, bool) {
	swapXY := isGeographicSRID(srid)
	return parseWKBSwap(data, offset, swapXY)
}

// parseWKB parses WKB at data[*offset] and advances offset.
// Returns the WKT string and true if successful.
func parseWKB(data []byte, offset *int) (string, bool) {
	return parseWKBSwap(data, offset, false)
}

// parseWKBSwap parses WKB at data[*offset] and advances offset.
// If swapXY is true, X and Y coordinates are swapped in the output WKT.
// Returns the WKT string and true if successful.
func parseWKBSwap(data []byte, offset *int, swapXY bool) (string, bool) {
	if *offset+5 > len(data) {
		return "", false
	}
	byteOrder := data[*offset]
	*offset++
	if byteOrder != 0 && byteOrder != 1 {
		return "", false
	}
	littleEndian := byteOrder == 1

	geomType := readUint32(data, offset, littleEndian)

	switch geomType {
	case 1: // POINT
		return parseWKBPointSwap(data, offset, littleEndian, swapXY)
	case 2: // LINESTRING
		return parseWKBLineStringSwap(data, offset, littleEndian, swapXY)
	case 3: // POLYGON
		return parseWKBPolygonSwap(data, offset, littleEndian, swapXY)
	case 4: // MULTIPOINT
		return parseWKBMultiSwap(data, offset, littleEndian, "MULTIPOINT", true, swapXY)
	case 5: // MULTILINESTRING
		return parseWKBMultiSwap(data, offset, littleEndian, "MULTILINESTRING", false, swapXY)
	case 6: // MULTIPOLYGON
		return parseWKBMultiSwap(data, offset, littleEndian, "MULTIPOLYGON", false, swapXY)
	case 7: // GEOMETRYCOLLECTION
		return parseWKBGeomCollSwap(data, offset, littleEndian, swapXY)
	}
	return "", false
}

func readUint32(data []byte, offset *int, littleEndian bool) uint32 {
	if *offset+4 > len(data) {
		return 0
	}
	var v uint32
	if littleEndian {
		v = binary.LittleEndian.Uint32(data[*offset : *offset+4])
	} else {
		v = binary.BigEndian.Uint32(data[*offset : *offset+4])
	}
	*offset += 4
	return v
}

func readFloat64(data []byte, offset *int, littleEndian bool) float64 {
	if *offset+8 > len(data) {
		return 0
	}
	var bits uint64
	if littleEndian {
		bits = binary.LittleEndian.Uint64(data[*offset : *offset+8])
	} else {
		bits = binary.BigEndian.Uint64(data[*offset : *offset+8])
	}
	*offset += 8
	return math.Float64frombits(bits)
}

func parseWKBPoint(data []byte, offset *int, littleEndian bool) (string, bool) {
	return parseWKBPointSwap(data, offset, littleEndian, false)
}

func parseWKBPointSwap(data []byte, offset *int, littleEndian bool, swapXY bool) (string, bool) {
	if *offset+16 > len(data) {
		return "", false
	}
	x := readFloat64(data, offset, littleEndian)
	y := readFloat64(data, offset, littleEndian)
	if swapXY {
		return fmt.Sprintf("POINT(%s %s)", formatSpatialFloat(y), formatSpatialFloat(x)), true
	}
	return fmt.Sprintf("POINT(%s %s)", formatSpatialFloat(x), formatSpatialFloat(y)), true
}

func parseWKBLineString(data []byte, offset *int, littleEndian bool) (string, bool) {
	return parseWKBLineStringSwap(data, offset, littleEndian, false)
}

func parseWKBLineStringSwap(data []byte, offset *int, littleEndian bool, swapXY bool) (string, bool) {
	n := readUint32(data, offset, littleEndian)
	if *offset+int(n)*16 > len(data) {
		return "", false
	}
	var parts []string
	for i := uint32(0); i < n; i++ {
		x := readFloat64(data, offset, littleEndian)
		y := readFloat64(data, offset, littleEndian)
		if swapXY {
			parts = append(parts, fmt.Sprintf("%s %s", formatSpatialFloat(y), formatSpatialFloat(x)))
		} else {
			parts = append(parts, fmt.Sprintf("%s %s", formatSpatialFloat(x), formatSpatialFloat(y)))
		}
	}
	return fmt.Sprintf("LINESTRING(%s)", strings.Join(parts, ",")), true
}

func parseWKBPolygon(data []byte, offset *int, littleEndian bool) (string, bool) {
	return parseWKBPolygonSwap(data, offset, littleEndian, false)
}

func parseWKBPolygonSwap(data []byte, offset *int, littleEndian bool, swapXY bool) (string, bool) {
	numRings := readUint32(data, offset, littleEndian)
	var rings []string
	for r := uint32(0); r < numRings; r++ {
		n := readUint32(data, offset, littleEndian)
		if *offset+int(n)*16 > len(data) {
			return "", false
		}
		var pts []string
		for i := uint32(0); i < n; i++ {
			x := readFloat64(data, offset, littleEndian)
			y := readFloat64(data, offset, littleEndian)
			if swapXY {
				pts = append(pts, fmt.Sprintf("%s %s", formatSpatialFloat(y), formatSpatialFloat(x)))
			} else {
				pts = append(pts, fmt.Sprintf("%s %s", formatSpatialFloat(x), formatSpatialFloat(y)))
			}
		}
		rings = append(rings, "("+strings.Join(pts, ",")+")")
	}
	return fmt.Sprintf("POLYGON(%s)", strings.Join(rings, ",")), true
}

// parseWKBMulti parses WKB for MULTIPOINT, MULTILINESTRING, MULTIPOLYGON.
// wrapSubsInParens controls whether sub-geometry WKTs get wrapped in extra parens
// (needed for MULTIPOINT display: MULTIPOINT((0 1),(2 3))).
func parseWKBMulti(data []byte, offset *int, littleEndian bool, typeName string, wrapSubsInParens bool) (string, bool) {
	return parseWKBMultiSwap(data, offset, littleEndian, typeName, wrapSubsInParens, false)
}

func parseWKBMultiSwap(data []byte, offset *int, littleEndian bool, typeName string, wrapSubsInParens bool, swapXY bool) (string, bool) {
	n := readUint32(data, offset, littleEndian)
	var parts []string
	for i := uint32(0); i < n; i++ {
		// Each sub-geometry has its own WKB header
		wkt, ok := parseWKBSwap(data, offset, swapXY)
		if !ok {
			return "", false
		}
		if wrapSubsInParens {
			// For MULTIPOINT, extract coords from POINT(x y) and wrap as (x y)
			coords := parseSpatialPointCoords(wkt)
			if coords != nil {
				parts = append(parts, fmt.Sprintf("(%s %s)", formatSpatialFloat(coords[0]), formatSpatialFloat(coords[1])))
			} else {
				parts = append(parts, wkt)
			}
		} else {
			// For MULTILINESTRING/MULTIPOLYGON, strip the type prefix and keep the coords
			switch typeName {
			case "MULTILINESTRING":
				// wkt is "LINESTRING(0 1,2 3,...)" → extract "0 1,2 3,..."
				parts = append(parts, extractInnerCoords(wkt))
			case "MULTIPOLYGON":
				// wkt is "POLYGON((0 0,...),...)" → extract "((0 0,...),...)"
				parts = append(parts, extractInnerCoords(wkt))
			default:
				parts = append(parts, wkt)
			}
		}
	}
	return fmt.Sprintf("%s(%s)", typeName, strings.Join(parts, ",")), true
}

// parseWKBGeomColl parses WKB for GEOMETRYCOLLECTION.
func parseWKBGeomColl(data []byte, offset *int, littleEndian bool) (string, bool) {
	return parseWKBGeomCollSwap(data, offset, littleEndian, false)
}

func parseWKBGeomCollSwap(data []byte, offset *int, littleEndian bool, swapXY bool) (string, bool) {
	n := readUint32(data, offset, littleEndian)
	if n == 0 {
		return "GEOMETRYCOLLECTION EMPTY", true
	}
	var parts []string
	for i := uint32(0); i < n; i++ {
		wkt, ok := parseWKBSwap(data, offset, swapXY)
		if !ok {
			return "", false
		}
		parts = append(parts, wkt)
	}
	return fmt.Sprintf("GEOMETRYCOLLECTION(%s)", strings.Join(parts, ",")), true
}

// extractInnerCoords strips the type name from a WKT string and returns the coordinate part.
// E.g. "LINESTRING(0 1,2 3)" → "(0 1,2 3)"
// E.g. "POLYGON((0 0,10 10))" → "((0 0,10 10))"
func extractInnerCoords(wkt string) string {
	idx := strings.Index(wkt, "(")
	if idx < 0 {
		return wkt
	}
	return wkt[idx:]
}

// wktTypeEmptyRe matches any geometry type followed by empty parens,
// e.g. "GEOMETRYCOLLECTION()" or "POINT ()" anywhere in a WKT string.
var wktTypeEmptyRe = regexp.MustCompile(`(?i)(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(\s*\)`)

// normalizeWKTForSF converts WKT quirks that simplefeatures doesn't accept into
// the canonical form it expects.  Iteratively replaces TYPE() with TYPE EMPTY
// to handle arbitrarily nested empty geometry collections.
func normalizeWKTForSF(wkt string) string {
	prev := ""
	for prev != wkt {
		prev = wkt
		wkt = wktTypeEmptyRe.ReplaceAllStringFunc(wkt, func(m string) string {
			loc := wktTypeEmptyRe.FindStringSubmatchIndex(m)
			typeName := m[loc[2]:loc[3]]
			return strings.ToUpper(typeName) + " EMPTY"
		})
	}
	return wkt
}

// wktToSimpleFeature parses a WKT (or EWKT) string into a simplefeatures Geometry.
// The SRID prefix is stripped before parsing; returns error on invalid WKT.
func wktToSimpleFeature(wkt string) (sfgeom.Geometry, error) {
	plain := geomStripSRID(strings.TrimSpace(wkt))
	plain = normalizeWKTForSF(plain)
	return sfgeom.UnmarshalWKT(plain, sfgeom.NoValidate{})
}

// wktSingleTypeEmptyRe matches single-geometry types followed by empty parens:
// POINT(), LINESTRING(), POLYGON() — these are invalid in MySQL.
var wktSingleTypeEmptyRe = regexp.MustCompile(`(?i)^(POINT|LINESTRING|POLYGON)\s*\(\s*\)$`)

// wktMultiTypeEmptyRe matches multi-geometry types followed by empty parens:
// MULTIPOINT(), MULTILINESTRING(), MULTIPOLYGON(), GEOMETRYCOLLECTION(), GEOMCOLLECTION() — valid empty geometries.
var wktMultiTypeEmptyRe = regexp.MustCompile(`(?i)(MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION|GEOMCOLLECTION)\s*\(\s*\)`)

// normalizeWKTForSFMultiOnly converts empty multi-type geometries (TYPE() -> TYPE EMPTY)
// but does NOT convert single-type empty geometries (POINT(), LINESTRING(), POLYGON()).
// This is used for WKT validation where POINT() should be rejected as invalid.
func normalizeWKTForSFMultiOnly(wkt string) string {
	prev := ""
	for prev != wkt {
		prev = wkt
		wkt = wktMultiTypeEmptyRe.ReplaceAllStringFunc(wkt, func(m string) string {
			loc := wktMultiTypeEmptyRe.FindStringSubmatchIndex(m)
			typeName := m[loc[2]:loc[3]]
			return strings.ToUpper(typeName) + " EMPTY"
		})
	}
	return wkt
}

// countCoordPairs counts the number of valid "x y" coordinate pairs in a ring string.
// The ringStr should be the bare coordinate content without wrapping parens, e.g. "x1 y1,x2 y2,...".
func countCoordPairs(ringStr string) int {
	count := 0
	for _, part := range strings.Split(ringStr, ",") {
		fields := strings.Fields(strings.TrimSpace(part))
		if len(fields) >= 2 {
			_, e1 := strconv.ParseFloat(fields[0], 64)
			_, e2 := strconv.ParseFloat(fields[1], 64)
			if e1 == nil && e2 == nil {
				count++
			}
		}
	}
	return count
}

// extractParenContent extracts the content between the outermost parentheses of a string.
// e.g. "(x1 y1,x2 y2)" → "x1 y1,x2 y2". Returns ("", false) if not wrapped in parens.
func extractParenContent(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return "", false
	}
	return s[1 : len(s)-1], true
}

// iterateParenBlocks iterates over parenthesized sub-blocks at depth=0 within an outer string.
// For each block, fn(content) is called with the content INSIDE the parens (without the parens).
func iterateParenBlocks(s string, fn func(content string) bool) bool {
	depth := 0
	start := -1
	for i, ch := range s {
		switch ch {
		case '(':
			if depth == 0 {
				start = i + 1
			}
			depth++
		case ')':
			depth--
			if depth == 0 && start >= 0 {
				if !fn(s[start:i]) {
					return false
				}
				start = -1
			}
		}
	}
	return true
}

// checkWKTPointCountConstraints validates MySQL's structural point count requirements:
//   - LINESTRING must have >= 2 coordinate pairs
//   - Each POLYGON ring must have >= 4 coordinate pairs (3 unique + closing)
//   - MULTILINESTRING components must each have >= 2 coordinate pairs
//   - MULTIPOLYGON polygon rings must each have >= 4 coordinate pairs
//   - GEOMETRYCOLLECTION/GEOMCOLLECTION: recursively validate components
//
// Returns false if any constraint is violated.
func checkWKTPointCountConstraints(wkt string) bool {
	plain := strings.TrimSpace(geomStripSRID(wkt))
	upper := strings.ToUpper(plain)

	switch {
	case strings.HasPrefix(upper, "GEOMETRYCOLLECTION"), strings.HasPrefix(upper, "GEOMCOLLECTION"):
		// Recursively validate each sub-geometry
		subGeoms := parseGeomCollection(plain)
		for _, sg := range subGeoms {
			if !checkWKTPointCountConstraints(sg) {
				return false
			}
		}
		return true

	case strings.HasPrefix(upper, "MULTIPOLYGON"):
		// Structure: MULTIPOLYGON( (polygon1) , (polygon2) , ... )
		// Each polygon: ( (ring1) , (ring2) , ... )
		// Each ring: ( x1 y1, x2 y2, ... ) — must have >= 4 coord pairs
		idx := strings.Index(plain, "(")
		if idx < 0 {
			return true
		}
		outerContent := plain[idx+1 : strings.LastIndex(plain, ")")]
		// Iterate over polygon blocks (each wrapped in parens)
		return iterateParenBlocks(outerContent, func(polygonContent string) bool {
			// Iterate over ring blocks within each polygon
			return iterateParenBlocks(polygonContent, func(ringContent string) bool {
				if countCoordPairs(ringContent) < 4 {
					return false
				}
				return true
			})
		})

	case strings.HasPrefix(upper, "POLYGON"):
		// Structure: POLYGON( (ring1) , (ring2) , ... )
		// Each ring must have >= 4 coordinate pairs
		idx := strings.Index(plain, "(")
		if idx < 0 {
			return true
		}
		outerContent := plain[idx+1 : strings.LastIndex(plain, ")")]
		return iterateParenBlocks(outerContent, func(ringContent string) bool {
			if countCoordPairs(ringContent) < 4 {
				return false
			}
			return true
		})

	case strings.HasPrefix(upper, "MULTILINESTRING"):
		// Structure: MULTILINESTRING( (line1) , (line2) , ... )
		// Each component line must have >= 2 coordinate pairs
		idx := strings.Index(plain, "(")
		if idx < 0 {
			return true
		}
		outerContent := plain[idx+1 : strings.LastIndex(plain, ")")]
		return iterateParenBlocks(outerContent, func(lineContent string) bool {
			if countCoordPairs(lineContent) < 2 {
				return false
			}
			return true
		})

	case strings.HasPrefix(upper, "LINESTRING"):
		// Must have >= 2 coordinate pairs
		pts := parseLineStringPoints(plain)
		if len(pts) < 2 {
			return false
		}
		return true
	}

	return true
}

// isValidWKTSyntax validates whether a WKT string is syntactically valid according
// to MySQL's rules for ST_GeomFromText. Returns false for invalid WKT such as:
//   - POINT() (no coordinates)
//   - LINESTRING with non-numeric coordinates
//   - Unbalanced parentheses
//   - MULTIPOINT with wrong number of coordinates per point
//   - LINESTRING with fewer than 2 points
//   - POLYGON ring with fewer than 4 coordinate pairs
//
// Valid empty multi-geometries (GEOMETRYCOLLECTION(), MULTIPOINT(), etc.) return true.
func isValidWKTSyntax(wkt string) bool {
	plain := geomStripSRID(strings.TrimSpace(wkt))
	// Normalize MULTIPOINT notation for simplefeatures
	plain = normalizeWKT(plain)
	upper := strings.ToUpper(strings.TrimSpace(plain))
	// Reject single-type empty geometries: POINT(), LINESTRING(), POLYGON()
	if wktSingleTypeEmptyRe.MatchString(plain) {
		return false
	}
	// Handle GEOMCOLLECTION alias
	if strings.HasPrefix(upper, "GEOMCOLLECTION") {
		plain = "GEOMETRYCOLLECTION" + plain[14:]
		upper = strings.ToUpper(plain)
	}
	// Check balanced parentheses
	depth := 0
	for _, ch := range plain {
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	if depth != 0 {
		return false
	}
	// Check MySQL structural point count constraints (LINESTRING >= 2 pts, POLYGON ring >= 4 pts)
	if !checkWKTPointCountConstraints(plain) {
		return false
	}
	// Convert multi-type empty geometries to "TYPE EMPTY" for simplefeatures
	normalized := normalizeWKTForSFMultiOnly(plain)
	_, err := sfgeom.UnmarshalWKT(normalized, sfgeom.NoValidate{})
	return err == nil
}

// safeSFOp calls fn() and recovers from any panic, returning it as an error.
func safeSFOp(fn func() (sfgeom.Geometry, error)) (result sfgeom.Geometry, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn()
}

// evalSpatialSetOp performs the named set operation (st_union, st_intersection,
// st_difference, st_symdifference) on two WKT geometry strings and returns the
// result as a WKT string.  The SRID of the first geometry is preserved.
func evalSpatialSetOp(op, aWKT, bWKT string) (interface{}, error) {
	// Preserve the SRID from the first argument
	srid := geomGetSRID(aWKT)

	a, err := wktToSimpleFeature(aWKT)
	if err != nil {
		return nil, fmt.Errorf("%s: invalid geometry A: %w", op, err)
	}
	b, err := wktToSimpleFeature(bWKT)
	if err != nil {
		return nil, fmt.Errorf("%s: invalid geometry B: %w", op, err)
	}

	// gcFallbackUnion tries UnaryUnion on a GeometryCollection containing both inputs,
	// which is more robust for some degenerate geometries.
	gcFallbackUnion := func() (sfgeom.Geometry, error) {
		gc := sfgeom.NewGeometryCollection([]sfgeom.Geometry{a, b})
		return safeSFOp(func() (sfgeom.Geometry, error) {
			return sfgeom.UnaryUnion(gc.AsGeometry())
		})
	}

	var result sfgeom.Geometry
	switch op {
	case "st_union":
		result, err = safeSFOp(func() (sfgeom.Geometry, error) { return sfgeom.Union(a, b) })
		if err != nil {
			// Fall back to UnaryUnion via GeometryCollection for degenerate inputs.
			result, err = gcFallbackUnion()
			if err != nil {
				// Both approaches failed on invalid input; return NULL.
				return nil, nil
			}
		}
	case "st_intersection":
		result, err = safeSFOp(func() (sfgeom.Geometry, error) { return sfgeom.Intersection(a, b) })
		if err != nil {
			// For intersection, return empty collection on failure with degenerate inputs.
			result = sfgeom.GeometryCollection{}.AsGeometry()
			err = nil
		}
	case "st_difference":
		result, err = safeSFOp(func() (sfgeom.Geometry, error) { return sfgeom.Difference(a, b) })
		if err != nil {
			// For difference, return A unchanged on failure.
			result = a
			err = nil
		}
	case "st_symdifference":
		result, err = safeSFOp(func() (sfgeom.Geometry, error) { return sfgeom.SymmetricDifference(a, b) })
		if err != nil {
			// Fall back to UnaryUnion via GeometryCollection for degenerate inputs.
			result, err = gcFallbackUnion()
			if err != nil {
				// Both approaches failed on invalid input; return NULL.
				return nil, nil
			}
		}
	default:
		return nil, fmt.Errorf("unknown spatial set op: %s", op)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: operation failed: %w", op, err)
	}

	wkt := result.AsText()
	// MySQL returns GEOMETRYCOLLECTION EMPTY for any empty set-operation result,
	// regardless of the specific geometry type (e.g. simplefeatures may return
	// POINT EMPTY, LINESTRING EMPTY, etc.).
	if result.IsEmpty() && !strings.HasPrefix(strings.ToUpper(wkt), "GEOMETRYCOLLECTION") {
		wkt = "GEOMETRYCOLLECTION EMPTY"
	}
	// MySQL outputs MULTIPOINT with points sorted by (X asc, Y asc).
	// simplefeatures uses insertion/overlay order which may differ.
	wkt = sortMultiPointWKT(wkt)
	// Re-apply the SRID if the first argument had one
	if srid != 0 {
		wkt = geomSetSRID(wkt, srid)
	}
	return wkt, nil
}

// sortMultiPointWKT reorders MULTIPOINT coordinates to match MySQL's (X asc, Y asc) ordering.
// Handles both plain MULTIPOINT and MULTIPOINT nested inside GEOMETRYCOLLECTION.
func sortMultiPointWKT(wkt string) string {
	upper := strings.ToUpper(wkt)
	if !strings.Contains(upper, "MULTIPOINT") {
		return wkt
	}
	// Use the sfgeom result to re-sort: parse, sort, regenerate.
	g, err := sfgeom.UnmarshalWKT(wkt, sfgeom.NoValidate{})
	if err != nil {
		return wkt
	}
	sorted := sortMultiPointGeometry(g)
	return sorted.AsText()
}

// sortMultiPointGeometry recursively sorts MULTIPOINT sub-geometries by (X, Y).
func sortMultiPointGeometry(g sfgeom.Geometry) sfgeom.Geometry {
	switch g.Type() {
	case sfgeom.TypeMultiPoint:
		mp := g.MustAsMultiPoint()
		n := mp.NumPoints()
		pts := make([]sfgeom.Point, n)
		for i := 0; i < n; i++ {
			pts[i] = mp.PointN(i)
		}
		// Sort by X asc, then Y asc to match MySQL output order.
		sort.Slice(pts, func(i, j int) bool {
			xyi, _ := pts[i].XY()
			xyj, _ := pts[j].XY()
			if xyi.X != xyj.X {
				return xyi.X < xyj.X
			}
			return xyi.Y < xyj.Y
		})
		var sb strings.Builder
		sb.WriteString("MULTIPOINT(")
		for i, p := range pts {
			if i > 0 {
				sb.WriteString(",")
			}
			// Format each point as "(x y)" which is the MULTIPOINT element syntax.
			// p.AsText() returns "POINT(x y)" but MULTIPOINT elements use "(x y)".
			pText := p.AsText() // e.g. "POINT(5 0)"
			if strings.HasPrefix(pText, "POINT(") && strings.HasSuffix(pText, ")") {
				sb.WriteString("(")
				sb.WriteString(pText[6 : len(pText)-1])
				sb.WriteString(")")
			} else {
				sb.WriteString(pText)
			}
		}
		sb.WriteString(")")
		sorted, err := sfgeom.UnmarshalWKT(sb.String(), sfgeom.NoValidate{})
		if err != nil {
			return g
		}
		return sorted
	case sfgeom.TypeGeometryCollection:
		gc := g.MustAsGeometryCollection()
		n := gc.NumGeometries()
		geoms := make([]sfgeom.Geometry, n)
		for i := 0; i < n; i++ {
			geoms[i] = sortMultiPointGeometry(gc.GeometryN(i))
		}
		return sfgeom.NewGeometryCollection(geoms).AsGeometry()
	default:
		return g
	}
}

// evalSpatialConvexHull computes the convex hull of the given WKT geometry
// and returns the result as WKT, preserving any SRID from the input.
func evalSpatialConvexHull(wkt string) (interface{}, error) {
	srid := geomGetSRID(wkt)
	g, err := wktToSimpleFeature(wkt)
	if err != nil {
		return nil, fmt.Errorf("st_convexhull: invalid geometry: %w", err)
	}
	hull := g.ConvexHull()
	result := hull.AsText()
	if srid != 0 {
		result = geomSetSRID(result, srid)
	}
	return result, nil
}

// evalSpatialSimplify simplifies the given WKT geometry using the
// Douglas-Peucker algorithm with the specified distance threshold,
// preserving any SRID from the input.
func evalSpatialSimplify(wkt string, threshold float64) (interface{}, error) {
	srid := geomGetSRID(wkt)
	g, err := wktToSimpleFeature(wkt)
	if err != nil {
		return nil, fmt.Errorf("st_simplify: invalid geometry: %w", err)
	}
	simplified, err := g.Simplify(threshold, sfgeom.NoValidate{})
	if err != nil {
		// Return the original geometry on simplification failure.
		result := g.AsText()
		if srid != 0 {
			result = geomSetSRID(result, srid)
		}
		return result, nil
	}
	result := simplified.AsText()
	if srid != 0 {
		result = geomSetSRID(result, srid)
	}
	return result, nil
}

// evalSpatialBuffer computes the buffer of the given WKT geometry with the
// specified distance using simplefeatures/JTS, preserving any SRID from the input.
// A zero or negative distance for points/linestrings returns the original geometry.
func evalSpatialBuffer(wkt string, distance float64) (interface{}, error) {
	srid := geomGetSRID(wkt)
	g, err := wktToSimpleFeature(wkt)
	if err != nil {
		return nil, fmt.Errorf("st_buffer: invalid geometry: %w", err)
	}
	result, err := safeSFOp(func() (sfgeom.Geometry, error) {
		return sfgeom.Buffer(g, distance)
	})
	if err != nil || result.IsEmpty() {
		// For zero-distance buffer of non-polygon types, MySQL returns the original geometry
		if distance == 0 {
			wktOut := g.AsText()
			if srid != 0 {
				wktOut = geomSetSRID(wktOut, srid)
			}
			return wktOut, nil
		}
		// Buffer failed; return NULL
		return nil, nil
	}
	wktOut := result.AsText()
	if srid != 0 {
		wktOut = geomSetSRID(wktOut, srid)
	}
	return wktOut, nil
}

// de9imPatterns maps spatial relation names to their DE-9IM intersection matrix patterns.
// These are the standard OGC/ISO SQL/MM patterns used by MySQL.
var de9imPatterns = map[string]string{
	"contains":   "T*****FF*",
	"within":     "T*F**F***",
	"intersects": "T********",
	"disjoint":   "FF2FF1212",
	"touches":    "FT*******",
	"overlaps":   "T*T***T**",
	"crosses":    "T*T******",
	"equals":     "TFFFTFFFT",
	"covers":     "T*****FF*",
	"coveredby":  "T*F**F***",
}

// sfContainsGeometry returns true if gA contains gB per the DE-9IM "T*****FF*" pattern.
// It handles known simplefeatures limitations for MULTIPOLYGON and GEOMETRYCOLLECTION:
// - For MULTIPOLYGON: also tries union(A).contains(B) to handle cases where B is inside
//   a single component but simplefeatures gives wrong EI due to multi-component geometry.
// - For GEOMETRYCOLLECTION: also checks whether any individual member contains B, since
//   simplefeatures may not properly account for lower-dimensional members whose interior
//   coincides with a polygon boundary.
func sfContainsGeometry(gA, gB sfgeom.Geometry) bool {
	// Standard DE-9IM check
	matrix, err := sfgeom.Relate(gA, gB)
	if err != nil {
		return false
	}
	match, _ := sfgeom.RelateMatches(matrix, "T*****FF*")
	if match {
		// MySQL returns 0 for ST_CONTAINS(MULTIPOLYGON, MULTIPOLYGON) when both are
		// topologically equal (same spatial extent). The specific DE-9IM pattern for
		// equal multi-component geometries is "2FFF1FFF2": interiors are equal (II=2),
		// boundaries share only a point (BB=1), and no cross-exterior intersections.
		// Under this pattern, MySQL returns 0 (does not consider A to "contain" B
		// when they are identical multi-component geometries sharing boundary points).
		if gA.Type() == sfgeom.TypeMultiPolygon && matrix == "2FFF1FFF2" {
			return false
		}
		return true
	}

	// Fallback for MULTIPOLYGON: try union of all components.
	// This is needed because simplefeatures computes DE-9IM for MULTIPOLYGON by
	// treating it as a single multi-component geometry, which can give incorrect EI
	// when B is inside only ONE component (e.g., a linestring inside one polygon).
	// We only apply this fallback when B is a lower-dimensional geometry (not a polygon
	// or multipolygon), because for polygon-in-component cases, the raw DE-9IM correctly
	// gives EI=1 (the component polygon's boundary lies on the multipolygon's boundary).
	if gA.Type() == sfgeom.TypeMultiPolygon {
		bDim := gB.Dimension()
		if bDim < 2 {
			uA, err := sfgeom.UnaryUnion(gA)
			if err == nil && !uA.IsEmpty() {
				uMatrix, err := sfgeom.Relate(uA, gB)
				if err == nil {
					uMatch, _ := sfgeom.RelateMatches(uMatrix, "T*****FF*")
					if uMatch {
						return true
					}
				}
			}
		}
	}

	// Fallback for GEOMETRYCOLLECTION: check each member
	if gA.Type() == sfgeom.TypeGeometryCollection {
		gc := gA.MustAsGeometryCollection()
		for i := 0; i < gc.NumGeometries(); i++ {
			member := gc.GeometryN(i)
			if member.IsEmpty() {
				continue
			}
			if sfContainsGeometry(member, gB) {
				return true
			}
		}
	}

	return false
}

// isLikelyWKT returns true if s looks like a WKT geometry string.
// This is used to distinguish genuine WKT parse failures from non-geometry arguments.
func isLikelyWKT(s string) bool {
	upper := strings.ToUpper(strings.TrimSpace(s))
	for _, prefix := range []string{
		"POINT", "LINESTRING", "POLYGON",
		"MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON",
		"GEOMETRYCOLLECTION", "GEOMCOLLECTION",
	} {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	// Also handle EWKT prefix
	if strings.HasPrefix(upper, "SRID=") {
		return true
	}
	return false
}

// evalSpatialRelationDE9IM evaluates ST_ spatial predicate functions using
// the exact DE-9IM model via simplefeatures, instead of MBR approximation.
func evalSpatialRelationDE9IM(e *Executor, exprs []sqlparser.Expr, rel string) (interface{}, bool, error) {
	if len(exprs) < 2 {
		return nil, true, nil
	}
	a, err := e.evalExpr(exprs[0])
	if err != nil {
		return nil, true, err
	}
	b, err := e.evalExpr(exprs[1])
	if err != nil {
		return nil, true, err
	}
	if a == nil || b == nil {
		return nil, true, nil
	}
	// Convert binary WKB inputs to EWKT strings so SRID can be extracted properly.
	sa, sridA := geomValueToEWKT(a)
	sb, sridB := geomValueToEWKT(b)

	// MySQL returns an error if two geometries have different SRIDs.
	if sridA != sridB {
		fnName := rel
		switch rel {
		case "contains":
			fnName = "st_contains"
		case "within":
			fnName = "st_within"
		case "intersects":
			fnName = "st_intersects"
		case "disjoint":
			fnName = "st_disjoint"
		case "touches":
			fnName = "st_touches"
		case "overlaps":
			fnName = "st_overlaps"
		case "crosses":
			fnName = "st_crosses"
		case "equals":
			fnName = "st_equals"
		case "covers":
			fnName = "st_covers"
		case "coveredby":
			fnName = "st_coveredby"
		}
		return nil, true, mysqlError(3572, "HY000", fmt.Sprintf("Binary geometry function %s given two geometries of different srids: %d and %d, which should have been identical.", fnName, sridA, sridB))
	}

	// For geographic SRIDs (e.g. 4326), validate coordinate bounds before computing relation.
	if isGeographicSRID(sridA) {
		stFnName := relToFuncName(rel, false)
		isLatLong := e.sridIsLatLongOrdered(sridA)
		if valErr := validateGeogCoordsForSpatialPredicate(sa, isLatLong, stFnName); valErr != nil {
			return nil, true, valErr
		}
		if valErr := validateGeogCoordsForSpatialPredicate(sb, isLatLong, stFnName); valErr != nil {
			return nil, true, valErr
		}
	}

	gA, errA := wktToSimpleFeature(sa)
	if errA != nil {
		// If the first geometry is not valid WKT (e.g. an integer column), return ERROR 22023.
		// Only fall back to MBR if the WKT "looks like" a geometry (has geometry type keyword).
		if !isLikelyWKT(sa) {
			fnName := rel
			switch rel {
			case "contains":
				fnName = "st_contains"
			case "within":
				fnName = "st_within"
			case "intersects":
				fnName = "st_intersects"
			case "disjoint":
				fnName = "st_disjoint"
			case "touches":
				fnName = "st_touches"
			case "overlaps":
				fnName = "st_overlaps"
			case "crosses":
				fnName = "st_crosses"
			case "equals":
				fnName = "st_equals"
			}
			return nil, true, mysqlError(3516, "22023", fmt.Sprintf("Invalid GIS data provided to function %s.", fnName))
		}
		return evalSpatialRelation(e, exprs, rel)
	}
	gB, errB := wktToSimpleFeature(sb)
	if errB != nil {
		if !isLikelyWKT(sb) {
			fnName := rel
			switch rel {
			case "contains":
				fnName = "st_contains"
			case "within":
				fnName = "st_within"
			case "intersects":
				fnName = "st_intersects"
			case "disjoint":
				fnName = "st_disjoint"
			case "touches":
				fnName = "st_touches"
			case "overlaps":
				fnName = "st_overlaps"
			case "crosses":
				fnName = "st_crosses"
			case "equals":
				fnName = "st_equals"
			}
			return nil, true, mysqlError(3516, "22023", fmt.Sprintf("Invalid GIS data provided to function %s.", fnName))
		}
		return evalSpatialRelation(e, exprs, rel)
	}

	// MySQL returns NULL for spatial predicates when either geometry is empty.
	if gA.IsEmpty() || gB.IsEmpty() {
		return nil, true, nil
	}

	// MySQL returns 0 for ST_CONTAINS/ST_WITHIN when either POLYGON argument is non-simple
	// (self-intersecting ring). simplefeatures with NoValidate accepts such polygons,
	// but validates ring simplicity only when the geometry type is strictly Polygon.
	// We detect this by re-parsing with full validation: if it fails for a Polygon, return 0.
	if rel == "contains" || rel == "covers" || rel == "within" || rel == "coveredby" {
		if gA.Type() == sfgeom.TypePolygon {
			plainA := geomStripSRID(strings.TrimSpace(sa))
			plainA = normalizeWKTForSF(plainA)
			if _, errValidA := sfgeom.UnmarshalWKT(plainA); errValidA != nil {
				return int64(0), true, nil
			}
		}
		if gB.Type() == sfgeom.TypePolygon {
			plainB := geomStripSRID(strings.TrimSpace(sb))
			plainB = normalizeWKTForSF(plainB)
			if _, errValidB := sfgeom.UnmarshalWKT(plainB); errValidB != nil {
				return int64(0), true, nil
			}
		}
	}

	// disjoint: use negation of intersects for efficiency.
	if rel == "disjoint" {
		matrix, relErr := sfgeom.Relate(gA, gB)
		if relErr != nil {
			return evalSpatialRelation(e, exprs, rel)
		}
		// disjoint means no intersection at all: II=F, IB=F, BI=F, BB=F
		intersectsPattern := "T********"
		matches, matchErr := sfgeom.RelateMatches(matrix, intersectsPattern)
		if matchErr != nil {
			return evalSpatialRelation(e, exprs, rel)
		}
		if matches {
			return int64(0), true, nil
		}
		return int64(1), true, nil
	}

	// touches has alternate patterns depending on geometry dimension.
	if rel == "touches" {
		matrix, relErr := sfgeom.Relate(gA, gB)
		if relErr != nil {
			return evalSpatialRelation(e, exprs, rel)
		}
		// touches: II=F and (IB or BI or BB intersects)
		patterns := []string{"FT*******", "F**T*****", "F***T****"}
		for _, pat := range patterns {
			matches, matchErr := sfgeom.RelateMatches(matrix, pat)
			if matchErr != nil {
				continue
			}
			if matches {
				return int64(1), true, nil
			}
		}
		return int64(0), true, nil
	}

	// overlaps has dimension-dependent patterns.
	if rel == "overlaps" {
		matrix, relErr := sfgeom.Relate(gA, gB)
		if relErr != nil {
			return evalSpatialRelation(e, exprs, rel)
		}
		patterns := []string{"T*T***T**", "1*T***T**"}
		for _, pat := range patterns {
			matches, matchErr := sfgeom.RelateMatches(matrix, pat)
			if matchErr != nil {
				continue
			}
			if matches {
				return int64(1), true, nil
			}
		}
		return int64(0), true, nil
	}

	// crosses has dimension-dependent patterns.
	if rel == "crosses" {
		matrix, relErr := sfgeom.Relate(gA, gB)
		if relErr != nil {
			return evalSpatialRelation(e, exprs, rel)
		}
		patterns := []string{"T*T******", "0********"}
		for _, pat := range patterns {
			matches, matchErr := sfgeom.RelateMatches(matrix, pat)
			if matchErr != nil {
				continue
			}
			if matches {
				return int64(1), true, nil
			}
		}
		return int64(0), true, nil
	}

	// intersects: true if not disjoint (any non-F entry in II,IB,BI,BB).
	if rel == "intersects" {
		matrix, relErr := sfgeom.Relate(gA, gB)
		if relErr != nil {
			return evalSpatialRelation(e, exprs, rel)
		}
		// intersects = not disjoint = II, IB, BI, or BB is not F
		disjointPattern := "FF*FF****"
		matches, matchErr := sfgeom.RelateMatches(matrix, disjointPattern)
		if matchErr != nil {
			return evalSpatialRelation(e, exprs, rel)
		}
		if matches {
			return int64(0), true, nil
		}
		return int64(1), true, nil
	}

	// "contains" and "within" use enhanced fallback for MULTIPOLYGON/GEOMETRYCOLLECTION
	// to work around simplefeatures DE-9IM limitations with multi-component geometries.
	if rel == "contains" || rel == "covers" {
		if sfContainsGeometry(gA, gB) {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	}
	if rel == "within" || rel == "coveredby" {
		// within(A, B) = contains(B, A)
		if sfContainsGeometry(gB, gA) {
			return int64(1), true, nil
		}
		return int64(0), true, nil
	}

	pat, ok := de9imPatterns[rel]
	if !ok {
		return evalSpatialRelation(e, exprs, rel)
	}

	matrix, relErr := sfgeom.Relate(gA, gB)
	if relErr != nil {
		// Fall back to MBR approximation on DE-9IM error.
		return evalSpatialRelation(e, exprs, rel)
	}

	matches, matchErr := sfgeom.RelateMatches(matrix, pat)
	if matchErr != nil {
		return evalSpatialRelation(e, exprs, rel)
	}
	if matches {
		return int64(1), true, nil
	}
	return int64(0), true, nil
}
