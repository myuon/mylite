package executor

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

// parseSpatialPointCoords extracts X,Y from a WKT POINT string like "POINT(1 2)" or "POINT (1 2)".
// Returns [x, y] or nil if parsing fails.
func parseSpatialPointCoords(wkt string) []float64 {
	wkt = strings.TrimSpace(wkt)
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
func setSpatialPointCoord(wkt string, prop sqlparser.PointPropertyType, newVal interface{}) (interface{}, error) {
	coords := parseSpatialPointCoords(wkt)
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
	return fmt.Sprintf("POINT(%s %s)", formatSpatialFloat(coords[0]), formatSpatialFloat(coords[1])), nil
}

// formatSpatialFloat formats a float for WKT output (no trailing zeros).
func formatSpatialFloat(f float64) string {
	if f == float64(int64(f)) && !math.IsInf(f, 0) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// evalGeomProperty evaluates geometry property functions.
func evalGeomProperty(wkt string, prop sqlparser.GeomPropertyType) (interface{}, error) {
	upper := strings.TrimSpace(strings.ToUpper(wkt))
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
		// Return the geometry itself as a simplified envelope stub
		return wkt, nil
	}
	return nil, nil
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
func normalizeWKT(wkt string) string {
	upper := strings.ToUpper(strings.TrimSpace(wkt))
	if !strings.HasPrefix(upper, "MULTIPOINT") {
		return wkt
	}
	// Find the content inside outermost parentheses
	idx := strings.Index(wkt, "(")
	if idx < 0 {
		return wkt
	}
	end := strings.LastIndex(wkt, ")")
	if end <= idx {
		return wkt
	}
	inner := strings.TrimSpace(wkt[idx+1 : end])
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
	prefix := wkt[:idx+1]
	return prefix + strings.Join(normalized, ",") + ")"
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
		// If setter form with 2 args, return the geometry (ignoring SRID change)
		if len(exprs) >= 2 {
			return toString(val), true, nil
		}
		return int64(0), true, nil
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
	case "st_contains", "mbrcontains":
		return evalSpatialRelation(e, exprs, "contains")
	case "st_within", "mbrwithin":
		return evalSpatialRelation(e, exprs, "within")
	case "st_intersects", "mbrintersects":
		return evalSpatialRelation(e, exprs, "intersects")
	case "st_disjoint", "mbrdisjoint":
		return evalSpatialRelation(e, exprs, "disjoint")
	case "st_touches", "mbrtouches":
		return evalSpatialRelation(e, exprs, "touches")
	case "st_overlaps", "mbroverlaps":
		return evalSpatialRelation(e, exprs, "overlaps")
	case "st_crosses":
		return evalSpatialRelation(e, exprs, "crosses")
	case "mbrequals":
		return evalSpatialRelation(e, exprs, "equals")
	case "mbrcovers":
		return evalSpatialRelation(e, exprs, "covers")
	case "mbrcoveredby":
		return evalSpatialRelation(e, exprs, "coveredby")
	case "st_union", "st_intersection", "st_difference", "st_symdifference":
		// Return first geometry as stub
		if len(exprs) < 2 {
			return nil, true, nil
		}
		val, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		return val, true, nil
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
		return val, true, nil
	case "st_convexhull":
		if len(exprs) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		return val, true, nil
	case "st_simplify":
		if len(exprs) < 1 {
			return nil, true, nil
		}
		val, err := e.evalExpr(exprs[0])
		if err != nil {
			return nil, true, err
		}
		return val, true, nil
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
	sa, sb := toString(a), toString(b)
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

func mbrContains(outer, inner string) bool {
	ox1, oy1, ox2, oy2 := getMBR(outer)
	ix1, iy1, ix2, iy2 := getMBR(inner)
	return ox1 <= ix1 && oy1 <= iy1 && ox2 >= ix2 && oy2 >= iy2
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
	minX := math.Min(ca[0], cb[0])
	minY := math.Min(ca[1], cb[1])
	maxX := math.Max(ca[0], cb[0])
	maxY := math.Max(ca[1], cb[1])
	return fmt.Sprintf("POLYGON((%s %s,%s %s,%s %s,%s %s,%s %s))",
		formatSpatialFloat(minX), formatSpatialFloat(minY),
		formatSpatialFloat(maxX), formatSpatialFloat(minY),
		formatSpatialFloat(maxX), formatSpatialFloat(maxY),
		formatSpatialFloat(minX), formatSpatialFloat(maxY),
		formatSpatialFloat(minX), formatSpatialFloat(minY))
}

// wktBoundingBox extracts the bounding box (MBR) from a WKT geometry string.
// Returns [minX, minY, maxX, maxY] or nil if the geometry can't be parsed.
func wktBoundingBox(wkt string) []float64 {
	wkt = strings.TrimSpace(wkt)
	upper := strings.ToUpper(wkt)

	var allPoints [][]float64

	switch {
	case strings.HasPrefix(upper, "POINT"):
		coords := parseSpatialPointCoords(wkt)
		if coords == nil {
			return nil
		}
		allPoints = append(allPoints, coords)
	case strings.HasPrefix(upper, "LINESTRING"):
		pts := parseLineStringPoints(wkt)
		allPoints = append(allPoints, pts...)
	case strings.HasPrefix(upper, "POLYGON"), strings.HasPrefix(upper, "MULTIPOLYGON"):
		rings := parsePolygonRings(wkt)
		for _, ring := range rings {
			allPoints = append(allPoints, ring...)
		}
	case strings.HasPrefix(upper, "MULTIPOINT"):
		rings := parsePolygonRings(wkt)
		for _, ring := range rings {
			allPoints = append(allPoints, ring...)
		}
	default:
		return nil
	}

	if len(allPoints) == 0 {
		return nil
	}

	minX, minY := allPoints[0][0], allPoints[0][1]
	maxX, maxY := minX, minY
	for _, p := range allPoints[1:] {
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
	return []float64{minX, minY, maxX, maxY}
}

// WktToWKB is the exported form of wktToWKB for use from other packages (e.g. server).
func WktToWKB(wkt string) []byte {
	return wktToWKB(wkt)
}

// wktToWKB converts a WKT geometry string (e.g. "POINT(1 2)") to MySQL's internal
// binary geometry format: 4-byte SRID (big-endian) + WKB (ISO).
// Returns nil if parsing fails.
func wktToWKB(wkt string) []byte {
	wkt = strings.TrimSpace(wkt)
	upper := strings.ToUpper(wkt)
	if strings.HasPrefix(upper, "POINT") {
		coords := parseSpatialPointCoords(wkt)
		if len(coords) < 2 {
			return nil
		}
		buf := make([]byte, 4+1+4+8+8) // SRID + byteOrder + type + X + Y
		// SRID = 0 (big-endian)
		binary.BigEndian.PutUint32(buf[0:4], 0)
		// Byte order = 1 (little-endian)
		buf[4] = 1
		// Geometry type = 1 (POINT), little-endian
		binary.LittleEndian.PutUint32(buf[5:9], 1)
		// X coordinate
		binary.LittleEndian.PutUint64(buf[9:17], math.Float64bits(coords[0]))
		// Y coordinate
		binary.LittleEndian.PutUint64(buf[17:25], math.Float64bits(coords[1]))
		return buf
	}
	return nil
}
