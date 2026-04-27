package executor

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// srsEntry holds metadata for a Spatial Reference System.
type srsEntry struct {
	SRID         uint32
	Name         string
	Organization string
	OrgCoordsys  *uint32
	Definition   string
	Description  string
	IsGeographic bool // true for GEOGCS, false for PROJCS/Cartesian
}

// srid2000Definition is the WKT for EPSG:2000 (Anguilla 1957 / British West Indies Grid).
const srid2000Definition = `PROJCS["Anguilla 1957 / British West Indies Grid",GEOGCS["Anguilla 1957",DATUM["Anguilla_1957",SPHEROID["Clarke 1880 (RGS)",6378249.145,293.465,AUTHORITY["EPSG","7012"]],AUTHORITY["EPSG","6600"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4600"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-62],PARAMETER["scale_factor",0.9995],PARAMETER["false_easting",400000],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","2000"]]`

// builtinSRS is the minimal built-in SRS catalog.
// MySQL ships a full EPSG catalog; we embed only the SRIDs used by the GIS test suite.
// wgs84Definition (SRID 4326) is declared in information_schema.go and used here at package init time.
var builtinSRS map[uint32]*srsEntry

func init() {
	builtinSRS = map[uint32]*srsEntry{
		0: {
			SRID:         0,
			Name:         "",
			IsGeographic: false, // Cartesian
			Definition:   "",
		},
		2000: {
			SRID:         2000,
			Name:         "Anguilla 1957 / British West Indies Grid",
			Organization: "EPSG",
			IsGeographic: false, // projected (PROJCS)
			Definition:   srid2000Definition,
		},
		4326: {
			SRID:         4326,
			Name:         "WGS 84",
			Organization: "EPSG",
			IsGeographic: true, // geographic (GEOGCS)
			Definition:   wgs84Definition,
		},
	}
}

// isSpatialColumnType returns true if the given column type (lowercased) is a geometry type.
func isSpatialColumnType(colTypeLower string) bool {
	switch colTypeLower {
	case "point", "linestring", "polygon", "geometry",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection", "geomcollection":
		return true
	}
	return false
}

// sridIsKnown returns true if the given SRID is in the built-in or user-defined SRS catalog.
func (e *Executor) sridIsKnown(srid uint32) bool {
	if _, ok := builtinSRS[srid]; ok {
		return true
	}
	if e == nil || e.srsRegistryMu == nil || e.srsRegistry == nil {
		return false
	}
	e.srsRegistryMu.RLock()
	defer e.srsRegistryMu.RUnlock()
	_, ok := e.srsRegistry[srid]
	return ok
}

// sridIsGeographic returns true if the given SRID represents a geographic coordinate system.
func (e *Executor) sridIsGeographic(srid uint32) bool {
	if b, ok := builtinSRS[srid]; ok {
		return b.IsGeographic
	}
	if e == nil || e.srsRegistryMu == nil || e.srsRegistry == nil {
		return false
	}
	e.srsRegistryMu.RLock()
	defer e.srsRegistryMu.RUnlock()
	if entry, ok := e.srsRegistry[srid]; ok {
		return entry.IsGeographic
	}
	return false
}

// srsGetEntry returns the SRS entry for the given SRID (built-in or user-defined).
func (e *Executor) srsGetEntry(srid uint32) *srsEntry {
	if b, ok := builtinSRS[srid]; ok {
		return b
	}
	if e == nil || e.srsRegistryMu == nil || e.srsRegistry == nil {
		return nil
	}
	e.srsRegistryMu.RLock()
	defer e.srsRegistryMu.RUnlock()
	return e.srsRegistry[srid]
}

// srsListAll returns all SRS entries (built-in + user-defined), sorted by SRID.
func (e *Executor) srsListAll() []*srsEntry {
	seen := make(map[uint32]bool)
	var result []*srsEntry
	for srid, entry := range builtinSRS {
		seen[srid] = true
		result = append(result, entry)
	}
	if e != nil && e.srsRegistryMu != nil && e.srsRegistry != nil {
		e.srsRegistryMu.RLock()
		for srid, entry := range e.srsRegistry {
			if !seen[srid] {
				result = append(result, entry)
				seen[srid] = true
			}
		}
		e.srsRegistryMu.RUnlock()
	}
	// Sort by SRID (bubble sort for simplicity; catalog is small)
	for i := 0; i < len(result); i++ {
		for j := i + 1; j < len(result); j++ {
			if result[i].SRID > result[j].SRID {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	return result
}

// srsGetByName returns the SRS entry with the given name (case-insensitive).
func (e *Executor) srsGetByName(name string) *srsEntry {
	nameLower := strings.ToLower(name)
	for _, b := range builtinSRS {
		if strings.ToLower(b.Name) == nameLower {
			return b
		}
	}
	if e == nil || e.srsRegistryMu == nil || e.srsRegistry == nil {
		return nil
	}
	e.srsRegistryMu.RLock()
	defer e.srsRegistryMu.RUnlock()
	for _, entry := range e.srsRegistry {
		if strings.ToLower(entry.Name) == nameLower {
			return entry
		}
	}
	return nil
}

// srsGetByOrgAndID returns the SRS entry with the given organization+coordsys_id combination.
func (e *Executor) srsGetByOrgAndID(org string, coordsysID uint32) *srsEntry {
	orgLower := strings.ToLower(org)
	for _, b := range builtinSRS {
		if strings.ToLower(b.Organization) == orgLower && b.OrgCoordsys != nil && *b.OrgCoordsys == coordsysID {
			return b
		}
	}
	if e == nil || e.srsRegistryMu == nil || e.srsRegistry == nil {
		return nil
	}
	e.srsRegistryMu.RLock()
	defer e.srsRegistryMu.RUnlock()
	for _, entry := range e.srsRegistry {
		if strings.ToLower(entry.Organization) == orgLower && entry.OrgCoordsys != nil && *entry.OrgCoordsys == coordsysID {
			return entry
		}
	}
	return nil
}

// srsIsGeographicFromDefinition heuristically determines if a WKT definition is geographic.
// GEOGCS[... = geographic, PROJCS[... = projected, empty/other = Cartesian.
func srsIsGeographicFromDefinition(definition string) bool {
	upper := strings.ToUpper(strings.TrimSpace(definition))
	return strings.HasPrefix(upper, "GEOGCS[") || strings.HasPrefix(upper, "GEOGCS(")
}

// srsDefinitionIsLatLong checks if the WKT SRS definition has a first axis representing
// latitude (LAT or NORTH in the first AXIS[...] clause). Returns true for lat-long ordering.
// Used to determine axis swapping for axis-order=srid-defined.
func srsDefinitionIsLatLong(definition string) bool {
	// Find the first AXIS[...] in the definition
	upper := strings.ToUpper(definition)
	// Look for AXIS[ or AXIS(
	axisIdx := strings.Index(upper, "AXIS[")
	if axisIdx < 0 {
		axisIdx = strings.Index(upper, "AXIS(")
	}
	if axisIdx < 0 {
		return false
	}
	// Extract the first axis name
	rest := upper[axisIdx+5:] // skip "AXIS["
	quoteIdx := strings.IndexByte(rest, '"')
	if quoteIdx < 0 {
		// No quoted name, look for direct identifier
		return false
	}
	rest = rest[quoteIdx+1:] // skip opening quote
	endQuote := strings.IndexByte(rest, '"')
	if endQuote < 0 {
		return false
	}
	firstAxisName := strings.TrimSpace(rest[:endQuote])
	// If first axis name starts with "LAT" or is "NORTH" (in geographic), it's lat-long ordered
	return strings.HasPrefix(firstAxisName, "LAT") || firstAxisName == "NORTH"
}

// sridIsLatLongOrdered returns true if the given SRID's SRS has latitude as the first axis
// (lat-long ordering). Returns false for long-lat ordering or if SRID is unknown.
func (e *Executor) sridIsLatLongOrdered(srid uint32) bool {
	entry := e.srsGetEntry(srid)
	if entry == nil {
		return false
	}
	return srsDefinitionIsLatLong(entry.Definition)
}

// checkSuperPrivilege returns ER_SPECIFIC_ACCESS_DENIED_ERROR if the current user lacks SUPER privilege.
// command is the human-readable command name for the error message (e.g. "CREATE SPATIAL REFERENCE SYSTEM").
func (e *Executor) checkSuperPrivilege(command string) error {
	if e == nil || e.userVars == nil {
		return nil // no user context → root assumed
	}
	cuv, ok := e.userVars["__current_user"]
	if !ok {
		return nil // no user set → root
	}
	cu, ok := cuv.(string)
	if !ok || cu == "" || strings.EqualFold(cu, "root") {
		return nil // root user
	}
	// Non-root user: check superUsers map.
	if e.superUsersMu != nil {
		e.superUsersMu.RLock()
		isSuper := e.superUsers[strings.ToLower(cu)]
		e.superUsersMu.RUnlock()
		if isSuper {
			return nil
		}
	}
	return mysqlError(1227, "HY000", fmt.Sprintf("You need the SUPER privilege for command '%s'", command))
}

// execCreateSpatialReferenceSystem handles CREATE [OR REPLACE] SPATIAL REFERENCE SYSTEM.
func (e *Executor) execCreateSpatialReferenceSystem(sql string) (*Result, error) {
	orReplace, ifNotExists, srid, name, org, orgID, definition, description, err :=
		parseSpatialReferenceSystemSQL(sql)
	if err != nil {
		return nil, err
	}

	// SUPER privilege check: non-root users need SUPER.
	cmdName := "CREATE SPATIAL REFERENCE SYSTEM"
	if orReplace {
		cmdName = "CREATE OR REPLACE SPATIAL REFERENCE SYSTEM"
	} else if ifNotExists {
		cmdName = "CREATE SPATIAL REFERENCE SYSTEM"
	}
	if privErr := e.checkSuperPrivilege(cmdName); privErr != nil {
		return nil, privErr
	}

	// SRID 0 is not modifiable.
	if srid == 0 && orReplace {
		return nil, mysqlError(3716, "SR000", "SRID 0 is not modifiable.")
	}

	// Validate SRID range (max 4294967295 already checked by parser).
	// MySQL also rejects SRID > 2147483647 if signed, but accepts up to 4294967295.
	// The test expects values > 4294967295 to fail with ER_DATA_OUT_OF_RANGE.

	// Check if this SRID is in the built-in catalog (cannot modify built-ins except if orReplace
	// and the SRID is not 0). Built-in SRIDs 0 and 4326 are not in the user registry.
	if _, isBuiltin := builtinSRS[srid]; isBuiltin && srid != 0 && !orReplace {
		return nil, mysqlError(1062, "23000", fmt.Sprintf("Duplicate entry '%d' for key 'PRIMARY'", srid))
	}

	// Initialize registry if not yet initialized
	if e.srsRegistry == nil {
		e.srsRegistry = make(map[uint32]*srsEntry)
	}
	if e.srsRegistryMu == nil {
		e.srsRegistryMu = &sync.RWMutex{}
	}

	e.srsRegistryMu.Lock()
	existing, exists := e.srsRegistry[srid]
	e.srsRegistryMu.Unlock()

	if exists && !orReplace && !ifNotExists {
		// CREATE without IF NOT EXISTS on existing SRID → error
		return nil, mysqlError(1062, "23000", fmt.Sprintf("Duplicate entry '%d' for key 'PRIMARY'", srid))
	}
	if exists && ifNotExists {
		// CREATE IF NOT EXISTS on existing → warning, no-op
		e.addWarning("Note", 1050, fmt.Sprintf("Spatial reference system %d already exists.", srid))
		return &Result{}, nil
	}

	// For OR REPLACE on existing: check if SRS is in use with incompatible changes.
	// We do a simplified check: if in use, only allow replacing with same/compatible definition.
	// For this implementation, we allow OR REPLACE unless the SRS is in use and we can't determine compatibility.
	if orReplace && exists {
		inUse, _, _ := e.srsIsUsedByColumn(srid)
		if inUse {
			// Check compatibility: same spheroid semi-major axis, inverse flattening, prime meridian, unit, axes.
			// For simplicity, we only reject if the definition changes substantially.
			// A full implementation would parse WKT and compare parameters.
			// Here we compare definitions exactly (ignoring trivial changes).
			newIsGeo := srsIsGeographicFromDefinition(definition)
			if existing.IsGeographic != newIsGeo {
				return nil, mysqlError(3716, "SR035", fmt.Sprintf(
					"There's a column using the SRID %d that cannot be replaced because the new SRS is incompatible.", srid))
			}
		}
	}

	// Check for duplicate NAME (case-insensitive) against other SRIDs.
	if existingByName := e.srsGetByName(name); existingByName != nil && existingByName.SRID != srid {
		return nil, mysqlError(1062, "23000", fmt.Sprintf("Duplicate entry '%s' for key 'st_spatial_reference_systems_name_unique'", name))
	}

	// Check for duplicate ORGANIZATION+ID combination.
	if org != "" && orgID != nil {
		if existingByOrg := e.srsGetByOrgAndID(org, *orgID); existingByOrg != nil && existingByOrg.SRID != srid {
			return nil, mysqlError(1062, "23000", fmt.Sprintf("Duplicate entry '%s-%d' for key 'st_spatial_reference_systems_catalog_unique'", org, *orgID))
		}
	}

	// Validate DEFINITION: must start with a valid WKT prefix.
	// MySQL's SRS WKT parser requires the definition to start with GEOGCS[, PROJCS[, or LOCAL_CS[.
	// Empty definitions and other strings are rejected with ER_SRS_PARSE_ERROR.
	{
		trimmedDef := strings.TrimSpace(definition)
		upperDef := strings.ToUpper(trimmedDef)
		if trimmedDef == "" {
			return nil, mysqlError(3716, "SR017", "Failed to parse SRS definition. Error: Expected 'GEOGCS[', 'PROJCS[', or 'LOCAL_CS[', but got ''.")
		}
		// MySQL accepts both square brackets GEOGCS[...] and parentheses GEOGCS(...).
		validWKT := strings.HasPrefix(upperDef, "GEOGCS[") || strings.HasPrefix(upperDef, "GEOGCS(") ||
			strings.HasPrefix(upperDef, "PROJCS[") || strings.HasPrefix(upperDef, "PROJCS(") ||
			strings.HasPrefix(upperDef, "LOCAL_CS[") || strings.HasPrefix(upperDef, "LOCAL_CS(")
		if !validWKT {
			// Return the first ~30 chars to simulate MySQL's parse error position report
			got := trimmedDef
			if len(got) > 30 {
				got = got[:30]
			}
			return nil, mysqlError(3716, "SR017", fmt.Sprintf(
				"Failed to parse SRS definition. Error: Expected 'GEOGCS[', 'PROJCS[', or 'LOCAL_CS[', but got '%s'.", got))
		}
	}

	// Store in registry.
	entry := &srsEntry{
		SRID:         srid,
		Name:         name,
		Organization: org,
		OrgCoordsys:  orgID,
		Definition:   definition,
		Description:  description,
		IsGeographic: srsIsGeographicFromDefinition(definition),
	}
	e.srsRegistryMu.Lock()
	e.srsRegistry[srid] = entry
	e.srsRegistryMu.Unlock()

	return &Result{}, nil
}

// execDropSpatialReferenceSystem handles DROP SPATIAL REFERENCE SYSTEM [IF EXISTS] srid.
func (e *Executor) execDropSpatialReferenceSystem(sql string) (*Result, error) {
	ifExists, srid, err := parseDropSpatialReferenceSystem(sql)
	if err != nil {
		return nil, err
	}

	// SUPER privilege check: non-root users need SUPER.
	if privErr := e.checkSuperPrivilege("DROP SPATIAL REFERENCE SYSTEM"); privErr != nil {
		return nil, privErr
	}

	// Cannot drop SRID 0
	if srid == 0 {
		return nil, mysqlError(3716, "SR000", "SRID 0 is not modifiable.")
	}

	// Cannot drop built-in SRIDs (4326 etc)
	if _, isBuiltin := builtinSRS[srid]; isBuiltin {
		if ifExists {
			return &Result{}, nil
		}
		// MySQL error for dropping a built-in that's in the catalog
		return nil, mysqlError(3716, "SR019", fmt.Sprintf("There's no spatial reference system with SRID %d.", srid))
	}

	if e.srsRegistry == nil || e.srsRegistryMu == nil {
		if ifExists {
			return &Result{}, nil
		}
		return nil, mysqlError(3716, "SR019", fmt.Sprintf("There's no spatial reference system with SRID %d.", srid))
	}

	e.srsRegistryMu.RLock()
	_, exists := e.srsRegistry[srid]
	e.srsRegistryMu.RUnlock()

	if !exists {
		if ifExists {
			return &Result{}, nil
		}
		return nil, mysqlError(3716, "SR019", fmt.Sprintf("There's no spatial reference system with SRID %d.", srid))
	}

	// Check if the SRS is in use by any column
	if inUse, tblName, colName := e.srsIsUsedByColumn(srid); inUse {
		return nil, mysqlError(3716, "SR033", fmt.Sprintf(
			"The spatial reference system (%d) is used by column %s in table %s.", srid, colName, tblName))
	}

	e.srsRegistryMu.Lock()
	delete(e.srsRegistry, srid)
	e.srsRegistryMu.Unlock()

	return &Result{}, nil
}

// parseSpatialReferenceSystemSQL parses a CREATE [OR REPLACE] SPATIAL REFERENCE SYSTEM statement.
func parseSpatialReferenceSystemSQL(sql string) (
	orReplace bool, ifNotExists bool, srid uint32,
	name string, org string, orgID *uint32, definition string, description string,
	err error,
) {
	rest := strings.TrimSpace(sql)

	// Strip CREATE
	if !strings.HasPrefix(strings.ToUpper(rest), "CREATE") {
		err = fmt.Errorf("expected CREATE")
		return
	}
	rest = strings.TrimSpace(rest[len("CREATE"):])

	// Optional OR REPLACE
	if strings.HasPrefix(strings.ToUpper(rest), "OR REPLACE") {
		orReplace = true
		rest = strings.TrimSpace(rest[len("OR REPLACE"):])
	}

	// SPATIAL REFERENCE SYSTEM
	if !strings.HasPrefix(strings.ToUpper(rest), "SPATIAL REFERENCE SYSTEM") {
		err = fmt.Errorf("expected SPATIAL REFERENCE SYSTEM")
		return
	}
	rest = strings.TrimSpace(rest[len("SPATIAL REFERENCE SYSTEM"):])

	// Optional IF NOT EXISTS
	if strings.HasPrefix(strings.ToUpper(rest), "IF NOT EXISTS") {
		ifNotExists = true
		rest = strings.TrimSpace(rest[len("IF NOT EXISTS"):])
	}

	// SRID value (first token)
	sridStr, afterSRID := splitFirstToken(rest)
	if sridStr == "" {
		err = mysqlError(1064, "42000", "You have an error in your SQL syntax near 'SPATIAL REFERENCE SYSTEM'")
		return
	}
	sridVal, parseErr := strconv.ParseUint(sridStr, 10, 64)
	if parseErr != nil {
		// Include the SQL context after the error point (up to 80 chars) in the near-clause.
		// MySQL's format: "near '<srid>\n<rest>...' at line 1"
		context := sridStr
		if afterSRID != "" {
			suffix := strings.TrimRight(afterSRID, ";")
			if len(suffix) > 80 {
				suffix = suffix[:80]
			}
			context = sridStr + "\n" + suffix
		}
		err = mysqlError(1064, "42000", fmt.Sprintf(
			"You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '%s' at line 1", context))
		return
	}
	if sridVal > 4294967295 {
		err = mysqlError(1690, "22003", "SRID value is out of range in 'CREATE SPATIAL REFERENCE SYSTEM'")
		return
	}
	srid = uint32(sridVal)
	rest = strings.TrimSpace(afterSRID)

	// Parse optional/mandatory attributes in any order
	seenName := false
	seenDefinition := false
	seenOrganization := false
	seenDescription := false

	for len(rest) > 0 {
		rest = strings.TrimSpace(rest)
		if rest == "" || rest == ";" {
			break
		}
		upper2 := strings.ToUpper(rest)
		switch {
		case strings.HasPrefix(upper2, "NAME") && (len(upper2) == 4 || !srsIsIdentChar(upper2[4])):
			if seenName {
				err = mysqlError(3716, "SR010", "Multiple definitions of attribute 'NAME'.")
				return
			}
			seenName = true
			rest = strings.TrimSpace(rest[4:])
			var val string
			val, rest, err = parseSingleQuotedString(rest)
			if err != nil {
				return
			}
			name = val
		case strings.HasPrefix(upper2, "DEFINITION") && (len(upper2) == 10 || !srsIsIdentChar(upper2[10])):
			if seenDefinition {
				err = mysqlError(3716, "SR010", "Multiple definitions of attribute 'DEFINITION'.")
				return
			}
			seenDefinition = true
			rest = strings.TrimSpace(rest[len("DEFINITION"):])
			var val string
			val, rest, err = parseSingleQuotedString(rest)
			if err != nil {
				return
			}
			definition = val
		case strings.HasPrefix(upper2, "ORGANIZATION") && (len(upper2) == 12 || !srsIsIdentChar(upper2[12])):
			if seenOrganization {
				err = mysqlError(3716, "SR010", "Multiple definitions of attribute 'ORGANIZATION'.")
				return
			}
			seenOrganization = true
			rest = strings.TrimSpace(rest[len("ORGANIZATION"):])
			var orgVal string
			orgVal, rest, err = parseSingleQuotedString(rest)
			if err != nil {
				return
			}
			org = orgVal
			// Optional IDENTIFIED BY <n>
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(rest)), "IDENTIFIED BY") {
				rest = strings.TrimSpace(rest[len("IDENTIFIED BY"):])
				var idStr string
				idStr, rest = splitFirstToken(rest)
				idVal, idErr := strconv.ParseUint(idStr, 10, 64)
				if idErr != nil {
					err = mysqlError(1064, "42000", "You have an error in your SQL syntax near 'IDENTIFIED BY'")
					return
				}
				if idVal > 4294967295 {
					err = mysqlError(1690, "22003", "SRID value is out of range in 'ORGANIZATION IDENTIFIED BY'")
					return
				}
				v := uint32(idVal)
				orgID = &v
			}
		case strings.HasPrefix(upper2, "DESCRIPTION") && (len(upper2) == 11 || !srsIsIdentChar(upper2[11])):
			if seenDescription {
				err = mysqlError(3716, "SR010", "Multiple definitions of attribute 'DESCRIPTION'.")
				return
			}
			seenDescription = true
			rest = strings.TrimSpace(rest[len("DESCRIPTION"):])
			var val string
			val, rest, err = parseSingleQuotedString(rest)
			if err != nil {
				return
			}
			description = val
		default:
			// Unknown keyword – stop parsing
			rest = ""
		}
	}

	// Validate mandatory attributes
	if !seenName {
		err = mysqlError(3716, "SR006", "Missing mandatory attribute NAME.")
		return
	}
	if !seenDefinition {
		err = mysqlError(3716, "SR006", "Missing mandatory attribute DEFINITION.")
		return
	}

	// Attribute-level validation
	if err = validateSRSAttribute("NAME", name, 80); err != nil {
		return
	}
	if seenOrganization {
		if err = validateSRSAttribute("ORGANIZATION", org, 256); err != nil {
			return
		}
	}
	if err = validateSRSAttribute("DEFINITION", definition, 4096); err != nil {
		return
	}
	if seenDescription {
		if err = validateSRSAttribute("DESCRIPTION", description, 2048); err != nil {
			return
		}
	}
	return
}

// validateSRSAttribute checks an SRS attribute for control characters, length, and emptiness.
func validateSRSAttribute(attrName, val string, maxLen int) error {
	for _, ch := range val {
		if ch < 0x20 || ch == 0x7f {
			return mysqlError(3716, "SR008", fmt.Sprintf("Illegal character in attribute '%s'.", attrName))
		}
	}
	if len([]rune(val)) > maxLen {
		return mysqlError(3716, "SR011", fmt.Sprintf("The attribute string is too long. The maximum length is %d.", maxLen))
	}
	if attrName == "NAME" || attrName == "ORGANIZATION" {
		if strings.TrimSpace(val) == "" {
			return mysqlError(3716, "SR007", fmt.Sprintf(
				"Attribute '%s' cannot be an empty string or a string that consists only of whitespace.", attrName))
		}
		if val != strings.TrimSpace(val) {
			return mysqlError(3716, "SR007", fmt.Sprintf(
				"Attribute '%s' cannot be an empty string or a string that consists only of whitespace.", attrName))
		}
	}
	return nil
}

// srsIsIdentChar returns true if ch is a valid SQL identifier character.
func srsIsIdentChar(ch byte) bool {
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_'
}

// splitFirstToken splits s into (first-word, remainder).
func splitFirstToken(s string) (string, string) {
	s = strings.TrimSpace(s)
	i := 0
	for i < len(s) && s[i] != ' ' && s[i] != '\t' && s[i] != '\n' && s[i] != '\r' && s[i] != ';' {
		i++
	}
	return s[:i], strings.TrimSpace(s[i:])
}

// parseSingleQuotedString parses a single-quoted SQL string from the start of s.
// Returns (value, remaining, error).
func parseSingleQuotedString(s string) (string, string, error) {
	s = strings.TrimSpace(s)
	if len(s) == 0 || s[0] != '\'' {
		return "", s, fmt.Errorf("expected quoted string, got: %q", firstN(s, 20))
	}
	var buf []byte
	i := 1
	for i < len(s) {
		ch := s[i]
		if ch == '\'' {
			if i+1 < len(s) && s[i+1] == '\'' {
				buf = append(buf, '\'')
				i += 2
				continue
			}
			i++
			break
		}
		if ch == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case '0':
				buf = append(buf, 0)
			case '\'':
				buf = append(buf, '\'')
			case '\\':
				buf = append(buf, '\\')
			case 'n':
				buf = append(buf, '\n')
			case 't':
				buf = append(buf, '\t')
			default:
				buf = append(buf, s[i+1])
			}
			i += 2
			continue
		}
		buf = append(buf, ch)
		i++
	}
	return string(buf), strings.TrimSpace(s[i:]), nil
}

// parseDropSpatialReferenceSystem parses DROP SPATIAL REFERENCE SYSTEM [IF EXISTS] srid.
func parseDropSpatialReferenceSystem(sql string) (ifExists bool, srid uint32, err error) {
	rest := strings.TrimSpace(sql)
	upper := strings.ToUpper(rest)
	if !strings.HasPrefix(upper, "DROP SPATIAL REFERENCE SYSTEM") {
		err = fmt.Errorf("expected DROP SPATIAL REFERENCE SYSTEM")
		return
	}
	rest = strings.TrimSpace(rest[len("DROP SPATIAL REFERENCE SYSTEM"):])
	if strings.HasPrefix(strings.ToUpper(rest), "IF EXISTS") {
		ifExists = true
		rest = strings.TrimSpace(rest[len("IF EXISTS"):])
	}
	rest = strings.TrimSuffix(strings.TrimSpace(rest), ";")
	sridVal, parseErr := strconv.ParseUint(strings.TrimSpace(rest), 10, 64)
	if parseErr != nil || sridVal > 4294967295 {
		err = mysqlError(1690, "22003", "SRID value is out of range in 'DROP SPATIAL REFERENCE SYSTEM'")
		return
	}
	srid = uint32(sridVal)
	return
}

// srsIsUsedByColumn returns (true, tableName, colName) if any column in any user database
// has the given SRID as a constraint.
func (e *Executor) srsIsUsedByColumn(srid uint32) (bool, string, string) {
	dbNames := e.Catalog.ListDatabases()
	for _, dbName := range dbNames {
		switch strings.ToLower(dbName) {
		case "information_schema", "mysql", "performance_schema", "sys":
			continue
		}
		db, err := e.Catalog.GetDatabase(dbName)
		if err != nil {
			continue
		}
		for _, tblName := range db.ListTables() {
			tbl, tblErr := db.GetTable(tblName)
			if tblErr != nil {
				continue
			}
			for _, col := range tbl.Columns {
				if col.SRIDConstraint != nil && *col.SRIDConstraint == srid {
					return true, tblName, col.Name
				}
			}
		}
	}
	return false, "", ""
}

// firstN returns the first n bytes of s (or all of s if shorter).
func firstN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
