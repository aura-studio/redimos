package command

// geo.go implements the GEO command family (GEOADD / GEODIST / GEOPOS / GEOHASH /
// GEORADIUS / GEORADIUSBYMEMBER). A GEO key is a Sorted Set whose member score
// encodes a location, so these handlers maintain the zset meta/type and cnt and
// delegate the spatial work to the redimo-backed GeoStore seam (r.Storage.Geo).
//
// Scope (functional v1): full GEOADD/GEODIST/GEOPOS/GEOHASH and GEORADIUS[BYMEMBER]
// with WITHCOORD / WITHDIST / WITHHASH / COUNT / ASC / DESC. STORE / STOREDIST are
// not yet supported. Because the backing store encodes positions with S2 rather
// than Redis' 52-bit geohash, GEOPOS/GEOHASH/GEODIST values are close to but not
// byte-identical with Redis; GEORADIUS membership (who is within the radius) is
// correct.

import (
	"context"
	"math"
	"sort"
	"strconv"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

const earthRadiusMeters = 6372797.560856

func (r *Router) registerGeo() {
	t := r.Table
	t.Register("GEOADD", -5, true, r.handleGeoAdd)
	t.Register("GEODIST", -4, false, r.handleGeoDist)
	t.Register("GEOPOS", -2, false, r.handleGeoPos)
	t.Register("GEOHASH", -2, false, r.handleGeoHash)
	t.Register("GEORADIUS", -6, true, r.handleGeoRadius)
	t.Register("GEORADIUSBYMEMBER", -5, true, r.handleGeoRadiusByMember)
}

// parseGeoUnit maps a Redis unit token to metres-per-unit. ok is false for an
// unknown unit.
func parseGeoUnit(b []byte) (float64, bool) {
	switch string(b) {
	case "m":
		return 1.0, true
	case "km":
		return 1000.0, true
	case "mi":
		return 1609.34, true
	case "ft":
		return 0.3048, true
	default:
		return 0, false
	}
}

const errNotGeoFloat = "ERR value is not a valid float"

// handleGeoAdd implements GEOADD key longitude latitude member [longitude latitude
// member ...]. Replies the number of elements newly added (not counting updates).
func (r *Router) handleGeoAdd(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key := args[1]
	rest := args[2:]
	if len(rest)%3 != 0 {
		w.Error(resp.ErrSyntax)
		return
	}

	members := make(map[string]storage.GeoPoint, len(rest)/3)
	memberBytes := make([][]byte, 0, len(rest)/3)
	for i := 0; i < len(rest); i += 3 {
		lon, ok := parseFloatArg(rest[i])
		if !ok {
			w.Error(errNotGeoFloat)
			return
		}
		lat, ok := parseFloatArg(rest[i+1])
		if !ok {
			w.Error(errNotGeoFloat)
			return
		}
		members[string(rest[i+2])] = storage.GeoPoint{Lon: lon, Lat: lat}
		memberBytes = append(memberBytes, rest[i+2])
	}

	pk := encodePK(c.DB(), key)
	if err := guard.CheckWrite(key, memberBytes, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeZSet, 0); err != nil {
		r.writeStoreError(c, err)
		return
	}

	added, err := r.Storage.Geo.GeoAdd(ctx, pk, members)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, pk, meta.TypeZSet, int64(added)); err != nil {
		r.writeStoreError(c, err)
		return
	}

	w.Int(int64(added))
}

// handleGeoDist implements GEODIST key member1 member2 [unit]. Replies the
// distance as a bulk string, or the null bulk string when either member is
// missing.
func (r *Router) handleGeoDist(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	if len(args) > 5 {
		w.Error(resp.ErrSyntax)
		return
	}
	unit := 1.0
	if len(args) == 5 {
		u, ok := parseGeoUnit(args[4])
		if !ok {
			w.Error(resp.ErrSyntax)
			return
		}
		unit = u
	}

	pk := encodePK(c.DB(), args[1])
	if wt, err := r.geoWrongType(ctx, c, pk); err != nil || wt {
		return
	}

	dist, ok, err := r.Storage.Geo.GeoDist(ctx, pk, string(args[2]), string(args[3]), unit)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !ok {
		w.NullBulk()
		return
	}
	w.BulkString([]byte(strconv.FormatFloat(dist, 'f', 4, 64)))
}

// handleGeoPos implements GEOPOS key member [member ...]. Replies an array with
// one entry per requested member: a two-element [longitude, latitude] array when
// present, or a null array when missing.
func (r *Router) handleGeoPos(ctx context.Context, c *server.Conn, args [][]byte) {
	pk := encodePK(c.DB(), args[1])
	if wt, err := r.geoWrongType(ctx, c, pk); err != nil || wt {
		return
	}

	names := bytesToStrings(args[2:])
	locs, err := r.Storage.Geo.GeoPos(ctx, pk, names)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	buf := resp.AppendArrayHeader(nil, len(names))
	for _, name := range names {
		p, ok := locs[name]
		if !ok {
			buf = resp.AppendNullArray(buf)
			continue
		}
		buf = resp.AppendArrayHeader(buf, 2)
		buf = resp.AppendBulkString(buf, []byte(formatGeoCoord(p.Lon)))
		buf = resp.AppendBulkString(buf, []byte(formatGeoCoord(p.Lat)))
	}
	c.Redcon().WriteRaw(buf)
}

// handleGeoHash implements GEOHASH key member [member ...]. Replies an array of
// geohash strings (null bulk for a missing member).
func (r *Router) handleGeoHash(ctx context.Context, c *server.Conn, args [][]byte) {
	pk := encodePK(c.DB(), args[1])
	if wt, err := r.geoWrongType(ctx, c, pk); err != nil || wt {
		return
	}

	names := bytesToStrings(args[2:])
	hashes, err := r.Storage.Geo.GeoHash(ctx, pk, names)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	buf := resp.AppendArrayHeader(nil, len(names))
	for _, name := range names {
		if h, ok := hashes[name]; ok {
			buf = resp.AppendBulkString(buf, []byte(h))
		} else {
			buf = resp.AppendNullBulk(buf)
		}
	}
	c.Redcon().WriteRaw(buf)
}

// geoRadiusOptions holds the parsed GEORADIUS trailer.
type geoRadiusOptions struct {
	withCoord bool
	withDist  bool
	withHash  bool
	count     int // 0 = unlimited
	sortAsc   bool
	sortDesc  bool
}

func parseGeoRadiusOptions(rest [][]byte) (geoRadiusOptions, bool) {
	var o geoRadiusOptions
	for i := 0; i < len(rest); i++ {
		switch toLower(string(rest[i])) {
		case "withcoord":
			o.withCoord = true
		case "withdist":
			o.withDist = true
		case "withhash":
			o.withHash = true
		case "asc":
			o.sortAsc = true
		case "desc":
			o.sortDesc = true
		case "count":
			if i+1 >= len(rest) {
				return o, false
			}
			n, err := strconv.Atoi(string(rest[i+1]))
			if err != nil || n <= 0 {
				return o, false
			}
			o.count = n
			i++
		default:
			// STORE / STOREDIST and any unknown token are not supported in v1.
			return o, false
		}
	}
	return o, true
}

func (r *Router) handleGeoRadius(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	lon, ok := parseFloatArg(args[2])
	if !ok {
		w.Error(errNotGeoFloat)
		return
	}
	lat, ok := parseFloatArg(args[3])
	if !ok {
		w.Error(errNotGeoFloat)
		return
	}
	radius, ok := parseFloatArg(args[4])
	if !ok {
		w.Error(errNotGeoFloat)
		return
	}
	unit, ok := parseGeoUnit(args[5])
	if !ok {
		w.Error(resp.ErrSyntax)
		return
	}
	opts, ok := parseGeoRadiusOptions(args[6:])
	if !ok {
		w.Error(resp.ErrSyntax)
		return
	}

	pk := encodePK(c.DB(), args[1])
	if wt, err := r.geoWrongType(ctx, c, pk); err != nil || wt {
		return
	}

	center := storage.GeoPoint{Lon: lon, Lat: lat}
	locs, err := r.Storage.Geo.GeoRadius(ctx, pk, center, radius, unit, opts.count)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	r.writeGeoRadiusReply(c, center, unit, locs, opts)
}

func (r *Router) handleGeoRadiusByMember(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	radius, ok := parseFloatArg(args[3])
	if !ok {
		w.Error(errNotGeoFloat)
		return
	}
	unit, ok := parseGeoUnit(args[4])
	if !ok {
		w.Error(resp.ErrSyntax)
		return
	}
	opts, ok := parseGeoRadiusOptions(args[5:])
	if !ok {
		w.Error(resp.ErrSyntax)
		return
	}

	pk := encodePK(c.DB(), args[1])
	if wt, err := r.geoWrongType(ctx, c, pk); err != nil || wt {
		return
	}

	member := string(args[2])
	// The center is the member's own position; resolve it so WITHDIST/ASC/DESC use
	// the same reference as the query.
	centers, err := r.Storage.Geo.GeoPos(ctx, pk, []string{member})
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	center, present := centers[member]
	if !present {
		// Redis replies an error when the reference member is missing.
		w.Error("ERR could not decode requested zset member")
		return
	}

	locs, err := r.Storage.Geo.GeoRadiusByMember(ctx, pk, member, radius, unit, opts.count)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	r.writeGeoRadiusReply(c, center, unit, locs, opts)
}

// geoResult pairs a member with its position and distance for ordering.
type geoResult struct {
	member string
	pos    storage.GeoPoint
	dist   float64 // in the query unit
}

func (r *Router) writeGeoRadiusReply(c *server.Conn, center storage.GeoPoint, unit float64, locs map[string]storage.GeoPoint, o geoRadiusOptions) {
	results := make([]geoResult, 0, len(locs))
	for name, p := range locs {
		results = append(results, geoResult{
			member: name,
			pos:    p,
			dist:   geoDistance(center, p) / unit,
		})
	}

	if o.sortAsc || o.sortDesc {
		sort.SliceStable(results, func(i, j int) bool {
			if o.sortDesc {
				return results[i].dist > results[j].dist
			}
			return results[i].dist < results[j].dist
		})
	} else {
		// Redis' unsorted order is unspecified; a stable member order keeps the
		// reply deterministic.
		sort.SliceStable(results, func(i, j int) bool { return results[i].member < results[j].member })
	}
	if o.count > 0 && o.count < len(results) {
		results = results[:o.count]
	}

	buf := resp.AppendArrayHeader(nil, len(results))
	withAny := o.withCoord || o.withDist || o.withHash
	for _, res := range results {
		if !withAny {
			buf = resp.AppendBulkString(buf, []byte(res.member))
			continue
		}
		n := 1
		if o.withDist {
			n++
		}
		if o.withHash {
			n++
		}
		if o.withCoord {
			n++
		}
		buf = resp.AppendArrayHeader(buf, n)
		buf = resp.AppendBulkString(buf, []byte(res.member))
		if o.withDist {
			buf = resp.AppendBulkString(buf, []byte(strconv.FormatFloat(res.dist, 'f', 4, 64)))
		}
		if o.withHash {
			buf = resp.AppendInt(buf, int64(encodeGeohash52(res.pos.Lat, res.pos.Lon)))
		}
		if o.withCoord {
			buf = resp.AppendArrayHeader(buf, 2)
			buf = resp.AppendBulkString(buf, []byte(formatGeoCoord(res.pos.Lon)))
			buf = resp.AppendBulkString(buf, []byte(formatGeoCoord(res.pos.Lat)))
		}
	}
	c.Redcon().WriteRaw(buf)
}

// geoWrongType checks that pk is absent/expired or a zset; a live non-zset key
// replies WRONGTYPE. Returns wrongType=true (reply already written) or an error.
func (r *Router) geoWrongType(ctx context.Context, c *server.Conn, pk string) (bool, error) {
	_, _, wrongType, err := r.zsetState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return true, err
	}
	if wrongType {
		resp.NewWriter(c.Redcon()).Error(resp.ErrWrongType)
		return true, nil
	}
	return false, nil
}

func formatGeoCoord(v float64) string {
	return strconv.FormatFloat(v, 'f', 17, 64)
}

// geoDistance returns the great-circle distance in metres between two points
// using the haversine formula and Redis' earth radius constant.
func geoDistance(a, b storage.GeoPoint) float64 {
	lat1 := a.Lat * math.Pi / 180
	lat2 := b.Lat * math.Pi / 180
	dLat := (b.Lat - a.Lat) * math.Pi / 180
	dLon := (b.Lon - a.Lon) * math.Pi / 180
	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusMeters * math.Asin(math.Sqrt(h))
}

// Redis GEO geohash bounds (Web-Mercator latitude clamp).
const (
	geoLatMin  = -85.05112878
	geoLatMax  = 85.05112878
	geoLonMin  = -180.0
	geoLonMax  = 180.0
	geoStepBits = 26
)

// encodeGeohash52 computes Redis' 52-bit interleaved geohash for WITHHASH. It
// matches Redis' geohashEncode: normalise lat/lon into their ranges, scale to 26
// bits each and Morton-interleave (lat in the low bit).
func encodeGeohash52(lat, lon float64) uint64 {
	if lat < geoLatMin {
		lat = geoLatMin
	} else if lat > geoLatMax {
		lat = geoLatMax
	}
	latOffset := (lat - geoLatMin) / (geoLatMax - geoLatMin)
	lonOffset := (lon - geoLonMin) / (geoLonMax - geoLonMin)
	ilat := uint32(latOffset * float64(uint64(1)<<geoStepBits))
	ilon := uint32(lonOffset * float64(uint64(1)<<geoStepBits))
	return interleave64(ilat, ilon)
}

// interleave64 Morton-interleaves two 32-bit values into a 64-bit value (x bits at
// even positions, y bits at odd positions), matching Redis' interleave64.
func interleave64(xlo, ylo uint32) uint64 {
	B := [...]uint64{
		0x5555555555555555, 0x3333333333333333,
		0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF,
		0x0000FFFF0000FFFF,
	}
	S := [...]uint{1, 2, 4, 8, 16}
	x := uint64(xlo)
	y := uint64(ylo)
	x = (x | (x << S[4])) & B[4]
	x = (x | (x << S[3])) & B[3]
	x = (x | (x << S[2])) & B[2]
	x = (x | (x << S[1])) & B[1]
	x = (x | (x << S[0])) & B[0]
	y = (y | (y << S[4])) & B[4]
	y = (y | (y << S[3])) & B[3]
	y = (y | (y << S[2])) & B[2]
	y = (y | (y << S[1])) & B[1]
	y = (y | (y << S[0])) & B[0]
	return x | (y << 1)
}
