package command

// geo.go implements the GEO command family (GEOADD / GEODIST / GEOPOS / GEOHASH /
// GEORADIUS / GEORADIUSBYMEMBER) the way Redis does: a GEO key IS a Sorted Set
// whose member score is the 52-bit geohash of the location (see geohash.go). So
// these handlers are pure command-layer logic over the zset store — GEOADD is a
// ZADD with a geohash score; GEOPOS/GEODIST/GEOHASH decode the score; GEORADIUS
// reads the members and filters by exact haversine distance. This makes
// GEOPOS/GEOHASH/GEODIST/WITHHASH byte-identical to Redis 3.2.
//
// Not yet supported: STORE / STOREDIST.

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/aura-studio/redimos/v2/internal/guard"
	"github.com/aura-studio/redimos/v2/internal/meta"
	"github.com/aura-studio/redimos/v2/internal/resp"
	"github.com/aura-studio/redimos/v2/internal/server"
	"github.com/aura-studio/redimos/v2/internal/storage"
)

const errNotGeoFloat = "ERR value is not a valid float"

// errGeoUnit matches Redis' reply for an unrecognised distance unit (geo.c).
const errGeoUnit = "ERR unsupported unit provided. please use m, km, ft, mi"

func (r *Router) registerGeo() {
	r.reg("GEOADD", -5, true, r.handleGeoAdd)
	r.reg("GEODIST", -4, false, r.handleGeoDist)
	r.reg("GEOPOS", -2, false, r.handleGeoPos)
	r.reg("GEOHASH", -2, false, r.handleGeoHash)
	r.reg("GEORADIUS", -6, true, r.handleGeoRadius)
	r.reg("GEORADIUSBYMEMBER", -5, true, r.handleGeoRadiusByMember)
	// GEORADIUS_RO / GEORADIUSBYMEMBER_RO (Redis 3.2.10+) are the read-only variants:
	// identical to the base commands but they forbid STORE / STOREDIST. Since these
	// handlers already reject STORE/STOREDIST (parseGeoRadiusOptions returns false on
	// any such token → syntax error), the _ro variants are exact aliases.
	r.reg("GEORADIUS_RO", -6, false, r.handleGeoRadius)
	r.reg("GEORADIUSBYMEMBER_RO", -5, false, r.handleGeoRadiusByMember)
}

// parseGeoUnit maps a Redis unit token to metres-per-unit.
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

func validGeoCoord(lon, lat float64) bool {
	return lon >= geoLonMin && lon <= geoLonMax && lat >= geoLatMin && lat <= geoLatMax
}

// handleGeoAdd implements GEOADD key longitude latitude member [...]. Replies the
// number of new members added.
func (r *Router) handleGeoAdd(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	key := args[1]
	rest := args[2:]
	if len(rest)%3 != 0 {
		w.Error(resp.ErrSyntax)
		return
	}

	members := make([]storage.ZMember, 0, len(rest)/3)
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
		if !validGeoCoord(lon, lat) {
			w.Error(fmt.Sprintf("ERR invalid longitude,latitude pair %.6f,%.6f", lon, lat))
			return
		}
		members = append(members, storage.ZMember{
			Member: string(rest[i+2]),
			Score:  float64(geohashEncode52(lat, lon)),
		})
		memberBytes = append(memberBytes, rest[i+2])
	}

	pk := encodePK(c.DB(), key)
	if err := guard.CheckWrite(key, memberBytes, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if _, err := r.Storage.Meta.EnsureType(ctx, pk, meta.TypeZSet, 0); err != nil {
		r.writeStoreError(c, err)
		return
	}
	added, err := r.Storage.Store.ZAdd(ctx, pk, members)
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

// memberPos resolves a member's stored geohash score to its (lat, lon) centre.
func (r *Router) memberPos(ctx context.Context, pk, member string) (lat, lon float64, found bool, err error) {
	score, ok, serr := r.Storage.Store.ZScore(ctx, pk, member)
	if serr != nil || !ok {
		return 0, 0, false, serr
	}
	lat, lon = geohashDecode52(uint64(score))
	return lat, lon, true, nil
}

// handleGeoDist implements GEODIST key member1 member2 [unit].
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
			w.Error(errGeoUnit)
			return
		}
		unit = u
	}

	pk := encodePK(c.DB(), args[1])
	// GEODIST on a missing key/member replies $-1 via the member-not-found path below,
	// matching Redis' shared.null fallback, so liveness is not needed here.
	if _, done := r.geoWrongType(ctx, c, pk); done {
		return
	}

	lat1, lon1, ok1, err := r.memberPos(ctx, pk, string(args[2]))
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	lat2, lon2, ok2, err := r.memberPos(ctx, pk, string(args[3]))
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !ok1 || !ok2 {
		w.NullBulk()
		return
	}
	dist := geoHaversine(lat1, lon1, lat2, lon2) / unit
	w.BulkString([]byte(strconv.FormatFloat(dist, 'f', 4, 64)))
}

// handleGeoPos implements GEOPOS key member [...].
func (r *Router) handleGeoPos(ctx context.Context, c *server.Conn, args [][]byte) {
	pk := encodePK(c.DB(), args[1])
	live, done := r.geoWrongType(ctx, c, pk)
	if done {
		return
	}
	// Redis' GEOPOS looks the key up with lookupKeyReadOrReply(..., emptymultibulk):
	// an absent key replies a single empty array (*0), NOT one nil per requested member.
	if !live {
		c.Redcon().WriteRaw(resp.AppendArrayHeader(nil, 0))
		return
	}

	names := args[2:]
	buf := resp.AppendArrayHeader(nil, len(names))
	for _, name := range names {
		lat, lon, found, err := r.memberPos(ctx, pk, string(name))
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if !found {
			buf = resp.AppendNullArray(buf)
			continue
		}
		buf = resp.AppendArrayHeader(buf, 2)
		buf = resp.AppendBulkString(buf, []byte(formatGeoCoord(lon)))
		buf = resp.AppendBulkString(buf, []byte(formatGeoCoord(lat)))
	}
	c.Redcon().WriteRaw(buf)
}

// handleGeoHash implements GEOHASH key member [...].
func (r *Router) handleGeoHash(ctx context.Context, c *server.Conn, args [][]byte) {
	pk := encodePK(c.DB(), args[1])
	live, done := r.geoWrongType(ctx, c, pk)
	if done {
		return
	}
	// As GEOPOS: an absent key replies an empty array (*0), not one nil per member.
	if !live {
		c.Redcon().WriteRaw(resp.AppendArrayHeader(nil, 0))
		return
	}

	names := args[2:]
	buf := resp.AppendArrayHeader(nil, len(names))
	for _, name := range names {
		lat, lon, found, err := r.memberPos(ctx, pk, string(name))
		if err != nil {
			r.writeStoreError(c, err)
			return
		}
		if !found {
			buf = resp.AppendNullBulk(buf)
			continue
		}
		buf = resp.AppendBulkString(buf, []byte(geohashStandard11(lat, lon)))
	}
	c.Redcon().WriteRaw(buf)
}

type geoRadiusOptions struct {
	withCoord bool
	withDist  bool
	withHash  bool
	count     int
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
			// STORE / STOREDIST and unknown tokens are not supported.
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
		w.Error(errGeoUnit)
		return
	}
	opts, ok := parseGeoRadiusOptions(args[6:])
	if !ok {
		w.Error(resp.ErrSyntax)
		return
	}

	pk := encodePK(c.DB(), args[1])
	// An absent key yields an empty scan → geoRadiusReply writes *0, matching Redis.
	if _, done := r.geoWrongType(ctx, c, pk); done {
		return
	}
	r.geoRadiusReply(ctx, c, pk, lat, lon, radius*unit, unit, opts)
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
		w.Error(errGeoUnit)
		return
	}
	opts, ok := parseGeoRadiusOptions(args[5:])
	if !ok {
		w.Error(resp.ErrSyntax)
		return
	}

	pk := encodePK(c.DB(), args[1])
	if _, done := r.geoWrongType(ctx, c, pk); done {
		return
	}
	lat, lon, found, err := r.memberPos(ctx, pk, string(args[2]))
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if !found {
		w.Error("ERR could not decode requested zset member")
		return
	}
	r.geoRadiusReply(ctx, c, pk, lat, lon, radius*unit, unit, opts)
}

type geoResult struct {
	member string
	lat    float64
	lon    float64
	score  uint64
	dist   float64 // in the query unit
}

// geoRadiusReply reads every member, keeps those within radiusMeters of the
// centre (exact haversine, the same set Redis returns), and writes the reply.
func (r *Router) geoRadiusReply(ctx context.Context, c *server.Conn, pk string, centerLat, centerLon, radiusMeters, unit float64, o geoRadiusOptions) {
	all, err := r.Storage.Store.ZRangeByRank(ctx, pk, 0, -1, false)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}

	results := make([]geoResult, 0)
	for _, m := range all {
		score := uint64(m.Score)
		plat, plon := geohashDecode52(score)
		d := geoHaversine(centerLat, centerLon, plat, plon)
		if d <= radiusMeters {
			results = append(results, geoResult{member: m.Member, lat: plat, lon: plon, score: score, dist: d / unit})
		}
	}

	if o.sortAsc || o.sortDesc {
		sort.SliceStable(results, func(i, j int) bool {
			if o.sortDesc {
				return results[i].dist > results[j].dist
			}
			return results[i].dist < results[j].dist
		})
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
			buf = resp.AppendInt(buf, int64(res.score))
		}
		if o.withCoord {
			buf = resp.AppendArrayHeader(buf, 2)
			buf = resp.AppendBulkString(buf, []byte(formatGeoCoord(res.lon)))
			buf = resp.AppendBulkString(buf, []byte(formatGeoCoord(res.lat)))
		}
	}
	c.Redcon().WriteRaw(buf)
}

// geoWrongType replies WRONGTYPE for a live non-zset key (a GEO key is a zset).
// done is true when a reply (WRONGTYPE or a store error) was already written; live
// reports whether the key logically exists, letting GEOPOS/GEOHASH reproduce Redis'
// lookupKeyReadOrReply(..., emptymultibulk) — an absent GEO key replies an empty
// array (*0), not a per-member array of nils.
func (r *Router) geoWrongType(ctx context.Context, c *server.Conn, pk string) (live, done bool) {
	_, live, wrongType, err := r.zsetState(ctx, pk)
	if err != nil {
		r.writeStoreError(c, err)
		return false, true
	}
	if wrongType {
		resp.NewWriter(c.Redcon()).Error(resp.ErrWrongType)
		return false, true
	}
	return live, false
}

// formatGeoCoord renders a coordinate the way Redis' GEOPOS does: 17 digits after
// the decimal point (matching "%.17Lf" on the double promoted to long double),
// with trailing zeros — and a bare trailing '.' — stripped (ld2string,
// humanfriendly). The stored value is a float64, so the 17-place rendering is
// identical to Redis' before stripping.
func formatGeoCoord(v float64) string {
	s := strconv.FormatFloat(v, 'f', 17, 64)
	if strings.ContainsRune(s, '.') {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	return s
}
