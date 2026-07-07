package command

// geo.go implements the GEO command family (GEOADD / GEODIST / GEOPOS / GEOHASH /
// GEORADIUS / GEORADIUSBYMEMBER) the way Redis does: a GEO key IS a Sorted Set
// whose member score is the 52-bit geohash of the location (see geohash.go). So
// these handlers are pure command-layer logic over the zset store — GEOADD is a
// ZADD with a geohash score; GEOPOS/GEODIST/GEOHASH decode the score; GEORADIUS
// reads the members and filters by exact haversine distance. This makes
// GEOPOS/GEOHASH/GEODIST/WITHHASH byte-identical to Redis 3.2. GEORADIUS STORE /
// STOREDIST write the matched members (geohash score / distance) to a destination
// zset, exactly like Redis (see geoStore).

import (
	"context"
	"math"
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

// GEO error strings copied byte-for-byte from Redis 3.2 geo.c.
const (
	// errGeoUnit is Redis' reply for an unrecognised distance unit.
	errGeoUnit = "ERR unsupported unit provided. please use m, km, ft, mi"
	// errGeoNeedRadius is extractDistanceOrReply's custom parse-failure message.
	errGeoNeedRadius = "ERR need numeric radius"
	// errGeoNegRadius is extractDistanceOrReply's negative-distance reply.
	errGeoNegRadius = "ERR radius cannot be negative"
	// errGeoCountPositive is the GEORADIUS COUNT<=0 reply.
	errGeoCountPositive = "ERR COUNT must be > 0"
	// errGeoAddSyntax is geoaddCommand's odd-argument-count reply (note the
	// trailing space, present in the C string literal).
	errGeoAddSyntax = "ERR syntax error. Try GEOADD key [x1] [y1] [name1] [x2] [y2] [name2] ... "
)

// geoInvalidPairErr formats Redis' "invalid longitude,latitude pair X,Y" using
// C printf %.6f semantics — importantly rendering a non-finite coordinate as
// "inf"/"-inf"/"nan" (C's %f), NOT Go's "+Inf"/"NaN".
func geoInvalidPairErr(lon, lat float64) string {
	return "ERR invalid longitude,latitude pair " + fmtGeoErrCoord(lon) + "," + fmtGeoErrCoord(lat)
}

func fmtGeoErrCoord(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	case math.IsNaN(f):
		return "nan"
	default:
		return strconv.FormatFloat(f, 'f', 6, 64)
	}
}

// parseGeoLongLat parses and range-checks a GEORADIUS query centre with Redis'
// exact errors (extractLongLatOrReply): a non-float coordinate is
// "value is not a valid float"; an out-of-range pair is the invalid-pair error.
func parseGeoLongLat(w *resp.Writer, lonArg, latArg []byte) (lon, lat float64, ok bool) {
	lon, ok = parseFloatArg(lonArg)
	if !ok {
		w.Error(errNotGeoFloat)
		return 0, 0, false
	}
	lat, ok = parseFloatArg(latArg)
	if !ok {
		w.Error(errNotGeoFloat)
		return 0, 0, false
	}
	if !validGeoCoord(lon, lat) {
		w.Error(geoInvalidPairErr(lon, lat))
		return 0, 0, false
	}
	return lon, lat, true
}

// parseGeoRadius parses a radius+unit pair with Redis' extractDistanceOrReply
// error precedence: parse failure ("need numeric radius") -> negative
// ("radius cannot be negative") -> bad unit. It returns radius in metres.
func parseGeoRadius(w *resp.Writer, radiusArg, unitArg []byte) (radiusMeters, unit float64, ok bool) {
	radius, rok := parseFloatArg(radiusArg)
	if !rok {
		w.Error(errGeoNeedRadius)
		return 0, 0, false
	}
	if radius < 0 {
		w.Error(errGeoNegRadius)
		return 0, 0, false
	}
	u, uok := parseGeoUnit(unitArg)
	if !uok {
		w.Error(errGeoUnit)
		return 0, 0, false
	}
	return radius * u, u, true
}

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
		// geoaddCommand replies a GEOADD-specific hint, not a generic syntax error.
		w.Error(errGeoAddSyntax)
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
			w.Error(geoInvalidPairErr(lon, lat))
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
	if err := r.ensureTypeExpiring(ctx, pk, meta.TypeZSet); err != nil {
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
	// A member too large to be stored can never exist, so report it not-found rather
	// than sending its oversized sort key to the backend (a GEO key is a zset; the
	// same member-SK limit applies). GEODIST/GEOPOS/GEOHASH then reply nil and
	// GEORADIUSBYMEMBER "could not decode requested zset member", matching Redis.
	if len(member) > maxStorableMemberLen {
		return 0, 0, false, nil
	}
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
	// Redis' geodistCommand distinguishes the two absent cases: a missing KEY replies
	// shared.emptybulk ($0) via lookupKeyReadOrReply(...emptybulk), while a missing MEMBER
	// of a present key replies shared.nullbulk ($-1) below. GEOPOS/GEOHASH make the same
	// live-vs-not split.
	live, done := r.geoWrongType(ctx, c, pk)
	if done {
		return
	}
	if !live {
		w.BulkString([]byte(""))
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
	store     bool   // STORE or STOREDIST given
	storeDist bool   // STOREDIST (store the distance) vs STORE (store the geohash score)
	storeKey  []byte // destination key for STORE/STOREDIST
}

// parseGeoRadiusOptions parses the trailing GEORADIUS options. It returns an empty
// errText on success, or the exact Redis error body to reply. COUNT uses string2ll
// semantics (getLongFromObjectOrReply): a non-integer is "value is not an integer
// or out of range" and a non-positive value is "COUNT must be > 0". STORE/STOREDIST
// take a destination key (the later of the two wins, as in Redis). A missing value
// or an unknown token is a syntax error.
func parseGeoRadiusOptions(rest [][]byte) (geoRadiusOptions, string) {
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
				return o, resp.ErrSyntax
			}
			n, err := ParseInt(rest[i+1])
			if err != nil {
				return o, resp.ErrNotInteger
			}
			if n <= 0 {
				return o, errGeoCountPositive
			}
			o.count = int(n)
			i++
		case "store", "storedist":
			if i+1 >= len(rest) {
				return o, resp.ErrSyntax
			}
			o.store = true
			o.storeDist = toLower(string(rest[i])) == "storedist"
			o.storeKey = rest[i+1]
			i++
		default:
			return o, resp.ErrSyntax
		}
	}
	return o, ""
}

// errGeoStoreWithWith is Redis' error when STORE/STOREDIST is combined with any of
// the WITH* reply options (geo.c: they are mutually exclusive).
const errGeoStoreWithWith = "ERR STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options"

// checkGeoStore validates the STORE/STOREDIST option against the WITH* flags and the
// read-only command variant, returning the RESP error body or "". readOnly is set for
// GEORADIUS_RO / GEORADIUSBYMEMBER_RO, which forbid writing a destination.
func (o geoRadiusOptions) checkGeoStore(readOnly bool) string {
	if !o.store {
		return ""
	}
	if readOnly {
		return resp.ErrSyntax
	}
	if o.withCoord || o.withDist || o.withHash {
		return errGeoStoreWithWith
	}
	return ""
}

func (r *Router) handleGeoRadius(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])
	// Redis georadiusGeneric looks the key up FIRST: a missing key replies an empty
	// array (*0) and a live wrong-type key replies WRONGTYPE — both BEFORE any
	// coordinate/radius/option is parsed. Parsing errors are only reachable on a
	// live GEO (zset) key.
	live, done := r.geoWrongType(ctx, c, pk)
	if done {
		return
	}
	if !live {
		c.Redcon().WriteRaw(resp.AppendArrayHeader(nil, 0))
		return
	}

	lon, lat, ok := parseGeoLongLat(w, args[2], args[3])
	if !ok {
		return
	}
	radiusMeters, unit, ok := parseGeoRadius(w, args[4], args[5])
	if !ok {
		return
	}
	opts, errText := parseGeoRadiusOptions(args[6:])
	if errText != "" {
		w.Error(errText)
		return
	}
	if e := opts.checkGeoStore(isReadOnlyGeo(args[0])); e != "" {
		w.Error(e)
		return
	}
	r.geoRadiusReply(ctx, c, pk, lat, lon, radiusMeters, unit, opts)
}

// isReadOnlyGeo reports whether the command is a GEORADIUS_RO / GEORADIUSBYMEMBER_RO
// read-only variant (which forbids STORE/STOREDIST).
func isReadOnlyGeo(name []byte) bool {
	return len(name) >= 3 && (name[len(name)-3] == '_') &&
		(name[len(name)-2] == 'r' || name[len(name)-2] == 'R') &&
		(name[len(name)-1] == 'o' || name[len(name)-1] == 'O')
}

func (r *Router) handleGeoRadiusByMember(ctx context.Context, c *server.Conn, args [][]byte) {
	w := resp.NewWriter(c.Redcon())
	pk := encodePK(c.DB(), args[1])
	// As GEORADIUS: lookup + type check first (missing -> *0, wrong type ->
	// WRONGTYPE), then the member is decoded, then radius/unit, then options.
	live, done := r.geoWrongType(ctx, c, pk)
	if done {
		return
	}
	if !live {
		c.Redcon().WriteRaw(resp.AppendArrayHeader(nil, 0))
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
	radiusMeters, unit, ok := parseGeoRadius(w, args[3], args[4])
	if !ok {
		return
	}
	opts, errText := parseGeoRadiusOptions(args[5:])
	if errText != "" {
		w.Error(errText)
		return
	}
	if e := opts.checkGeoStore(isReadOnlyGeo(args[0])); e != "" {
		w.Error(e)
		return
	}
	r.geoRadiusReply(ctx, c, pk, lat, lon, radiusMeters, unit, opts)
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

	// Redis' georadiusGeneric forces ascending (by-distance) order whenever COUNT is
	// given without an explicit ASC/DESC ("if (count != 0 && sort == SORT_NONE) sort =
	// SORT_ASC;"), so `COUNT n` returns the NEAREST n. Without this, results are in
	// geohash-score order (ZRangeByRank) and the truncation would keep an arbitrary n,
	// not the closest n.
	sortAsc := o.sortAsc || (o.count > 0 && !o.sortDesc)
	if sortAsc || o.sortDesc {
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

	// STORE / STOREDIST: write the (sorted, COUNT-limited) matches to a destination
	// zset and reply the number stored, instead of returning the member array.
	if o.store {
		r.geoStore(ctx, c, o.storeKey, o.storeDist, results)
		return
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

// geoStore writes the STORE/STOREDIST matches to the destination zset (score = the
// geohash for STORE, the distance-in-query-unit for STOREDIST) and replies the number
// of members stored. It replaces the destination entirely (like the *STORE family):
// an empty result set leaves dest deleted and replies 0.
func (r *Router) geoStore(ctx context.Context, c *server.Conn, storeKey []byte, storeDist bool, results []geoResult) {
	w := resp.NewWriter(c.Redcon())
	destPK := encodePK(c.DB(), storeKey)

	members := make([]storage.ZMember, len(results))
	memberBytes := make([][]byte, len(results))
	for i, res := range results {
		score := float64(res.score)
		if storeDist {
			score = res.dist
		}
		members[i] = storage.ZMember{Member: res.member, Score: score}
		memberBytes[i] = []byte(res.member)
	}
	if err := guard.CheckWrite(storeKey, memberBytes, nil); err != nil {
		r.writeStoreError(c, err)
		return
	}

	// Replace dest: drop its meta (clears any prior type) and reclaim its members,
	// matching *STORE overwrite-regardless-of-type semantics.
	if _, err := r.Storage.Meta.DeleteMeta(ctx, destPK); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if _, err := r.Storage.Store.DeleteMembers(ctx, destPK); err != nil {
		r.writeStoreError(c, err)
		return
	}
	if len(members) == 0 {
		// An empty result leaves dest deleted (an empty zset does not exist) and replies 0.
		w.Int(0)
		return
	}
	if err := r.ensureTypeExpiring(ctx, destPK, meta.TypeZSet); err != nil {
		r.writeStoreError(c, err)
		return
	}
	added, err := r.Storage.Store.ZAdd(ctx, destPK, members)
	if err != nil {
		r.writeStoreError(c, err)
		return
	}
	if err := r.adjustCount(ctx, destPK, meta.TypeZSet, int64(added)); err != nil {
		r.writeStoreError(c, err)
		return
	}
	w.Int(int64(added))
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
