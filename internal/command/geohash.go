package command

// geohash.go is a faithful port of the geohash math Redis uses for GEO: the
// 52-bit interleaved geohash (26 bits lat in even positions, 26 bits lon in odd
// positions), with lat clamped to the Web-Mercator range. GEO is stored as a
// Sorted Set whose member score is this 52-bit geohash (exactly representable as
// a float64), so GEOPOS/GEODIST/GEOHASH decode the score the same way Redis does.

import "math"

// Redis GEO ranges (geohash.c).
const (
	geoLatMin   = -85.05112878
	geoLatMax   = 85.05112878
	geoLonMin   = -180.0
	geoLonMax   = 180.0
	geoStepBits = 26

	// earthRadiusMeters matches Redis' EARTH_RADIUS_IN_METERS (geohash_helper.c).
	earthRadiusMeters = 6372797.560856
)

// geohashEncode52 computes Redis' 52-bit geohash of (lat, lon): normalise into the
// GEO ranges, scale to 26 bits each and Morton-interleave (lat in the low/even
// bit). Matches Redis geohashEncode with step 26.
func geohashEncode52(lat, lon float64) uint64 {
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

// geohashDecode52 decodes a 52-bit geohash back to the centre (lat, lon) of its
// cell, matching Redis geohashDecodeToLongLatType.
func geohashDecode52(bits uint64) (lat, lon float64) {
	ilat, ilon := deinterleave64(bits)
	scale := float64(uint64(1) << geoStepBits)
	latMin := geoLatMin + (float64(ilat)/scale)*(geoLatMax-geoLatMin)
	latMax := geoLatMin + (float64(ilat+1)/scale)*(geoLatMax-geoLatMin)
	lonMin := geoLonMin + (float64(ilon)/scale)*(geoLonMax-geoLonMin)
	lonMax := geoLonMin + (float64(ilon+1)/scale)*(geoLonMax-geoLonMin)
	return (latMin + latMax) / 2, (lonMin + lonMax) / 2
}

// geohashStandard11 returns the 11-character standard geohash string of (lat,
// lon), matching Redis' GEOHASH command: re-encode with the FULL latitude range
// [-90, 90] at step 26, then emit 11 base-32 characters (the last is always '0').
func geohashStandard11(lat, lon float64) string {
	const alpha = "0123456789bcdefghjkmnpqrstuvwxyz"
	latOffset := (lat - (-90.0)) / 180.0
	lonOffset := (lon - (-180.0)) / 360.0
	ilat := uint32(latOffset * float64(uint64(1)<<geoStepBits))
	ilon := uint32(lonOffset * float64(uint64(1)<<geoStepBits))
	bits := interleave64(ilat, ilon)
	buf := make([]byte, 11)
	for i := 0; i < 11; i++ {
		var idx int
		if i == 10 {
			idx = 0
		} else {
			idx = int((bits >> uint(52-((i+1)*5))) & 0x1f)
		}
		buf[i] = alpha[idx]
	}
	return string(buf)
}

// geoHaversine returns the great-circle distance in metres between two points,
// using the haversine formula and Redis' earth radius constant.
func geoHaversine(lat1, lon1, lat2, lon2 float64) float64 {
	la1 := lat1 * math.Pi / 180
	la2 := lat2 * math.Pi / 180
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180
	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(la1)*math.Cos(la2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusMeters * math.Asin(math.Sqrt(h))
}

// interleave64 Morton-interleaves two 32-bit values into a 64-bit value (x bits at
// even positions, y at odd positions), matching Redis interleave64.
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

// deinterleave64 is the inverse of interleave64, returning the two 32-bit values
// (even bits, odd bits), matching Redis deinterleave64.
func deinterleave64(interleaved uint64) (uint32, uint32) {
	B := [...]uint64{
		0x5555555555555555, 0x3333333333333333,
		0x0F0F0F0F0F0F0F0F, 0x00FF00FF00FF00FF,
		0x0000FFFF0000FFFF, 0x00000000FFFFFFFF,
	}
	S := [...]uint{0, 1, 2, 4, 8, 16}
	x := interleaved
	y := interleaved >> 1
	x = (x | (x >> S[0])) & B[0]
	x = (x | (x >> S[1])) & B[1]
	x = (x | (x >> S[2])) & B[2]
	x = (x | (x >> S[3])) & B[3]
	x = (x | (x >> S[4])) & B[4]
	x = (x | (x >> S[5])) & B[5]
	y = (y | (y >> S[0])) & B[0]
	y = (y | (y >> S[1])) & B[1]
	y = (y | (y >> S[2])) & B[2]
	y = (y | (y >> S[3])) & B[3]
	y = (y | (y >> S[4])) & B[4]
	y = (y | (y >> S[5])) & B[5]
	return uint32(x), uint32(y)
}
