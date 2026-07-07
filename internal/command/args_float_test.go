package command

import (
	"math"
	"testing"
)

func TestParseFloat(t *testing.T) {
	cases := []struct {
		in   string
		want float64
		ok   bool
	}{
		{"3.14", 3.14, true},
		{"-1.5", -1.5, true},
		{"0", 0, true},
		{"1e10", 1e10, true},
		{"", 0, true},      // Redis strtod: empty string -> 0.0 (INCRBYFLOAT/HINCRBYFLOAT/GEO)
		{"abc", 0, false},
		{"nan", 0, false},  // NaN is rejected
		{"NaN", 0, false},  // NaN is rejected
		{" ", 0, false},    // a single space is NOT empty -> rejected
		{" 3 ", 0, false},  // surrounding whitespace rejected
		{"3.5x", 0, false}, // trailing garbage rejected
	}
	for _, tc := range cases {
		got, err := ParseFloat([]byte(tc.in))
		if tc.ok {
			if err != nil {
				t.Errorf("ParseFloat(%q) errored: %v", tc.in, err)
				continue
			}
			if got != tc.want {
				t.Errorf("ParseFloat(%q) = %v, want %v", tc.in, got, tc.want)
			}
		} else if err == nil {
			t.Errorf("ParseFloat(%q) = %v, want an error", tc.in, got)
		}
	}
	// A NaN value must never leak through even if strconv would accept the literal.
	if _, err := ParseFloat([]byte("nan")); err != ErrNotFloat {
		t.Errorf("ParseFloat(nan) err = %v, want ErrNotFloat", err)
	}
	_ = math.NaN
}

func TestArgsAccessor(t *testing.T) {
	a := Args{[]byte("INCRBY"), []byte("key"), []byte("42")}

	if a.Len() != 3 {
		t.Fatalf("Len = %d, want 3", a.Len())
	}
	if a.Str(0) != "INCRBY" {
		t.Errorf("Str(0) = %q, want INCRBY", a.Str(0))
	}
	if n, err := a.Int(2); err != nil || n != 42 {
		t.Errorf("Int(2) = %d, %v; want 42, nil", n, err)
	}
	// A non-integer argument surfaces the canonical ErrNotInteger.
	bad := Args{[]byte("INCRBY"), []byte("key"), []byte("notanint")}
	if _, err := bad.Int(2); err != ErrNotInteger {
		t.Errorf("Int on non-integer err = %v, want ErrNotInteger", err)
	}

	f := Args{[]byte("ZADD"), []byte("z"), []byte("3.5")}
	if v, err := f.Float(2); err != nil || v != 3.5 {
		t.Errorf("Float(2) = %v, %v; want 3.5, nil", v, err)
	}
}
