package guard

import (
	"errors"
	"sync"
	"testing"
)

// bytesOf returns a byte slice of length n (contents are irrelevant to size
// enforcement, which only inspects len).
func bytesOf(n int) []byte {
	return make([]byte, n)
}

func TestCheckKeyBoundary(t *testing.T) {
	ResetInterceptions()
	cases := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{"empty", 0, false},
		{"one below limit", MaxNameSize - 1, false},
		{"exactly at limit", MaxNameSize, false},
		{"one over limit", MaxNameSize + 1, true},
		{"far over limit", MaxNameSize * 4, true},
	}
	wantIntercepts := uint64(0)
	for _, c := range cases {
		err := CheckKey(bytesOf(c.size))
		if c.wantErr {
			wantIntercepts++
			if !errors.Is(err, ErrSizeExceeded) {
				t.Errorf("%s: CheckKey(%d) err = %v, want ErrSizeExceeded", c.name, c.size, err)
			}
		} else if err != nil {
			t.Errorf("%s: CheckKey(%d) err = %v, want nil", c.name, c.size, err)
		}
	}
	if got := Interceptions(); got != wantIntercepts {
		t.Errorf("Interceptions() = %d, want %d", got, wantIntercepts)
	}
}

func TestCheckMemberBoundary(t *testing.T) {
	ResetInterceptions()
	// Members are sort keys (1-byte prefix + name <= 1024B), so the member limit
	// is one byte tighter than the key limit: exactly MaxMemberNameSize (1023)
	// passes, one more byte is rejected. An exactly-1024B member used to pass the
	// guard and then fail at the backend with a misleading "backend error".
	if err := CheckMember(bytesOf(MaxMemberNameSize)); err != nil {
		t.Errorf("CheckMember at limit err = %v, want nil", err)
	}
	if err := CheckMember(bytesOf(MaxMemberNameSize + 1)); !errors.Is(err, ErrSizeExceeded) {
		t.Errorf("CheckMember over limit err = %v, want ErrSizeExceeded", err)
	}
	if got := Interceptions(); got != 1 {
		t.Errorf("Interceptions() = %d, want 1", got)
	}
}

func TestCheckValueBoundary(t *testing.T) {
	ResetInterceptions()
	cases := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{"one below limit", MaxValueSize - 1, false},
		{"exactly at limit", MaxValueSize, false},
		{"one over limit", MaxValueSize + 1, true},
	}
	wantIntercepts := uint64(0)
	for _, c := range cases {
		err := CheckValue(bytesOf(c.size))
		if c.wantErr {
			wantIntercepts++
			if !errors.Is(err, ErrSizeExceeded) {
				t.Errorf("%s: CheckValue(%d) err = %v, want ErrSizeExceeded", c.name, c.size, err)
			}
		} else if err != nil {
			t.Errorf("%s: CheckValue(%d) err = %v, want nil", c.name, c.size, err)
		}
	}
	if got := Interceptions(); got != wantIntercepts {
		t.Errorf("Interceptions() = %d, want %d", got, wantIntercepts)
	}
}

func TestCheckWritePasses(t *testing.T) {
	ResetInterceptions()
	err := CheckWrite(
		bytesOf(MaxNameSize),
		[][]byte{bytesOf(0), bytesOf(MaxMemberNameSize)},
		[][]byte{bytesOf(MaxValueSize), bytesOf(10)},
	)
	if err != nil {
		t.Fatalf("CheckWrite at limits err = %v, want nil", err)
	}
	if got := Interceptions(); got != 0 {
		t.Errorf("Interceptions() = %d, want 0", got)
	}
}

func TestCheckWriteRejectsAndCountsOnce(t *testing.T) {
	cases := []struct {
		name    string
		key     []byte
		members [][]byte
		values  [][]byte
	}{
		{"oversized key", bytesOf(MaxNameSize + 1), nil, nil},
		{"oversized member", bytesOf(4), [][]byte{bytesOf(MaxMemberNameSize + 1)}, nil},
		{"oversized value", bytesOf(4), nil, [][]byte{bytesOf(MaxValueSize + 1)}},
		{
			"multiple violations count once",
			bytesOf(MaxNameSize + 1),
			[][]byte{bytesOf(MaxMemberNameSize + 1)},
			[][]byte{bytesOf(MaxValueSize + 1)},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ResetInterceptions()
			err := CheckWrite(c.key, c.members, c.values)
			if !errors.Is(err, ErrSizeExceeded) {
				t.Fatalf("CheckWrite err = %v, want ErrSizeExceeded", err)
			}
			if got := Interceptions(); got != 1 {
				t.Errorf("Interceptions() = %d, want 1 (single event per rejected write)", got)
			}
		})
	}
}

func TestInterceptionCounterConcurrent(t *testing.T) {
	ResetInterceptions()
	const goroutines = 50
	const perGoroutine = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				_ = CheckValue(bytesOf(MaxValueSize + 1))
			}
		}()
	}
	wg.Wait()
	if got, want := Interceptions(), uint64(goroutines*perGoroutine); got != want {
		t.Errorf("Interceptions() = %d, want %d", got, want)
	}
}
