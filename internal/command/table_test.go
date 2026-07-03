package command

import (
	"context"
	"testing"

	"github.com/aura-studio/redimos/v2/internal/server"
)

// noopHandler is a stand-in handler for table-primitive tests that do not
// invoke it.
func noopHandler(context.Context, *server.Conn, [][]byte) {}

func TestRegisterNormalizesNameAndStoresSpec(t *testing.T) {
	tbl := NewTable()
	tbl.Register("GET", 2, false, noopHandler)

	// Registered under the lowercase name.
	spec, ok := tbl["get"]
	if !ok {
		t.Fatalf("Register did not store command under lowercase key")
	}
	if spec.Name != "get" {
		t.Errorf("spec.Name = %q, want %q (lowercased)", spec.Name, "get")
	}
	if spec.Arity != 2 {
		t.Errorf("spec.Arity = %d, want 2", spec.Arity)
	}
	if spec.Write {
		t.Errorf("spec.Write = true, want false")
	}
	if spec.Handler == nil {
		t.Errorf("spec.Handler is nil")
	}
}

func TestLookupIsCaseInsensitive(t *testing.T) {
	tbl := NewTable()
	tbl.Register("set", -3, true, noopHandler)

	for _, name := range []string{"set", "SET", "Set", "sEt"} {
		spec, ok := tbl.Lookup(name)
		if !ok {
			t.Errorf("Lookup(%q) not found", name)
			continue
		}
		if spec.Name != "set" {
			t.Errorf("Lookup(%q).Name = %q, want %q", name, spec.Name, "set")
		}
	}

	if _, ok := tbl.Lookup("nope"); ok {
		t.Errorf("Lookup(\"nope\") should not be found")
	}
}

func TestArityOK(t *testing.T) {
	cases := []struct {
		name  string
		arity int
		argc  int
		want  bool
	}{
		{"exact match", 2, 2, true},
		{"exact too few", 2, 1, false},
		{"exact too many", 2, 3, false},
		{"exact one arg command", 1, 1, true},
		{"negative at minimum", -3, 3, true},
		{"negative above minimum", -3, 5, true},
		{"negative below minimum", -3, 2, false},
		{"negative one arg", -1, 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			spec := CmdSpec{Arity: c.arity}
			if got := spec.arityOK(c.argc); got != c.want {
				t.Errorf("arityOK(arity=%d, argc=%d) = %v, want %v", c.arity, c.argc, got, c.want)
			}
		})
	}
}

func TestRegisterPanicsOnDuplicate(t *testing.T) {
	tbl := NewTable()
	tbl.Register("ping", 1, false, noopHandler)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on duplicate registration")
		}
	}()
	tbl.Register("PING", 1, false, noopHandler)
}

func TestRegisterPanicsOnEmptyNameOrNilHandler(t *testing.T) {
	t.Run("empty name", func(t *testing.T) {
		tbl := NewTable()
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on empty name")
			}
		}()
		tbl.Register("", 1, false, noopHandler)
	})
	t.Run("nil handler", func(t *testing.T) {
		tbl := NewTable()
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on nil handler")
			}
		}()
		tbl.Register("x", 1, false, nil)
	})
}
