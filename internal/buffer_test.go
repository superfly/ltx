package internal_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/superfly/ltx/internal"
)

func TestBuffer(t *testing.T) {
	var b internal.Buffer
	if n, err := b.Write([]byte("foo")); err != nil {
		t.Fatal(err)
	} else if got, want := n, 3; got != want {
		t.Fatalf("n=%d, want %d", got, want)
	}

	if n, err := b.Write([]byte("bar")); err != nil {
		t.Fatal(err)
	} else if got, want := n, 3; got != want {
		t.Fatalf("n=%d, want %d", got, want)
	}

	if n, err := b.Write([]byte("baz!")); err != nil {
		t.Fatal(err)
	} else if got, want := n, 4; got != want {
		t.Fatalf("n=%d, want %d", got, want)
	}

	if off, err := b.Seek(3, io.SeekStart); err != nil {
		t.Fatal(err)
	} else if got, want := off, int64(3); got != want {
		t.Fatalf("off=%d, want %d", got, want)
	}

	if n, err := b.Write([]byte("BAR")); err != nil {
		t.Fatal(err)
	} else if got, want := n, 3; got != want {
		t.Fatalf("n=%d, want %d", got, want)
	}

	if got, want := b.Bytes(), []byte(`fooBARbaz!`); !bytes.Equal(got, want) {
		t.Fatalf("Bytes()=%s, want %s", got, want)
	}

	buf := make([]byte, 1024)
	if n, err := b.Read(buf[:5]); err != nil {
		t.Fatal(err)
	} else if got, want := n, 5; got != want {
		t.Fatalf("n=%d, want %d", got, want)
	} else if got, want := string(buf[:n]), "fooBA"; got != want {
		t.Fatalf("n=%s, want %s", got, want)
	}

	if n, err := b.Read(buf[:1]); err != nil {
		t.Fatal(err)
	} else if got, want := n, 1; got != want {
		t.Fatalf("n=%d, want %d", got, want)
	} else if got, want := string(buf[:n]), "R"; got != want {
		t.Fatalf("n=%s, want %s", got, want)
	}

	if n, err := b.Read(buf); err != nil {
		t.Fatal(err)
	} else if got, want := n, 4; got != want {
		t.Fatalf("n=%d, want %d", got, want)
	} else if got, want := string(buf[:n]), "baz!"; got != want {
		t.Fatalf("n=%s, want %s", got, want)
	}

	if _, err := b.Read(buf); err != io.EOF {
		t.Fatalf("unexpected error: %s", err)
	}
}
