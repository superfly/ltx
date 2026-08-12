package ltx_test

import (
	"bytes"
	"testing"

	"github.com/superfly/ltx"
)

func TestGenerateKeyPair(t *testing.T) {
	pub, priv, err := ltx.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	if len(pub) != 32 {
		t.Fatalf("public key length=%d, want 32", len(pub))
	}
	if len(priv) != 32 {
		t.Fatalf("private key length=%d, want 32", len(priv))
	}
}

func TestSealOpenCEK(t *testing.T) {
	t.Run("SingleRecipient", func(t *testing.T) {
		pub, priv, err := ltx.GenerateKeyPair()
		if err != nil {
			t.Fatal(err)
		}

		cek, err := ltx.GenerateCEK()
		if err != nil {
			t.Fatal(err)
		}

		rb, err := ltx.SealCEK(cek, [][]byte{pub})
		if err != nil {
			t.Fatal(err)
		}
		if len(rb.Entries) != 1 {
			t.Fatalf("entries=%d, want 1", len(rb.Entries))
		}

		got, err := ltx.OpenCEK(rb, priv)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, cek) {
			t.Fatal("CEK mismatch")
		}
	})

	t.Run("MultiRecipient", func(t *testing.T) {
		pub1, priv1, _ := ltx.GenerateKeyPair()
		pub2, priv2, _ := ltx.GenerateKeyPair()
		pub3, priv3, _ := ltx.GenerateKeyPair()

		cek, _ := ltx.GenerateCEK()

		rb, err := ltx.SealCEK(cek, [][]byte{pub1, pub2, pub3})
		if err != nil {
			t.Fatal(err)
		}
		if len(rb.Entries) != 3 {
			t.Fatalf("entries=%d, want 3", len(rb.Entries))
		}

		for i, priv := range [][]byte{priv1, priv2, priv3} {
			got, err := ltx.OpenCEK(rb, priv)
			if err != nil {
				t.Fatalf("recipient %d: %v", i, err)
			}
			if !bytes.Equal(got, cek) {
				t.Fatalf("recipient %d: CEK mismatch", i)
			}
		}
	})

	t.Run("WrongKey", func(t *testing.T) {
		pub, _, _ := ltx.GenerateKeyPair()
		_, wrongPriv, _ := ltx.GenerateKeyPair()

		cek, _ := ltx.GenerateCEK()
		rb, err := ltx.SealCEK(cek, [][]byte{pub})
		if err != nil {
			t.Fatal(err)
		}

		if _, err := ltx.OpenCEK(rb, wrongPriv); err != ltx.ErrDecryptionKeyInvalid {
			t.Fatalf("expected ErrDecryptionKeyInvalid, got: %v", err)
		}
	})
}

func TestRecipientBlock_MarshalUnmarshal(t *testing.T) {
	pub, _, _ := ltx.GenerateKeyPair()
	cek, _ := ltx.GenerateCEK()

	rb, err := ltx.SealCEK(cek, [][]byte{pub})
	if err != nil {
		t.Fatal(err)
	}

	data, err := rb.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != ltx.RecipientEntrySize {
		t.Fatalf("marshal size=%d, want %d", len(data), ltx.RecipientEntrySize)
	}

	var rb2 ltx.RecipientBlock
	if err := rb2.UnmarshalBinary(data, 1); err != nil {
		t.Fatal(err)
	}
	if len(rb2.Entries) != 1 {
		t.Fatalf("entries=%d, want 1", len(rb2.Entries))
	}
	if !bytes.Equal(rb2.Entries[0], rb.Entries[0]) {
		t.Fatal("entry mismatch")
	}
}

func TestEncryptDecryptPage(t *testing.T) {
	t.Run("RoundTrip", func(t *testing.T) {
		cek, _ := ltx.GenerateCEK()
		pageKey, _ := ltx.DerivePageKey(cek)

		compressed := []byte("compressed page data here for testing round-trip encryption")
		aad := ltx.BuildPageAAD([32]byte{1, 2, 3}, 42, []byte{0, 0, 0, 42, 0, 1})

		encrypted, err := ltx.EncryptPage(pageKey, compressed, aad)
		if err != nil {
			t.Fatal(err)
		}
		if len(encrypted) < ltx.NonceSize+ltx.AuthTagSize+len(compressed) {
			t.Fatal("encrypted data too short")
		}

		decrypted, err := ltx.DecryptPage(pageKey, encrypted, aad)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(decrypted, compressed) {
			t.Fatal("plaintext mismatch")
		}
	})

	t.Run("TamperedCiphertext", func(t *testing.T) {
		cek, _ := ltx.GenerateCEK()
		pageKey, _ := ltx.DerivePageKey(cek)
		aad := ltx.BuildPageAAD([32]byte{}, 1, []byte{0, 0, 0, 1, 0, 0})

		encrypted, _ := ltx.EncryptPage(pageKey, []byte("data"), aad)
		encrypted[ltx.NonceSize+2] ^= 0xFF

		if _, err := ltx.DecryptPage(pageKey, encrypted, aad); err == nil {
			t.Fatal("expected error on tampered ciphertext")
		}
	})

	t.Run("WrongAAD", func(t *testing.T) {
		cek, _ := ltx.GenerateCEK()
		pageKey, _ := ltx.DerivePageKey(cek)
		aad := ltx.BuildPageAAD([32]byte{}, 1, []byte{0, 0, 0, 1, 0, 0})
		wrongAAD := ltx.BuildPageAAD([32]byte{}, 2, []byte{0, 0, 0, 2, 0, 0})

		encrypted, _ := ltx.EncryptPage(pageKey, []byte("data"), aad)

		if _, err := ltx.DecryptPage(pageKey, encrypted, wrongAAD); err == nil {
			t.Fatal("expected error with wrong AAD")
		}
	})
}

func TestAuthenticateVerifyIndex(t *testing.T) {
	t.Run("RoundTrip", func(t *testing.T) {
		cek, _ := ltx.GenerateCEK()
		indexKey, _ := ltx.DeriveIndexKey(cek)
		indexBytes := []byte("varint encoded page index data here")

		tag, err := ltx.AuthenticateIndex(indexKey, indexBytes)
		if err != nil {
			t.Fatal(err)
		}
		if len(tag) != ltx.AuthTagSize {
			t.Fatalf("tag size=%d, want %d", len(tag), ltx.AuthTagSize)
		}

		if err := ltx.VerifyIndexAuth(indexKey, indexBytes, tag); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("TamperedIndex", func(t *testing.T) {
		cek, _ := ltx.GenerateCEK()
		indexKey, _ := ltx.DeriveIndexKey(cek)
		indexBytes := []byte("some index data")

		tag, _ := ltx.AuthenticateIndex(indexKey, indexBytes)

		tampered := append([]byte{}, indexBytes...)
		tampered[0] ^= 0xFF

		if err := ltx.VerifyIndexAuth(indexKey, tampered, tag); err != ltx.ErrIndexAuthFailed {
			t.Fatalf("expected ErrIndexAuthFailed, got: %v", err)
		}
	})
}

func TestDeriveKeys(t *testing.T) {
	cek, _ := ltx.GenerateCEK()

	pageKey, err := ltx.DerivePageKey(cek)
	if err != nil {
		t.Fatal(err)
	}
	indexKey, err := ltx.DeriveIndexKey(cek)
	if err != nil {
		t.Fatal(err)
	}

	if len(pageKey) != 32 {
		t.Fatalf("page key length=%d, want 32", len(pageKey))
	}
	if len(indexKey) != 32 {
		t.Fatalf("index key length=%d, want 32", len(indexKey))
	}
	if bytes.Equal(pageKey, indexKey) {
		t.Fatal("page key and index key must be different")
	}
}
