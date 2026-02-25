package ltx

import (
	"crypto/ecdh"
	"crypto/hpke"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

const (
	CEKSize       = 32
	X25519PubSize = 32
)

var (
	ErrDecryptionKeyRequired = fmt.Errorf("decryption key required for encrypted file")
	ErrDecryptionKeyInvalid  = fmt.Errorf("failed to decrypt: no matching recipient entry")
	ErrIndexAuthFailed       = fmt.Errorf("page index authentication failed")
)

func hpkeKEM() hpke.KEM   { return hpke.DHKEM(ecdh.X25519()) }
func hpkeKDF() hpke.KDF   { return hpke.HKDFSHA256() }
func hpkeAEAD() hpke.AEAD { return hpke.ChaCha20Poly1305() }

func GenerateKeyPair() (pub, priv []byte, err error) {
	kem := hpkeKEM()
	privKey, err := kem.GenerateKey()
	if err != nil {
		return nil, nil, fmt.Errorf("generate key: %w", err)
	}
	privBytes, err := privKey.Bytes()
	if err != nil {
		return nil, nil, fmt.Errorf("serialize private key: %w", err)
	}
	return privKey.PublicKey().Bytes(), privBytes, nil
}

func GenerateCEK() ([]byte, error) {
	cek := make([]byte, CEKSize)
	if _, err := io.ReadFull(rand.Reader, cek); err != nil {
		return nil, fmt.Errorf("generate CEK: %w", err)
	}
	return cek, nil
}

type RecipientBlock struct {
	Entries [][]byte
}

func (rb *RecipientBlock) MarshalBinary() ([]byte, error) {
	b := make([]byte, len(rb.Entries)*RecipientEntrySize)
	for i, entry := range rb.Entries {
		if len(entry) != RecipientEntrySize {
			return nil, fmt.Errorf("invalid recipient entry size: %d", len(entry))
		}
		copy(b[i*RecipientEntrySize:], entry)
	}
	return b, nil
}

func (rb *RecipientBlock) UnmarshalBinary(b []byte, count int) error {
	if len(b) < count*RecipientEntrySize {
		return io.ErrShortBuffer
	}
	rb.Entries = make([][]byte, count)
	for i := range count {
		rb.Entries[i] = make([]byte, RecipientEntrySize)
		copy(rb.Entries[i], b[i*RecipientEntrySize:])
	}
	return nil
}

func SealCEK(cek []byte, recipientPubKeys [][]byte) (*RecipientBlock, error) {
	if len(cek) != CEKSize {
		return nil, fmt.Errorf("invalid CEK size: %d", len(cek))
	}

	kem := hpkeKEM()
	kdf := hpkeKDF()
	aead := hpkeAEAD()
	info := []byte("ltx-cek-wrap")

	rb := &RecipientBlock{Entries: make([][]byte, len(recipientPubKeys))}

	for i, pubKeyBytes := range recipientPubKeys {
		pubKey, err := kem.NewPublicKey(pubKeyBytes)
		if err != nil {
			return nil, fmt.Errorf("parse public key %d: %w", i, err)
		}

		sealed, err := hpke.Seal(pubKey, kdf, aead, info, cek)
		if err != nil {
			return nil, fmt.Errorf("seal CEK for recipient %d: %w", i, err)
		}

		if len(sealed) != RecipientEntrySize {
			return nil, fmt.Errorf("unexpected sealed size: got %d, want %d", len(sealed), RecipientEntrySize)
		}

		rb.Entries[i] = sealed
	}

	return rb, nil
}

func OpenCEK(rb *RecipientBlock, privKeyBytes []byte) ([]byte, error) {
	kem := hpkeKEM()
	kdf := hpkeKDF()
	aead := hpkeAEAD()
	info := []byte("ltx-cek-wrap")

	privKey, err := kem.NewPrivateKey(privKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}

	for _, entry := range rb.Entries {
		cek, err := hpke.Open(privKey, kdf, aead, info, entry)
		if err != nil {
			continue
		}
		return cek, nil
	}

	return nil, ErrDecryptionKeyInvalid
}

func DerivePageKey(cek []byte) ([]byte, error) {
	return deriveKey(cek, "ltx-page-key")
}

func DeriveIndexKey(cek []byte) ([]byte, error) {
	return deriveKey(cek, "ltx-index-key")
}

func deriveKey(cek []byte, info string) ([]byte, error) {
	r := hkdf.Expand(sha256.New, cek, []byte(info))
	key := make([]byte, CEKSize)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, fmt.Errorf("derive key (%s): %w", info, err)
	}
	return key, nil
}

func EncryptPage(pageKey, compressed, aad []byte) ([]byte, error) {
	aead, err := chacha20poly1305.New(pageKey)
	if err != nil {
		return nil, fmt.Errorf("create AEAD: %w", err)
	}

	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	ciphertext := aead.Seal(nil, nonce, compressed, aad)

	result := make([]byte, NonceSize+len(ciphertext))
	copy(result, nonce)
	copy(result[NonceSize:], ciphertext)
	return result, nil
}

func DecryptPage(pageKey, encrypted, aad []byte) ([]byte, error) {
	if len(encrypted) < NonceSize+AuthTagSize {
		return nil, fmt.Errorf("encrypted data too short")
	}

	aead, err := chacha20poly1305.New(pageKey)
	if err != nil {
		return nil, fmt.Errorf("create AEAD: %w", err)
	}

	nonce := encrypted[:NonceSize]
	ciphertext := encrypted[NonceSize:]

	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("decrypt page: %w", err)
	}
	return plaintext, nil
}

func BuildPageAAD(headerHash [32]byte, pgno uint32, pageHeaderBytes []byte) []byte {
	aad := make([]byte, 32+4+len(pageHeaderBytes))
	copy(aad, headerHash[:])
	binary.BigEndian.PutUint32(aad[32:], pgno)
	copy(aad[36:], pageHeaderBytes)
	return aad
}

func AuthenticateIndex(indexKey, indexBytes []byte) ([]byte, error) {
	aead, err := chacha20poly1305.New(indexKey)
	if err != nil {
		return nil, fmt.Errorf("create AEAD: %w", err)
	}

	nonce := []byte("ltx-idx-auth")
	tag := aead.Seal(nil, nonce, nil, indexBytes)
	return tag, nil
}

func VerifyIndexAuth(indexKey, indexBytes, tag []byte) error {
	aead, err := chacha20poly1305.New(indexKey)
	if err != nil {
		return fmt.Errorf("create AEAD: %w", err)
	}

	nonce := []byte("ltx-idx-auth")
	if _, err := aead.Open(nil, nonce, tag, indexBytes); err != nil {
		return ErrIndexAuthFailed
	}
	return nil
}

func HeaderHash(headerBytes []byte) [32]byte {
	return sha256.Sum256(headerBytes)
}
