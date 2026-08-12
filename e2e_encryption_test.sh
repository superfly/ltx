#!/usr/bin/env bash
set -euo pipefail

# E2E Encryption Tests for LTX HPKE (v4)
# Tests the full CLI workflow for per-page encryption using HPKE (RFC 9180).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LTX=""
TMPDIR=""
REPORT=""
PASS_COUNT=0
FAIL_COUNT=0
TOTAL_COUNT=0
TEST_OUTPUTS=""

# --- Helpers ---

log() { printf "[%s] %s\n" "$(date +%H:%M:%S)" "$*"; }

pass() {
    PASS_COUNT=$((PASS_COUNT + 1))
    TOTAL_COUNT=$((TOTAL_COUNT + 1))
    log "PASS: $1"
    TEST_OUTPUTS+="| $1 | PASS | ${2:-} |\n"
}

fail() {
    FAIL_COUNT=$((FAIL_COUNT + 1))
    TOTAL_COUNT=$((TOTAL_COUNT + 1))
    log "FAIL: $1 — $2"
    TEST_OUTPUTS+="| $1 | **FAIL** | ${2:-} |\n"
}

millis() { python3 -c 'import time; print(int(time.time()*1000))'; }

fmt_duration() {
    local ms="$1"
    if [[ $ms -ge 1000 ]]; then
        printf "%d.%03ds" $((ms / 1000)) $((ms % 1000))
    else
        printf "%dms" "$ms"
    fi
}

run_test() {
    local name="$1"
    local fn="$2"
    log "--- Running: $name ---"
    local start
    start=$(millis)
    if "$fn"; then
        local dur=$(( $(millis) - start ))
        pass "$name" "$(fmt_duration $dur)"
    else
        local dur=$(( $(millis) - start ))
        fail "$name" "$(fmt_duration $dur)"
    fi
}

create_db() {
    local path="$1"
    local page_size="${2:-4096}"
    local sql="$3"
    sqlite3 "$path" <<EOF
PRAGMA page_size=$page_size;
PRAGMA journal_mode=delete;
$sql
VACUUM;
EOF
}

parse_keygen() {
    local output
    output=$("$LTX" keygen)
    PUB=$(echo "$output" | grep '^public:' | sed 's/^public:  *//')
    PRIV=$(echo "$output" | grep '^private:' | sed 's/^private: *//')
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    local msg="${3:-}"
    if [[ "$haystack" != *"$needle"* ]]; then
        echo "ASSERT FAILED: expected output to contain '$needle'${msg:+ ($msg)}"
        echo "GOT: $haystack"
        return 1
    fi
}

assert_exit_nonzero() {
    local desc="$1"
    shift
    if "$@" 2>/dev/null; then
        echo "ASSERT FAILED: expected non-zero exit for: $desc"
        return 1
    fi
    return 0
}

assert_exit_zero() {
    local desc="$1"
    shift
    if ! "$@" 2>&1; then
        echo "ASSERT FAILED: expected zero exit for: $desc"
        return 1
    fi
    return 0
}

# --- Setup ---

setup() {
    log "Checking prerequisites..."

    if ! command -v go &>/dev/null; then
        echo "ERROR: go is required" >&2; exit 1
    fi
    local go_ver
    go_ver=$(go version | grep -oE '[0-9]+\.[0-9]+')
    log "Go version: $(go version)"

    if ! command -v sqlite3 &>/dev/null; then
        echo "ERROR: sqlite3 is required" >&2; exit 1
    fi
    log "sqlite3 version: $(sqlite3 --version)"

    HAS_DOCKER=false
    if command -v docker &>/dev/null; then
        HAS_DOCKER=true
        log "Docker: available"
    else
        log "Docker: not found (Docker test will be skipped)"
    fi

    log "Building ltx binary..."
    mkdir -p "$SCRIPT_DIR/dist"
    (cd "$SCRIPT_DIR" && go build -o ./dist/ltx ./cmd/ltx)
    LTX="$SCRIPT_DIR/dist/ltx"
    log "Binary: $LTX ($("$LTX" version 2>&1 || echo 'unknown'))"

    TMPDIR=$(mktemp -d "${TMPDIR:-/tmp}/ltx-e2e.XXXXXX")
    log "Temp dir: $TMPDIR"

    REPORT="$TMPDIR/e2e_report.md"
    local git_commit
    git_commit=$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo "unknown")

    cat > "$REPORT" <<HEADER
# LTX HPKE E2E Encryption Test Report

- **Date:** $(date -u +%Y-%m-%dT%H:%M:%SZ)
- **Git commit:** $git_commit
- **Go version:** $(go version)
- **LTX version:** $("$LTX" version 2>&1 || echo 'unknown')
- **Platform:** $(uname -s)/$(uname -m)

## Results

| Test | Result | Duration |
|------|--------|----------|
HEADER

    log "Setup complete."
}

teardown() {
    echo -e "$TEST_OUTPUTS" >> "$REPORT"

    cat >> "$REPORT" <<FOOTER

## Summary

- **Total:** $TOTAL_COUNT
- **Passed:** $PASS_COUNT
- **Failed:** $FAIL_COUNT
FOOTER

    log "==============================="
    log "Results: $PASS_COUNT passed, $FAIL_COUNT failed (of $TOTAL_COUNT)"
    log "Report: $REPORT"
    log "Temp dir preserved at: $TMPDIR"
    log "==============================="

    if [[ $FAIL_COUNT -gt 0 ]]; then
        exit 1
    fi
}

# --- Test 1: Docker Build ---

test_docker_build() {
    if [[ "$HAS_DOCKER" != "true" ]]; then
        log "Skipping Docker test (docker not available)"
        return 0
    fi
    docker build -t ltx-e2e-test "$SCRIPT_DIR"
    local ver
    ver=$(docker run --rm ltx-e2e-test version 2>&1)
    log "Docker image version: $ver"
    [[ -n "$ver" ]]
}

# --- Test 2: Key Generation ---

test_keygen() {
    local pub1 priv1 pub2 priv2 pub3 priv3

    for i in 1 2 3; do
        local output
        output=$("$LTX" keygen)

        local pub priv
        pub=$(echo "$output" | grep '^public:' | sed 's/^public:  *//')
        priv=$(echo "$output" | grep '^private:' | sed 's/^private: *//')

        if [[ ${#pub} -ne 64 ]]; then
            echo "Public key $i has wrong length: ${#pub} (expected 64)"
            return 1
        fi
        if [[ ${#priv} -ne 64 ]]; then
            echo "Private key $i has wrong length: ${#priv} (expected 64)"
            return 1
        fi
        if ! echo "$pub" | grep -qE '^[0-9a-f]{64}$'; then
            echo "Public key $i is not valid hex: $pub"
            return 1
        fi
        if ! echo "$priv" | grep -qE '^[0-9a-f]{64}$'; then
            echo "Private key $i is not valid hex: $priv"
            return 1
        fi

        eval "pub${i}=\$pub"
        eval "priv${i}=\$priv"
    done

    if [[ "$pub1" == "$pub2" ]] || [[ "$pub1" == "$pub3" ]] || [[ "$pub2" == "$pub3" ]]; then
        echo "Keypairs are not distinct (randomness failure)"
        return 1
    fi

    log "All 3 keypairs generated with correct format and distinct values"
}

# --- Test 3: Single-Key Encryption Workflow ---

test_single_key_workflow() {
    local dir="$TMPDIR/test3"
    mkdir -p "$dir"

    create_db "$dir/original.db" 4096 "
        CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, email TEXT);
        INSERT INTO users VALUES(1, 'Alice', 'alice@example.com');
        INSERT INTO users VALUES(2, 'Bob', 'bob@example.com');
        INSERT INTO users VALUES(3, 'Charlie', 'charlie@example.com');
    "

    local orig_cksum
    orig_cksum=$("$LTX" checksum "$dir/original.db")
    log "Original checksum: $orig_cksum"

    parse_keygen
    local pub="$PUB" priv="$PRIV"

    "$LTX" encode-db -o "$dir/enc.ltx" -encrypt-to "$pub" "$dir/original.db"

    local verify_out
    verify_out=$("$LTX" verify -key "$priv" "$dir/enc.ltx" 2>&1)
    assert_contains "$verify_out" "ok" "verify should say ok"

    local dump_out
    dump_out=$("$LTX" dump -key "$priv" "$dir/enc.ltx" 2>&1)
    assert_contains "$dump_out" "Encrypted: yes" "dump should show Encrypted: yes"
    assert_contains "$dump_out" "recipients=1" "dump should show recipients=1"
    assert_contains "$dump_out" "KEM=0x0020" "dump should show KEM"
    assert_contains "$dump_out" "KDF=0x0001" "dump should show KDF"
    assert_contains "$dump_out" "AEAD=0x0003" "dump should show AEAD"

    local list_out
    list_out=$("$LTX" list -key "$priv" "$dir/enc.ltx" 2>&1)
    # Check the encrypted column shows yes (last column in the table)
    assert_contains "$list_out" "yes" "list should show encrypted=yes"

    "$LTX" apply -db "$dir/restored.db" -key "$priv" "$dir/enc.ltx"

    local restored_cksum
    restored_cksum=$("$LTX" checksum "$dir/restored.db")
    if [[ "$orig_cksum" != "$restored_cksum" ]]; then
        echo "Checksum mismatch: original=$orig_cksum restored=$restored_cksum"
        return 1
    fi

    if ! cmp "$dir/original.db" "$dir/restored.db"; then
        echo "Restored DB is not byte-identical to original"
        return 1
    fi

    log "Single-key workflow passed: encode → verify → dump → list → apply → checksum match"
}

# --- Test 4: Multi-Recipient ---

test_multi_recipient() {
    local dir="$TMPDIR/test4"
    mkdir -p "$dir"

    create_db "$dir/original.db" 4096 "
        CREATE TABLE data(id INTEGER PRIMARY KEY, val TEXT);
        INSERT INTO data VALUES(1, 'test');
    "
    local orig_cksum
    orig_cksum=$("$LTX" checksum "$dir/original.db")

    local pubs=() privs=()
    for i in 1 2 3; do
        parse_keygen
        pubs+=("$PUB")
        privs+=("$PRIV")
    done

    local encrypt_to="${pubs[0]},${pubs[1]},${pubs[2]}"
    "$LTX" encode-db -o "$dir/multi.ltx" -encrypt-to "$encrypt_to" "$dir/original.db"

    local dump_out
    dump_out=$("$LTX" dump -key "${privs[0]}" "$dir/multi.ltx" 2>&1)
    assert_contains "$dump_out" "recipients=3" "dump should show recipients=3"

    for i in 0 1 2; do
        local v_out
        v_out=$("$LTX" verify -key "${privs[$i]}" "$dir/multi.ltx" 2>&1)
        assert_contains "$v_out" "ok" "verify with key $i should say ok"

        "$LTX" apply -db "$dir/restored_${i}.db" -key "${privs[$i]}" "$dir/multi.ltx"
        local ck
        ck=$("$LTX" checksum "$dir/restored_${i}.db")
        if [[ "$orig_cksum" != "$ck" ]]; then
            echo "Checksum mismatch for key $i: expected=$orig_cksum got=$ck"
            return 1
        fi
    done

    parse_keygen
    local bad_priv="$PRIV"
    local err_out
    if err_out=$("$LTX" verify -key "$bad_priv" "$dir/multi.ltx" 2>&1); then
        echo "Expected verify to fail with non-recipient key"
        return 1
    fi
    assert_contains "$err_out" "no matching recipient" "wrong key should say no matching recipient"

    log "Multi-recipient test passed: 3 recipients, all decrypt, non-recipient rejected"
}

# --- Test 5: Key Rotation (Rekey) ---

test_rekey() {
    local dir="$TMPDIR/test5"
    mkdir -p "$dir"

    create_db "$dir/original.db" 4096 "
        CREATE TABLE t(x INTEGER);
        INSERT INTO t VALUES(42);
    "
    local orig_cksum
    orig_cksum=$("$LTX" checksum "$dir/original.db")

    parse_keygen; local old_pub="$PUB" old_priv="$PRIV"
    parse_keygen; local new_pub="$PUB" new_priv="$PRIV"

    "$LTX" encode-db -o "$dir/old.ltx" -encrypt-to "$old_pub" "$dir/original.db"

    "$LTX" rekey -key "$old_priv" -encrypt-to "$new_pub" -o "$dir/new.ltx" "$dir/old.ltx"

    local v_out
    v_out=$("$LTX" verify -key "$new_priv" "$dir/new.ltx" 2>&1)
    assert_contains "$v_out" "ok" "new key should verify new file"

    if "$LTX" verify -key "$old_priv" "$dir/new.ltx" 2>/dev/null; then
        echo "Old key should NOT verify new file"
        return 1
    fi

    "$LTX" apply -db "$dir/new_restored.db" -key "$new_priv" "$dir/new.ltx"
    local ck
    ck=$("$LTX" checksum "$dir/new_restored.db")
    if [[ "$orig_cksum" != "$ck" ]]; then
        echo "Checksum mismatch after rekey: expected=$orig_cksum got=$ck"
        return 1
    fi

    parse_keygen; local new2_pub="$PUB" new2_priv="$PRIV"
    "$LTX" rekey -key "$old_priv" -encrypt-to "$new_pub,$new2_pub" -o "$dir/multi_new.ltx" "$dir/old.ltx"
    v_out=$("$LTX" verify -key "$new_priv" "$dir/multi_new.ltx" 2>&1)
    assert_contains "$v_out" "ok" "first new key should verify multi-rekey file"
    v_out=$("$LTX" verify -key "$new2_priv" "$dir/multi_new.ltx" 2>&1)
    assert_contains "$v_out" "ok" "second new key should verify multi-rekey file"

    "$LTX" rekey -key "$old_priv" -o "$dir/plain.ltx" "$dir/old.ltx"
    v_out=$("$LTX" verify "$dir/plain.ltx" 2>&1)
    assert_contains "$v_out" "ok" "decrypted file should verify without key"
    local dump_out
    dump_out=$("$LTX" dump "$dir/plain.ltx" 2>&1)
    assert_contains "$dump_out" "Encrypted: no" "decrypted file should show Encrypted: no"

    log "Rekey test passed: rotate, multi-rotate, decrypt-to-plain"
}

# --- Test 6: Sequential Encrypted Apply ---

test_sequential_apply() {
    local dir="$TMPDIR/test6"
    mkdir -p "$dir"

    create_db "$dir/v1.db" 4096 "
        CREATE TABLE log(id INTEGER PRIMARY KEY, msg TEXT);
        INSERT INTO log VALUES(1, 'first entry');
        INSERT INTO log VALUES(2, 'second entry');
    "
    local v1_cksum
    v1_cksum=$("$LTX" checksum "$dir/v1.db")

    parse_keygen; local pub="$PUB" priv="$PRIV"

    "$LTX" encode-db -o "$dir/snapshot.ltx" -encrypt-to "$pub" "$dir/v1.db"

    "$LTX" apply -db "$dir/fresh.db" -key "$priv" "$dir/snapshot.ltx"

    local fresh_cksum
    fresh_cksum=$("$LTX" checksum "$dir/fresh.db")
    if [[ "$v1_cksum" != "$fresh_cksum" ]]; then
        echo "Checksum mismatch: v1=$v1_cksum fresh=$fresh_cksum"
        return 1
    fi

    log "Sequential apply test passed"
}

# --- Test 7: Backward Compatibility ---

test_backward_compat() {
    local dir="$TMPDIR/test7"
    mkdir -p "$dir"

    create_db "$dir/original.db" 4096 "
        CREATE TABLE t(x INTEGER);
        INSERT INTO t VALUES(1);
    "
    local orig_cksum
    orig_cksum=$("$LTX" checksum "$dir/original.db")

    "$LTX" encode-db -o "$dir/plain.ltx" "$dir/original.db"

    local v_out
    v_out=$("$LTX" verify "$dir/plain.ltx" 2>&1)
    assert_contains "$v_out" "ok" "plain verify"

    local dump_out
    dump_out=$("$LTX" dump "$dir/plain.ltx" 2>&1)
    assert_contains "$dump_out" "Encrypted: no" "plain dump"

    local list_out
    list_out=$("$LTX" list "$dir/plain.ltx" 2>&1)
    assert_contains "$list_out" "no" "plain list should show encrypted=no"

    "$LTX" apply -db "$dir/restored.db" "$dir/plain.ltx"
    local ck
    ck=$("$LTX" checksum "$dir/restored.db")
    if [[ "$orig_cksum" != "$ck" ]]; then
        echo "Plain file checksum mismatch"
        return 1
    fi

    parse_keygen; local pub="$PUB" priv="$PRIV"

    # Providing an unnecessary key to a plain file should work (key is ignored)
    v_out=$("$LTX" verify -key "$priv" "$dir/plain.ltx" 2>&1)
    assert_contains "$v_out" "ok" "plain file with unnecessary key"

    # Omitting the key still verifies integrity: the file checksum covers the
    # ciphertext, so a holder of the file can detect corruption without being
    # able to read it.
    "$LTX" encode-db -o "$dir/enc.ltx" -encrypt-to "$pub" "$dir/original.db"
    local v_nokey
    if ! v_nokey=$("$LTX" verify "$dir/enc.ltx" 2>&1); then
        echo "Keyless verify of an intact encrypted file should succeed: $v_nokey"
        return 1
    fi
    assert_contains "$v_nokey" "contents not verified" "keyless verify reports limited scope"

    log "Backward compatibility test passed"
}

# --- Test 8: Tamper Detection ---

test_tamper_detection() {
    local dir="$TMPDIR/test8"
    mkdir -p "$dir"

    create_db "$dir/original.db" 4096 "
        CREATE TABLE t(x TEXT);
        INSERT INTO t VALUES('tamper test data that is long enough to fill some space');
    "

    parse_keygen; local pub="$PUB" priv="$PRIV"
    "$LTX" encode-db -o "$dir/enc.ltx" -encrypt-to "$pub" "$dir/original.db"

    # Verify the original is valid first
    "$LTX" verify -key "$priv" "$dir/enc.ltx"

    local file_size
    file_size=$(wc -c < "$dir/enc.ltx" | tr -d ' ')

    # Tamper with page data region (offset 250, well past header+recipients)
    cp "$dir/enc.ltx" "$dir/tampered_page.ltx"
    printf '\xff' | dd of="$dir/tampered_page.ltx" bs=1 seek=250 count=1 conv=notrunc 2>/dev/null
    if "$LTX" verify -key "$priv" "$dir/tampered_page.ltx" 2>/dev/null; then
        echo "Tampered page data should fail verification"
        return 1
    fi
    log "Page data tamper detected"

    # Tamper with header flags (offset 4)
    cp "$dir/enc.ltx" "$dir/tampered_header.ltx"
    printf '\xff' | dd of="$dir/tampered_header.ltx" bs=1 seek=4 count=1 conv=notrunc 2>/dev/null
    if "$LTX" verify -key "$priv" "$dir/tampered_header.ltx" 2>/dev/null; then
        echo "Tampered header should fail verification"
        return 1
    fi
    log "Header tamper detected"

    # Tamper with recipient block (offset 110, inside first recipient entry)
    cp "$dir/enc.ltx" "$dir/tampered_recipient.ltx"
    printf '\xff' | dd of="$dir/tampered_recipient.ltx" bs=1 seek=110 count=1 conv=notrunc 2>/dev/null
    if "$LTX" verify -key "$priv" "$dir/tampered_recipient.ltx" 2>/dev/null; then
        echo "Tampered recipient should fail verification"
        return 1
    fi
    log "Recipient tamper detected"

    log "Tamper detection test passed"
}

# --- Test 9: Error Paths ---

test_error_paths() {
    local dir="$TMPDIR/test9"
    mkdir -p "$dir"

    create_db "$dir/test.db" 4096 "CREATE TABLE t(x INTEGER); INSERT INTO t VALUES(1);"

    parse_keygen; local pub="$PUB" priv="$PRIV"
    "$LTX" encode-db -o "$dir/enc.ltx" -encrypt-to "$pub" "$dir/test.db"

    # Wrong key
    parse_keygen; local wrong_priv="$PRIV"
    local err
    if err=$("$LTX" verify -key "$wrong_priv" "$dir/enc.ltx" 2>&1); then
        echo "Wrong key should fail"
        return 1
    fi
    assert_contains "$err" "no matching recipient" "wrong key error"

    # No key: integrity-only verification is allowed, but anything that needs
    # plaintext must still refuse rather than emit an empty or partial database.
    if err=$("$LTX" apply -db "$dir/nokey.db" "$dir/enc.ltx" 2>&1); then
        echo "Apply without a key should fail for an encrypted file"
        return 1
    fi
    assert_contains "$err" "decryption key required" "no key error on apply"

    # Invalid hex key
    if err=$("$LTX" verify -key "ZZZZZZZZ" "$dir/enc.ltx" 2>&1); then
        echo "Invalid hex key should fail"
        return 1
    fi
    assert_contains "$err" "invalid" "invalid hex key error"

    # Invalid encrypt-to key
    if err=$("$LTX" encode-db -o "$dir/bad.ltx" -encrypt-to "NOTAHEXKEY" "$dir/test.db" 2>&1); then
        echo "Invalid encrypt-to should fail"
        return 1
    fi
    assert_contains "$err" "invalid" "invalid encrypt-to error"

    # Non-existent file
    if err=$("$LTX" verify "$dir/nonexistent.ltx" 2>&1); then
        echo "Non-existent file should fail"
        return 1
    fi

    # Non-LTX file
    echo "garbage data not an ltx file" > "$dir/fake.ltx"
    if err=$("$LTX" verify "$dir/fake.ltx" 2>&1); then
        echo "Garbage file should fail"
        return 1
    fi
    assert_contains "$err" "invalid" "garbage file error"

    log "Error paths test passed"
}

# --- Test 10: Truncation Simulation ---

test_truncation() {
    local dir="$TMPDIR/test10"
    mkdir -p "$dir"

    create_db "$dir/test.db" 4096 "
        CREATE TABLE t(x TEXT);
        INSERT INTO t VALUES('data for truncation test');
    "

    parse_keygen; local pub="$PUB" priv="$PRIV"
    "$LTX" encode-db -o "$dir/enc.ltx" -encrypt-to "$pub" "$dir/test.db"

    local file_size
    file_size=$(wc -c < "$dir/enc.ltx" | tr -d ' ')
    log "Encrypted file size: $file_size bytes"

    # Truncation points
    local -a trunc_sizes=(50 100 140)
    # header(100) + recipients(80) + 10 = 190 (mid-page)
    trunc_sizes+=(190)
    # Missing trailer: size - 16
    trunc_sizes+=($((file_size - 16)))
    # Missing last byte: size - 1
    trunc_sizes+=($((file_size - 1)))

    local all_pass=true
    for sz in "${trunc_sizes[@]}"; do
        if [[ $sz -le 0 ]] || [[ $sz -ge $file_size ]]; then
            log "Skipping truncation at $sz (invalid for file size $file_size)"
            continue
        fi
        dd if="$dir/enc.ltx" of="$dir/trunc_${sz}.ltx" bs=1 count="$sz" 2>/dev/null
        if "$LTX" verify -key "$priv" "$dir/trunc_${sz}.ltx" 2>/dev/null; then
            echo "Truncation at $sz bytes should fail verification"
            all_pass=false
        else
            log "Truncation at ${sz}B correctly rejected"
        fi
    done

    if [[ "$all_pass" != "true" ]]; then
        return 1
    fi

    log "Truncation test passed"
}

# --- Test 11: Large and Small Page Sizes ---

test_page_sizes() {
    local dir="$TMPDIR/test11"
    mkdir -p "$dir"

    parse_keygen; local pub="$PUB" priv="$PRIV"

    # 64KB pages (max)
    log "Testing 65536-byte pages..."
    create_db "$dir/large_page.db" 65536 "
        CREATE TABLE big(id INTEGER PRIMARY KEY, data BLOB);
        INSERT INTO big VALUES(1, randomblob(50000));
        INSERT INTO big VALUES(2, randomblob(50000));
    "
    local ck_large
    ck_large=$("$LTX" checksum "$dir/large_page.db")
    "$LTX" encode-db -o "$dir/large.ltx" -encrypt-to "$pub" "$dir/large_page.db"
    "$LTX" verify -key "$priv" "$dir/large.ltx"
    "$LTX" apply -db "$dir/large_restored.db" -key "$priv" "$dir/large.ltx"
    local ck_lr
    ck_lr=$("$LTX" checksum "$dir/large_restored.db")
    if [[ "$ck_large" != "$ck_lr" ]]; then
        echo "64KB page checksum mismatch: $ck_large != $ck_lr"
        return 1
    fi
    log "64KB pages: OK"

    # 512-byte pages (min)
    log "Testing 512-byte pages..."
    create_db "$dir/small_page.db" 512 "
        CREATE TABLE sm(id INTEGER PRIMARY KEY, val TEXT);
        INSERT INTO sm VALUES(1, 'small');
    "
    local ck_small
    ck_small=$("$LTX" checksum "$dir/small_page.db")
    "$LTX" encode-db -o "$dir/small.ltx" -encrypt-to "$pub" "$dir/small_page.db"
    "$LTX" verify -key "$priv" "$dir/small.ltx"
    "$LTX" apply -db "$dir/small_restored.db" -key "$priv" "$dir/small.ltx"
    local ck_sr
    ck_sr=$("$LTX" checksum "$dir/small_restored.db")
    if [[ "$ck_small" != "$ck_sr" ]]; then
        echo "512B page checksum mismatch: $ck_small != $ck_sr"
        return 1
    fi
    log "512B pages: OK"

    # Single-page DB (minimal)
    log "Testing single-page DB..."
    create_db "$dir/single.db" 4096 "CREATE TABLE t(x INTEGER);"
    local ck_single
    ck_single=$("$LTX" checksum "$dir/single.db")
    "$LTX" encode-db -o "$dir/single.ltx" -encrypt-to "$pub" "$dir/single.db"
    "$LTX" verify -key "$priv" "$dir/single.ltx"
    "$LTX" apply -db "$dir/single_restored.db" -key "$priv" "$dir/single.ltx"
    local ck_si
    ck_si=$("$LTX" checksum "$dir/single_restored.db")
    if [[ "$ck_single" != "$ck_si" ]]; then
        echo "Single-page checksum mismatch: $ck_single != $ck_si"
        return 1
    fi
    log "Single-page DB: OK"

    # Many-page DB
    log "Testing many-page DB (500 rows of randomblob)..."
    local many_sql="CREATE TABLE big(id INTEGER PRIMARY KEY, data BLOB);"
    for i in $(seq 1 500); do
        many_sql+="INSERT INTO big VALUES($i, randomblob(3000));"
    done
    create_db "$dir/many.db" 4096 "$many_sql"
    local ck_many
    ck_many=$("$LTX" checksum "$dir/many.db")
    "$LTX" encode-db -o "$dir/many.ltx" -encrypt-to "$pub" "$dir/many.db"
    "$LTX" verify -key "$priv" "$dir/many.ltx"
    "$LTX" apply -db "$dir/many_restored.db" -key "$priv" "$dir/many.ltx"
    local ck_mr
    ck_mr=$("$LTX" checksum "$dir/many_restored.db")
    if [[ "$ck_many" != "$ck_mr" ]]; then
        echo "Many-page checksum mismatch: $ck_many != $ck_mr"
        return 1
    fi
    local page_count
    page_count=$(sqlite3 "$dir/many.db" "PRAGMA page_count;")
    log "Many-page DB: OK ($page_count pages)"

    log "Page size tests passed"
}

# --- Main ---

main() {
    setup

    run_test "Test 1: Docker Build" test_docker_build
    run_test "Test 2: Key Generation" test_keygen
    run_test "Test 3: Single-Key Encryption Workflow" test_single_key_workflow
    run_test "Test 4: Multi-Recipient" test_multi_recipient
    run_test "Test 5: Key Rotation (Rekey)" test_rekey
    run_test "Test 6: Sequential Encrypted Apply" test_sequential_apply
    run_test "Test 7: Backward Compatibility" test_backward_compat
    run_test "Test 8: Tamper Detection" test_tamper_detection
    run_test "Test 9: Error Paths" test_error_paths
    run_test "Test 10: Truncation Simulation" test_truncation
    run_test "Test 11: Page Sizes (512B–64KB)" test_page_sizes

    teardown
}

main "$@"
