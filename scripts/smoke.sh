#!/usr/bin/env bash
# End-to-end smoke test for the supportgenius server binary.
#
#   scripts/smoke.sh [path-to-binary]
#
# Boots the binary on a free loopback port with a throwaway SQLite
# database, then asserts the health endpoints, the waitlist endpoint, the
# single boot-time REDIS_URL notice, and that --check-ready agrees with
# the live server. Works against either build variant — the plain or the
# dev-fakes build — with the expected waitlist status depending on whether
# a Captcha port is mounted (202 for a dev-fakes join, the documented 503
# mail-not-configured from the plain build). Exits non-zero on the first
# failed assertion.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default: the fully static musl release build.
BIN="${1:-$REPO_ROOT/target/x86_64-unknown-linux-musl/release/supportgenius}"

if [ ! -x "$BIN" ]; then
  echo "FAIL: binary not found or not executable: $BIN" >&2
  echo "Build it first: cargo build --release --target x86_64-unknown-linux-musl -p supportgenius-bin" >&2
  exit 1
fi

SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/supportgenius-smoke.XXXXXX")"
LOG="$SCRATCH/server.log"
DB="$SCRATCH/smoke.db"

SERVER_PID=""
cleanup() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$SCRATCH"
}
trap cleanup EXIT

# Pick a free loopback port (python3 if present, else a random high port).
PORT="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()' 2>/dev/null \
  || echo "$(( (RANDOM % 10000) + 20000 ))")"
LISTEN_ADDR="127.0.0.1:$PORT"
BASE_URL="http://$LISTEN_ADDR"

# Random secret so the runtime-native Signer port wires up; without
# HARNESS_SECRET the harness refuses to build at boot.
HARNESS_SECRET="$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')"

echo "== booting $BIN on $LISTEN_ADDR (log: $LOG)"
# No REDIS_URL on purpose: the RateLimiter and KeyValue ports stay
# unconfigured, rate limiting fails open, and the boot log must say so
# exactly once (asserted below).
#
# RESEND_API_KEY and TURNSTILE_SECRET are unset on purpose too: the
# assertions below branch on the port wiring those secrets choose (a
# Turnstile secret mounts a real Captcha port; a Resend key makes the
# plain build attempt real sends), so an ambient secret must not be able
# to flip the expected outcome.
env -u REDIS_URL -u RESEND_API_KEY -u TURNSTILE_SECRET \
  HARNESS_SECRET="$HARNESS_SECRET" \
  LISTEN_ADDR="$LISTEN_ADDR" \
  DATABASE_URL="sqlite://$DB" \
  SUPPORTGENIUS_DEV_FAKES=1 \
  "$BIN" >"$LOG" 2>&1 &
SERVER_PID=$!

code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
fail() {
  echo "FAIL: $*" >&2
  echo "--- server log tail ($LOG) ---" >&2
  tail -n 40 "$LOG" >&2 || true
  exit 1
}

echo "== polling /__ready (up to 30s)"
READY_DEADLINE=$((SECONDS + 30))
until [ "$(code "$BASE_URL/__ready")" = "200" ]; do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    fail "server exited during startup"
  fi
  if [ "$SECONDS" -ge "$READY_DEADLINE" ]; then
    fail "server did not become ready within 30s"
  fi
  sleep 0.5
done

echo "== /__ready returns 200 and /__health lists the waitlist module"
[ "$(code "$BASE_URL/__ready")" = "200" ] || fail "/__ready is not 200 after readiness"
HEALTH="$(curl -s "$BASE_URL/__health")" || fail "/__health request failed"

# Parse /__health exactly: which modules are composed (by NAME in the
# `modules` array — never a substring match of the document, where
# `"venture":"supportgenius"` makes any `grep support` true) and whether
# the Captcha port is mounted. Both facts drive the assertions below.
read -r WAITLIST_COMPOSED SUPPORT_COMPOSED CAPTCHA_STATE < <(
  printf '%s' "$HEALTH" | python3 -c '
import json, sys
try:
    health = json.load(sys.stdin)
except json.JSONDecodeError:
    sys.exit(1)
names = {m.get("name") for m in health.get("modules", [])}
print(
    "yes" if "waitlist" in names else "no",
    "yes" if "support" in names else "no",
    health.get("captcha", "unknown"),
)
'
) || fail "/__health is not valid JSON; got: $HEALTH"
[ "$WAITLIST_COMPOSED" = "yes" ] || fail "/__health does not list the waitlist module; got: $HEALTH"
case "$CAPTCHA_STATE" in
  configured | absent) ;;
  *) fail "/__health reports captcha state '$CAPTCHA_STATE' (want configured|absent); got: $HEALTH" ;;
esac

echo "== --check-ready must exit 0 against the live server"
LISTEN_ADDR="$LISTEN_ADDR" "$BIN" --check-ready >/dev/null 2>&1 \
  || fail "--check-ready exited non-zero against a live server at $LISTEN_ADDR"

echo "== generating some extra traffic, then the REDIS_URL notice must appear exactly once"
for _ in 1 2 3; do
  code "$BASE_URL/__ready" >/dev/null
  curl -s "$BASE_URL/__health" >/dev/null
done
NOTICE_COUNT="$(grep -c 'REDIS_URL unset' "$LOG" || true)"
[ "$NOTICE_COUNT" = "1" ] || fail "expected exactly 1 'REDIS_URL unset' log line, found: $NOTICE_COUNT"

echo "== POST /v1/waitlist: a valid join, asserted against the exact expected status"
# The body the site form posts, plus the captcha token: Turnstile's fixed
# dummy widget token `1x00000000000000000000AA`, the one the dev-fakes
# StubCaptcha accepts (bin/supportgenius/src/dev_fakes.rs); when no
# Captcha port is mounted the handler ignores it. A mounted Captcha port
# refuses a tokenless join with a 400 `captcha-failed` problem — core's
# `verify_human_form` demands a token whenever the port is present
# (cratefield-core src/route_policy.rs) — which is exactly the 400 this
# check used to hit and then wave through as "not 5xx".
#
# HARNESS_ALLOW_UNPROTECTED_WRITES is deliberately not needed: the venture
# compiles as Development (crates/composition never calls
# `.env(VentureEnv::Production)`; /__health reports "env":"development"),
# and core's unprotected-write refusal only fires in production.
WAITLIST_PAYLOAD='{"email":"smoke@example.test","product":"supportgenius","captchaToken":"1x00000000000000000000AA"}'
WAITLIST_RESP="$(curl -s -w $'\n%{http_code}' -X POST -H 'Content-Type: application/json' \
  -d "$WAITLIST_PAYLOAD" "$BASE_URL/v1/waitlist")" || WAITLIST_RESP=$'\n000'
WAITLIST_STATUS="${WAITLIST_RESP##*$'\n'}"
WAITLIST_BODY="${WAITLIST_RESP%$'\n'*}"
case "$CAPTCHA_STATE" in
  configured)
    # Dev fakes: StubMailer reports Sent, so the join must genuinely
    # succeed — exactly `202 Accepted` with {"ok":true} (the waitlist
    # module's `accepted()`).
    [ "$WAITLIST_STATUS" = "202" ] ||
      fail "POST /v1/waitlist returned $WAITLIST_STATUS, expected exactly 202 with dev fakes active; got: $WAITLIST_BODY"
    printf '%s' "$WAITLIST_BODY" | grep -q '"ok"[[:space:]]*:[[:space:]]*true' ||
      fail "POST /v1/waitlist answered $WAITLIST_STATUS but not {\"ok\":true}; got: $WAITLIST_BODY"
    ;;
  absent)
    # Plain build (no dev-fakes feature — what CI's native job builds and
    # release.yml ships): no Captcha port, and Development env lets the
    # join pass the human-form gate. Mail is then sent BEFORE the row is
    # written (waitlist handlers.rs `join`), and the keyless Resend
    # adapter reports NotConfigured without a network call; the venture
    # refuses to fake a send, so the documented, correct answer is
    # exactly `503` carrying the `mail-not-configured` problem (core
    # problems.rs maps that slug to SERVICE_UNAVAILABLE).
    [ "$WAITLIST_STATUS" = "503" ] ||
      fail "POST /v1/waitlist returned $WAITLIST_STATUS, expected exactly 503 mail-not-configured without dev fakes; got: $WAITLIST_BODY"
    printf '%s' "$WAITLIST_BODY" | grep -q 'problems/mail-not-configured' ||
      fail "POST /v1/waitlist answered 503 but not the mail-not-configured problem; got: $WAITLIST_BODY"
    ;;
esac
echo "   POST /v1/waitlist -> $WAITLIST_STATUS (Captcha port: $CAPTCHA_STATE)"

# The support module (source ingest + message reply, issues #2 and #3) is not
# composed into the binary yet. The guard reads SUPPORT_COMPOSED, decided
# by an exact `"name":"support"` match in /__health's `modules` array —
# the previous `grep "support"` was always true because the document
# contains `"venture":"supportgenius"`, so this block "composed" a module
# that was not there and POSTed to 404s. Once crates/composition lists
# module-support, this block activates on its own; when it first runs,
# re-check the endpoint paths and payload shapes against the module's
# actual router.
if [ "$SUPPORT_COMPOSED" = "yes" ]; then
  echo "== support module composed: exercising source + message endpoints"
  S_STATUS="$(code -X POST -H 'Content-Type: application/json' \
    -d '{"external_id":"smoke-source-1","channel":"email"}' \
    "$BASE_URL/v1/support/sources")" || S_STATUS="000"
  case "$S_STATUS" in
    5* | 000) fail "POST /v1/support/sources returned $S_STATUS" ;;
    *) echo "   POST /v1/support/sources -> $S_STATUS" ;;
  esac
  M_STATUS="$(code -X POST -H 'Content-Type: application/json' \
    -d '{"source_external_id":"smoke-source-1","body":"smoke test message"}' \
    "$BASE_URL/v1/support/messages")" || M_STATUS="000"
  case "$M_STATUS" in
    5* | 000) fail "POST /v1/support/messages returned $M_STATUS" ;;
    *) echo "   POST /v1/support/messages -> $M_STATUS" ;;
  esac
else
  echo "SKIP: module-support not composed yet (issue #2/#3)"
fi

echo "PASS: supportgenius smoke ok ($BIN on $LISTEN_ADDR; scratch log kept at $LOG until exit)"
