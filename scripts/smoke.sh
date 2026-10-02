#!/usr/bin/env bash
# End-to-end smoke test for the supportgenius server binary.
#
#   scripts/smoke.sh [path-to-binary]
#
# Boots the binary on a free loopback port with a throwaway SQLite
# database, then asserts the health endpoints, the waitlist endpoint, the
# single boot-time REDIS_URL notice, and that --check-ready agrees with
# the live server. Works against either build variant — the plain or the
# dev-fakes build — and both must answer a valid join with the same
# `202 {"ok":true}`: the dev-fakes build sends the confirm mail through
# its stub mailer, the plain build writes the row and defers the mail it
# cannot send. Exits non-zero on the first failed assertion.
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
# HARNESS_SECRET the harness refuses to build at boot. ADMIN_TOKEN gates
# the support module's tenant provisioning (issue #2); it must be at
# least 32 bytes when set, so it gets the same generator.
HARNESS_SECRET="$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')"
ADMIN_TOKEN="$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')"

echo "== booting $BIN on $LISTEN_ADDR (log: $LOG)"
# No REDIS_URL on purpose: the RateLimiter and KeyValue ports stay
# unconfigured, rate limiting fails open, and the boot log must say so
# exactly once (asserted below).
#
# RESEND_API_KEY, TURNSTILE_SECRET and ANTHROPIC_API_KEY are unset on
# purpose too: the assertions below branch on the port wiring those
# secrets choose (a Turnstile secret mounts a real Captcha port; a Resend
# key makes the plain build attempt real sends; an Anthropic key mounts
# the TextModel port instead of the dev stub), so an ambient secret must
# not be able to flip the expected outcome. RUST_LOG is unset for the
# same reason: an ambient level could suppress the WARN lines the
# assertions grep for (the dev-fakes build is detected by one).
env -u REDIS_URL -u RESEND_API_KEY -u TURNSTILE_SECRET -u RUST_LOG \
  -u ANTHROPIC_API_KEY -u SUPPORTGENIUS_MODEL_FAST -u SUPPORTGENIUS_MODEL_STRONG \
  HARNESS_SECRET="$HARNESS_SECRET" \
  ADMIN_TOKEN="$ADMIN_TOKEN" \
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
# Both build variants must answer exactly `202 Accepted` with {"ok":true}
# (the waitlist module's `accepted()`), for different reasons: the
# dev-fakes build's StubMailer reports Sent, so the join genuinely
# succeeds; the plain build has no Mailer at all, and the pinned waitlist
# writes the row first and defers the confirm mail through the Defer port
# AFTER the response (harness crates/module-waitlist `join`) — a
# mailer-less join still succeeds, and only the log carries the ERROR.
[ "$WAITLIST_STATUS" = "202" ] ||
  fail "POST /v1/waitlist returned $WAITLIST_STATUS, expected exactly 202; got: $WAITLIST_BODY"
printf '%s' "$WAITLIST_BODY" | grep -q '"ok"[[:space:]]*:[[:space:]]*true' ||
  fail "POST /v1/waitlist answered $WAITLIST_STATUS but not {\"ok\":true}; got: $WAITLIST_BODY"
echo "   POST /v1/waitlist -> $WAITLIST_STATUS (Captcha port: $CAPTCHA_STATE)"

# The support module (issues #2 and #3): tenant provisioning, source
# ingest, then one message turn — the full auth chain, asserted exactly.
# The guard reads SUPPORT_COMPOSED, decided by an exact `"name":"support"`
# match in /__health's `modules` array — the previous `grep "support"`
# was always true because the document contains
# `"venture":"supportgenius"`.
#
# /__ready's `text_model` field is NOT asserted here, on purpose: on the
# Worker the venture decorates the harness's DB-only probe with it
# (ventures/supportgenius src/lib.rs), but the native runtime's `serve`
# builds the router internally and offers no venture hook to do the same,
# so the native /__ready stays DB-only. The message outcome below is the
# native evidence of the port's state instead.
if [ "$SUPPORT_COMPOSED" = "yes" ]; then
  echo "== support module composed: provisioning a tenant and ingesting a source"
  # The auth chain never mixes: the admin token (Bearer $ADMIN_TOKEN,
  # generated at boot) provisions a tenant and mints its API key; the
  # key guards everything else. The key is stored nowhere — the mint
  # response is the only time it is ever sent — so it is carried from
  # this response in a variable.
  MINT_RESP="$(curl -s -w $'\n%{http_code}' -X POST -H 'Content-Type: application/json' \
    -H "Authorization: Bearer $ADMIN_TOKEN" \
    -d '{"name":"smoke"}' \
    "$BASE_URL/v1/support/admin/tenants")" || MINT_RESP=$'\n000'
  MINT_STATUS="${MINT_RESP##*$'\n'}"
  MINT_BODY="${MINT_RESP%$'\n'*}"
  [ "$MINT_STATUS" = "201" ] ||
    fail "POST /v1/support/admin/tenants returned $MINT_STATUS, expected exactly 201; got: $MINT_BODY"
  API_KEY="$(printf '%s' "$MINT_BODY" | python3 -c 'import json, sys; print(json.load(sys.stdin)["api_key"])')" ||
    fail "tenant response carried no api_key; got: $MINT_BODY"

  # Ingest one real source: `{"text": ...}` (or `{"url": ...}`), with a
  # stable external_id the re-ingest below could replace in place. The
  # text is what the message question is answered — and cited — from.
  S_RESP="$(curl -s -w $'\n%{http_code}' -X POST -H 'Content-Type: application/json' \
    -H "Authorization: Bearer $API_KEY" \
    -d '{"title":"Password resets","external_id":"smoke-source-1","text":"To reset your password, open the settings page and choose Sign-in options, then follow the reset link we email you."}' \
    "$BASE_URL/v1/support/sources")" || S_RESP=$'\n000'
  S_STATUS="${S_RESP##*$'\n'}"
  S_BODY="${S_RESP%$'\n'*}"
  [ "$S_STATUS" = "201" ] ||
    fail "POST /v1/support/sources returned $S_STATUS, expected exactly 201; got: $S_BODY"

  echo "== POST /v1/support/messages: the outcome depends on which build is running"
  # The grep below only tells the two build variants apart: the dev-fakes
  # build logs its stubs' "dev fakes active" warnings, the plain build
  # warns that SUPPORTGENIUS_DEV_FAKES was set on a build compiled without
  # the feature. It is the 200-vs-503 status assertion that actually
  # checks the TextModel wiring: with no ANTHROPIC_API_KEY (unset at boot),
  # the dev-fakes build's StubTextModel answers from the ingested source
  # and the plain build has no TextModel port at all.
  M_RESP="$(curl -s -w $'\n%{http_code}' -X POST -H 'Content-Type: application/json' \
    -H "Authorization: Bearer $API_KEY" \
    -d '{"message":"How do I reset my password?"}' \
    "$BASE_URL/v1/support/messages")" || M_RESP=$'\n000'
  M_STATUS="${M_RESP##*$'\n'}"
  M_BODY="${M_RESP%$'\n'*}"
  if grep -q 'dev fakes active' "$LOG"; then
    [ "$M_STATUS" = "200" ] ||
      fail "dev-fakes build: POST /v1/support/messages returned $M_STATUS, expected exactly 200; got: $M_BODY"
    printf '%s' "$M_BODY" | python3 -c '
import json, sys
try:
    body = json.load(sys.stdin)
except json.JSONDecodeError:
    sys.exit(1)
sys.exit(0 if body.get("outcome") == "answered" and body.get("citations") else 1)
' ||
      fail "dev-fakes build: expected outcome=answered with a citation; got: $M_BODY"
    echo "   POST /v1/support/messages -> 200 outcome=answered (StubTextModel)"
  else
    [ "$M_STATUS" = "503" ] ||
      fail "plain build: POST /v1/support/messages returned $M_STATUS, expected exactly 503; got: $M_BODY"
    printf '%s' "$M_BODY" | grep -q 'text-model-not-configured' ||
      fail "plain build: expected the text-model-not-configured problem; got: $M_BODY"
    echo "   POST /v1/support/messages -> 503 text-model-not-configured (no TextModel port)"
  fi
else
  echo "SKIP: module-support not composed yet (issue #2/#3)"
fi

echo "PASS: supportgenius smoke ok ($BIN on $LISTEN_ADDR; scratch log kept at $LOG until exit)"
