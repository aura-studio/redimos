#!/usr/bin/env python3
"""redis-py 5+ handshake / HELLO-fallback / AUTH smoke test for redimos (task 6.2).

This script is a MANUAL, cross-language client-matrix check. It is NOT run by Go
CI (Go CI has no CPython + redis-py runtime); it targets a *live* redimos proxy
started separately. See README.md in this directory.

It verifies the Requirement 2 connection surface through the real redis-py
client:

  * Requirement 2.1 - redis-py 5+ negotiates RESP3 with HELLO; redimos replies
    "-ERR unknown command 'HELLO'", so the client must fall back to RESP2 and
    still connect. A successful PING proves the fallback worked.
  * Requirement 2.4 - ECHO round-trips its argument.
  * Requirement 2.5 - the correct AUTH password authenticates the connection.
  * Requirement 2.6 - a wrong password does not authenticate, and a pre-auth
    business command is rejected with NOAUTH.

Usage:

    python3 -m pip install "redis>=5"

    # No-auth handshake / fallback / PING / ECHO
    REDIMOS_ADDR=127.0.0.1:6380 python3 redis_py_smoke.py

    # AUTH flow (requires redimos started with --requirepass s3cret)
    REDIMOS_ADDR=127.0.0.1:6380 REDIMOS_PASS=s3cret python3 redis_py_smoke.py

Exit code 0 means all checks passed; non-zero means a check failed.
"""

import os
import sys

try:
    import redis  # redis-py 5+
except ImportError:
    sys.stderr.write(
        "redis-py is not installed. Run: python3 -m pip install 'redis>=5'\n"
    )
    sys.exit(2)


def parse_addr(addr):
    host, _, port = addr.partition(":")
    return host or "127.0.0.1", int(port or "6380")


def check(name, ok, detail=""):
    status = "PASS" if ok else "FAIL"
    print(f"[{status}] {name}{(': ' + detail) if detail else ''}")
    return ok


def main():
    addr = os.environ.get("REDIMOS_ADDR", "127.0.0.1:6380")
    password = os.environ.get("REDIMOS_PASS", "")
    host, port = parse_addr(addr)

    all_ok = True

    if not password:
        # No-auth mode: handshake fallback + PING + ECHO.
        # protocol=3 forces redis-py to attempt HELLO 3; redimos rejects it and
        # the client must fall back to RESP2 (Requirement 2.1).
        client = redis.Redis(host=host, port=port, protocol=3,
                             socket_timeout=3, socket_connect_timeout=3)
        try:
            pong = client.ping()
            all_ok &= check("PING after HELLO fallback (req 2.1/2.2)", pong is True,
                            f"ping()={pong!r}")
            echo = client.echo("hello-redimos")
            all_ok &= check("ECHO round-trip (req 2.4)",
                            echo == b"hello-redimos", f"echo={echo!r}")
        finally:
            client.close()
    else:
        # Auth mode: correct password authenticates (Requirement 2.5).
        client = redis.Redis(host=host, port=port, password=password, protocol=3,
                             socket_timeout=3, socket_connect_timeout=3)
        try:
            echo = client.echo("authed")
            all_ok &= check("ECHO after correct AUTH (req 2.5)",
                            echo == b"authed", f"echo={echo!r}")
        finally:
            client.close()

        # Wrong password must not authenticate (Requirement 2.5/2.6).
        bad = redis.Redis(host=host, port=port, password="definitely-wrong",
                          protocol=3, socket_timeout=3, socket_connect_timeout=3)
        try:
            bad.echo("should-fail")
            all_ok &= check("wrong password rejected (req 2.5)", False,
                            "command unexpectedly succeeded")
        except redis.exceptions.RedisError as exc:
            all_ok &= check("wrong password rejected (req 2.5)", True, str(exc))
        finally:
            bad.close()

        # Pre-auth business command must get NOAUTH (Requirement 2.6).
        anon = redis.Redis(host=host, port=port, protocol=3,
                           socket_timeout=3, socket_connect_timeout=3)
        try:
            anon.echo("should-fail")
            all_ok &= check("pre-auth NOAUTH (req 2.6)", False,
                            "command unexpectedly succeeded")
        except redis.exceptions.RedisError as exc:
            all_ok &= check("pre-auth NOAUTH (req 2.6)",
                            "NOAUTH" in str(exc).upper(), str(exc))
        finally:
            anon.close()

    print()
    print("ALL PASSED" if all_ok else "SOME CHECKS FAILED")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
