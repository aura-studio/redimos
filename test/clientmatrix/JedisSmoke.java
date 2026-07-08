// JedisSmoke.java - jedis handshake / AUTH smoke test for redimos (task 6.2).
//
// This is a MANUAL, cross-language client-matrix check. It is NOT run by Go CI
// (Go CI has no JVM + jedis runtime); it targets a *live* redimos proxy started
// separately. See README.md in this directory.
//
// It verifies the Requirement 2 connection surface through the real jedis
// client:
//
//   * Requirement 2.2 - PING returns PONG. jedis speaks RESP2 by default and
//     does not force HELLO, so it connects directly; this is the baseline
//     RESP2 path that Requirement 2.1's HELLO fallback exists to preserve for
//     RESP3 clients.
//   * Requirement 2.4 - ECHO round-trips its argument.
//   * Requirement 2.5 - the correct AUTH password authenticates the connection.
//   * Requirement 2.6 - a pre-auth business command is rejected with NOAUTH.
//
// Build & run (example classpath; adjust jar versions/paths for your setup):
//
//   javac -cp "jedis-5.1.0.jar" JedisSmoke.java
//   REDIMOS_ADDR=127.0.0.1:6380 \
//     java -cp ".:jedis-5.1.0.jar:slf4j-api.jar:commons-pool2.jar" JedisSmoke
//
//   # AUTH flow (redimos started with --requirepass s3cret):
//   REDIMOS_ADDR=127.0.0.1:6380 REDIMOS_PASS=s3cret \
//     java -cp ".:jedis-5.1.0.jar:..." JedisSmoke
//
// Exit code 0 means all checks passed; non-zero means a check failed.

import java.util.Objects;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

public class JedisSmoke {

    private static boolean allOk = true;

    private static void check(String name, boolean ok, String detail) {
        System.out.println("[" + (ok ? "PASS" : "FAIL") + "] " + name
                + (detail.isEmpty() ? "" : ": " + detail));
        allOk &= ok;
    }

    public static void main(String[] args) {
        String addr = System.getenv().getOrDefault("REDIMOS_ADDR", "127.0.0.1:6380");
        String password = System.getenv().getOrDefault("REDIMOS_PASS", "");
        String[] hp = addr.split(":", 2);
        String host = hp[0].isEmpty() ? "127.0.0.1" : hp[0];
        int port = hp.length > 1 ? Integer.parseInt(hp[1]) : 6380;

        if (password.isEmpty()) {
            // No-auth mode: PING + ECHO over the default RESP2 path.
            try (Jedis jedis = new Jedis(host, port)) {
                String pong = jedis.ping();
                check("PING (req 2.2)", "PONG".equals(pong), "ping()=" + pong);
                String echo = jedis.echo("hello-redimos");
                check("ECHO round-trip (req 2.4)",
                        Objects.equals(echo, "hello-redimos"), "echo=" + echo);
            }
        } else {
            // Auth mode: correct password authenticates (Requirement 2.5).
            try (Jedis jedis = new Jedis(host, port)) {
                String authReply = jedis.auth(password);
                check("AUTH correct password (req 2.5)",
                        "OK".equals(authReply), "auth()=" + authReply);
                String echo = jedis.echo("authed");
                check("ECHO after AUTH (req 2.5)",
                        Objects.equals(echo, "authed"), "echo=" + echo);
            }

            // Pre-auth business command must get NOAUTH (Requirement 2.6).
            try (Jedis jedis = new Jedis(host, port)) {
                jedis.echo("should-fail");
                check("pre-auth NOAUTH (req 2.6)", false,
                        "command unexpectedly succeeded");
            } catch (JedisException exc) {
                check("pre-auth NOAUTH (req 2.6)",
                        exc.getMessage() != null
                                && exc.getMessage().toUpperCase().contains("NOAUTH"),
                        String.valueOf(exc.getMessage()));
            }
        }

        System.out.println();
        System.out.println(allOk ? "ALL PASSED" : "SOME CHECKS FAILED");
        System.exit(allOk ? 0 : 1);
    }
}
