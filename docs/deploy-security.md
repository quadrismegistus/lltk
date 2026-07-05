# Deploying the LLTK web apps securely

The explorer (`lltk app`) and annotator (`lltk annotate`) are safe to run on
`localhost` with no configuration. **Before exposing either on a public
interface (e.g. lltk.net), do all of the following.**

## 1. Bind + authenticate

Both apps default to `--host 127.0.0.1` (loopback only). To expose one, pass
`--host 0.0.0.0` **and** enable HTTP Basic auth:

```bash
export LLTK_WEB_USER=someuser
export LLTK_WEB_PASSWORD=$(openssl rand -base64 24)
lltk app --host 0.0.0.0 --port 8899
```

If you bind non-loopback without setting those two vars, the app prints a loud
warning. The annotator has **write** endpoints (annotate, match link/unlink) —
never expose it without auth; prefer keeping it loopback-only behind an SSH
tunnel.

## 2. Use a read-only ClickHouse user for the explorer

The explorer only issues `SELECT`s. Create a restricted user and point the
explorer at it so an unexpected bug (or injection) can't mutate or drop data:

```sql
-- run as an admin CH user
CREATE USER lltk_ro IDENTIFIED BY 'CHANGE_ME_STRONG';
GRANT SELECT ON lltk.* TO lltk_ro;
-- do NOT grant file()/url()/CREATE/INSERT/ALTER/DROP
```

```bash
export LLTK_CLICKHOUSE_URL_READONLY='clickhouse://lltk_ro:CHANGE_ME_STRONG@localhost:8123/lltk'
```

`MetaDBCH(readonly=True)` (used by the explorer) picks this up; it falls back to
the read-write URL when unset, so local dev is unaffected.

## 3. Rotate the default credentials

The dev fallback ships `lltk`/`lltk` (localhost only). For any real deploy,
set a strong password on the ClickHouse user (in `users.xml`) and provide it
via env — never in code:

```bash
export LLTK_CLICKHOUSE_URL='clickhouse://lltk:STRONG_PASSWORD@localhost:8123/lltk'
```

All entry points (`lltk db-*`, the web apps, `MetaDBCH`) resolve the URL through
`resolve_ch_url()` — env first, dev fallback last — so setting these two env
vars is sufficient; no code change needed.

## 4. Bind ClickHouse itself to loopback

The bundled `config.xml` already sets `<listen_host>127.0.0.1</listen_host>`.
Keep it that way (or firewall ports 8123/9000) so the database is reachable
only through the app, not directly from the internet.

## Still on the roadmap (not yet implemented)

- Server-side **bound parameters** for the web query paths (stronger than the
  current backslash-safe escaping — see `ch_quote`).
