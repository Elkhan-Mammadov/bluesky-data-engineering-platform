# Privacy

This document explains exactly what data this platform keeps, what it never
keeps, and why. It implements the rules from `CLAUDE.md` section 8.1
(Ingestor) and the "Out of scope" list in section 4.

## What we read

The ingestor connects to the public Bluesky **Jetstream** feed
(`wss://jetstream1.us-east.bsky.network/subscribe` and other official
hosts). Jetstream only carries **public** actions that Bluesky already
broadcasts to anyone: posts, likes, reposts, follows, blocks and profile
updates. No login, password, API key, private message, IP address or device
identifier is ever available through this feed, so none of it can end up in
this platform either.

## What we keep

| Data | Kept? | Form it is kept in |
|---|---|---|
| User identifier (DID) | Yes | Salted SHA-256 hash (see below) - never the raw DID |
| Target of a like/repost/follow/block | Yes | Also a salted hash of the target's DID - the target account is never stored in the clear either |
| Handle / display name / avatar | **No** | Never read into any table |
| Post text | **No** | Never stored |
| Post language | Yes | Read from Bluesky's own `record.langs` field (the language the author selected) - post text is never analyzed |
| Post length | Yes | Character count only |
| Hashtags | Yes | Extracted list of tags, not the surrounding text |
| Has link / has media / is reply | Yes | Boolean flags derived from the record |
| Action type (post/like/repost/follow/block/profile update) | Yes | Needed for all analytics |
| Timestamps | Yes | When the action happened, per Jetstream's `time_us` |
| IP address, device, login data | **No** | Not present in Jetstream at all |

## How user IDs are hashed

`ingestion/common/privacy.py` implements this:

```python
hash_user_id(user_did, salt) = sha256(f"{salt}:{user_did}")
```

- The salt lives only in `.env` as `USER_ID_HASH_SALT` and is never
  committed to git.
- The same real user always produces the same hash, so we can still track
  one account's activity and segment over time (e.g. for `dim_user` SCD2)
  without ever storing or displaying who that account actually is.
- Without the salt, the hash cannot practically be reversed back to a DID.
- Sampling (`is_sampled`) uses the same hash, so the same 10% of users are
  sampled consistently across restarts, rather than a random subset every
  time.

## Where this is enforced

- **Ingestor** (Stage 3): hashes the DID before the first write to
  `source-db`. The raw DID is never written to any table, log, or file.
- **Spark** (Stage 5) and **dbt** (Stage 6): only ever see the hashed ID
  that already exists in `source-db` / `raw`; they have no access to
  Jetstream and cannot recover the original DID.
- **Grafana** (Stage 7): every panel is built on marts that only expose the
  hash (shown as a short "user hash" string) - never the DID, handle, or
  post text. This is checked again in Stage 7's dashboard review.

## Retention

Old data is deleted daily by DAG `06_data_retention`, after
`DATA_RETENTION_DAYS` (default 7, configurable in `.env`). This prunes
`source-db` and the warehouse's operational layers (`raw`, `realtime`,
`dq.quarantine`/`stream_batches`) - shorter retention means less exposure
even for the hashed, already-anonymized data. Aggregated marts (e.g.
trending hashtags, engagement) are kept longer as analytical history,
since they no longer carry anything more identifying than a hash.

For a full reset (course demos, not routine use), DAG `99_reset_demo` /
`make reset-demo` empties every table, including marts.

## Requesting deletion

Because every stored identifier is a one-way salted hash, this platform
itself cannot look up "which row belongs to me" from a handle - by
design, it never learns the mapping in the first place. Rotating
`USER_ID_HASH_SALT` and restarting the ingestor makes every future hash
for every user unrelated to past hashes, and `make reset-demo` clears all
data already collected.

## Bluesky terms of use

Only data Bluesky already publishes openly through Jetstream is used, at a
sampled rate, for a course analytics project. No content is re-published;
Grafana never displays post text or any identifying information.
