# Presence & contacts (I5)

## Model

`PresenceKind`: `available` (0), `unavailable` (1), `lastSeen` (2).

Contacts live in SQLite `contacts` and are joined into inbox rows for the
presence dot.

## Privacy

- Never invent `last_seen` if the server omitted it.
- On `LAST_SEEN` without a timestamp, keep any previously stored value only.
- Do **not** invent typing packets (not in proto).

## Bootstrap

Until an HTTP contacts directory exists:

- DEBUG builds seed `alice` / `bob` / `carol` (excluding self) for local
  gateway demos.
- Users can start a chat by user id from **New chat**.

## Subscribe

After login / contact changes, call FFI `subscribePresence` with the roster
CSV (`lane_subscribe_presence`).
