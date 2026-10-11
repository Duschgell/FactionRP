# FactionRP

_FactionRP_ is a continuation of the original CrossRP AddOn for World of Warcraft.

It attempts to pull data across faction boundaries by leveraging people's Battle.NET connections.
The more people are connected and using this AddOn, the more data can be shared.

Unlike CrossRP, the AddOn does not designate people as "servers", but tries to utilize the ingame
communication possibilities to spread information.

## Communication Structure

```mermaid
---
title: FactionRP Message Flow
---

flowchart LR
    start[Select Player]
    extract[Extract Profile]
    broadcast[Broadcast Data]
    process[Process Message]
    
    start --> extract
    extract --> broadcast
    broadcast --> process
```

### Select Player

When a player is "selected", their profile is extracted and stored in the internal
database of the AddOn. A player is considered "selected" when the mouse hovers over someone,
or someone is selected as target.

A player is also "selected" indirectly: whenever the RP AddOn itself receives profile
data over its own comm channels -- like TRP3's exchange protocol, or MSP for non-TRP3 AddOn --
that profile is treated like a selection and re-broadcast through the network. Because
receipts arrive per profile section, they are coalesced with a short debounce plus a
random jitter (so many clients receiving the same profile don't all flood at once —
the first copy to arrive suppresses the rest), and re-broadcasts drain at a bounded
rate through a capped queue, so busy environments cannot overwhelm the comm throttle.

Feedback from our own traffic is deliberately ignored: profiles we push into the RP
AddOn's register (directory injection and tooltip enrichment) fire the same
"profile updated" signals as real data. Re-originating on those would form a loop
(one received profile ⇒ inject ⇒ update event ⇒ re-broadcast ⇒ peers inject again),
which was the root cause of duplicated TRP3 map-scan results (issue #1). The
Scanner therefore only re-originates for units the user actually selected this
session (mouseover/target, or ourselves), and skips a share whose content version
is unchanged since our last originate (`lastSharedVersion` bookkeeping in
`Scanner.lua`).

### Extract Profile

A player's profile will be extracted from the supported AddOns. (Initially TRP3)
Since every player has a unique GUID in the game, we use that GUID to store the profile.
We can also use the same GUID to look up a profile, allowing the AddOn to display the profile when data is available.

### Broadcast Data

Every time we store/refresh a profile in our cache, we will broadcast that data.
This is done in the following approach:

* Scan our battle.net connections and online friends.
* If they are online, send them a AddOn Battle.NET message (see message structure)
* The receivers store the data in their own cache and propagate the information further through their own battle.net connections and friends as well as group/raid.
* They do not send the message to the broadcaster to avoid loops.

### Message Structure

Because we're dealing with Lua and limitations imposed by the World of Warcraft client,
we keep our messages scoped to the minimum data. 
We use `||` as separator, as this does not conflict with most AddOns.

**structure**:
```
SENDER-PLAYER-GUID||SUBJECT-PLAYER-GUID||SUBJECT-NAME-REALM||SOURCE-ADDON||ORIGIN-TIME||CONTENT-VERSION||PROFILE-DATA
```

* `SUBJECT-NAME-REALM` is required because a GUID alone cannot be resolved to a character
  name for players who are not in our vicinity. The supported RP AddOns (e.g. TRP3) key their
  data store by `Name-Realm`, so we must carry it to be able to display the profile later.
* `SOURCE-ADDON` is the AddOn the profile originated from (e.g. `TRP3`), so the receiver knows
  which adapter to use.
* `ORIGIN-TIME` is the server `time()` captured by the client that originally extracted the
  profile from its real (local) RP AddOn data. It is the ordering key: a copy with a newer
  origin-time always supersedes an older one. Relayers never modify it — they propagate the
  origin's value unchanged.
* `CONTENT-VERSION` identifies the profile content. For TRP3 it is the combination of its
  per-section version counters (`.v`). Because those counters wrap (1..99) and are not globally
  ordered, content-version is only ever compared for *equality* (to detect "same vs changed");
  `ORIGIN-TIME` provides the actual ordering. Together they de-duplicate gossip (see Broadcast
  Flow) and prevent stale and fresh copies from flapping between mutual peers.

Because a single in-game AddOn message is capped at 255 bytes, larger profiles are split into
numbered chunks and re-assembled by the receiver before processing.

### Broadcast Flow
This section illustrates what happens when a message needs to be broadcast:

* When not in a party/raid: 
  * Broadcast the data through the AddOn Comm Channel to share with same faction users
  * Broadcast the data through Battle.NET friends
  * Broadcast the data through friends
  * Whisper the data to a capped set of opposite-faction members of our own communities
* when in a part/raid:
  * Same as alone + broadcast through party/raid AddON messaging.

The "AddOn Comm Channel" is a hidden chat channel (joined via `JoinTemporaryChannel`) over which
same-faction clients exchange addon messages. Cross-faction sharing is possible over
Battle.NET connections and via addon whispers to opposite-faction community members
(see below).

All chunk sends go through a paced internal queue (`SEND_BURST` messages per
`SEND_INTERVAL`), and each primitive is additionally routed through **Chomp** —
the ChatThrottleLib wrapper TRP3 already ships — on the `LOW` priority queue,
falling back to the raw `C_ChatInfo.SendAddonMessage` / `BNSendGameData` calls
when Chomp is unavailable. This coalesces our bulk gossip traffic into the same
global throttle queues every addon shares, so FactionRP cannot starve another
addon's sends (notably TRP3's unthrottled map-scan `C_SCAN` requests, which are
sensitive to chat-frame send backoff — see issue #1). The comm channel join is
also idempotent: it is skipped when we are already in the channel, since every
join fires channel-list churn that other addons' channel bookkeeping reacts to.

### Community relays

WoW communities cannot carry addon messages directly, but cross-faction *whispers* —
including addon whispers — work between members of a shared community. Every broadcast
therefore also whispers up to `COMM_RELAY_LIMIT` opposite-faction members of our own
subscribed clubs (online only, deduplicated by GUID across clubs). Sends are blind:
clients without FactionRP silently discard the message, so the only cost is send-queue
capacity. Whispers are the costliest leg, hence the small cap — a few seeded peers
flood their own faction side anyway.

Candidate order, so the limited slots go to the most promising relays first:

1. **Confirmed FRP peers** — an opposite-faction club member we have already received
   an FactionRP whisper from this session provably runs the AddOn.
2. **Known cross-faction contacts** — opposite-faction club members whose profiles
   reached us via the RP AddOn's own comms (e.g. a TRP3 profile arriving cross-faction
   through the community): proven reachable, almost certainly RP-AddOn users.
3. **The remaining opposite-faction roster.**

No single member should act as a relay server, so fairness is enforced two ways:
a **round-robin cursor** spreads picks across the candidate list rather than always
starting from the front, and each contact may be used at most `COMM_USAGE_LIMIT`
times per `COMM_USAGE_WINDOW` seconds. Exhausted contacts are only re-used when too
few fresh candidates remain — coverage is never dropped for lack of fresh targets.

Club member lists, member factions (`ClubMemberInfo.faction` is `NeverSecret`), and
presence come from `C_Club`. The roster is rebuilt incrementally (`ROSTER_SLICE`
member lookups per frame) so large communities never stall a frame, and the same
memoized roster serves GUID resolution. During chat-messaging lockdown those APIs
yield secrets, so the leg silently no-ops; whisper sends also fail gracefully
(`AddOnMessageLockdown`) — see "Security restrictions" below.

### Directory injection

Profiles stored from gossip are also injected into the source RP AddOn's register,
so its directory/character list populates as data arrives — not only on tooltip.
Injections drain through a small bounded queue (`INJECT_BURST` per `INJECT_INTERVAL`)
because register writes fire the AddOn's own update and UI-refresh events.

A message should be ignored when any of the following is true:

* The sender GUID is the same as the user's GUID (loop avoidance).
* We already hold the subject's profile with the same `CONTENT-VERSION`, or with an `ORIGIN-TIME`
  newer than the incoming message (de-duplication / staleness rejection). We only store and
  re-broadcast when the content differs *and* the incoming copy is not older.

When re-broadcasting, we never send the message back toward the peer we received it from.

### Cache Structure

The cache needs to do a little bit more than just track the GUID and Profile.
It needs to track the following data:

* The GUID of the player who's profile is stored (cache key)
* The `Name-Realm` of that player (needed to display/inject the profile)
* When the entry was added locally (drives the 5 minute TTL / expiry)
* The `ORIGIN-TIME` and `CONTENT-VERSION` of the profile (used for ordering and de-duplication)
* The AddOn of the profile, e.g. TRP3 or something
* The actual Profile data

### Tooltip update

The AddOn needs to rebuild tooltips for players of the opposite faction.
If the player of the opposite faction has an entry in our cache, load it and apply it.
For this we can probably leverage the functionality of TRP3 to achieve the same result.

In practice this means: when an opposite-faction unit is hovered/targeted and we hold a cached
profile for their GUID, we inject that data into TRP3's own register (keyed by `Name-Realm`) via
its public API (`addCharacter` → `saveCurrentProfileID` → `saveInformation`, gated by
`shouldUpdateInformation`). TRP3 then renders the tooltip itself, so we do not maintain a separate
tooltip implementation. Injected entries are treated as transient and respect the 5 minute TTL.

### Slash Commands

All commands are available under `/factionrp` or the shorter alias `/frp`.
Running the base command with no (or an unrecognised) sub-command prints the usage list.

* `/factionrp status` — Show the version, number of cached profiles, the active RP adapter
  (e.g. TRP3) and whether debug mode is on.
* `/factionrp debug` — Toggle debug mode on/off (see below).

### Debug Mode

The `/factionrp debug` command enables/disables debug mode.
In debug mode, a message is printed out every time the following happens:

* A broadcast is sent, including target GUID
* A message is received, including sender GUID
* A profile is stored in the cache
* A profile is refreshed in the cache

### Additional Plans

These points are to be kept in consideration, but will not be part of the current v1 release:

* Ability to load/store additional data such as TRP3 about pages
* Ability to track which battle.net/friend connection sent us the data
* Use the previous point to fetch profiles on demand when they are online.

### AddOn documentation

* [World of Warcraft API](https://warcraft.wiki.gg/wiki/World_of_Warcraft_API)
* [Lua Functions](https://warcraft.wiki.gg/wiki/Lua_functions)
* [Widget API](https://warcraft.wiki.gg/wiki/Widget_API)
* [Events](https://warcraft.wiki.gg/wiki/Events)
* [CVARS](https://warcraft.wiki.gg/wiki/Console_variables)