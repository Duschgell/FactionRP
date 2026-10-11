# FactionRP Changelog

## 0.2.1

### Fixed

- Map scan breakage and duplicated results (issue #1): a gossip loop around
  the RP AddOn's register flooded the comm bus while TRP3 scan traffic ran.
  The Scanner now only re-originates for units the user actually selected
  (mouseover/target) and collapses update events whose content version is
  unchanged since our last originate (`lastSharedVersion` guard), so our own
  register injections no longer cycle back as new gossip.
- Unthrottled addon-message bursts contending with other add-ons during
  bursts: outbound sends are now routed through **Chomp** (the ChatThrottleLib
  wrapper TRP3 ships) on the `LOW` priority queue, with a raw fallback when
  Chomp is absent. Battle.net game-data sends are throttled the same way.
- The hidden comm channel is joined only when not already joined; repeated
  joins fired channel-list churn events that could transiently confuse other
  add-ons' channel bookkeeping.

## 0.2.0

### Added

- Re-broadcast profiles reported as received over the RP AddOn's own comm
  channels (TRP3 exchange protocol and MSP), debounced with a random jitter
  and drained through a capped queue so receipt bursts cannot flood the
  send queue.
- Cross-faction reach via communities: opposite-faction members of our own
  WoW clubs are pinged with capped addon whispers; confirmed FRP peers and
  known cross-faction contacts are preferred, with round-robin fair use so
  no member acts as a relay server.
- Registration into the RP AddOn's update/receipt signals (`ba05b62`), plus
  tentative MSP support and MSP-version folding into content versions.
- Directory injection: gossip-stored profiles populate the RP AddOn's register
  through a bounded queue (no more tooltip-only population).
- Best-effort GUID resolution helpers (unit tokens, Battle.net friends, club
  rosters) with memoization and incremental club-roster building.
- Expanded debug output and a richer `/factionrp status`.

## 0.1.1

- Bump interface to `120100`

## 0.1.0

- Initial implementation
- Implement communication protocol