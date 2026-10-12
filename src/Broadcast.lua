-- Broadcast.lua
--
-- The gossip engine. It turns a locally-extracted profile into a wire message
-- and floods it, and it processes inbound messages: loop avoidance, version
-- de-duplication (via the Cache), storage, and re-broadcast to everyone except
-- the peer/medium we received it from.
--
-- SENDER vs SUBJECT: SENDER is always the *immediate* sender (us, whenever we
-- originate or relay) and is used purely for loop avoidance. SUBJECT is the
-- profile owner and never changes as a message is relayed.

local _, ns = ...
local C = ns.Const
local Serializer = ns.Serializer
local Util = ns.Util

local Broadcast = ns.Class("Broadcast")
ns.Broadcast = Broadcast

function Broadcast:init()
	self.playerGUID = nil -- resolved at login
	-- Cross-faction relay knowledge learned from traffic (session-only),
	-- keyed by normalized "name-realm":
	--   knownRelays:  opposite-faction club members confirmed to run FactionRP
	--                 (we received an FRP whisper from them).
	--   xfacContacts: opposite-faction club members whose profiles reached us
	--                 via the RP AddOn's own comms — proven cross-faction
	--                 reachable RP-AddOn users, though maybe not FRP users.
	self.knownRelays = {}
	self.xfacContacts = {}

	-- Community-relay fairness state: per-contact usage counts (so no single
	-- member becomes a relay server) and a round-robin cursor spreading picks
	-- across the candidate list.
	self.commUsage = {} -- [normalized name] = { count, at }
	self.commCursor = 0

	-- Profiles queued for injection into the RP AddOn's register so its
	-- directory fills as gossip lands (drained at a bounded rate).
	self.injectQueue = {} -- [guid] = candidate
	self.injectOrder = { head = 1, tail = 0 } -- FIFO of guids

	-- Pinned whisper targets for testing (/frp target), keyed by normalized
	-- name. Always sent on every fanout, bypassing leg membership and caps.
	self.forcedTargets = {}

	-- Reused fanout scratch, wiped per call so a fanout allocates nothing:
	-- fanoutSentKeys dedups targets across legs, legCandidates/legSeen hold
	-- one leg's ordered candidate keys and their display names.
	self.fanoutSentKeys = {}
	self.legCandidates = {}
	self.legSeen = {}

	-- The last fanout's chosen targets per leg ("/frp relays" + the fanout
	-- debug line): arrays wiped and refilled each fanout.
	self.lastLegPicks = { community = {}, guild = {}, bnet = {}, pinned = {} }
end

function Broadcast:start()
	self.playerGUID = UnitGUID("player")
	ns.comms:setMessageHandler(function(raw, context)
		self:onMessage(raw, context)
	end)
	C_Timer.NewTicker(C.INJECT_INTERVAL, function()
		self:drainInject()
	end)
end

-- Helpers -------------------------------------------------------------------

local function isGroupMedium(medium)
	return medium == "PARTY" or medium == "RAID" or medium == "INSTANCE_CHAT"
end

-- Leg-membership predicates for _sendLeg, hoisted so fanout allocates no
-- closures.
local function inCommunity(unitID)
	return Util.isCommunityClubMember(unitID)
end

local function inGuild(unitID)
	return Util.isGuildClubMember(unitID)
end

--- Record `name` as a leg candidate if not already seen.
-- @param candidates table Ordered array of normalized keys.
-- @param seen       table Normalized key -> display name.
local function addCandidate(candidates, seen, name)
	local key = name and Util.normalizeUnitName(name)
	if key and not seen[key] then
		seen[key] = name
		candidates[#candidates + 1] = key
	end
end

--- Usage-budget helpers for the community/guild whisper legs (methods so no
-- per-fanout closures). `now` is stamped once per fanout by the caller.
function Broadcast:usageAt(key, now)
	local u = self.commUsage[key]
	if not u or (now - u.at) > C.COMM_USAGE_WINDOW then
		return 0
	end
	return u.count
end

function Broadcast:bumpUsage(key, now)
	local u = self.commUsage[key]
	if not u then
		u = { count = 0, at = 0 }
		self.commUsage[key] = u
	end
	-- Reuse the entry across window expiries (reset in place, no realloc).
	if (now - u.at) <= C.COMM_USAGE_WINDOW then
		u.count = u.count + 1
	else
		u.count = 1
	end
	u.at = now
end

--- One leg's selection: confirmed FRP relays and known xfac contacts first
-- (filtered to that leg's membership), then the leg's opposite-faction
-- roster. A rotating cursor spreads picks across candidates; pass 1 skips
-- usage-exhausted contacts, pass 2 fills with them if coverage requires.
-- Uses shared wiped scratch (self.legCandidates/legSeen/fanoutSentKeys) so
-- candidate building allocates nothing.
-- @return number Sends made this leg.
function Broadcast:_sendLeg(chunks, roster, inLeg, limit, skipName, now, picks)
	local comms = ns.comms
	local sentKeys = self.fanoutSentKeys
	local candidates = self.legCandidates
	local seen = self.legSeen
	for i = #candidates, 1, -1 do
		candidates[i] = nil
	end
	for key in pairs(seen) do
		seen[key] = nil
	end
	if picks then
		for i = #picks, 1, -1 do
			picks[i] = nil
		end
	end

	if skipName then
		seen[skipName] = skipName
	end
	for _, unitID in pairs(self.knownRelays) do
		if inLeg(unitID) then
			addCandidate(candidates, seen, unitID)
		end
	end
	for _, unitID in pairs(self.xfacContacts) do
		if inLeg(unitID) then
			addCandidate(candidates, seen, unitID)
		end
	end
	for i = 1, #roster do
		addCandidate(candidates, seen, roster[i])
	end

	local n = #candidates
	if n == 0 then
		return 0
	end
	self.commCursor = self.commCursor + 1
	local offset = ((self.commCursor - 1) % n) + 1
	local sent = 0
	for pass = 1, 2 do
		for i = 0, n - 1 do
			if sent >= limit then
				break
			end
			local key = candidates[((offset + i - 1) % n) + 1]
			if not sentKeys[key]
				and not (pass == 1 and self:usageAt(key, now) >= C.COMM_USAGE_LIMIT)
			then
				sentKeys[key] = true
				comms:sendChunksAddon(chunks, "WHISPER", seen[key])
				if picks then
					picks[#picks + 1] = seen[key]
				end
				self:bumpUsage(key, now)
				sent = sent + 1
			end
		end
		if sent >= limit then
			break
		end
	end
	return sent
end

-- Fan a list of chunks out across every medium, skipping the medium/peer the
-- message arrived on (broadcast mediums need no relay back to themselves).
-- @param context table|nil Transport context of the inbound message, or nil
--   when we are the origin (then we send everywhere).
function Broadcast:fanOut(chunks, context)
	local comms = ns.comms
	local srcMedium = context and context.medium

	local leg = ns.statsLeg

	-- Same-faction channel: skip if the message already came over the channel
	-- (every channel member already received it directly).
	if srcMedium ~= "CHANNEL" then
		local idx = comms:channelIndex()
		if idx then
			comms:sendChunksAddon(chunks, "CHANNEL", idx)
			if leg then
				leg("channel", 1)
			end
		end
	end

	-- Party/raid: skip if it already came through the group.
	local groupKind = comms:groupChannel()
	if groupKind and not isGroupMedium(srcMedium) then
		comms:sendChunksAddon(chunks, groupKind)
		if leg then
			leg("group", 1)
		end
	end

	-- Battle.net (cross-faction relays): every eligible account except the one
	-- we received this from. The account list is memoized (see Comms:bnetAccounts)
	-- so fanning out does not rescan the friends list per message.
	local srcGameAccountID = context and context.gameAccountID
	local bnetPicks = self.lastLegPicks.bnet
	for i = #bnetPicks, 1, -1 do
		bnetPicks[i] = nil
	end
	local bnetIds = comms:bnetAccounts()
	for i = 1, #bnetIds do
		local id = bnetIds[i]
		if not (srcMedium == "BNET" and srcGameAccountID == id) then
			comms:sendChunksBNet(chunks, id)
			bnetPicks[#bnetPicks + 1] = id
			if leg then
				leg("bnet", 1)
			end
		end
	end

	-- Regular friends: optional (see C.RELAY_TO_FRIENDS), redundant with channel.
	if C.RELAY_TO_FRIENDS then
		comms:eachOnlineFriend(function(name)
			if srcMedium == "WHISPER" and context.senderName == name then
				return
			end
			comms:sendChunksAddon(chunks, "WHISPER", name)
			if leg then
				leg("friends", 1)
			end
		end)
	end

	-- Cross-faction relays: addon whispers to members of our own clubs, run as
	-- two independent legs — communities and, if we are in one, our (possibly
	-- cross-faction) guild. Each leg has its own capped candidate list with
	-- the same round-robin cursor and per-contact usage caps (shared across
	-- legs: a person who is both a guildmate and a community member still has
	-- one usage budget). See Constants for the limits.
	local skipName = (srcMedium == "WHISPER" and context.senderName)
		and Util.normalizeUnitName(context.senderName)
	local now = GetTime()
	local sentKeys = self.fanoutSentKeys
	for key in pairs(sentKeys) do
		sentKeys[key] = nil
	end

	-- Pinned debug targets: guaranteed delivery, bypassing leg membership and
	-- the caps (the echo-skip still applies — never whisper the source back).
	-- "targets" debug category only (/frp debug targets), so normal users
	-- cannot defeat the round-robin fairness by pinning.
	local sentPinned = 0
	local pinnedPicks = self.lastLegPicks.pinned
	for i = #pinnedPicks, 1, -1 do
		pinnedPicks[i] = nil
	end
	if ns.debugEnabled("targets") then
		for key, name in pairs(self.forcedTargets) do
			if key ~= skipName and not sentKeys[key] then
				sentKeys[key] = true
				comms:sendChunksAddon(chunks, "WHISPER", name)
				pinnedPicks[#pinnedPicks + 1] = name
				sentPinned = sentPinned + 1
			end
		end
	end

	local sentComm = self:_sendLeg(
		chunks, Util.xfacClubMembers(), inCommunity, C.COMM_RELAY_LIMIT,
		skipName, now, self.lastLegPicks.community
	)
	local sentGuild = 0
	if IsInGuild() then
		sentGuild = self:_sendLeg(
			chunks, Util.xfacGuildMembers(), inGuild, C.GUILD_RELAY_LIMIT,
			skipName, now, self.lastLegPicks.guild
		)
	end
	if leg then
		if sentComm > 0 then
			leg("community", sentComm)
		end
		if sentGuild > 0 then
			leg("guild", sentGuild)
		end
		if sentPinned > 0 then
			leg("pinned", sentPinned)
		end
	end
	if (sentComm + sentGuild + sentPinned) > 0 and ns.debugFanout then
		ns.debugFanout(self.lastLegPicks.community, self.lastLegPicks.guild, self.lastLegPicks.pinned)
	end
end

-- Origination ---------------------------------------------------------------

--- Originate a broadcast for a profile we just extracted from real local data.
-- Stamps the authoritative origin-time, stores it (which applies de-dup), and
-- floods only if it represents new/changed content.
-- @param guid    string The subject's GUID.
-- @param unitID  string The subject's "Name-Realm".
-- @param profile table  { addon, contentVersion, data } from an adapter.
function Broadcast:originate(guid, unitID, profile)
	local encoded = Serializer.encodeProfile(profile.data)
	if not encoded then
		if ns.statsError then
			ns.statsError("encode_fail")
		end
		return
	end

	local candidate = {
		guid = guid,
		unitID = unitID,
		addon = profile.addon,
		originTime = time(), -- we are the origin: authoritative timestamp
		contentVersion = profile.contentVersion,
		data = profile.data,
	}
	local result = ns.cache:tryStore(candidate)
	if ns.debugStore then
		ns.debugStore(result, guid)
	end
	if ns.statsCache then
		ns.statsCache(result)
	end
	if result ~= ns.Cache.RESULT.NEW and result ~= ns.Cache.RESULT.UPDATED then
		return -- nothing new to share
	end

	local message = Serializer.buildMessage({
		sender = self.playerGUID,
		subject = guid,
		unitID = unitID,
		addon = profile.addon,
		originTime = candidate.originTime,
		contentVersion = profile.contentVersion,
		data = encoded,
	})
	if ns.debugBroadcast then
		ns.debugBroadcast(guid)
	end
	local chunks = Serializer.chunk(message)
	if ns.statsOutMessage then
		ns.statsOutMessage(#message, #chunks, guid, "originate")
	end
	self:fanOut(chunks, nil)
end

-- Reception -----------------------------------------------------------------

--- Handle a fully-reassembled inbound message.
function Broadcast:onMessage(raw, context)
	local msg = Serializer.parseMessage(raw)
	-- Reject malformed messages, including a non-numeric origin-time which would
	-- otherwise break the Cache's ordering comparison.
	if not msg or type(msg.originTime) ~= "number" then
		if ns.statsError then
			ns.statsError("malformed")
		end
		return
	end

	-- Loop avoidance: ignore our own traffic echoed back to us.
	if msg.sender == self.playerGUID then
		if ns.statsError then
			ns.statsError("self_echo")
		end
		return
	end
	-- A whispered message from an opposite-faction club member confirms they
	-- run the AddOn and are reachable cross-faction: a known relay.
	if context.medium == "WHISPER" and context.senderName then
		local key = Util.normalizeUnitName(context.senderName)
		if key and Util.isXfacClubMember(context.senderName) then
			self.knownRelays[key] = context.senderName
		end
	end
	if ns.debugReceive then
		ns.debugReceive(msg.sender, msg.subject)
	end
	if ns.statsPeer then
		ns.statsPeer(msg.sender, msg.subject)
	end
	if ns.statsTagSubject then
		ns.statsTagSubject(msg.subject)
	end
	if ns.statsOriginAge then
		-- Rough cross-client staleness measure; sender's time() may skew.
		ns.statsOriginAge(time() - msg.originTime)
	end

	local candidate = {
		guid = msg.subject,
		unitID = msg.unitID,
		addon = msg.addon,
		originTime = msg.originTime,
		contentVersion = msg.contentVersion,
	}

	-- Cheap path first: duplicates and stale copies resolve without the
	-- payload, so the (expensive) decode only runs for NEW/UPDATED content.
	local result = ns.cache:peek(candidate)
	if not result then
		local profileData = Serializer.decodeProfile(msg.data)
		if not profileData then
			if ns.statsError then
				ns.statsError("undecodable")
			end
			return -- corrupt or undecodable
		end
		candidate.data = profileData
		result = ns.cache:tryStore(candidate)
	end
	if ns.debugStore then
		ns.debugStore(result, msg.subject)
	end
	if ns.statsCache then
		ns.statsCache(result)
	end

	-- Only relay genuinely new/changed content; this is what converges the flood.
	if result ~= ns.Cache.RESULT.NEW and result ~= ns.Cache.RESULT.UPDATED then
		return
	end

	-- Populate the RP AddOn's register as gossip lands so its directory fills
	-- with received profiles (queued: register writes are expensive, bursty).
	self:queueInject(candidate)

	-- Relay unchanged except for SENDER (now us). Re-use the already-encoded
	-- data verbatim rather than re-encoding.
	local relay = Serializer.buildMessage({
		sender = self.playerGUID,
		subject = msg.subject,
		unitID = msg.unitID,
		addon = msg.addon,
		originTime = msg.originTime,
		contentVersion = msg.contentVersion,
		data = msg.data,
	})
	local chunks = Serializer.chunk(relay)
	if ns.statsOutMessage then
		ns.statsOutMessage(#relay, #chunks, msg.subject, "relay")
	end
	self:fanOut(chunks, context)
end

-- Injection into the RP AddOn ------------------------------------------------

--- Number of profiles queued for injection.
function Broadcast:injectDepth()
	local o = self.injectOrder
	return o.tail - o.head + 1
end

--- Queue a stored profile for injection into its source AddOn's register.
-- Dedup'd by GUID: a newer copy replaces any older pending one.
function Broadcast:queueInject(candidate)
	-- Our own profile is already authoritative locally; never inject it back.
	if candidate.guid == self.playerGUID then
		return
	end
	if not self.injectQueue[candidate.guid] then
		local o = self.injectOrder
		o.tail = o.tail + 1
		o[o.tail] = candidate.guid
		if ns.statsQueueDepth then
			ns.statsQueueDepth("inject", o.tail - o.head + 1)
		end
	end
	self.injectQueue[candidate.guid] = candidate
end

--- Drain up to INJECT_BURST pending injections per tick.
function Broadcast:drainInject()
	local o = self.injectOrder
	local head = o.head
	local budget = C.INJECT_BURST
	while budget > 0 and head <= o.tail do
		local guid = o[head]
		o[head] = nil
		head = head + 1
		local candidate = self.injectQueue[guid]
		self.injectQueue[guid] = nil
		if candidate then
			local adapter = ns.getAdapter(candidate.addon)
			if adapter and adapter:isAvailable() then
				adapter:inject(candidate.unitID, candidate.data)
			end
		end
		budget = budget - 1
	end
	o.head = head
	if head > o.tail then
		o.head, o.tail = 1, 0
	end
end

--- Pin a whisper target for testing: always sent on every fanout, bypassing
--- leg membership and caps. Returns the normalized "Name-Realm" or nil.
function Broadcast:forceTarget(unitID)
	local key = unitID and Util.normalizeUnitName(unitID)
	local display = key and Util.fullUnitName(unitID)
	if key and display then
		self.forcedTargets[key] = display
		return display
	end
end

--- Drop all pinned targets. Returns the number removed.
function Broadcast:clearForcedTargets()
	local n = 0
	for _ in pairs(self.forcedTargets) do
		n = n + 1
	end
	self.forcedTargets = {}
	return n
end

--- Record an opposite-faction club member whose profile reached us over the
--- RP AddOn's own comm channels. They are proven reachable by cross-faction
--- whisper and almost certainly run an RP AddOn, so they are preferred relay
--- targets for the community fanout (see fanOut).
function Broadcast:noteXfacContact(unitID)
	local key = unitID and Util.normalizeUnitName(unitID)
	if key then
		self.xfacContacts[key] = unitID
	end
end

-- Singleton.
ns.broadcast = Broadcast:new()
ns.OnLogin(function()
	ns.broadcast:start()
end)
