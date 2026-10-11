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
	self.injectOrder = {}

	-- Pinned whisper targets for testing (/frp target), keyed by normalized
	-- name. Always sent on every fanout, bypassing leg membership and caps.
	self.forcedTargets = {}
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

-- Fan a list of chunks out across every medium, skipping the medium/peer the
-- message arrived on (broadcast mediums need no relay back to themselves).
-- @param context table|nil Transport context of the inbound message, or nil
--   when we are the origin (then we send everywhere).
function Broadcast:fanOut(chunks, context)
	local comms = ns.comms
	local srcMedium = context and context.medium

	-- Same-faction channel: skip if the message already came over the channel
	-- (every channel member already received it directly).
	if srcMedium ~= "CHANNEL" then
		local idx = comms:channelIndex()
		if idx then
			comms:sendChunksAddon(chunks, "CHANNEL", idx)
		end
	end

	-- Party/raid: skip if it already came through the group.
	local groupKind = comms:groupChannel()
	if groupKind and not isGroupMedium(srcMedium) then
		comms:sendChunksAddon(chunks, groupKind)
	end

	-- Battle.net (cross-faction relays): every eligible account except the one
	-- we received this from.
	comms:eachBNetGameAccount(function(gameAccountID)
		if srcMedium == "BNET" and context.gameAccountID == gameAccountID then
			return
		end
		comms:sendChunksBNet(chunks, gameAccountID)
	end)

	-- Regular friends: optional (see C.RELAY_TO_FRIENDS), redundant with channel.
	if C.RELAY_TO_FRIENDS then
		comms:eachOnlineFriend(function(name)
			if srcMedium == "WHISPER" and context.senderName == name then
				return
			end
			comms:sendChunksAddon(chunks, "WHISPER", name)
		end)
	end

	-- Cross-faction relays: addon whispers to members of our own clubs, run as
	-- two independent legs — communities and, if we are in one, our (possibly
	-- cross-faction) guild. Each leg has its own capped candidate list with
	-- the same round-robin cursor and per-contact usage caps (shared across
	-- legs: a person who is both a guildmate and a community member still has
	-- one usage budget). See Constants for the limits.
	local skipName = (srcMedium == "WHISPER" and context.senderName)
		and ns.Util.normalizeUnitName(context.senderName)
	local now = GetTime()
	local sentKeys = {}

	-- Pinned debug targets: guaranteed delivery, bypassing leg membership and
	-- the caps (the echo-skip still applies — never whisper the source back).
	-- Debug mode only (/frp debug), so normal users cannot defeat the
	-- round-robin fairness by pinning.
	local sentPinned = 0
	if ns.db and ns.db.debug then
		for key, name in pairs(self.forcedTargets) do
			if key ~= skipName and not sentKeys[key] then
				sentKeys[key] = true
				comms:sendChunksAddon(chunks, "WHISPER", name)
				sentPinned = sentPinned + 1
			end
		end
	end

	local function usageAt(key)
		local u = self.commUsage[key]
		if not u or (now - u.at) > C.COMM_USAGE_WINDOW then
			return 0
		end
		return u.count
	end
	local function bumpUsage(key)
		local u = self.commUsage[key]
		if u and (now - u.at) <= C.COMM_USAGE_WINDOW then
			u.count = u.count + 1
		else
			self.commUsage[key] = { count = 1, at = now }
		end
	end

	-- One leg's selection: confirmed FRP relays and known xfac contacts first
	-- (filtered to that leg's membership), then the leg's opposite-faction
	-- roster. A rotating cursor spreads picks across candidates; pass 1 skips
	-- usage-exhausted contacts, pass 2 fills with them if coverage requires.
	local function sendLeg(roster, inLeg, limit)
		local candidates, seen = {}, {}
		if skipName then
			seen[skipName] = true
		end
		local function add(name)
			local key = name and ns.Util.normalizeUnitName(name)
			if key and not seen[key] then
				seen[key] = true
				candidates[#candidates + 1] = { name = name, key = key }
			end
		end
		for _, unitID in pairs(self.knownRelays) do
			if inLeg(unitID) then
				add(unitID)
			end
		end
		for _, unitID in pairs(self.xfacContacts) do
			if inLeg(unitID) then
				add(unitID)
			end
		end
		for _, name in ipairs(roster) do
			add(name)
		end
		local n = #candidates
		local sent = 0
		if n == 0 then
			return 0
		end
		self.commCursor = self.commCursor + 1
		local offset = ((self.commCursor - 1) % n) + 1
		local function trySend(cand)
			if sent >= limit or sentKeys[cand.key] then
				return
			end
			sentKeys[cand.key] = true
			comms:sendChunksAddon(chunks, "WHISPER", cand.name)
			bumpUsage(cand.key)
			sent = sent + 1
		end
		for pass = 1, 2 do
			for i = 0, n - 1 do
				local cand = candidates[((offset + i - 1) % n) + 1]
				if not (pass == 1 and usageAt(cand.key) >= C.COMM_USAGE_LIMIT) then
					trySend(cand)
				end
			end
			if sent >= limit then
				break
			end
		end
		return sent
	end

	local sentComm = sendLeg(
		ns.Util.xfacClubMembers(),
		function(u) return ns.Util.isCommunityClubMember(u) end,
		C.COMM_RELAY_LIMIT
	)
	local sentGuild = 0
	if IsInGuild() then
		sentGuild = sendLeg(
			ns.Util.xfacGuildMembers(),
			function(u) return ns.Util.isGuildClubMember(u) end,
			C.GUILD_RELAY_LIMIT
		)
	end
	if (sentComm + sentGuild + sentPinned) > 0 and ns.debugFanout then
		ns.debugFanout(sentComm, sentGuild, sentPinned)
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
	self:fanOut(Serializer.chunk(message), nil)
end

-- Reception -----------------------------------------------------------------

--- Handle a fully-reassembled inbound message.
function Broadcast:onMessage(raw, context)
	local msg = Serializer.parseMessage(raw)
	-- Reject malformed messages, including a non-numeric origin-time which would
	-- otherwise break the Cache's ordering comparison.
	if not msg or type(msg.originTime) ~= "number" then
		return
	end

	-- Loop avoidance: ignore our own traffic echoed back to us.
	if msg.sender == self.playerGUID then
		return
	end
	-- A whispered message from an opposite-faction club member confirms they
	-- run the AddOn and are reachable cross-faction: a known relay.
	if context.medium == "WHISPER" and context.senderName then
		local key = ns.Util.normalizeUnitName(context.senderName)
		if key and ns.Util.isXfacClubMember(context.senderName) then
			self.knownRelays[key] = context.senderName
		end
	end
	if ns.debugReceive then
		ns.debugReceive(msg.sender, msg.subject)
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
			return -- corrupt or undecodable
		end
		candidate.data = profileData
		result = ns.cache:tryStore(candidate)
	end
	if ns.debugStore then
		ns.debugStore(result, msg.subject)
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
	self:fanOut(Serializer.chunk(relay), context)
end

-- Injection into the RP AddOn ------------------------------------------------

--- Queue a stored profile for injection into its source AddOn's register.
-- Dedup'd by GUID: a newer copy replaces any older pending one.
function Broadcast:queueInject(candidate)
	-- Our own profile is already authoritative locally; never inject it back.
	if candidate.guid == self.playerGUID then
		return
	end
	if not self.injectQueue[candidate.guid] then
		self.injectOrder[#self.injectOrder + 1] = candidate.guid
	end
	self.injectQueue[candidate.guid] = candidate
end

--- Drain up to INJECT_BURST pending injections per tick.
function Broadcast:drainInject()
	local budget = C.INJECT_BURST
	while budget > 0 and #self.injectOrder > 0 do
		local guid = table.remove(self.injectOrder, 1)
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
end

--- Pin a whisper target for testing: always sent on every fanout, bypassing
--- leg membership and caps. Returns the normalized "Name-Realm" or nil.
function Broadcast:forceTarget(unitID)
	local key = unitID and ns.Util.normalizeUnitName(unitID)
	local display = key and ns.Util.fullUnitName(unitID)
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
	local key = unitID and ns.Util.normalizeUnitName(unitID)
	if key then
		self.xfacContacts[key] = unitID
	end
end

-- Singleton.
ns.broadcast = Broadcast:new()
ns.OnLogin(function()
	ns.broadcast:start()
end)
