-- Comms.lua
--
-- The transport layer. It owns the addon-message prefix, the hidden same-
-- faction chat channel, Battle.net peer enumeration, a paced outbound queue
-- (so the client throttle does not silently drop messages), and chunk
-- reassembly of inbound traffic. It deals only in opaque wire strings; the
-- message envelope is built/parsed one layer up (Broadcast + Serializer).

local _, ns = ...
local C = ns.Const

local Comms = ns.Class("Comms")
ns.Comms = Comms

local bnKeys = {} -- [gameAccountID] = "bn:<id>" senderKey (persistent)

function Comms:init()
	self.prefix = C.MSG_PREFIX
	self.assembler = ns.Serializer.ChunkReassembler:new()
	-- The paced outbound queue holds *descriptors* (pooled records), not
	-- closures: a send at steady state allocates nothing.
	self.queue = { head = 1, tail = 0 }
	self.descAcquire, self.descRelease = ns.Util.newPool(C.POOL_SEND_DESC)
	self.messageHandler = nil -- set by Broadcast: fn(rawMessage, context)
	self.started = false
	-- Memoized online WoW BNet account ids (see bnetAccounts).
	self.bnetAcctMemo = { ids = {}, at = 0 }
	-- Scratch transport contexts, mutated per inbound event. Passed to the
	-- message handler — valid only for the duration of that synchronous call
	-- (scratch: do not retain).
	self.chatCtx = { medium = nil, senderName = nil, senderKey = nil, gameAccountID = nil }
	self.bnCtx = { medium = "BNET", senderName = nil, senderKey = nil, gameAccountID = nil }
end

-- Lifecycle -----------------------------------------------------------------

--- Begin transport: register prefix, hook receive events, join the channel,
-- and start the paced sender. Safe to call once.
function Comms:start()
	if self.started then
		return
	end
	self.started = true

	if C_ChatInfo and C_ChatInfo.RegisterAddonMessagePrefix then
		C_ChatInfo.RegisterAddonMessagePrefix(self.prefix)
	end

	ns.RegisterEvent("CHAT_MSG_ADDON", function(_, prefix, text, channel, sender)
		self:onChatMsgAddon(prefix, text, channel, sender)
	end)
	ns.RegisterEvent("BN_CHAT_MSG_ADDON", function(_, prefix, text, kind, bnetIDGameAccount)
		self:onBNChatMsgAddon(prefix, text, bnetIDGameAccount)
	end)

	-- (Re)join the hidden channel whenever we (re)enter the world.
	ns.RegisterEvent("PLAYER_ENTERING_WORLD", function()
		self:joinChannel()
	end)
	self:joinChannel()

	-- Paced sender and stale-buffer pruning.
	self.sendTicker = C_Timer.NewTicker(C.SEND_INTERVAL, function()
		self:flushQueue()
	end)
	self.pruneTicker = C_Timer.NewTicker(C.REASSEMBLY_TIMEOUT / 2, function()
		local dropped = self.assembler:prune(C.REASSEMBLY_TIMEOUT)
		if dropped > 0 and ns.statsPruned then
			ns.statsPruned(dropped)
		end
	end)
end

--- Set the callback that receives fully-reassembled messages.
-- @param fn function Invoked as fn(rawMessage, context).
function Comms:setMessageHandler(fn)
	self.messageHandler = fn
end

-- Channel -------------------------------------------------------------------

--- Join the hidden same-faction relay channel (no chat tab).
-- Joining is idempotent (client no-ops when already joined), but every join
-- still fires CHANNEL_UI_UPDATE/CHANNEL_COUNT_UPDATE toggles in the client.
-- Other add-ons listening to those events (notably TRP3's broadcast channel
-- manager, which repeatedly re-evaluates its own channel index while the list
-- churns) see extra work and can transiently resolve the wrong index.
function Comms:joinChannel()
	if self:channelIndex() then
		return -- already in the channel; nothing to do
	end
	JoinTemporaryChannel(C.COMM_CHANNEL)
end

--- Current channel index for the relay channel, or nil if not joined.
function Comms:channelIndex()
	local id = GetChannelName(C.COMM_CHANNEL)
	if not id or id == 0 then
		return nil
	end
	return id
end

-- Outbound queue ------------------------------------------------------------

--- Number of sends currently queued.
function Comms:queueDepth()
	local q = self.queue
	return q.tail - q.head + 1
end

--- Queue a send descriptor for paced delivery.
function Comms:enqueue(desc)
	local q = self.queue
	q.tail = q.tail + 1
	q[q.tail] = desc
	if ns.statsQueue then
		ns.statsQueue(q.tail - q.head + 1)
	end
end

--- Interpret a queued descriptor and dispatch it through Chomp (or the raw
-- API fallback), recording the chunk/leg stats hooks. After dispatch the
-- descriptor is released back to the pool.
function Comms:sendDescriptor(desc)
	local text = desc.text
	if ns.statsChunkOut then
		ns.statsChunkOut(desc.kind, #text, GetTime() - desc.enqueuedAt)
	end
	if ns.debugChunk then
		ns.debugChunk("out", desc.kind, desc.target, #text)
	end
	-- The final chunk of a leg carries leg-completion fields so the stats
	-- collector can time the full message drain — no callback closure needed.
	if desc.legTotal and ns.statsOutLeg then
		ns.statsOutLeg(desc.kind, desc.target, desc.legBytes, desc.legTotal, GetTime() - desc.legStartedAt)
	end
	local chomp = AddOn_Chomp
	if desc.mode == "bnet" then
		if chomp and chomp.BNSendGameData then
			chomp.BNSendGameData(desc.target, self.prefix, text, "LOW")
			return
		end
		if BNSendGameData then
			BNSendGameData(desc.target, self.prefix, text)
		end
		return
	end
	-- Routing through Chomp (shipped by TRP3, and hence by every client
	-- running any major RP AddOn) means our bulk gossip traffic is coalesced
	-- into the same global ChatThrottleLib queues as everyone else's addon
	-- traffic. Raw C_ChatInfo.SendAddonMessage bursts from the queue can
	-- stall other add-ons' queued CHANNEL messages (notably TRP3's C_SCAN
	-- map-scan requests, which are sent unthrottled at scan time and are
	-- sensitive to chat-frame send backoff).
	if chomp and chomp.SendAddonMessage then
		chomp.SendAddonMessage(self.prefix, text, desc.kind, desc.target, "LOW")
		return
	end
	if C_ChatInfo and C_ChatInfo.SendAddonMessage then
		C_ChatInfo.SendAddonMessage(self.prefix, text, desc.kind, desc.target)
	end
end

--- Release up to SEND_BURST queued sends. Invoked on a timer.
function Comms:flushQueue()
	local q = self.queue
	local head = q.head
	local budget = C.SEND_BURST
	while budget > 0 and head <= q.tail do
		local desc = q[head]
		q[head] = nil
		head = head + 1
		self:sendDescriptor(desc)
		self.descRelease(desc)
		budget = budget - 1
	end
	q.head = head
	if head > q.tail then
		-- Queue drained: reset indices so the array part stays small.
		q.head, q.tail = 1, 0
	end
end

-- Send primitives (all paced via the queue) ---------------------------------

--- Send one raw chunk over an in-game addon channel.
-- @param text   string The chunk.
-- @param kind   string "CHANNEL" | "PARTY" | "RAID" | "INSTANCE_CHAT" | "WHISPER".
-- @param target any    Channel index (for CHANNEL) or player name (for WHISPER).
-- @return table The queued descriptor (callers may stamp leg-completion fields).
function Comms:sendAddon(text, kind, target)
	local desc = self.descAcquire()
	desc.mode = "addon"
	desc.text = text
	desc.kind = kind
	desc.target = target
	desc.enqueuedAt = GetTime()
	self:enqueue(desc)
	return desc
end

--- Send one raw chunk to a Battle.net game account.
function Comms:sendBNet(text, gameAccountID)
	local desc = self.descAcquire()
	desc.mode = "bnet"
	desc.text = text
	desc.kind = "BNET"
	desc.target = gameAccountID
	desc.enqueuedAt = GetTime()
	self:enqueue(desc)
	return desc
end

-- Convenience: send a list of chunks over a single medium ------------------
-- The final chunk's descriptor carries leg-completion fields so the stats
-- collector can time how long a full message took to drain to this leg.

function Comms:sendChunksAddon(chunks, kind, target)
	local n = #chunks
	local bytes, startedAt = 0, GetTime()
	for i = 1, n do
		bytes = bytes + #chunks[i]
	end
	for i = 1, n do
		local desc = self:sendAddon(chunks[i], kind, target)
		if i == n then
			desc.legStartedAt = startedAt
			desc.legBytes = bytes
			desc.legTotal = n
		end
	end
end

function Comms:sendChunksBNet(chunks, gameAccountID)
	local n = #chunks
	local bytes, startedAt = 0, GetTime()
	for i = 1, n do
		bytes = bytes + #chunks[i]
	end
	for i = 1, n do
		local desc = self:sendBNet(chunks[i], gameAccountID)
		if i == n then
			desc.legStartedAt = startedAt
			desc.legBytes = bytes
			desc.legTotal = n
		end
	end
end

-- Peer enumeration ----------------------------------------------------------

--- The addon channel kind for the current group, or nil if solo.
-- Handles instanced groups, where PARTY/RAID do not deliver.
function Comms:groupChannel()
	if IsInGroup(LE_PARTY_CATEGORY_INSTANCE) then
		return "INSTANCE_CHAT"
	elseif IsInRaid() then
		return "RAID"
	elseif IsInGroup() then
		return "PARTY"
	end
	return nil
end

--- Invoke callback(gameAccountID) for every online WoW Battle.net friend in
-- the current region. These are our cross-faction relays.
function Comms:eachBNetGameAccount(callback)
	if BNFeaturesEnabledAndConnected and not BNFeaturesEnabledAndConnected() then
		return
	end
	local numFriends = BNGetNumFriends() or 0
	for i = 1, numFriends do
		local numAccounts = C_BattleNet.GetFriendNumGameAccounts(i) or 0
		for j = 1, numAccounts do
			local acct = C_BattleNet.GetFriendGameAccountInfo(i, j)
			if acct
				and acct.gameAccountID
				and acct.isOnline
				and acct.clientProgram == BNET_CLIENT_WOW
				and acct.isInCurrentRegion
			then
				callback(acct.gameAccountID)
			end
		end
	end
end

--- Online WoW Battle.net game account ids, memoized for C.BNET_MEMO seconds.
-- Enumerating friends allocates client tables per account; the fanout uses
-- this memoized list instead of a full scan per message.
-- @return table List of gameAccountIDs (do not mutate — shared reference).
function Comms:bnetAccounts()
	local memo = self.bnetAcctMemo
	local now = GetTime()
	if (now - memo.at) > C.BNET_MEMO then
		local ids = memo.ids
		for i = #ids, 1, -1 do
			ids[i] = nil
		end
		self:eachBNetGameAccount(function(id)
			ids[#ids + 1] = id
		end)
		memo.at = now
	end
	return memo.ids
end

--- Invoke callback(characterName) for every online (same-faction) WoW friend.
function Comms:eachOnlineFriend(callback)
	if not C_FriendList then
		return
	end
	local num = C_FriendList.GetNumFriends() or 0
	for i = 1, num do
		local info = C_FriendList.GetFriendInfoByIndex(i)
		if info and info.connected and info.name then
			callback(info.name)
		end
	end
end

-- Inbound -------------------------------------------------------------------

function Comms:onChatMsgAddon(prefix, text, channel, sender)
	if prefix ~= self.prefix then
		return
	end
	local ctx = self.chatCtx
	ctx.medium = channel
	ctx.senderName = sender
	ctx.senderKey = sender
	ctx.gameAccountID = nil
	self:handleIncoming(text, ctx)
end

function Comms:onBNChatMsgAddon(prefix, text, bnetIDGameAccount)
	if prefix ~= self.prefix then
		return
	end
	local key = bnetIDGameAccount and bnKeys[bnetIDGameAccount]
	if not key then
		key = "bn:" .. tostring(bnetIDGameAccount)
		if bnetIDGameAccount then
			bnKeys[bnetIDGameAccount] = key
		end
	end
	local ctx = self.bnCtx
	ctx.medium = "BNET"
	ctx.senderName = nil
	ctx.senderKey = key
	ctx.gameAccountID = bnetIDGameAccount
	self:handleIncoming(text, ctx)
end

--- Reassemble a chunk and, once a full message is available, hand it to the
-- registered message handler with transport context (used by Broadcast for
-- loop avoidance and to avoid echoing back to the source peer).
-- NOTE: `context` is a shared scratch table — valid only for the duration of
-- this synchronous call chain (incl. messageHandler). Never retain it.
function Comms:handleIncoming(chunkString, context)
	-- Reassembly is keyed by a stable per-sender identity.
	local senderKey = context.senderKey
	if ns.statsChunkIn then
		ns.statsChunkIn(context.medium, #chunkString)
	end
	if ns.debugChunk then
		ns.debugChunk("in", context.medium, senderKey, #chunkString)
	end
	local full, chunks, elapsed, dupes = self.assembler:add(senderKey, chunkString)
	if full then
		if ns.statsIn then
			ns.statsIn(full, senderKey, context.medium, chunks, elapsed, dupes)
		end
		if self.messageHandler then
			self.messageHandler(full, context)
		end
	end
end

-- Singleton, started after login.
ns.comms = Comms:new()
ns.OnLogin(function()
	ns.comms:start()
end)
