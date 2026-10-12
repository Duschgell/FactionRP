-- Stats.lua
--
-- Session-only traffic statistics for debugging: message rates, byte sizes
-- (average AND median), profile reassembly time (first to last chunk), per-leg
-- transmit time, per-medium breakdowns, cache outcomes, error tallies, queue
-- health, and a ring buffer of the most recent messages for close inspection.
--
-- Collection is gated on the "stats" debug category (or `all`): while
-- disabled, the recorder entry points (ns.statsIn / ns.statsOutLeg / ...) are
-- simply absent, so instrumentation call sites behind nil-guards cost one
-- table lookup and nothing runs. Debug.lua's ns.refreshDebugHooks re-applies
-- the installation via ns.applyStatsHooks whenever flags change.
--
-- Memory is bounded: sizes/durations live in fixed-cap reservoirs (only used
-- for medians and last-N queries), rates in a bucketed ring, and the recent
-- log in a ring buffer.

local _, ns = ...
local C = ns.Const

local GetTime = GetTime
local floor = math.floor
local sort = table.sort

local RES_CAP = C.STATS_RESERVOIR

-- Store ----------------------------------------------------------------------
-- One stable table, published as ns.stats and reset in place, so the recorder
-- implementations below can upvalue it and never look it up per call.

local stats = {}
ns.stats = stats

local function freshBuckets()
	local b = {}
	for i = 0, C.STATS_RATE_BUCKETS - 1 do
		b[i] = { s = -1, c = 0 }
	end
	return b
end

local function resetStore()
	for k in pairs(stats) do
		stats[k] = nil
	end
	stats.startedAt = GetTime()

	-- Logical messages (reassembled inbound / built outbound).
	stats.inMsgs, stats.inBytes = 0, 0
	stats.outMsgs, stats.outBytes = 0, 0
	-- Per-leg outbound completions and wire-level chunk counters.
	stats.outSends = 0
	stats.inChunks, stats.inWireBytes = 0, 0
	stats.outChunks, stats.outWireBytes = 0, 0
	stats.dupChunks = 0    -- duplicate chunk arrivals (retransmits)
	stats.incomplete = 0   -- reassembly buffers pruned (lost chunks)

	-- Fixed-cap sample reservoirs: { cap, n = live count, i = write cursor, ... }.
	local function res() return { cap = RES_CAP, n = 0, i = 0 } end
	stats.inSizes = res()
	stats.outSizes = res()
	stats.inChunkSizes = res()
	stats.outChunkSizes = res()
	stats.assembleMs = res() -- first->last chunk, inbound
	stats.transmitMs = res() -- enqueue->last chunk sent, per leg
	stats.originAge = res()  -- time() - originTime on receipt

	-- Per-medium counters keyed by channel kind ("CHANNEL", "BNET", ...).
	stats.mediumsIn = {}  -- [medium] = { msgs, bytes, chunks }
	stats.mediumsOut = {}
	stats.legs = {}       -- [leg] = sends ("channel","group","bnet","community","guild","pinned","friends")

	stats.cache = { new = 0, updated = 0, refreshed = 0, rejected = 0 }
	stats.errors = {} -- [kind] = count

	stats.queue = { highWater = 0, dwellSum = 0, dwellN = 0, dwellMax = 0 }
	stats.queues = {} -- [name] = high-water depth ("inject", "received")

	stats.senders, stats.senderN = {}, 0
	stats.subjects, stats.subjectN = {}, 0

	stats.rateIn = { b = freshBuckets() }
	stats.rateOut = { b = freshBuckets() }

	stats.recent = { cap = C.STATS_RECENT, n = 0, i = 0 } -- {dir, medium, peer, subject, bytes, chunks, ms, at}
end

resetStore()

-- Sample helpers ---------------------------------------------------------------

--- Append a value to a fixed-cap ring reservoir (cap is stored on the table).
local function push(t, v)
	local cap = t.cap
	local i = t.i + 1
	if i > cap then
		i = 1
	end
	t.i = i
	t[i] = v
	if t.n < cap then
		t.n = t.n + 1
	end
end

--- Median of a reservoir's live entries (copy + sort; query-time only).
local function median(t)
	local n = t.n
	if n == 0 then
		return 0
	end
	local copy = {}
	for i = 1, n do
		copy[i] = t[i]
	end
	sort(copy)
	local mid = floor((n + 1) / 2)
	if n % 2 == 1 then
		return copy[mid]
	end
	return (copy[mid] + copy[mid + 1]) / 2
end

--- Smallest live entry of a reservoir.
local function reservoirMin(t)
	local n = t.n
	if n == 0 then
		return 0
	end
	local m = t[1]
	for i = 2, n do
		if t[i] < m then
			m = t[i]
		end
	end
	return m
end

--- Largest live entry of a reservoir.
local function reservoirMax(t)
	local n = t.n
	if n == 0 then
		return 0
	end
	local m = t[1]
	for i = 2, n do
		if t[i] > m then
			m = t[i]
		end
	end
	return m
end

-- Rolling rate ------------------------------------------------------------------
-- The window is divided into BUCKETS slots, each stamped with its slot index;
-- a bump re-zeros the bucket its slot rotates into, so the live window is at
-- most STATS_RATE_WINDOW seconds without any per-query pruning.

local RATE_SPAN = C.STATS_RATE_WINDOW / C.STATS_RATE_BUCKETS
local RATE_BUCKETS = C.STATS_RATE_BUCKETS

local function bumpRate(r, now)
	local slot = floor(now / RATE_SPAN)
	local b = r.b[slot % RATE_BUCKETS]
	if b.s ~= slot then
		b.s = slot
		b.c = 0
	end
	b.c = b.c + 1
end

--- Count of events within the last STATS_RATE_WINDOW seconds.
local function rateCount(r, now)
	local slot = floor(now / RATE_SPAN)
	local total = 0
	for i = 0, RATE_BUCKETS - 1 do
		local b = r.b[i]
		if b.s > slot - RATE_BUCKETS and b.s <= slot then
			total = total + b.c
		end
	end
	return total
end

local function bumpMedium(t, medium, msgs, bytes, chunks)
	local m = t[medium]
	if not m then
		m = { msgs = 0, bytes = 0, chunks = 0 }
		t[medium] = m
	end
	m.msgs = m.msgs + msgs
	m.bytes = m.bytes + bytes
	m.chunks = m.chunks + chunks
end

local function pushRecent(entry)
	local r = stats.recent
	push(r, entry)
end

-- Recorders --------------------------------------------------------------------
-- Installed onto ns while the "stats" flag is on; absent otherwise. Each
-- upvalues the store and helpers directly — no flag checks, no extra lookups.

local function recChunkIn(medium, bytes)
	stats.inChunks = stats.inChunks + 1
	stats.inWireBytes = stats.inWireBytes + bytes
	push(stats.inChunkSizes, bytes)
	bumpMedium(stats.mediumsIn, medium, 0, bytes, 1)
end

local function recIn(full, senderKey, medium, meta)
	local bytes = #full
	stats.inMsgs = stats.inMsgs + 1
	stats.inBytes = stats.inBytes + bytes
	stats.dupChunks = stats.dupChunks + (meta.dupes or 0)
	bumpMedium(stats.mediumsIn, medium, 1, 0, 0)
	push(stats.inSizes, bytes)
	push(stats.assembleMs, meta.elapsed * 1000)
	bumpRate(stats.rateIn, GetTime())
	pushRecent({
		dir = "in", medium = medium, peer = senderKey,
		bytes = bytes, chunks = meta.chunks, ms = meta.elapsed * 1000,
		at = GetTime(),
	})
end

--- Stamp the subject GUID onto the newest inbound recent entry once Broadcast
--- has parsed the envelope (Comms only sees opaque bytes).
local function recTagSubject(subject)
	local r = stats.recent
	local e = r.n > 0 and r[r.i]
	if e and e.dir == "in" then
		e.subject = subject
	end
end

local function recChunkOut(medium, bytes, dwell)
	stats.outChunks = stats.outChunks + 1
	stats.outWireBytes = stats.outWireBytes + bytes
	push(stats.outChunkSizes, bytes)
	local q = stats.queue
	q.dwellSum = q.dwellSum + dwell
	q.dwellN = q.dwellN + 1
	if dwell > q.dwellMax then
		q.dwellMax = dwell
	end
end

--- A full message finished draining to one leg (last chunk dequeued).
-- @param elapsed number Seconds from first enqueue to last-chunk send.
local function recOutLeg(medium, target, bytes, chunks, elapsed)
	stats.outSends = stats.outSends + 1
	bumpMedium(stats.mediumsOut, medium, 1, bytes, chunks)
	push(stats.transmitMs, elapsed * 1000)
	bumpRate(stats.rateOut, GetTime())
	pushRecent({
		dir = "out", medium = medium, peer = target,
		bytes = bytes, chunks = chunks, ms = elapsed * 1000,
		at = GetTime(),
	})
end

--- A logical outbound message was built (originate or relay).
local function recOutMessage(bytes, chunks, subject, how)
	stats.outMsgs = stats.outMsgs + 1
	stats.outBytes = stats.outBytes + bytes
	push(stats.outSizes, bytes)
	local key = how or "send"
	stats.legs[key] = (stats.legs[key] or 0) + 1
end

local function recQueue(depth)
	if depth > stats.queue.highWater then
		stats.queue.highWater = depth
	end
end

local function recQueueDepth(name, depth)
	local cur = stats.queues[name]
	if not cur or depth > cur then
		stats.queues[name] = depth
	end
end

local function recLeg(name, n)
	stats.legs[name] = (stats.legs[name] or 0) + n
end

local function recCache(result)
	local key = tostring(result)
	stats.cache[key] = (stats.cache[key] or 0) + 1
end

local function recError(kind)
	stats.errors[kind] = (stats.errors[kind] or 0) + 1
end

local function recPruned(n)
	stats.incomplete = stats.incomplete + n
end

local function recOriginAge(seconds)
	push(stats.originAge, seconds)
end

local function recPeer(sender, subject)
	if sender and not stats.senders[sender] then
		stats.senders[sender] = true
		stats.senderN = stats.senderN + 1
	end
	if subject and not stats.subjects[subject] then
		stats.subjects[subject] = true
		stats.subjectN = stats.subjectN + 1
	end
end

local RECORDERS = {
	statsChunkIn = recChunkIn,
	statsIn = recIn,
	statsTagSubject = recTagSubject,
	statsChunkOut = recChunkOut,
	statsOutLeg = recOutLeg,
	statsOutMessage = recOutMessage,
	statsQueue = recQueue,
	statsQueueDepth = recQueueDepth,
	statsLeg = recLeg,
	statsCache = recCache,
	statsError = recError,
	statsPruned = recPruned,
	statsOriginAge = recOriginAge,
	statsPeer = recPeer,
}

--- Install the recorders while the "stats" category (or `all`) is enabled;
--- remove them otherwise so call sites noop on a single nil check.
function ns.applyStatsHooks()
	local on = ns.debugEnabled and ns.debugEnabled("stats")
	for name, fn in pairs(RECORDERS) do
		ns[name] = on and fn or nil
	end
end

-- Snapshot ---------------------------------------------------------------------

ns.Stats = {}

--- Computed read-only summary for inspection (slash output now, UI later).
-- @return table
function ns.Stats.snapshot()
	local now = GetTime()
	local uptime = now - stats.startedAt
	local inN, outN = stats.inMsgs, stats.outMsgs
	return {
		uptime = uptime,
		inMsgs = inN,
		outMsgs = outN,
		outSends = stats.outSends,
		inBytes = stats.inBytes,
		outBytes = stats.outBytes,
		inChunks = stats.inChunks,
		outChunks = stats.outChunks,
		inWireBytes = stats.inWireBytes,
		outWireBytes = stats.outWireBytes,
		avgInSize = inN > 0 and stats.inBytes / inN or 0,
		avgOutSize = outN > 0 and stats.outBytes / outN or 0,
		medInSize = median(stats.inSizes),
		medOutSize = median(stats.outSizes),
		medInChunk = median(stats.inChunkSizes),
		medOutChunk = median(stats.outChunkSizes),
		medAssembleMs = median(stats.assembleMs),
		maxAssembleMs = reservoirMax(stats.assembleMs),
		medTransmitMs = median(stats.transmitMs),
		maxTransmitMs = reservoirMax(stats.transmitMs),
		medOriginAge = median(stats.originAge),
		rateIn = uptime > 0 and rateCount(stats.rateIn, now) / (uptime < C.STATS_RATE_WINDOW and uptime or C.STATS_RATE_WINDOW) or 0,
		rateOut = uptime > 0 and rateCount(stats.rateOut, now) / (uptime < C.STATS_RATE_WINDOW and uptime or C.STATS_RATE_WINDOW) or 0,
		rateInSession = uptime > 0 and inN / uptime or 0,
		rateOutSession = uptime > 0 and outN / uptime or 0,
		dupChunks = stats.dupChunks,
		incomplete = stats.incomplete,
		mediumsIn = stats.mediumsIn,
		mediumsOut = stats.mediumsOut,
		legs = stats.legs,
		cache = stats.cache,
		errors = stats.errors,
		queue = stats.queue,
		queues = stats.queues,
		senderN = stats.senderN,
		subjectN = stats.subjectN,
		recent = stats.recent,
	}
end

-- /factionrp stats -------------------------------------------------------------

local function fmtBytes(n)
	if n >= 1024 then
		return ("%.1fKB"):format(n / 1024)
	end
	return ("%dB"):format(n)
end

local function fmtMs(ms)
	if ms >= 1000 then
		return ("%.2fs"):format(ms / 1000)
	end
	return ("%.0fms"):format(ms)
end

local function offHint()
	if not ns.debugEnabled("stats") then
		ns.Print("|cffff5555(stats collection is off — '/frp debug stats' to enable)|r")
	end
end

local SECTIONS = {}

local function printSummary()
	local s = ns.Stats.snapshot()
	ns.Print(("stats | uptime %s | in %d msgs (%s, %.2f/s) | out %d msgs (%s, %.2f/s) | legs %d"):format(
		("%ds"):format(s.uptime), s.inMsgs, fmtBytes(s.inBytes), s.rateIn,
		s.outMsgs, fmtBytes(s.outBytes), s.rateOut, s.outSends
	))
	ns.Print(("  sizes in avg/med %s/%s | out avg/med %s/%s | assemble med/max %s/%s | transmit med/max %s/%s"):format(
		fmtBytes(s.avgInSize), fmtBytes(s.medInSize),
		fmtBytes(s.avgOutSize), fmtBytes(s.medOutSize),
		fmtMs(s.medAssembleMs), fmtMs(s.maxAssembleMs),
		fmtMs(s.medTransmitMs), fmtMs(s.maxTransmitMs)
	))
	ns.Print(("  peers %d senders / %d subjects | incomplete %d | dup chunks %d | queue hw %d"):format(
		s.senderN, s.subjectN, s.incomplete, s.dupChunks, s.queue.highWater
	))
	ns.Print("  sections: mediums sizes timing cache errors queue last [n] | 'reset' zeroes")
end

function SECTIONS.mediums()
	local s = ns.Stats.snapshot()
	ns.Print("mediums in:")
	for m, v in pairs(s.mediumsIn) do
		ns.Print(("  %s: %d msgs, %d chunks, %s"):format(m, v.msgs, v.chunks, fmtBytes(v.bytes)))
	end
	ns.Print("mediums out:")
	for m, v in pairs(s.mediumsOut) do
		ns.Print(("  %s: %d msgs, %d chunks, %s"):format(m, v.msgs, v.chunks, fmtBytes(v.bytes)))
	end
	ns.Print("legs:")
	for leg, n in pairs(s.legs) do
		ns.Print(("  %s: %d"):format(leg, n))
	end
end

function SECTIONS.sizes()
	local s = ns.Stats.snapshot()
	ns.Print(("messages in:  avg %s | median %s | n=%d"):format(fmtBytes(s.avgInSize), fmtBytes(s.medInSize), s.inMsgs))
	ns.Print(("messages out: avg %s | median %s | n=%d"):format(fmtBytes(s.avgOutSize), fmtBytes(s.medOutSize), s.outMsgs))
	ns.Print(("chunks in:  median %s | n=%d | wire %s"):format(fmtBytes(s.medInChunk), s.inChunks, fmtBytes(s.inWireBytes)))
	ns.Print(("chunks out: median %s | n=%d | wire %s"):format(fmtBytes(s.medOutChunk), s.outChunks, fmtBytes(s.outWireBytes)))
end

function SECTIONS.timing()
	local s = ns.Stats.snapshot()
	ns.Print(("reassembly (first->last chunk): median %s | max %s"):format(fmtMs(s.medAssembleMs), fmtMs(s.maxAssembleMs)))
	ns.Print(("transmit (enqueue->last sent, per leg): median %s | max %s"):format(fmtMs(s.medTransmitMs), fmtMs(s.maxTransmitMs)))
	ns.Print(("origin age on receipt (approx, sender clock): median %s"):format(
		s.medOriginAge > 0 and ("%.0fs"):format(s.medOriginAge) or "n/a"))
	ns.Print(("queue dwell per chunk: avg %s | max %s"):format(
		s.queue.dwellN > 0 and fmtMs(s.queue.dwellSum / s.queue.dwellN * 1000) or "n/a",
		fmtMs(s.queue.dwellMax * 1000)))
end

function SECTIONS.cache()
	local c = ns.Stats.snapshot().cache
	ns.Print(("cache outcomes: new %d | updated %d | refreshed %d | rejected %d"):format(
		c.new or 0, c.updated or 0, c.refreshed or 0, c.rejected or 0))
end

function SECTIONS.errors()
	local e = ns.Stats.snapshot().errors
	local any = false
	for kind, n in pairs(e) do
		ns.Print(("  %s: %d"):format(kind, n))
		any = true
	end
	if not any then
		ns.Print("errors: none recorded")
	end
end

function SECTIONS.queue()
	local s = ns.Stats.snapshot()
	ns.Print(("send queue: depth %d | high-water %d | dwell avg %s max %s"):format(
		ns.comms and #ns.comms.queue or 0, s.queue.highWater,
		s.queue.dwellN > 0 and fmtMs(s.queue.dwellSum / s.queue.dwellN * 1000) or "n/a",
		fmtMs(s.queue.dwellMax * 1000)))
	for name, depth in pairs(s.queues) do
		ns.Print(("  %s queue high-water: %d"):format(name, depth))
	end
end

local function printLast(n)
	local r = ns.stats.recent
	local total = r.n
	if total == 0 then
		ns.Print("no recent messages recorded")
		return
	end
	n = math.min(n or 10, total)
	ns.Print(("last %d message(s):"):format(n))
	-- Walk the ring backwards from the newest entry.
	for k = 0, n - 1 do
		local idx = ((r.i - 1 - k) % r.cap) + 1
		local e = r[idx]
		if e then
			local arrow = e.dir == "in" and "|cff66ccff<-|r" or "|cffffd200->|r"
			local extra = e.dir == "in" and (e.subject and (" subject " .. e.subject) or "") or ""
			ns.Print(("  %s %s %s%s: %s, %d chunks, %s"):format(
				arrow, tostring(e.medium), tostring(e.peer or "?"), extra,
				fmtBytes(e.bytes), e.chunks, fmtMs(e.ms)))
		end
	end
end

ns.RegisterCommand("stats", function(arg)
	arg = arg and ns.Util.trim(arg) or ""
	offHint()
	if arg == "reset" then
		resetStore()
		ns.Print("stats reset")
		return
	end
	local section, rest = arg:match("^(%S*)%s*(.-)$")
	if section == "last" then
		printLast(tonumber(rest))
		return
	end
	if section ~= "" and SECTIONS[section] then
		SECTIONS[section]()
		return
	end
	if section ~= "" then
		ns.Print("unknown stats section '" .. section .. "'")
	end
	printSummary()
end, "Show traffic statistics; sections: mediums sizes timing cache errors queue last [n] | 'reset'")
