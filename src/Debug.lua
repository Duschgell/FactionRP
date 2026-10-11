-- Debug.lua
--
-- Optional verbose logging, toggled with "/factionrp debug". When enabled, the
-- four lifecycle events called out in the README are printed: a broadcast sent
-- (with subject GUID), a message received (with sender GUID), and a profile
-- stored or refreshed in the cache.
--
-- The logging entry points (ns.debugBroadcast / ns.debugReceive / ns.debugStore /
-- ns.debugReceived / ns.debugSkip / ns.debugFanout) are called from Broadcast and
-- Scanner behind nil-guards, so this module is purely additive.

local _, ns = ...
local C = ns.Const

local function enabled()
	return ns.db and ns.db.debug
end

--- Log: a broadcast was sent for the given subject.
function ns.debugBroadcast(subjectGUID)
	if not enabled() then
		return
	end
	ns.Print("|cffffd200[broadcast]|r sent profile for " .. tostring(subjectGUID))
end

--- Log: a message was received from the given sender (about a subject).
function ns.debugReceive(senderGUID, subjectGUID)
	if not enabled() then
		return
	end
	ns.Print(("|cff66ccff[received]|r from %s (subject %s)"):format(tostring(senderGUID), tostring(subjectGUID)))
end

--- Log: the cache stored/updated/refreshed an entry. Mirrors Cache.RESULT.
function ns.debugStore(result, guid)
	if not enabled() then
		return
	end
	local R = ns.Cache.RESULT
	if result == R.NEW then
		ns.Print("|cff33ff66[cache]|r stored " .. tostring(guid))
	elseif result == R.UPDATED then
		ns.Print("|cff33ff66[cache]|r updated " .. tostring(guid))
	elseif result == R.REFRESHED then
		ns.Print("|cff999999[cache]|r refreshed " .. tostring(guid))
	end
	-- REJECTED (stale) entries are intentionally not logged to avoid noise.
end

--- Log: the RP AddOn reported a network-received profile; it was queued for
--- re-broadcast after the debounce/jitter delay.
function ns.debugReceived(unitID)
	if not enabled() then
		return
	end
	ns.Print("|cff66ccff[received]|r profile data for " .. tostring(unitID) .. " (queued)")
end

--- Log: a pending re-broadcast was skipped (unresolvable GUID, queue drop).
function ns.debugSkip(unitID, reason)
	if not enabled() then
		return
	end
	ns.Print(("|cffffd200[skip]|r %s (%s)"):format(tostring(unitID), tostring(reason)))
end

--- Log: a fanout sent the given number of cross-faction community whispers.
function ns.debugFanout(targets)
	if not enabled() then
		return
	end
	ns.Print("|cffffd200[broadcast]|r community whispers: " .. tostring(targets))
end

local function mapSize(t)
	local n = 0
	for _ in pairs(t) do
		n = n + 1
	end
	return n
end

-- /factionrp debug ----------------------------------------------------------

ns.RegisterCommand("debug", function()
	if not ns.db then
		return
	end
	ns.db.debug = not ns.db.debug
	ns.Print("debug mode " .. (ns.db.debug and "|cff33ff66ON|r" or "|cffff5555OFF|r"))
end, "Toggle verbose debug output")

-- Richer status command (overrides the placeholder registered in Core).
ns.RegisterCommand("status", function()
	local adapter = ns.getAvailableAdapter()
	local relays = ns.broadcast and mapSize(ns.broadcast.knownRelays) or 0
	local contacts = ns.broadcast and mapSize(ns.broadcast.xfacContacts) or 0
	ns.Print(("version %s | cache: %d | pending: %d | send queue: %d | relays: %d | contacts: %d | RP adapter: %s | debug: %s"):format(
		C.VERSION,
		ns.cache and ns.cache:count() or 0,
		ns.scanner and #ns.scanner.receivedQueue or 0,
		ns.comms and #ns.comms.queue or 0,
		relays,
		contacts,
		adapter and adapter.name or "|cffff5555none|r",
		(ns.db and ns.db.debug) and "on" or "off"
	))
end, "Show version, cache size, pending sends, adapter and debug state")
