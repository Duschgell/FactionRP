-- Util.lua
--
-- Small, pure helper functions shared across modules. Nothing here touches
-- AddOn state; everything is a stateless utility.

local _, ns = ...
local C = ns.Const

local Util = {}
ns.Util = Util

--- Split a string on a literal (non-pattern) separator.
-- The optional `maxFields` caps the number of returned fields: once that many
-- have been produced, the entire remaining string (including any further
-- separators) becomes the final field. This is important for our wire format,
-- where the trailing PROFILE-DATA field may itself legitimately contain "||".
-- @param input     string  The string to split.
-- @param sep       string  Literal separator (treated verbatim, not as a pattern).
-- @param maxFields number?  Optional maximum number of fields to produce.
-- @return table A list of string fields.
function Util.split(input, sep, maxFields)
	local fields = {}
	local start = 1
	while true do
		-- If we are one field short of the cap, the rest is the last field.
		if maxFields and #fields == maxFields - 1 then
			fields[#fields + 1] = input:sub(start)
			break
		end
		local s, e = input:find(sep, start, true) -- plain = true: literal match
		if not s then
			fields[#fields + 1] = input:sub(start)
			break
		end
		fields[#fields + 1] = input:sub(start, s - 1)
		start = e + 1
	end
	return fields
end

--- Join a list of values into a string using the wire separator.
-- @param parts table A list of values (converted with tostring).
-- @return string
function Util.join(parts)
	return table.concat(parts, C.SEPARATOR)
end

--- Trim leading/trailing whitespace.
-- @param s string
-- @return string
function Util.trim(s)
	return (s:gsub("^%s*(.-)%s*$", "%1"))
end

--- Build the "Name-Realm" identifier for a unit, matching the convention used
-- by RP AddOns for keying their character store.
--
-- NOTE: realm names are normalized by stripping spaces (e.g. "Argent Dawn" ->
-- "ArgentDawn"). The TRP3 adapter prefers TRP3's own unit-id helper when
-- available (see TRP3Adapter) so register keys match exactly; this generic
-- version is the fallback used for our own bookkeeping.
-- @param unit string A unit token (e.g. "mouseover", "target", "player").
-- @return string|nil "Name-Realm", or nil if the unit is not a player.
function Util.getUnitID(unit)
	if not UnitIsPlayer(unit) then
		return nil
	end
	local name, realm = UnitName(unit)
	if not name or name == "" then
		return nil
	end
	if not realm or realm == "" then
		realm = GetNormalizedRealmName()
	else
		realm = realm:gsub("%s+", "")
	end
	return name .. "-" .. realm
end

--- Whether `unit` is a player belonging to the faction opposite the viewer.
-- @param unit string A unit token.
-- @return boolean
function Util.isOppositeFaction(unit)
	if not UnitIsPlayer(unit) then
		return false
	end
	local theirs = UnitFactionGroup(unit)
	local mine = UnitFactionGroup("player")
	-- UnitFactionGroup returns "Alliance"/"Horde"/"Neutral"/nil. Treat any
	-- mismatch between two known, differing groups as opposite faction.
	return theirs ~= nil and mine ~= nil and theirs ~= mine
end

-- GUID resolution -----------------------------------------------------------
--
-- The wire format and cache key profiles by the subject's real GUID, but
-- profiles received over the RP AddOn's comm channels arrive keyed only by
-- "Name-Realm". resolveGUID is a best-effort, name-based lookup over every
-- source that can map a character name to a GUID: nearby unit tokens,
-- Battle.net friends, and WoW community/club rosters.

-- Unit tokens that can refer to other players near enough to have a GUID.
local GUID_UNIT_TOKENS = { "target", "targettarget", "mouseover", "focus", "softenemy" }
for i = 1, MAX_PARTY_MEMBERS or 4 do
	GUID_UNIT_TOKENS[#GUID_UNIT_TOKENS + 1] = "party" .. i
end
for i = 1, MAX_RAID_MEMBERS or 40 do
	GUID_UNIT_TOKENS[#GUID_UNIT_TOKENS + 1] = "raid" .. i
end
for i = 1, 40 do
	GUID_UNIT_TOKENS[#GUID_UNIT_TOKENS + 1] = "nameplate" .. i
end
for i = 1, 5 do
	GUID_UNIT_TOKENS[#GUID_UNIT_TOKENS + 1] = "arena" .. i
end

--- Normalize a character name to "Name-Realm" for comparison: strips spaces
-- from the realm (matching getUnitID) and defaults a missing realm to the
-- local realm.
-- @param name string "Name" or "Name-Realm".
-- @return string|nil
local function normalizeUnitName(name)
	if not name or name == "" then
		return nil
	end
	local char, realm = name:match("^([^-]+)-?(.*)$")
	if not char or char == "" then
		return nil
	end
	if not realm or realm == "" then
		realm = GetNormalizedRealmName() or ""
	else
		realm = realm:gsub("%s+", "")
	end
	return (char .. "-" .. realm):lower()
end

local function guidFromUnits(unitID)
	for _, token in ipairs(GUID_UNIT_TOKENS) do
		-- Unit APIs may yield secret values during lockdown; skip those.
		local ok, guid = pcall(UnitGUID, token)
		if ok and guid and not issecretvalue(guid) then
			local okID, id = pcall(Util.getUnitID, token)
			if okID and id and id:lower() == unitID then
				return guid
			end
		end
	end
end

local function guidFromBNetFriends(unitID)
	if BNFeaturesEnabledAndConnected and not BNFeaturesEnabledAndConnected() then
		return
	end
	if not C_BattleNet then
		return
	end
	local numFriends = BNGetNumFriends() or 0
	for i = 1, numFriends do
		local numAccounts = C_BattleNet.GetFriendNumGameAccounts(i) or 0
		for j = 1, numAccounts do
			local acct = C_BattleNet.GetFriendGameAccountInfo(i, j)
			if acct
				and acct.clientProgram == BNET_CLIENT_WOW
				and acct.characterName
				and acct.realmName
				and acct.playerGuid
				and normalizeUnitName(acct.characterName .. "-" .. acct.realmName) == unitID
			then
				return acct.playerGuid
			end
		end
	end
end

local function guidFromClubs(unitID)
	if not C_Club or not C_Club.GetSubscribedClubs or not C_Club.GetClubMembers then
		return
	end
	-- Club APIs can fail before clubs are initialized or yield secrets during
	-- lockdown, so each call is guarded.
	local ok, clubs = pcall(C_Club.GetSubscribedClubs)
	if not ok or type(clubs) ~= "table" then
		return
	end
	for _, club in ipairs(clubs) do
		local okMembers, members = pcall(C_Club.GetClubMembers, club.clubId)
		if okMembers and type(members) == "table" then
			for _, memberId in ipairs(members) do
				local okInfo, info = pcall(C_Club.GetMemberInfo, club.clubId, memberId)
				if okInfo
					and info
					and info.guid
					and canaccessvalue(info.guid)
					and info.name
					and normalizeUnitName(info.name) == unitID
				then
					return info.guid
				end
			end
		end
	end
end

local guidMemo = {} -- [lowered unitID] = { guid = guid|false, expires = GetTime() }
local memoPruneCounter = 0

--- Best-effort resolution of a "Name-Realm" unitID to a real player GUID.
-- Results (including misses) are memoized for C.GUID_RESOLVE_MEMO seconds.
-- @param unitID string "Name-Realm".
-- @return string|nil The GUID, or nil if it cannot be resolved.
function Util.resolveGUID(unitID)
	if not unitID then
		return nil
	end
	local key = unitID:lower()
	local now = GetTime()
	local memo = guidMemo[key]
	if memo and memo.expires > now then
		return memo.guid or nil
	end

	local guid = guidFromUnits(key)
		or guidFromBNetFriends(key)
		or guidFromClubs(key)

	-- Periodically prune expired memos so the table cannot grow unbounded
	-- over a long session.
	memoPruneCounter = memoPruneCounter + 1
	if memoPruneCounter >= 50 then
		memoPruneCounter = 0
		for k, m in pairs(guidMemo) do
			if m.expires <= now then
				guidMemo[k] = nil
			end
		end
	end

	guidMemo[key] = { guid = guid or false, expires = now + C.GUID_RESOLVE_MEMO }
	return guid
end
