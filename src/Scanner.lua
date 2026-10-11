-- Scanner.lua
--
-- Watches for player "selection" (mouseover / target) and originates a
-- broadcast of the selected player's profile. We only originate for units the
-- RP AddOn has *authoritative* data for: the local player and same-faction
-- players. Opposite-faction units are display-only (see Tooltip).
--
-- Because the RP AddOn fetches a freshly-seen same-faction profile
-- asynchronously, we also listen for the adapter's "profile updated" signal and
-- originate when the data actually lands.

local _, ns = ...
local C = ns.Const

local Scanner = ns.Class("Scanner")
ns.Scanner = Scanner

function Scanner:init()
	-- Units the user has selected this session that we may originate for.
	-- [unitID] = guid. Only ever holds same-faction units; self is handled
	-- explicitly via the adapter's player unit id.
	self.selected = {}

	-- Profiles reported as received over the RP AddOn's own comm channels,
	-- queued for re-broadcast. receivedPending maps unitID -> { adapter,
	-- readyAt }; receivedQueue is the FIFO order, capped so sustained receipt
	-- bursts cannot flood the send queue.
	self.receivedPending = {}
	self.receivedQueue = {}
end

function Scanner:start()
	-- React when an adapter reports a profile changed/arrived, or profile
	-- data was received over the AddOn's own comm channels.
	for _, adapter in pairs(ns.adapters) do
		adapter:registerUpdateListener(function(unitID)
			self:onAdapterUpdate(adapter, unitID)
		end)
		adapter:registerReceivedListener(function(unitID)
			self:onAdapterReceived(adapter, unitID)
		end)
	end

	ns.RegisterEvent("UPDATE_MOUSEOVER_UNIT", function()
		self:onSelect("mouseover")
	end)
	ns.RegisterEvent("PLAYER_TARGET_CHANGED", function()
		self:onSelect("target")
	end)

	-- Share our own profile as soon as possible (and again whenever we edit it,
	-- via onAdapterUpdate's self handling).
	self:shareSelf()

	-- Drain received-profile re-broadcasts at a bounded rate so this trigger
	-- cannot, on its own, overwhelm the paced send queue.
	C_Timer.NewTicker(C.RECEIVED_ORIGIN_INTERVAL, function()
		self:drainReceived()
	end)
end

--- Attempt to broadcast the local player's own profile.
function Scanner:shareSelf()
	local adapter = ns.getAvailableAdapter()
	if not adapter then
		return
	end
	local unitID = adapter:getPlayerUnitID()
	local guid = UnitGUID("player")
	if unitID and guid then
		self:share(adapter, unitID, guid)
	end
end

--- Handle a mouseover/target change.
function Scanner:onSelect(unit)
	if not UnitIsPlayer(unit) or UnitIsUnit(unit, "player") then
		return -- non-players and ourselves are handled elsewhere
	end
	-- Opposite-faction players have no authoritative local data to originate;
	-- they are enriched for display only (Tooltip module).
	if ns.Util.isOppositeFaction(unit) then
		return
	end
	local adapter = ns.getAvailableAdapter()
	if not adapter then
		return
	end
	local unitID = adapter:getUnitID(unit)
	local guid = UnitGUID(unit)
	if not unitID or not guid then
		return
	end
	self.selected[unitID] = guid
	self:share(adapter, unitID, guid)
end

--- Handle an asynchronous profile update from an adapter.
function Scanner:onAdapterUpdate(adapter, unitID)
	local guid
	if unitID == adapter:getPlayerUnitID() then
		guid = UnitGUID("player") -- our own profile changed
	else
		guid = self.selected[unitID] -- only previously-selected units
	end
	if guid then
		self:share(adapter, unitID, guid)
	end
end

--- Handle an adapter's "profile data received over the network" signal.
-- Receipts fire per section while data is still landing, so rather than
-- sharing immediately we queue the unit with a jittered ready time: this
-- collapses the burst into one broadcast AND staggers simultaneous
-- originators, letting the first peer copy to reach us suppress ours.
function Scanner:onAdapterReceived(adapter, unitID)
	if not unitID or unitID == adapter:getPlayerUnitID() then
		return -- never re-broadcast ourselves
	end

	local readyAt = GetTime() + C.RECEIVED_DEBOUNCE + math.random() * C.RECEIVED_JITTER
	if self.receivedPending[unitID] then
		-- Re-arm on each section receipt.
		self.receivedPending[unitID].readyAt = readyAt
		return
	end

	self.receivedPending[unitID] = { adapter = adapter, readyAt = readyAt }
	self.receivedQueue[#self.receivedQueue + 1] = unitID
	if ns.debugReceived then
		ns.debugReceived(unitID)
	end

	-- Bound the queue; the oldest pending receipts are the least valuable.
	while #self.receivedQueue > C.RECEIVED_ORIGIN_QUEUE do
		local dropped = table.remove(self.receivedQueue, 1)
		self.receivedPending[dropped] = nil
		if ns.debugSkip then
			ns.debugSkip(dropped, "received queue full")
		end
	end
end

--- Fire at most one ready received-profile broadcast per tick.
function Scanner:drainReceived()
	local now = GetTime()
	local i = 1
	while i <= #self.receivedQueue do
		local unitID = self.receivedQueue[i]
		local pending = self.receivedPending[unitID]
		if not pending then
			-- Stale queue entry (shouldn't happen); drop it.
			table.remove(self.receivedQueue, i)
		elseif pending.readyAt <= now then
			table.remove(self.receivedQueue, i)
			self.receivedPending[unitID] = nil

			local adapter = pending.adapter
			local guid = adapter and adapter:isAvailable() and ns.Util.resolveGUID(unitID)
			if guid then
				self:share(adapter, unitID, guid)
			elseif ns.debugSkip then
				ns.debugSkip(unitID, "could not resolve GUID")
			end
			return -- at most one per tick
		else
			i = i + 1
		end
	end
end

--- Extract the profile and hand it to the broadcaster (which de-dups).
function Scanner:share(adapter, unitID, guid)
	local profile = adapter:extract(unitID)
	if profile then
		ns.broadcast:originate(guid, unitID, profile)
	end
end

-- Singleton.
ns.scanner = Scanner:new()
ns.OnLogin(function()
	ns.scanner:start()
end)
