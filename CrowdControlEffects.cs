using System;
using System.Collections.Generic;
using System.Reflection;
using Newtonsoft.Json.Linq;
using Oxide.Core;
using Oxide.Core.Plugins;
using UnityEngine;

namespace Oxide.Plugins
{
    [Info("CrowdControlEffects", "jaku", "0.1.0")]
    [Description("Built-in Crowd Control Rust effect provider.")]
    public class CrowdControlEffects : RustPlugin
    {
        [PluginReference]
        private Plugin CrowdControl;

        private const string ProviderName = "CrowdControlEffects";
        private readonly Dictionary<string, TimedEffectState> _activeTimedEffects = new Dictionary<string, TimedEffectState>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, MovementFreezeState> _movementFreezeTimers = new Dictionary<string, MovementFreezeState>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, Oxide.Plugins.Timer> _activeHandcuffTimers = new Dictionary<string, Oxide.Plugins.Timer>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, Oxide.Plugins.Timer> _activePowerModeTimers = new Dictionary<string, Oxide.Plugins.Timer>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, Oxide.Plugins.Timer> _activeBurnTimers = new Dictionary<string, Oxide.Plugins.Timer>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _godModeSteamIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _flyModeSteamIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, Vector3> _lastDeathPositionBySteamId = new Dictionary<string, Vector3>(StringComparer.OrdinalIgnoreCase);
        private Oxide.Plugins.Timer _registerEffectsRetryTimer;
        private const bool VerboseLogging = true;

        private sealed class TimedEffectState
        {
            public string RequestId;
            public string EffectId;
            public string SteamId;
            public Oxide.Plugins.Timer TickTimer;
            public Oxide.Plugins.Timer EndTimer;
        }

        private sealed class MovementFreezeState
        {
            public Vector3 AnchorPosition;
            public Oxide.Plugins.Timer EnforceTimer;
            public Oxide.Plugins.Timer EndTimer;
        }

        private void OnServerInitialized()
        {
            RegisterBuiltInEffects();
            UpdateTeleportAvailability();
        }

        private void Unload()
        {
            _registerEffectsRetryTimer?.Destroy();
            _registerEffectsRetryTimer = null;
            StopAllTimedEffects("Built-in effect provider unloaded.");
            ClearAllGameplayState();
            CrowdControl?.Call("CC_UnregisterEffects", ProviderName);
        }

        private void OnPluginLoaded(Plugin plugin)
        {
            if (plugin == null)
            {
                return;
            }

            if (string.Equals(plugin.Name, "CrowdControl", StringComparison.OrdinalIgnoreCase))
            {
                RegisterBuiltInEffects();
                UpdateTeleportAvailability();
            }
        }

        private void OnCrowdControlSessionsChanged()
        {
            UpdateTeleportAvailability();
        }

        private void OnPlayerDisconnected(BasePlayer player, string reason)
        {
            if (player == null)
            {
                return;
            }

            StopTimedEffectsForSteamId(player.UserIDString, "Player disconnected.");
            ClearMovementFreeze(player);
            EndHandcuffEffect(player.UserIDString);
            EndTemporaryPowerMode(player.UserIDString, restoreFly: false, showUi: false);
            EndBurnEffect(player.UserIDString);
            UpdateTeleportAvailability();
        }

        private void OnPlayerDeath(BasePlayer player, HitInfo info)
        {
            if (player == null)
            {
                return;
            }

            _lastDeathPositionBySteamId[player.UserIDString] = player.transform.position;
        }

        private object OnEntityTakeDamage(BaseCombatEntity entity, HitInfo hitInfo)
        {
            var player = entity as BasePlayer;
            if (player == null || hitInfo == null)
            {
                return null;
            }

            if (!_godModeSteamIds.Contains(player.UserIDString))
            {
                return null;
            }

            hitInfo.damageTypes?.ScaleAll(0f);
            return null;
        }

        private void RegisterBuiltInEffects()
        {
            if (CrowdControl == null)
            {
                PrintWarning("CrowdControl plugin not loaded; cannot register built-in effects yet.");
                QueueRegisterBuiltInEffectsRetry();
                return;
            }

            var effects = LoadBuiltInEffects();
            if (effects.Count == 0)
            {
                PrintWarning("No built-in Crowd Control effects are declared in code.");
                return;
            }

            var registerResult = CrowdControl.Call("CC_RegisterLocalEffects", ProviderName, effects);
            var ok = registerResult is bool registered && registered;
            Puts($"Built-in local effect registration result: {(ok ? "success" : "failed")} ({effects.Count} effect(s)).");
            if (!ok)
            {
                QueueRegisterBuiltInEffectsRetry();
                return;
            }

            _registerEffectsRetryTimer?.Destroy();
            _registerEffectsRetryTimer = null;
        }

        private void QueueRegisterBuiltInEffectsRetry()
        {
            if (_registerEffectsRetryTimer != null && !_registerEffectsRetryTimer.Destroyed)
            {
                return;
            }

            _registerEffectsRetryTimer = timer.Once(3f, () =>
            {
                _registerEffectsRetryTimer = null;
                RegisterBuiltInEffects();
                UpdateTeleportAvailability();
            });
        }

        private JArray LoadBuiltInEffects()
        {
            var results = new JArray();
            AddBuiltInEffect(results, "player_kill", "Player Kill", "Instantly kill the player.", 300);
            AddBuiltInEffect(results, "player_hunger_strike", "Player Hunger Strike", "Set hunger and hydration to zero.", 120);
            AddBuiltInEffect(results, "player_fill_hunger", "Fill Hunger", "Restore hunger and hydration.", 90);
            AddBuiltInEffect(results, "player_full_heal", "Full Heal", "Fully heal the player and clear bleeding.", 120);
            AddBuiltInEffect(results, "give_fuel", "Give Fuel", "Give low grade fuel to the player.", 90);
            AddBuiltInEffect(results, "give_hazmat", "Give Hazmat", "Give a hazmat suit.", 160);
            AddBuiltInEffect(results, "give_armor_kit", "Give Armor Kit", "Give an armor clothing set.", 180);
            AddBuiltInEffect(results, "player_strip_armor", "Strip Armor", "Remove worn armor/clothing.", 150);
            AddBuiltInEffect(results, "player_break_armor", "Break Armor", "Destroy armor durability.", 170);
            AddBuiltInEffect(results, "world_set_day", "Set Day", "Set world time to day.", 140);
            AddBuiltInEffect(results, "world_set_night", "Set Night", "Set world time to night.", 140);
            AddBuiltInEffect(results, "player_hurt", "Player Hurt", "Deal light damage to the player.", 80);
            AddBuiltInEffect(results, "player_handcuff", "Handcuff Player", "Immobilize the player briefly.", 180, "0:0:12.0");
            AddBuiltInEffect(results, "player_fire", "Player Fire", "Set the player on fire briefly.", 220);
            AddBuiltInEffect(results, "player_heal", "Player Heal", "Heal a small amount.", 70);
            AddBuiltInEffect(results, "player_drop_item", "Player Drop Item", "Drop active inventory item.", 110);
            AddBuiltInEffect(results, "player_drop_some", "Player Drop Some", "Drop roughly half of inventory items.", 170);
            AddBuiltInEffect(results, "player_drop_all", "Player Drop All", "Drop all inventory items.", 320);
            AddBuiltInEffect(results, "player_unload_ammo", "Player Unload Ammo", "Unload active weapon ammo.", 110);
            AddBuiltInEffect(results, "give_item_wood", "Give Wood", "Give wood resources.", 60);
            AddBuiltInEffect(results, "give_item_stone", "Give Stone", "Give stone resources.", 60);
            AddBuiltInEffect(results, "give_item_metal_fragments", "Give Metal Fragments", "Give metal fragments.", 80);
            AddBuiltInEffect(results, "give_item_sulfur_ore", "Give Sulfur Ore", "Give sulfur ore.", 90);
            AddBuiltInEffect(results, "give_torch", "Give Torch", "Give a torch.", 40);
            AddBuiltInEffect(results, "give_rock", "Give Rock", "Give a rock.", 40);
            AddBuiltInEffect(results, "give_sleeping_bag", "Give Sleeping Bag", "Give a sleeping bag item.", 120);
            AddBuiltInEffect(results, "player_drop_hotbar_item", "Player Drop Hotbar Item", "Drop held hotbar item.", 120);
            AddBuiltInEffect(results, "give_scrap_bonus", "Give Scrap Bonus", "Give scrap to the player.", 100);
            AddBuiltInEffect(results, "player_scrap_tax", "Player Scrap Tax", "Take scrap from the player.", 110);
            AddBuiltInEffect(results, "player_remove_med_items", "Player Remove Med Items", "Remove medical items from inventory.", 130);
            AddBuiltInEffect(results, "give_weapon_revolver", "Give Revolver", "Give revolver with ammo.", 120);
            AddBuiltInEffect(results, "give_bandage", "Give Bandage", "Give bandages to the player.", 40);
            AddBuiltInEffect(results, "give_syringe", "Give Syringe", "Give medical syringes to the player.", 70);
            AddBuiltInEffect(results, "give_large_medkit", "Give Large Medkit", "Give a large medkit to the player.", 100);
            AddBuiltInEffect(results, "give_weapon_pumpshotgun", "Give Pump Shotgun", "Give pump shotgun with ammo.", 180);
            AddBuiltInEffect(results, "give_weapon_ak", "Give AK", "Give AK with ammo.", 350);
            AddBuiltInEffect(results, "give_weapon_thompson", "Give Thompson", "Give Thompson with ammo.", 220);
            AddBuiltInEffect(results, "give_weapon_rpg", "Give RPG", "Give RPG with rockets.", 420);
            AddBuiltInEffect(results, "give_weapon_grenade_f1", "Give F1 Grenades", "Give F1 grenades.", 160);
            AddBuiltInEffect(results, "give_weapon_grenade_beancan", "Give Beancan Grenades", "Give beancan grenades.", 150);
            AddBuiltInEffect(results, "give_weapon_mgl", "Give MGL", "Give MGL with 40mm ammo.", 420);
            AddBuiltInEffect(results, "give_explosive_satchel", "Give Satchel Charges", "Give satchel charges.", 320);
            AddBuiltInEffect(results, "give_explosive_timed", "Give C4 (Timed Explosive)", "Give timed explosive charges (C4).", 650);
            AddBuiltInEffect(results, "give_ammo_pistol", "Give Pistol Ammo", "Give pistol ammo.", 70);
            AddBuiltInEffect(results, "give_ammo_pistol_hv", "Give Pistol HV Ammo", "Give high velocity pistol ammo.", 95);
            AddBuiltInEffect(results, "give_ammo_pistol_incendiary", "Give Pistol Incendiary Ammo", "Give incendiary pistol ammo.", 120);
            AddBuiltInEffect(results, "give_ammo_rifle", "Give Rifle Ammo", "Give rifle ammo.", 110);
            AddBuiltInEffect(results, "give_ammo_rifle_hv", "Give Rifle HV Ammo", "Give high velocity rifle ammo.", 140);
            AddBuiltInEffect(results, "give_ammo_rifle_incendiary", "Give Rifle Incendiary Ammo", "Give incendiary rifle ammo.", 170);
            AddBuiltInEffect(results, "give_ammo_shotgun_buckshot", "Give Buckshot Ammo", "Give shotgun buckshot ammo.", 90);
            AddBuiltInEffect(results, "give_ammo_shotgun_slug", "Give Shotgun Slug Ammo", "Give shotgun slug ammo.", 110);
            AddBuiltInEffect(results, "give_ammo_shotgun_incendiary", "Give Shotgun Incendiary Ammo", "Give incendiary shotgun ammo.", 135);
            AddBuiltInEffect(results, "give_ammo_rockets", "Give Rockets", "Give basic rockets.", 220);
            AddBuiltInEffect(results, "give_airdrop_signal", "Give Airdrop Signal", "Give a supply signal.", 180);
            AddBuiltInEffect(results, "spawn_minicopter", "Spawn Minicopter", "Spawn a minicopter nearby.", 250);
            AddBuiltInEffect(results, "spawn_supply_drop", "Spawn Supply Drop", "Spawn a supply drop nearby.", 240);
            AddBuiltInEffect(results, "spawn_attack_helicopter", "Spawn Attack Helicopter", "Spawn an attack helicopter nearby.", 500);
            AddBuiltInEffect(results, "spawn_nodes", "Spawn Nodes", "Spawn random ore nodes around the player on safe ground.", 260);
            AddBuiltInEffect(results, "spawn_nodes_stone", "Spawn Stone Nodes", "Spawn stone ore nodes around the player on safe ground.", 240);
            AddBuiltInEffect(results, "spawn_nodes_metal", "Spawn Metal Nodes", "Spawn metal ore nodes around the player on safe ground.", 280);
            AddBuiltInEffect(results, "spawn_nodes_sulfur", "Spawn Sulfur Nodes", "Spawn sulfur ore nodes around the player on safe ground.", 320);
            AddBuiltInEffect(results, "spawn_sleeping_bag_here", "Spawn Sleeping Bag Here", "Force-place a sleeping bag at the player's current position.", 180);
            AddBuiltInEffect(results, "test_hype_train", "TEST: Hype Train", "Test effect: spawn a short-lived hype train with stub rider names and sound.", 25);
            AddBuiltInEffect(results, "player_teleport_to_sleeping_bag", "Teleport To Sleeping Bag", "Teleport player to one of their sleeping bags.", 220);
            AddBuiltInEffect(results, "player_teleport_to_cc_player", "Teleport To CC Player", "Teleport to another active CC player.", 160);
            AddBuiltInEffect(results, "player_teleport_swap_cc_player", "Teleport Swap CC Player", "Swap position with another active CC player.", 180);
            AddBuiltInEffect(results, "player_reload_active_weapon", "Reload Active Weapon", "Reload the active weapon.", 90);
            AddBuiltInEffect(results, "player_drain_active_weapon_ammo", "Drain Active Weapon Ammo", "Drain ammo from active weapon.", 120);
            AddBuiltInEffect(results, "player_bleed", "Player Bleed", "Increase player bleeding.", 130);
            AddBuiltInEffect(results, "player_fracture", "Player Fracture", "Apply fracture-like penalty.", 140);
            AddBuiltInEffect(results, "player_freeze_short", "Player Freeze Short", "Freeze movement briefly.", 150);
            AddBuiltInEffect(results, "player_god_mode_15s", "Player God Mode (15s)", "Temporary god mode for 15 seconds.", 260, "0:0:15.0");
            AddBuiltInEffect(results, "player_fly_mode_15s", "Player Fly Mode (15s)", "Temporary fly mode for 15 seconds.", 260, "0:0:15.0");
            AddBuiltInEffect(results, "player_admin_power_15s", "Player Admin Power (15s)", "Temporary god+fly for 15 seconds.", 400, "0:0:15.0");
            AddBuiltInEffect(results, "player_revive", "Player Revive", "Revive player at last death location.", 260);
            AddBuiltInEffect(results, "player_damage_over_time", "Player Damage Over Time", "Apply timed damage-over-time.", 170, "0:0:15.0");
            AddBuiltInEffect(results, "player_heal_over_time", "Player Heal Over Time", "Apply timed healing-over-time.", 170, "0:0:15.0");
            AddBuiltInEffect(results, "spawn_testridablehorse", "Spawn Horse", "Spawn a ridable horse near player.", 180);
            AddBuiltInEffect(results, "spawn_boar", "Spawn Boar", "Spawn a boar near player.", 160);
            AddBuiltInEffect(results, "spawn_wolf", "Spawn Wolf", "Spawn a wolf near player.", 170);
            AddBuiltInEffect(results, "spawn_wolves", "Spawn Wolves", "Spawn a wolf pack nearby.", 260);
            AddBuiltInEffect(results, "spawn_bear", "Spawn Bear", "Spawn a bear near player.", 220);
            AddBuiltInEffect(results, "spawn_chicken", "Spawn Chicken", "Spawn a chicken near player.", 100);
            AddBuiltInEffect(results, "spawn_stag", "Spawn Stag", "Spawn a stag near player.", 150);
            AddBuiltInEffect(results, "spawn_scarecrow", "Spawn Zombie (Scarecrow)", "Spawn a scarecrow zombie near player.", 200);
            AddBuiltInEffect(results, "spawn_scarecrows", "Spawn Zombies (Scarecrows)", "Spawn a zombie pack near player.", 350);
            AddBuiltInEffect(results, "spawn_simpleshark", "Spawn Shark", "Spawn a shark near player.", 260);
            AddBuiltInEffect(results, "spawn_scientistnpc", "Spawn Scientist NPC", "Spawn a scientist NPC near player.", 260);
            AddBuiltInEffect(results, "spawn_pedalbike", "Spawn Pedal Bike", "Spawn a pedal bike near player.", 190);
            AddBuiltInEffect(results, "spawn_motorbike", "Spawn Motorbike", "Spawn a motorbike near player.", 220);
            AddBuiltInEffect(results, "spawn_vehicle_car_small", "Spawn Small Car", "Spawn a small drivable modular car.", 320);
            AddBuiltInEffect(results, "spawn_vehicle_car_large", "Spawn Large Car", "Spawn a large drivable modular car.", 380);
            AddBuiltInEffect(results, "spawn_vehicle_truck", "Spawn Truck", "Spawn a drivable truck.", 380);
            AddBuiltInEffect(results, "spawn_rowboat", "Spawn Rowboat", "Spawn a rowboat near player.", 260);
            AddBuiltInEffect(results, "spawn_rhib", "Spawn RHIB", "Spawn a RHIB near player.", 320);
            AddBuiltInEffect(results, "spawn_scraptransporthelicopter", "Spawn Scrap Transport Helicopter", "Spawn a scrap transport helicopter near player.", 500);
            return results;
        }

        private void AddBuiltInEffect(JArray results, string effectId, string name, string description, int price, string durationValue = null)
        {
            if (results == null || !IsSupportedBuiltInEffectId(effectId))
            {
                return;
            }

            var registerObj = new JObject
            {
                ["effectID"] = effectId,
                ["name"] = name,
                ["description"] = description,
                ["price"] = Math.Max(0, price),
                ["syncMenu"] = false
            };

            if (!string.IsNullOrWhiteSpace(durationValue))
            {
                registerObj["duration"] = new JObject
                {
                    ["value"] = durationValue
                };
            }

            results.Add(registerObj);
        }

        [HookMethod("OnCrowdControlEffect")]
        private object OnCrowdControlEffect(JObject context)
        {
            var effectId = (context?.Value<string>("effectID") ?? string.Empty).Trim().ToLowerInvariant();
            if (string.IsNullOrWhiteSpace(effectId))
            {
                return new JObject
                {
                    ["status"] = "failPermanent",
                    ["reason"] = "Missing effectID."
                };
            }

            if (IsTimedEffect(effectId))
            {
                return StartTimedEffect(context, effectId);
            }

            var playerSteamId = context?.Value<string>("playerSteamID") ?? string.Empty;
            var player = FindPlayerBySteamId(playerSteamId);
            var requestPayload = context?["payload"] as JObject;
            var effectPayload = context?["effect"] as JObject;
            if (TryApplyInstantEffect(player, effectId, requestPayload, effectPayload, out var error))
            {
                return new JObject
                {
                    ["status"] = "success"
                };
            }

            return new JObject
            {
                ["status"] = "failTemporary",
                ["reason"] = error ?? "Built-in effect failed."
            };
        }

        private object StartTimedEffect(JObject context, string effectId)
        {
            if (CrowdControl == null)
            {
                return new JObject
                {
                    ["status"] = "failTemporary",
                    ["reason"] = "CrowdControl base plugin is unavailable."
                };
            }

            var requestId = context?.Value<string>("requestID") ?? string.Empty;
            var steamId = context?.Value<string>("playerSteamID") ?? string.Empty;
            if (string.IsNullOrWhiteSpace(requestId) || string.IsNullOrWhiteSpace(steamId))
            {
                return new JObject
                {
                    ["status"] = "failTemporary",
                    ["reason"] = "Timed effect is missing request or player information."
                };
            }

            var durationSeconds = GetTimedEffectDurationSeconds(context?["effect"] as JObject, effectId);
            if (durationSeconds <= 0)
            {
                return new JObject
                {
                    ["status"] = "failTemporary",
                    ["reason"] = $"Timed effect '{effectId}' is missing a valid duration."
                };
            }

            var player = FindPlayerBySteamId(steamId);
            if (player == null || !player.IsConnected || player.IsDead())
            {
                return new JObject
                {
                    ["status"] = "failTemporary",
                    ["reason"] = "Timed effect requires a living connected player."
                };
            }

            StopTimedEffect(requestId, completeRequest: false, reason: string.Empty);
            StopTimedEffectsForPlayerEffect(steamId, effectId, completeRequest: false, reason: string.Empty);

            var state = new TimedEffectState
            {
                RequestId = requestId,
                EffectId = effectId,
                SteamId = steamId
            };

            if (!TryStartTimedEffectBehavior(state, player, durationSeconds, out var startError))
            {
                return new JObject
                {
                    ["status"] = "failTemporary",
                    ["reason"] = startError ?? $"Timed effect '{effectId}' failed to start."
                };
            }

            state.TickTimer = timer.Every(1f, () => ApplyTimedEffectTick(state));
            state.EndTimer = timer.Once(durationSeconds, () => StopTimedEffect(requestId, completeRequest: true, reason: string.Empty));
            _activeTimedEffects[requestId] = state;

            return new JObject
            {
                ["status"] = "timedBegin",
                ["timeRemainingMs"] = durationSeconds * 1000L
            };
        }

        private void ApplyTimedEffectTick(TimedEffectState state)
        {
            if (state == null)
            {
                return;
            }

            var player = FindPlayerBySteamId(state.SteamId);
            if (player == null || !player.IsConnected)
            {
                return;
            }

            switch (state.EffectId)
            {
                case "player_handcuff":
                    TrySetRestrainedStatus(player, true, out _);
                    break;
                case "player_damage_over_time":
                    player.Hurt(5f);
                    break;
                case "player_heal_over_time":
                    player.health = Mathf.Min(player.MaxHealth(), player.health + 5f);
                    player.SendNetworkUpdateImmediate();
                    break;
            }
        }

        private void StopTimedEffect(string requestId, bool completeRequest, string reason)
        {
            if (string.IsNullOrWhiteSpace(requestId))
            {
                return;
            }

            if (!_activeTimedEffects.TryGetValue(requestId, out var state))
            {
                return;
            }

            state.TickTimer?.Destroy();
            state.EndTimer?.Destroy();
            _activeTimedEffects.Remove(requestId);

            switch (state.EffectId)
            {
                case "player_handcuff":
                    EndHandcuffEffect(state.SteamId);
                    break;
            }

            if (completeRequest && CrowdControl != null)
            {
                CrowdControl.Call("CC_SendEffectResponse", requestId, "timedEnd", reason ?? string.Empty, string.Empty, null);
            }
        }

        private void StopAllTimedEffects(string reason)
        {
            var requestIds = new List<string>(_activeTimedEffects.Keys);
            foreach (var requestId in requestIds)
            {
                StopTimedEffect(requestId, completeRequest: true, reason: reason);
            }
        }

        private bool IsTimedEffect(string effectId)
        {
            return string.Equals(effectId, "player_handcuff", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(effectId, "player_damage_over_time", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(effectId, "player_heal_over_time", StringComparison.OrdinalIgnoreCase);
        }

        private bool TryStartTimedEffectBehavior(TimedEffectState state, BasePlayer player, int durationSeconds, out string error)
        {
            error = string.Empty;
            if (state == null || player == null)
            {
                error = "Timed effect state is invalid.";
                return false;
            }

            switch (state.EffectId)
            {
                case "player_handcuff":
                    return TryHandcuffPlayer(player, durationSeconds, manageLifetime: false, out error);
                case "player_damage_over_time":
                case "player_heal_over_time":
                    return true;
                default:
                    error = $"Unknown timed effectID '{state.EffectId}'.";
                    return false;
            }
        }

        private void StopTimedEffectsForPlayerEffect(string steamId, string effectId, bool completeRequest, string reason)
        {
            if (string.IsNullOrWhiteSpace(steamId) || string.IsNullOrWhiteSpace(effectId))
            {
                return;
            }

            var requestIds = new List<string>();
            foreach (var kvp in _activeTimedEffects)
            {
                if (string.Equals(kvp.Value?.SteamId, steamId, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(kvp.Value?.EffectId, effectId, StringComparison.OrdinalIgnoreCase))
                {
                    requestIds.Add(kvp.Key);
                }
            }

            foreach (var requestId in requestIds)
            {
                StopTimedEffect(requestId, completeRequest, reason);
            }
        }

        private void StopTimedEffectsForSteamId(string steamId, string reason)
        {
            var toStop = new List<string>();
            foreach (var kvp in _activeTimedEffects)
            {
                if (string.Equals(kvp.Value?.SteamId, steamId, StringComparison.OrdinalIgnoreCase))
                {
                    toStop.Add(kvp.Key);
                }
            }

            foreach (var requestId in toStop)
            {
                StopTimedEffect(requestId, completeRequest: true, reason: reason);
            }
        }

        private void ClearAllGameplayState()
        {
            foreach (var state in _movementFreezeTimers.Values)
            {
                state?.EnforceTimer?.Destroy();
                state?.EndTimer?.Destroy();
            }
            _movementFreezeTimers.Clear();

            foreach (var timerHandle in _activeHandcuffTimers.Values)
            {
                timerHandle?.Destroy();
            }
            _activeHandcuffTimers.Clear();

            foreach (var timerHandle in _activePowerModeTimers.Values)
            {
                timerHandle?.Destroy();
            }
            _activePowerModeTimers.Clear();

            foreach (var timerHandle in _activeBurnTimers.Values)
            {
                timerHandle?.Destroy();
            }
            _activeBurnTimers.Clear();

            _godModeSteamIds.Clear();
            _flyModeSteamIds.Clear();
        }

        private void UpdateTeleportAvailability()
        {
            if (CrowdControl == null)
            {
                return;
            }

            var activeCount = GetActiveCcPlayers(null).Count;
            var status = activeCount > 1 ? "menuAvailable" : "menuUnavailable";
            CrowdControl.Call(
                "CC_ReportEffectAvailability",
                new JArray { "player_teleport_swap_cc_player", "player_teleport_to_cc_player" },
                status);
        }

        private bool IsSupportedBuiltInEffectId(string effectId)
        {
            switch (NormalizeEffectId(effectId))
            {
                case "player_kill":
                case "player_hunger_strike":
                case "player_fill_hunger":
                case "player_full_heal":
                case "give_fuel":
                case "give_hazmat":
                case "give_armor_kit":
                case "player_strip_armor":
                case "player_break_armor":
                case "world_set_day":
                case "world_set_night":
                case "player_hurt":
                case "player_handcuff":
                case "player_fire":
                case "player_heal":
                case "player_drop_item":
                case "player_drop_some":
                case "player_drop_all":
                case "player_unload_ammo":
                case "give_item_wood":
                case "give_item_stone":
                case "give_item_metal_fragments":
                case "give_item_sulfur_ore":
                case "give_torch":
                case "give_rock":
                case "give_sleeping_bag":
                case "player_drop_hotbar_item":
                case "give_scrap_bonus":
                case "player_scrap_tax":
                case "player_remove_med_items":
                case "give_weapon_revolver":
                case "give_bandage":
                case "give_syringe":
                case "give_large_medkit":
                case "give_weapon_pumpshotgun":
                case "give_weapon_ak":
                case "give_weapon_thompson":
                case "give_weapon_rpg":
                case "give_weapon_grenade_f1":
                case "give_weapon_grenade_beancan":
                case "give_weapon_mgl":
                case "give_explosive_satchel":
                case "give_explosive_timed":
                case "give_ammo_pistol":
                case "give_ammo_pistol_hv":
                case "give_ammo_pistol_incendiary":
                case "give_ammo_rifle":
                case "give_ammo_rifle_hv":
                case "give_ammo_rifle_incendiary":
                case "give_ammo_shotgun_buckshot":
                case "give_ammo_shotgun_slug":
                case "give_ammo_shotgun_incendiary":
                case "give_ammo_rockets":
                case "give_airdrop_signal":
                case "spawn_minicopter":
                case "spawn_supply_drop":
                case "spawn_attack_helicopter":
                case "spawn_nodes":
                case "spawn_nodes_stone":
                case "spawn_nodes_metal":
                case "spawn_nodes_sulfur":
                case "spawn_sleeping_bag_here":
                case "test_hype_train":
                case "player_teleport_to_sleeping_bag":
                case "player_teleport_to_cc_player":
                case "player_teleport_swap_cc_player":
                case "player_reload_active_weapon":
                case "player_drain_active_weapon_ammo":
                case "player_bleed":
                case "player_fracture":
                case "player_freeze_short":
                case "player_god_mode_15s":
                case "player_fly_mode_15s":
                case "player_admin_power_15s":
                case "player_revive":
                case "player_damage_over_time":
                case "player_heal_over_time":
                    return true;
                default:
                    return IsGenericSpawnEffect(NormalizeEffectId(effectId));
            }
        }

        private bool TryApplyInstantEffect(BasePlayer player, string effectId, JObject requestPayload, JObject effectPayload, out string error)
        {
            error = string.Empty;
            var normalized = NormalizeEffectId(effectId);

            if (normalized != "player_revive" && (player == null || player.IsDead()))
            {
                error = "This effect requires the player to be alive.";
                return false;
            }

            if (IsGenericSpawnEffect(normalized))
            {
                return TryHandleGenericSpawnEffect(player, normalized, out error);
            }

            switch (normalized)
            {
                case "player_kill":
                    player.Hurt(9999f);
                    return true;
                case "player_hunger_strike":
                    return TryApplyHungerStrike(player, out error);
                case "player_fill_hunger":
                    return TryApplyFillHunger(player, out error);
                case "player_full_heal":
                    return TryFullHeal(player, out error);
                case "give_fuel":
                    return TryGiveFuel(player, GetEffectAmount(effectPayload, 100), out error);
                case "give_hazmat":
                    return TryGiveHazmatSuit(player, out error);
                case "give_armor_kit":
                    return TryGiveArmorKit(player, out error);
                case "player_strip_armor":
                    return TryStripArmor(player, out error);
                case "player_break_armor":
                    return TryBreakArmor(player, out error);
                case "world_set_day":
                    return TrySetTimeOfDay(player, true, out error);
                case "world_set_night":
                    return TrySetTimeOfDay(player, false, out error);
                case "player_hurt":
                    player.Hurt(25f);
                    return true;
                case "player_handcuff":
                    return TryHandcuffPlayer(player, GetEffectAmount(effectPayload, 12), out error);
                case "player_fire":
                    return TrySetPlayerOnFire(player, GetEffectAmount(effectPayload, 5), out error);
                case "player_heal":
                    return TryHealPlayer(player, 25f, out error);
                case "player_drop_item":
                case "player_drop_hotbar_item":
                    return TryDropHotbarItem(player, out error);
                case "player_drop_some":
                    return TryDropInventorySome(player, out error);
                case "player_drop_all":
                    return TryDropInventoryAll(player, out error);
                case "player_unload_ammo":
                case "player_drain_active_weapon_ammo":
                    return TryDrainActiveWeaponAmmo(player, out error);
                case "give_item_wood":
                    return TryGiveItem(player, "wood", GetEffectAmount(effectPayload, 1000), out error);
                case "give_item_stone":
                    return TryGiveItem(player, "stones", GetEffectAmount(effectPayload, 1000), out error);
                case "give_item_metal_fragments":
                    return TryGiveItem(player, "metal.fragments", GetEffectAmount(effectPayload, 500), out error);
                case "give_item_sulfur_ore":
                    return TryGiveItem(player, "sulfur.ore", GetEffectAmount(effectPayload, 750), out error);
                case "give_torch":
                    return TryGiveItem(player, "torch", 1, out error);
                case "give_rock":
                    return TryGiveItem(player, "rock", 1, out error);
                case "give_sleeping_bag":
                    return TryGiveItem(player, "sleepingbag", 1, out error);
                case "give_scrap_bonus":
                    return TryGiveItem(player, "scrap", GetEffectAmount(effectPayload, 100), out error);
                case "player_scrap_tax":
                    return TryTakeItem(player, "scrap", GetEffectAmount(effectPayload, 100), out error);
                case "player_remove_med_items":
                    return TryRemoveMedicalItems(player, GetEffectAmount(effectPayload, 2), out error);
                case "give_weapon_revolver":
                    return TryGiveWeaponWithAmmo(player, "pistol.revolver", "ammo.pistol", GetEffectAmount(effectPayload, 24), out error);
                case "give_bandage":
                    return TryGiveItem(player, "bandage", GetEffectAmount(effectPayload, 5), out error);
                case "give_syringe":
                    return TryGiveItem(player, "syringe.medical", GetEffectAmount(effectPayload, 2), out error);
                case "give_large_medkit":
                    return TryGiveItem(player, "largemedkit", GetEffectAmount(effectPayload, 1), out error);
                case "give_weapon_pumpshotgun":
                    return TryGiveWeaponWithAmmo(player, "shotgun.pump", "ammo.shotgun", GetEffectAmount(effectPayload, 18), out error);
                case "give_weapon_ak":
                    return TryGiveWeaponWithAmmo(player, "rifle.ak", "ammo.rifle", GetEffectAmount(effectPayload, 60), out error);
                case "give_weapon_thompson":
                    return TryGiveWeaponWithAmmo(player, "smg.thompson", "ammo.pistol", GetEffectAmount(effectPayload, 64), out error);
                case "give_weapon_rpg":
                    return TryGiveWeaponWithAmmo(player, "rocket.launcher", "ammo.rocket.basic", GetEffectAmount(effectPayload, 2), out error);
                case "give_weapon_grenade_f1":
                    return TryGiveItem(player, "grenade.f1", GetEffectAmount(effectPayload, 3), out error);
                case "give_weapon_grenade_beancan":
                    return TryGiveItem(player, "grenade.beancan", GetEffectAmount(effectPayload, 3), out error);
                case "give_weapon_mgl":
                    return TryGiveWeaponWithAmmo(player, "multiplegrenadelauncher", "ammo.grenadelauncher.he", GetEffectAmount(effectPayload, 6), out error);
                case "give_explosive_satchel":
                    return TryGiveItem(player, "explosive.satchel", GetEffectAmount(effectPayload, 2), out error);
                case "give_explosive_timed":
                    return TryGiveItem(player, "explosive.timed", GetEffectAmount(effectPayload, 1), out error);
                case "give_ammo_pistol":
                    return TryGiveItem(player, "ammo.pistol", GetEffectAmount(effectPayload, 64), out error);
                case "give_ammo_pistol_hv":
                    return TryGiveItem(player, "ammo.pistol.hv", GetEffectAmount(effectPayload, 64), out error);
                case "give_ammo_pistol_incendiary":
                    return TryGiveItem(player, "ammo.pistol.fire", GetEffectAmount(effectPayload, 32), out error);
                case "give_ammo_rifle":
                    return TryGiveItem(player, "ammo.rifle", GetEffectAmount(effectPayload, 64), out error);
                case "give_ammo_rifle_hv":
                    return TryGiveItem(player, "ammo.rifle.hv", GetEffectAmount(effectPayload, 64), out error);
                case "give_ammo_rifle_incendiary":
                    return TryGiveItem(player, "ammo.rifle.incendiary", GetEffectAmount(effectPayload, 32), out error);
                case "give_ammo_shotgun_buckshot":
                    return TryGiveItem(player, "ammo.shotgun", GetEffectAmount(effectPayload, 32), out error);
                case "give_ammo_shotgun_slug":
                    return TryGiveItem(player, "ammo.shotgun.slug", GetEffectAmount(effectPayload, 24), out error);
                case "give_ammo_shotgun_incendiary":
                    return TryGiveItem(player, "ammo.shotgun.fire", GetEffectAmount(effectPayload, 16), out error);
                case "give_ammo_rockets":
                    return TryGiveItem(player, "ammo.rocket.basic", GetEffectAmount(effectPayload, 2), out error);
                case "give_airdrop_signal":
                    return TryGiveItem(player, "supply.signal", 1, out error);
                case "spawn_minicopter":
                    return TryGiveMiniWithFuel(player, out error);
                case "spawn_supply_drop":
                    return TrySpawnSupplyDropAtPlayer(player, out error);
                case "spawn_attack_helicopter":
                    return TrySpawnByShortnameAtPlayer(player, "attackhelicopter.entity", 20f, 12f, "Attack helicopter spawned.", out error);
                case "spawn_nodes":
                    return TrySpawnOreNodes(player, "random", 4, out error);
                case "spawn_nodes_stone":
                    return TrySpawnOreNodes(player, "stone", 4, out error);
                case "spawn_nodes_metal":
                    return TrySpawnOreNodes(player, "metal", 4, out error);
                case "spawn_nodes_sulfur":
                    return TrySpawnOreNodes(player, "sulfur", 4, out error);
                case "spawn_sleeping_bag_here":
                    return TryForcePlaceSleepingBagAtPlayer(player, out error);
                case "test_hype_train":
                    return TryRunHypeTrainTestEffect(player, requestPayload, effectPayload, out error);
                case "player_teleport_to_sleeping_bag":
                    return TryTeleportToSleepingBag(player, out error);
                case "player_teleport_to_cc_player":
                    return TryTeleportToCcPlayer(player, out error);
                case "player_teleport_swap_cc_player":
                    return TrySwapTeleportWithCcPlayer(player, out error);
                case "player_reload_active_weapon":
                    return TryReloadActiveWeapon(player, out error);
                case "player_bleed":
                    return TryBleedPlayer(player, GetEffectAmount(effectPayload, 20), out error);
                case "player_fracture":
                    return TryFracturePlayer(player, out error);
                case "player_freeze_short":
                    return TryFreezeMovement(player, GetEffectAmount(effectPayload, 8), out error);
                case "player_god_mode_15s":
                    return TryActivateTemporaryPowerMode(player, 15, false, out error);
                case "player_fly_mode_15s":
                case "player_admin_power_15s":
                    return TryActivateTemporaryPowerMode(player, 15, true, out error);
                case "player_revive":
                    return TryRevivePlayer(player, out error);
                default:
                    error = $"Unknown instant effectID '{effectId}'.";
                    return false;
            }
        }

        private int GetEffectAmount(JObject effectPayload, int fallback)
        {
            if (effectPayload == null)
            {
                return fallback;
            }

            var direct = effectPayload.Value<int?>("amount") ??
                effectPayload.Value<int?>("quantity") ??
                effectPayload.Value<int?>("value");
            if (direct.HasValue && direct.Value > 0)
            {
                return direct.Value;
            }

            var options = effectPayload["options"] as JObject;
            if (options != null)
            {
                var optionValue = options.Value<int?>("amount") ??
                    options.Value<int?>("quantity") ??
                    options.Value<int?>("value");
                if (optionValue.HasValue && optionValue.Value > 0)
                {
                    return optionValue.Value;
                }
            }

            return fallback;
        }

        private string NormalizeEffectId(string effectId)
        {
            return (effectId ?? string.Empty).Trim().ToLowerInvariant();
        }

        private List<BasePlayer> GetActiveCcPlayers(string excludeSteamId)
        {
            var results = new List<BasePlayer>();
            if (CrowdControl == null)
            {
                return results;
            }

            var ids = CrowdControl.Call("CC_GetActiveCcPlayerSteamIds", excludeSteamId ?? string.Empty) as JArray;
            if (ids == null)
            {
                return results;
            }

            foreach (var token in ids)
            {
                var steamId = token?.ToString();
                var player = FindPlayerBySteamId(steamId);
                if (player != null && player.IsConnected)
                {
                    results.Add(player);
                }
            }

            return results;
        }

        private bool TryTeleportToCcPlayer(BasePlayer player, out string error)
        {
            error = string.Empty;
            var candidates = GetActiveCcPlayers(player?.UserIDString);
            if (candidates.Count == 0)
            {
                error = "No other active Crowd Control player is available to teleport to.";
                return false;
            }

            var target = candidates[UnityEngine.Random.Range(0, candidates.Count)];
            var destination = target.transform.position + new Vector3(0f, 0f, 1.5f);
            player.Teleport(destination);
            return true;
        }

        private bool TrySwapTeleportWithCcPlayer(BasePlayer player, out string error)
        {
            error = string.Empty;
            var candidates = GetActiveCcPlayers(player?.UserIDString);
            if (candidates.Count == 0)
            {
                error = "No other active Crowd Control player is available for swap teleport.";
                return false;
            }

            var target = candidates[UnityEngine.Random.Range(0, candidates.Count)];
            var sourcePos = player.transform.position;
            var targetPos = target.transform.position;
            var verticalOffset = new Vector3(0f, 0.35f, 0f);
            player.Teleport(targetPos + verticalOffset);
            target.Teleport(sourcePos + verticalOffset);
            return true;
        }

        private bool TryTeleportToSleepingBag(BasePlayer player, out string error)
        {
            error = string.Empty;
            var bags = GetPlayerSleepingBags(player);
            if (bags.Count == 0)
            {
                error = "Player has no sleeping bags to teleport to.";
                return false;
            }

            var bag = bags[UnityEngine.Random.Range(0, bags.Count)];
            if (bag == null || bag.IsDestroyed)
            {
                error = "Selected sleeping bag is no longer valid.";
                return false;
            }

            var destination = bag.transform.position + new Vector3(0f, 0.2f, 0f);
            if (Physics.Raycast(destination + Vector3.up * 2f, Vector3.down, out var hit, 6f))
            {
                destination.y = hit.point.y + 0.1f;
            }

            player.Teleport(destination);
            return true;
        }

        private List<SleepingBag> GetPlayerSleepingBags(BasePlayer player)
        {
            var results = new List<SleepingBag>();
            if (player == null)
            {
                return results;
            }

            foreach (var networkable in BaseNetworkable.serverEntities)
            {
                var bag = networkable as SleepingBag;
                if (bag == null || bag.IsDestroyed || bag.deployerUserID != player.userID)
                {
                    continue;
                }

                results.Add(bag);
            }

            return results;
        }

        private bool TryRevivePlayer(BasePlayer player, out string error)
        {
            error = string.Empty;
            if (player == null)
            {
                error = "Player is not valid.";
                return false;
            }

            if (!player.IsDead())
            {
                error = "Revive only works while the player is dead.";
                return false;
            }

            if (!_lastDeathPositionBySteamId.TryGetValue(player.UserIDString, out var deathPos))
            {
                error = "No recorded death position for this player yet.";
                return false;
            }

            var steamId = player.UserIDString;
            player.Respawn();
            timer.Once(0.25f, () =>
            {
                var revived = FindPlayerBySteamId(steamId);
                if (revived == null || !revived.IsConnected)
                {
                    return;
                }

                revived.Teleport(deathPos);
                revived.SendNetworkUpdateImmediate();
                ShowEffectUi(revived, "Crowd Control", "You were revived at your death location.");
            });

            return true;
        }

        private bool TryFreezeMovement(BasePlayer player, int seconds, out string error)
        {
            error = string.Empty;
            var steamId = player.UserIDString;
            if (_movementFreezeTimers.TryGetValue(steamId, out var existing))
            {
                existing.EnforceTimer?.Destroy();
                existing.EndTimer?.Destroy();
                _movementFreezeTimers.Remove(steamId);
            }

            var freezeState = new MovementFreezeState
            {
                AnchorPosition = player.transform.position
            };

            freezeState.EnforceTimer = timer.Every(0.1f, () =>
            {
                var current = FindPlayerBySteamId(steamId);
                if (current != null && current.IsConnected)
                {
                    current.Teleport(freezeState.AnchorPosition);
                }
            });
            freezeState.EndTimer = timer.Once(Mathf.Clamp(seconds, 1, 30), () => ClearMovementFreeze(player));
            _movementFreezeTimers[steamId] = freezeState;
            return true;
        }

        private void ClearMovementFreeze(BasePlayer player)
        {
            if (player == null)
            {
                return;
            }

            var steamId = player.UserIDString;
            if (_movementFreezeTimers.TryGetValue(steamId, out var existing))
            {
                existing.EnforceTimer?.Destroy();
                existing.EndTimer?.Destroy();
                _movementFreezeTimers.Remove(steamId);
            }
        }

        private bool TryHandcuffPlayer(BasePlayer player, int seconds, out string error)
        {
            return TryHandcuffPlayer(player, seconds, manageLifetime: true, out error);
        }

        private bool TryHandcuffPlayer(BasePlayer player, int seconds, bool manageLifetime, out string error)
        {
            error = string.Empty;
            if (!TrySetRestrainedStatus(player, true, out error))
            {
                return false;
            }

            var steamId = player.UserIDString;
            if (_activeHandcuffTimers.TryGetValue(steamId, out var existing))
            {
                existing?.Destroy();
                _activeHandcuffTimers.Remove(steamId);
            }

            var duration = Mathf.Clamp(seconds, 3, 30);
            if (!TryFreezeMovement(player, duration, out var freezeError))
            {
                TrySetRestrainedStatus(player, false, out _);
                error = string.IsNullOrWhiteSpace(freezeError) ? "Unable to immobilize the player." : freezeError;
                return false;
            }

            if (manageLifetime)
            {
                _activeHandcuffTimers[steamId] = timer.Once(duration, () => EndHandcuffEffect(steamId));
            }

            ShowEffectUi(player, "Crowd Control", $"Handcuffed for {duration}s.");
            return true;
        }

        private void EndHandcuffEffect(string steamId)
        {
            if (_activeHandcuffTimers.TryGetValue(steamId, out var endTimer))
            {
                endTimer?.Destroy();
                _activeHandcuffTimers.Remove(steamId);
            }

            var player = FindPlayerBySteamId(steamId);
            if (player == null || !player.IsConnected)
            {
                return;
            }

            TrySetRestrainedStatus(player, false, out _);
            ClearMovementFreeze(player);
        }

        private bool TrySetRestrainedStatus(BasePlayer player, bool restrained, out string error)
        {
            error = string.Empty;
            try
            {
                player.SetPlayerFlag(BasePlayer.PlayerFlags.IsRestrained, restrained);

                if (restrained)
                {
                    TryClosePlayerLooting(player);
                }

                foreach (var methodName in new[] { "SetRestrained", "SetHandcuffed" })
                {
                    var method = player.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null, new[] { typeof(bool) }, null);
                    if (method == null)
                    {
                        continue;
                    }

                    method.Invoke(player, new object[] { restrained });
                    player.SendNetworkUpdateImmediate();
                    return true;
                }

                var updatedState = true;
                MethodInfo setFlagMethod = null;
                foreach (var method in player.GetType().GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
                {
                    if (!string.Equals(method.Name, "SetPlayerFlag", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    var parameters = method.GetParameters();
                    if (parameters.Length == 2 && parameters[1].ParameterType == typeof(bool) && parameters[0].ParameterType.IsEnum)
                    {
                        setFlagMethod = method;
                        break;
                    }
                }

                if (setFlagMethod != null)
                {
                    var enumType = setFlagMethod.GetParameters()[0].ParameterType;
                    object restrainedFlag = null;
                    foreach (var flagName in new[] { "IsRestrained", "Restrained", "Handcuffed", "IsHandcuffed" })
                    {
                        if (Enum.IsDefined(enumType, flagName))
                        {
                            restrainedFlag = Enum.Parse(enumType, flagName);
                            break;
                        }
                    }

                    if (restrainedFlag != null)
                    {
                        setFlagMethod.Invoke(player, new[] { restrainedFlag, (object)restrained });
                        updatedState = true;
                    }
                }

                var modelState = GetMemberObject(player, "modelState");
                if (modelState != null)
                {
                    foreach (var memberName in new[] { "restrained", "handcuffed", "isRestrained", "isHandcuffed" })
                    {
                        if (TrySetBoolMember(modelState, memberName, restrained))
                        {
                            updatedState = true;
                        }
                    }
                }

                if (!updatedState)
                {
                    error = "Handcuff status API is unavailable on this server build.";
                    return false;
                }

                player.SendNetworkUpdateImmediate();
                return true;
            }
            catch (Exception ex)
            {
                error = $"Unable to update handcuff status: {ex.Message}";
                return false;
            }
        }

        private void TryClosePlayerLooting(BasePlayer player)
        {
            if (player == null)
            {
                return;
            }

            try
            {
                var endLootingMethod = player.GetType().GetMethod("EndLooting", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null, Type.EmptyTypes, null);
                endLootingMethod?.Invoke(player, null);
            }
            catch
            {
            }

            try
            {
                var loot = GetMemberObject(player.inventory, "loot");
                var clearMethod = loot?.GetType().GetMethod("Clear", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null, Type.EmptyTypes, null);
                clearMethod?.Invoke(loot, null);
            }
            catch
            {
            }

            try
            {
                player.SendConsoleCommand("inventory.close");
            }
            catch
            {
            }
        }

        private bool TrySetPlayerOnFire(BasePlayer player, int seconds, out string error)
        {
            error = string.Empty;
            var steamId = player.UserIDString;
            EndBurnEffect(steamId);
            TryIgnitePlayer(player, seconds);

            var remainingTicks = Mathf.Clamp(seconds, 2, 12);
            _activeBurnTimers[steamId] = timer.Every(1f, () =>
            {
                var current = FindPlayerBySteamId(steamId);
                if (current == null || !current.IsConnected || current.IsDead())
                {
                    EndBurnEffect(steamId);
                    return;
                }

                current.Hurt(3f);
                TrySpawnFireAroundPlayer(current, 2);
                remainingTicks--;
                if (remainingTicks <= 0)
                {
                    EndBurnEffect(steamId);
                }
            });

            ShowEffectUi(player, "Crowd Control", "Set you on fire!.");
            return true;
        }

        private void EndBurnEffect(string steamId)
        {
            if (_activeBurnTimers.TryGetValue(steamId, out var timerHandle))
            {
                timerHandle?.Destroy();
                _activeBurnTimers.Remove(steamId);
            }
        }

        private void TryIgnitePlayer(BasePlayer player, int seconds)
        {
            try
            {
                var method = player.GetType().GetMethod("Ignite", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                if (method != null)
                {
                    var args = method.GetParameters().Length == 0 ? Array.Empty<object>() : new object[] { Mathf.Max(1f, seconds) };
                    method.Invoke(player, args);
                }
            }
            catch
            {
            }
        }

        private void TrySpawnFireAroundPlayer(BasePlayer player, int count)
        {
            var center = player.transform.position;
            for (var i = 0; i < Mathf.Clamp(count, 1, 4); i++)
            {
                var offset = UnityEngine.Random.insideUnitSphere;
                offset.y = 0f;
                offset = offset.normalized * UnityEngine.Random.Range(1.2f, 2.8f);
                var probe = center + offset;
                var groundY = TerrainMeta.HeightMap != null ? TerrainMeta.HeightMap.GetHeight(probe) : probe.y;
                var position = new Vector3(probe.x, groundY + 0.05f, probe.z);

                BaseEntity fireEntity = null;
                try
                {
                    fireEntity = GameManager.server.CreateEntity("assets/bundled/prefabs/oilfireballsmall.prefab", position, Quaternion.identity, true) ??
                        GameManager.server.CreateEntity("assets/bundled/prefabs/fireball_small.prefab", position, Quaternion.identity, true);
                    if (fireEntity == null)
                    {
                        continue;
                    }

                    fireEntity.Spawn();
                    var spawned = fireEntity;
                    timer.Once(3f, () =>
                    {
                        if (spawned != null && !spawned.IsDestroyed)
                        {
                            spawned.Kill();
                        }
                    });
                }
                catch
                {
                    fireEntity?.Kill();
                }
            }
        }

        private bool TryActivateTemporaryPowerMode(BasePlayer player, int seconds, bool enableFly, out string error)
        {
            error = string.Empty;
            var steamId = player.UserIDString;
            EndTemporaryPowerMode(steamId, enableFly, false);
            _godModeSteamIds.Add(steamId);
            player.health = player.MaxHealth();
            if (player.metabolism?.bleeding != null)
            {
                player.metabolism.bleeding.value = 0f;
                player.metabolism.SendChangesToClient();
            }

            if (enableFly && !_flyModeSteamIds.Contains(steamId))
            {
                TryTogglePlayerNoClip(player);
                _flyModeSteamIds.Add(steamId);
            }

            ShowEffectUi(player, "Crowd Control", enableFly ? "Admin Power active for 15s" : "God Mode active for 15s");
            _activePowerModeTimers[steamId] = timer.Once(Mathf.Max(1, seconds), () => EndTemporaryPowerMode(steamId, enableFly, true));
            return true;
        }

        private void EndTemporaryPowerMode(string steamId, bool restoreFly, bool showUi)
        {
            if (_activePowerModeTimers.TryGetValue(steamId, out var timerHandle))
            {
                timerHandle?.Destroy();
                _activePowerModeTimers.Remove(steamId);
            }

            var hadGod = _godModeSteamIds.Remove(steamId);
            var hadFly = _flyModeSteamIds.Remove(steamId);
            if (!hadGod && !hadFly && !restoreFly)
            {
                return;
            }

            var player = FindPlayerBySteamId(steamId);
            if (player == null || !player.IsConnected)
            {
                return;
            }

            if (restoreFly && hadFly)
            {
                TryTogglePlayerNoClip(player);
            }

            if (showUi)
            {
                ShowEffectUi(player, "Crowd Control", "Power effect ended");
            }
        }

        private void TryTogglePlayerNoClip(BasePlayer player)
        {
            try
            {
                player.SendConsoleCommand("noclip");
            }
            catch (Exception ex)
            {
                LogVerbose($"Failed toggling noclip for {player?.displayName}: {ex.Message}");
            }
        }

        private bool TryGiveItem(BasePlayer player, string shortName, int amount, out string error)
        {
            error = string.Empty;
            var item = ItemManager.CreateByName(shortName, amount);
            if (item == null)
            {
                error = $"Unable to create item '{shortName}'.";
                return false;
            }

            player.GiveItem(item);
            return true;
        }

        private bool TryGiveWeaponWithAmmo(BasePlayer player, string weaponShortName, string ammoShortName, int ammoAmount, out string error)
        {
            if (!TryGiveItem(player, weaponShortName, 1, out error))
            {
                return false;
            }

            if (!string.IsNullOrEmpty(ammoShortName))
            {
                TryGiveItem(player, ammoShortName, Mathf.Max(1, ammoAmount), out _);
            }

            return true;
        }

        private bool TryApplyHungerStrike(BasePlayer player, out string error)
        {
            error = string.Empty;
            if (player?.metabolism == null || player.metabolism.calories == null || player.metabolism.hydration == null)
            {
                error = "Metabolism state is unavailable.";
                return false;
            }

            player.metabolism.calories.value = 0f;
            player.metabolism.hydration.value = 0f;
            player.metabolism.SendChangesToClient();
            ShowEffectUi(player, "Crowd Control", "Hunger strike!");
            return true;
        }

        private bool TryApplyFillHunger(BasePlayer player, out string error)
        {
            error = string.Empty;
            if (player?.metabolism == null || player.metabolism.calories == null || player.metabolism.hydration == null)
            {
                error = "Metabolism state is unavailable.";
                return false;
            }

            const float caloriesFull = 500f;
            const float hydrationFull = 250f;
            const float refillThresholdPct = 0.8f;
            if (player.metabolism.calories.value >= caloriesFull * refillThresholdPct &&
                player.metabolism.hydration.value >= hydrationFull * refillThresholdPct)
            {
                error = "Player hunger/thirst levels are not severe enough.";
                return false;
            }

            player.metabolism.calories.value = caloriesFull;
            player.metabolism.hydration.value = hydrationFull;
            player.metabolism.SendChangesToClient();
            ShowEffectUi(player, "Crowd Control", "FillHunger restored food/water.");
            return true;
        }

        private bool TryFullHeal(BasePlayer player, out string error)
        {
            error = string.Empty;
            if (player.health >= player.MaxHealth() * 0.9f)
            {
                error = "Player is healed enough.";
                return false;
            }

            player.health = player.MaxHealth();
            if (player.metabolism?.bleeding != null)
            {
                player.metabolism.bleeding.value = 0f;
                player.metabolism.SendChangesToClient();
            }
            player.SendNetworkUpdateImmediate();
            ShowEffectUi(player, "Crowd Control", "Full Heal restored your health.");
            return true;
        }

        private bool TryGiveFuel(BasePlayer player, int amount, out string error)
        {
            return TryGiveItem(player, "lowgradefuel", Mathf.Clamp(amount, 1, 1000), out error);
        }

        private bool TryGiveHazmatSuit(BasePlayer player, out string error)
        {
            return TryGiveWearItem(player, "hazmatsuit", out error);
        }

        private bool TryGiveArmorKit(BasePlayer player, out string error)
        {
            error = string.Empty;
            var ok = true;
            ok &= TryGiveWearItem(player, "metal.facemask", out _);
            ok &= TryGiveWearItem(player, "metal.plate.torso", out _);
            ok &= TryGiveWearItem(player, "roadsign.kilt", out _);
            ok &= TryGiveWearItem(player, "hoodie", out _);
            ok &= TryGiveWearItem(player, "pants", out _);
            ok &= TryGiveWearItem(player, "shoes.boots", out _);
            if (!ok)
            {
                error = "One or more armor items could not be created.";
            }
            return ok;
        }

        private bool TryGiveWearItem(BasePlayer player, string shortName, out string error)
        {
            error = string.Empty;
            var item = ItemManager.CreateByName(shortName, 1);
            if (item == null)
            {
                error = $"Unable to create wearable '{shortName}'.";
                return false;
            }

            var wear = player?.inventory?.containerWear;
            if (wear != null && item.MoveToContainer(wear))
            {
                player.SendNetworkUpdateImmediate();
                return true;
            }

            player.GiveItem(item);
            return true;
        }

        private bool TryStripArmor(BasePlayer player, out string error)
        {
            error = string.Empty;
            var wear = player?.inventory?.containerWear;
            if (wear == null || wear.itemList == null || wear.itemList.Count == 0)
            {
                error = "No worn armor/clothing to strip.";
                return false;
            }

            foreach (var item in wear.itemList.ToArray())
            {
                item?.RemoveFromContainer();
                item?.Drop(player.GetDropPosition(), player.GetDropVelocity());
            }

            player.SendNetworkUpdateImmediate();
            ShowEffectUi(player, "Crowd Control", "Armor/clothing stripped.");
            return true;
        }

        private bool TryBreakArmor(BasePlayer player, out string error)
        {
            error = string.Empty;
            var wear = player?.inventory?.containerWear;
            if (wear == null || wear.itemList == null || wear.itemList.Count == 0)
            {
                error = "No armor equipped to break.";
                return false;
            }

            var changed = 0;
            foreach (var item in wear.itemList)
            {
                if (item != null && item.condition > 0f)
                {
                    item.LoseCondition(item.condition + 9999f);
                    changed++;
                }
            }

            if (changed <= 0)
            {
                error = "Armor is already broken or no breakable armor is equipped.";
                return false;
            }

            player.SendNetworkUpdateImmediate();
            ShowEffectUi(player, "Crowd Control", "Armor durability destroyed.");
            return true;
        }

        private bool TrySetTimeOfDay(BasePlayer player, bool setDay, out string error)
        {
            error = string.Empty;
            var sky = TOD_Sky.Instance;
            if (sky?.Cycle == null)
            {
                error = "Time cycle system is unavailable.";
                return false;
            }

            var hour = sky.Cycle.Hour;
            if ((setDay && hour >= 8f && hour < 18f) || (!setDay && (hour >= 20f || hour < 5f)))
            {
                error = setDay ? $"It is already daytime ({hour:0.0}h)." : $"It is already nighttime ({hour:0.0}h).";
                return false;
            }

            sky.Cycle.Hour = setDay ? 13f : 23f;
            ShowEffectUi(player, "Crowd Control", setDay ? "Time changed: Day" : "Time changed: Night");
            return true;
        }

        private bool TryTakeItem(BasePlayer player, string shortName, int amount, out string error)
        {
            error = string.Empty;
            var definition = ItemManager.FindItemDefinition(shortName);
            if (definition == null)
            {
                error = $"Unknown item shortname '{shortName}'.";
                return false;
            }

            var removed = player.inventory.Take(null, definition.itemid, Mathf.Max(1, amount));
            if (removed <= 0)
            {
                error = $"Player has no '{shortName}' to remove.";
                return false;
            }

            return true;
        }

        private bool TryDropHotbarItem(BasePlayer player, out string error)
        {
            error = string.Empty;
            var activeItem = player.GetActiveItem();
            if (activeItem == null)
            {
                error = "No active hotbar item to drop.";
                return false;
            }

            activeItem.Drop(player.GetDropPosition(), player.GetDropVelocity());
            return true;
        }

        private bool TryDropInventorySome(BasePlayer player, out string error)
        {
            error = string.Empty;
            var items = CollectDroppableInventoryItems(player);
            if (items.Count == 0)
            {
                error = "No inventory items available to drop.";
                return false;
            }

            for (var i = items.Count - 1; i > 0; i--)
            {
                var j = UnityEngine.Random.Range(0, i + 1);
                var temp = items[i];
                items[i] = items[j];
                items[j] = temp;
            }

            var targetDrops = Mathf.Clamp(Mathf.CeilToInt(items.Count * 0.5f), 1, items.Count);
            var dropped = 0;
            for (var i = 0; i < items.Count && dropped < targetDrops; i++)
            {
                var item = items[i];
                if (item?.parent == null)
                {
                    continue;
                }

                item.Drop(player.GetDropPosition(), player.GetDropVelocity());
                dropped++;
            }

            if (dropped <= 0)
            {
                error = "No inventory items were dropped.";
                return false;
            }

            return true;
        }

        private bool TryDropInventoryAll(BasePlayer player, out string error)
        {
            error = string.Empty;
            var items = CollectDroppableInventoryItems(player);
            if (items.Count == 0)
            {
                error = "No inventory items available to drop.";
                return false;
            }

            var dropped = 0;
            foreach (var item in items)
            {
                if (item?.parent == null)
                {
                    continue;
                }

                item.Drop(player.GetDropPosition(), player.GetDropVelocity());
                dropped++;
            }

            if (dropped <= 0)
            {
                error = "No inventory items were dropped.";
                return false;
            }

            return true;
        }

        private List<Item> CollectDroppableInventoryItems(BasePlayer player)
        {
            var results = new List<Item>();
            if (player?.inventory == null)
            {
                return results;
            }

            CollectItemsFromContainer(player.inventory.containerMain, results);
            CollectItemsFromContainer(player.inventory.containerBelt, results);
            CollectItemsFromContainer(player.inventory.containerWear, results);
            return results;
        }

        private void CollectItemsFromContainer(ItemContainer container, List<Item> target)
        {
            if (container?.itemList == null)
            {
                return;
            }

            foreach (var item in container.itemList.ToArray())
            {
                if (item != null)
                {
                    target.Add(item);
                }
            }
        }

        private bool TryRemoveMedicalItems(BasePlayer player, int perTypeAmount, out string error)
        {
            error = string.Empty;
            var countPerType = Mathf.Max(1, perTypeAmount);
            var removed = TryTakeAmount(player, "bandage", countPerType) +
                TryTakeAmount(player, "syringe.medical", countPerType) +
                TryTakeAmount(player, "largemedkit", countPerType);
            if (removed <= 0)
            {
                error = "No medical items were found to remove.";
                return false;
            }

            return true;
        }

        private int TryTakeAmount(BasePlayer player, string shortName, int amount)
        {
            var definition = ItemManager.FindItemDefinition(shortName);
            return definition == null ? 0 : player.inventory.Take(null, definition.itemid, Mathf.Max(1, amount));
        }

        private bool TryReloadActiveWeapon(BasePlayer player, out string error)
        {
            error = string.Empty;
            var weapon = player.GetHeldEntity() as BaseProjectile;
            if (weapon == null || weapon.primaryMagazine == null)
            {
                error = "You must hold a reloadable weapon.";
                return false;
            }

            weapon.primaryMagazine.contents = weapon.primaryMagazine.capacity;
            weapon.SendNetworkUpdateImmediate();
            return true;
        }

        private bool TryDrainActiveWeaponAmmo(BasePlayer player, out string error)
        {
            error = string.Empty;
            var weapon = player.GetHeldEntity() as BaseProjectile;
            if (weapon == null || weapon.primaryMagazine == null)
            {
                error = "You must hold a weapon with ammo.";
                return false;
            }

            var removedFromMag = weapon.primaryMagazine.contents;
            weapon.primaryMagazine.contents = 0;
            weapon.SendNetworkUpdateImmediate();

            var removedFromInv = 0;
            if (weapon.primaryMagazine.ammoType != null)
            {
                removedFromInv = player.inventory.Take(null, weapon.primaryMagazine.ammoType.itemid, 64);
            }

            if (removedFromMag <= 0 && removedFromInv <= 0)
            {
                error = "No ammo was available to remove.";
                return false;
            }

            return true;
        }

        private bool TryBleedPlayer(BasePlayer player, int amount, out string error)
        {
            error = string.Empty;
            if (player.metabolism?.bleeding == null)
            {
                error = "Bleeding metabolism is not available.";
                return false;
            }

            player.metabolism.bleeding.value = Mathf.Clamp(player.metabolism.bleeding.value + Mathf.Clamp(amount, 1, 100), 0f, 200f);
            player.metabolism.SendChangesToClient();
            return true;
        }

        private bool TryFracturePlayer(BasePlayer player, out string error)
        {
            error = string.Empty;
            player.Hurt(15f);
            if (player.metabolism?.bleeding != null)
            {
                player.metabolism.bleeding.value = Mathf.Clamp(player.metabolism.bleeding.value + 10f, 0f, 200f);
                player.metabolism.SendChangesToClient();
            }

            return TryFreezeMovement(player, 6, out error);
        }

        private bool TryHealPlayer(BasePlayer player, float amount, out string error)
        {
            error = string.Empty;
            var maxHealth = player.MaxHealth();
            if (player.health >= maxHealth * 0.91f)
            {
                error = "Player is healed enough.";
                return false;
            }

            player.health = Mathf.Min(maxHealth, player.health + Mathf.Max(1f, amount));
            player.SendNetworkUpdateImmediate();
            return true;
        }

        private bool IsGenericSpawnEffect(string effectId)
        {
            if (string.IsNullOrEmpty(effectId) || !effectId.StartsWith("spawn_", StringComparison.Ordinal))
            {
                return false;
            }

            if (effectId.StartsWith("spawn_item_", StringComparison.Ordinal) ||
                effectId.StartsWith("spawn_weapon_", StringComparison.Ordinal) ||
                effectId.StartsWith("spawn_explosive", StringComparison.Ordinal) ||
                effectId.StartsWith("spawn_nodes", StringComparison.Ordinal) ||
                effectId == "spawn_airdrop_signal")
            {
                return false;
            }

            return true;
        }

        private bool TryHandleGenericSpawnEffect(BasePlayer player, string effectId, out string error)
        {
            error = string.Empty;
            var spawnType = effectId.Substring("spawn_".Length);
            string spawnShortName = spawnType;
            float forwardDistance = 9f;
            float upOffset = 0.2f;
            int fuelAmount = 0;
            var isEnemySingleSpawn = false;

            switch (spawnType)
            {
                case "testridablehorse":
                    forwardDistance = 10f;
                    upOffset = 0.5f;
                    break;
                case "boar":
                case "wolf":
                case "chicken":
                    isEnemySingleSpawn = true;
                    forwardDistance = 8f;
                    break;
                case "bear":
                case "stag":
                    isEnemySingleSpawn = true;
                    break;
                case "wolves":
                    return TrySpawnWolfPack(player, 3, out error);
                case "scarecrow":
                    isEnemySingleSpawn = true;
                    forwardDistance = 10f;
                    break;
                case "scarecrows":
                    return TrySpawnZombiePack(player, 3, out error);
                case "simpleshark":
                    isEnemySingleSpawn = true;
                    forwardDistance = 12f;
                    break;
                case "scientistnpc":
                    isEnemySingleSpawn = true;
                    forwardDistance = 10f;
                    break;
                case "pedalbike":
                    forwardDistance = 8f;
                    upOffset = 1.5f;
                    fuelAmount = 25;
                    break;
                case "motorbike":
                    forwardDistance = 8f;
                    upOffset = 1.5f;
                    fuelAmount = 50;
                    break;
                case "vehicle_car_small":
                    return TrySpawnModularCarByShortnameAtPlayer(player, "2module_car_spawned", 10f, 1.5f, "Small car spawned.", 75, 1, out error);
                case "vehicle_car_large":
                    return TrySpawnModularCarByShortnameAtPlayer(player, "4module_car_spawned", 10f, 1.5f, "Large car spawned.", 100, 2, out error);
                case "vehicle_truck":
                    return TrySpawnModularCarByShortnameAtPlayer(player, "3module_car_spawned", 10f, 1.5f, "Truck spawned.", 100, 2, out error);
                case "rowboat":
                    forwardDistance = 12f;
                    upOffset = 0.9f;
                    fuelAmount = 100;
                    break;
                case "rhib":
                    forwardDistance = 12f;
                    upOffset = 0.9f;
                    fuelAmount = 100;
                    break;
                case "scraptransporthelicopter":
                    forwardDistance = 16f;
                    upOffset = 6f;
                    fuelAmount = 150;
                    break;
                case "sleeping_bag_here":
                    return TryForcePlaceSleepingBagAtPlayer(player, out error);
                default:
                    error = $"Unknown spawn type '{spawnType}'.";
                    return false;
            }

            if (isEnemySingleSpawn)
            {
                return TrySpawnEnemyByShortnameAtPlayer(player, spawnShortName, forwardDistance, out error);
            }

            return TrySpawnByShortnameAtPlayer(player, spawnShortName, forwardDistance, upOffset, null, fuelAmount, out error);
        }

        private bool TryGiveMiniWithFuel(BasePlayer player, out string error)
        {
            if (!TrySpawnByShortnameAtPlayer(player, "minicopter.entity", 10f, 2f, "Minicopter spawned.", out error))
            {
                return false;
            }

            TryGiveItem(player, "lowgradefuel", 50, out _);
            return true;
        }

        private bool TrySpawnSupplyDropAtPlayer(BasePlayer player, out string error)
        {
            error = string.Empty;
            if (!TryResolveGroundSpawnPosition(player, 20f, out var basePosition, out error))
            {
                return false;
            }

            var position = basePosition + new Vector3(0f, 30f, 0f);
            var prefabCandidates = new[]
            {
                "assets/prefabs/misc/supply drop/supply_drop.prefab",
                "assets/prefabs/misc/supplydrop/supply_drop.prefab"
            };

            BaseEntity drop = null;
            foreach (var prefabPath in prefabCandidates)
            {
                drop = GameManager.server.CreateEntity(prefabPath, position, Quaternion.identity, true);
                if (drop != null)
                {
                    break;
                }
            }

            if (drop == null)
            {
                error = "Failed to spawn supply drop.";
                return false;
            }

            drop.Spawn();
            ShowEffectUi(player, "Crowd Control", "Supply drop spawned.");
            return true;
        }

        private bool TrySpawnWolfPack(BasePlayer player, int count, out string error)
        {
            error = string.Empty;
            if (!TryResolveGroundSpawnPosition(player, 8f, out var centerPosition, out error))
            {
                return false;
            }

            var spawned = 0;
            var occupiedPositions = new List<Vector3>();
            for (var i = 0; i < Mathf.Clamp(count, 1, 8); i++)
            {
                if (!TryResolveEnemySpawnPosition(player, centerPosition, 4f, 8f, 2.5f, occupiedPositions, out var position))
                {
                    continue;
                }

                var wolf = GameManager.server.CreateEntity("assets/rust.ai/agents/wolf/wolf.prefab", position, Quaternion.identity, true);
                if (wolf == null)
                {
                    continue;
                }

                wolf.Spawn();
                occupiedPositions.Add(position);
                spawned++;
            }

            if (spawned <= 0)
            {
                error = "Failed to spawn wolves.";
                return false;
            }

            ShowEffectUi(player, "Crowd Control", $"Wolf pack spawned ({spawned}).");
            return true;
        }

        private bool TrySpawnZombiePack(BasePlayer player, int count, out string error)
        {
            error = string.Empty;
            if (!TryResolveGroundSpawnPosition(player, 9f, out var centerPosition, out error))
            {
                return false;
            }

            var spawned = 0;
            var occupiedPositions = new List<Vector3>();
            for (var i = 0; i < Mathf.Clamp(count, 1, 8); i++)
            {
                if (!TryResolveEnemySpawnPosition(player, centerPosition, 4f, 8f, 2.5f, occupiedPositions, out var position))
                {
                    continue;
                }

                try
                {
                    ConsoleSystem.Run(ConsoleSystem.Option.Server.Quiet(), $"entity.spawn scarecrow {position.x:0.00},{position.y:0.00},{position.z:0.00}");
                    occupiedPositions.Add(position);
                    spawned++;
                }
                catch
                {
                }
            }

            if (spawned <= 0)
            {
                error = "Failed to spawn zombies.";
                return false;
            }

            ShowEffectUi(player, "Crowd Control", $"Zombie pack spawned ({spawned}).");
            return true;
        }

        private bool TrySpawnEnemyByShortnameAtPlayer(BasePlayer player, string shortName, float forwardDistance, out string error)
        {
            error = string.Empty;
            if (!TryResolveGroundSpawnPosition(player, forwardDistance, out var centerPosition, out error))
            {
                return false;
            }

            if (!TryResolveEnemySpawnPosition(player, centerPosition, 0.5f, 4f, 2.5f, null, out var spawnPosition))
            {
                error = "Unable to find safe enemy spawn ground.";
                return false;
            }

            try
            {
                ConsoleSystem.Run(ConsoleSystem.Option.Server.Quiet(), $"entity.spawn {shortName} {spawnPosition.x:0.00},{spawnPosition.y:0.00},{spawnPosition.z:0.00}");
                ShowEffectUi(player, "Crowd Control", BuildSpawnSuccessMessage(shortName));
                return true;
            }
            catch (Exception ex)
            {
                error = $"Failed to spawn enemy. {ex.Message}";
                return false;
            }
        }

        private bool TrySpawnModularCarByShortnameAtPlayer(BasePlayer player, string shortName, float forwardDistance, float upOffset, string successMessage, int fuelAmount, int engineKits, out string error)
        {
            error = string.Empty;
            if (!TryResolveVehicleSpawnPosition(player, forwardDistance, out var basePosition, out error))
            {
                return false;
            }

            if (!TrySpawnByShortnameAtPosition(player, shortName, basePosition, upOffset, successMessage, fuelAmount, out error))
            {
                return false;
            }

            if (!TryGiveModularCarEngineParts(player, Mathf.Max(1, engineKits), out var partsError))
            {
                LogVerbose($"Car parts grant issue after spawn: {partsError}");
            }

            return true;
        }

        private bool TryGiveModularCarEngineParts(BasePlayer player, int engineKits, out string error)
        {
            error = string.Empty;
            var ok = true;
            for (var i = 0; i < Mathf.Clamp(engineKits, 1, 3); i++)
            {
                ok &= TryGiveItem(player, "carburetor3", 1, out _);
                ok &= TryGiveItem(player, "crankshaft3", 1, out _);
                ok &= TryGiveItem(player, "piston3", 1, out _);
                ok &= TryGiveItem(player, "sparkplug3", 1, out _);
                ok &= TryGiveItem(player, "valve3", 1, out _);
            }

            if (!ok)
            {
                error = "Unable to create one or more engine parts.";
            }
            return ok;
        }

        private bool TrySpawnByShortnameAtPlayer(BasePlayer player, string shortName, float forwardDistance, float upOffset, string successMessage, out string error)
        {
            return TrySpawnByShortnameAtPlayer(player, shortName, forwardDistance, upOffset, successMessage, 0, out error);
        }

        private bool TrySpawnByShortnameAtPlayer(BasePlayer player, string shortName, float forwardDistance, float upOffset, string successMessage, int fuelAmount, out string error)
        {
            error = string.Empty;
            if (!TryResolveGroundSpawnPosition(player, forwardDistance, out var basePosition, out error))
            {
                return false;
            }

            return TrySpawnByShortnameAtPosition(player, shortName, basePosition, upOffset, successMessage, fuelAmount, out error);
        }

        private bool TrySpawnByShortnameAtPosition(BasePlayer player, string shortName, Vector3 basePosition, float upOffset, string successMessage, int fuelAmount, out string error)
        {
            error = string.Empty;
            var position = basePosition + new Vector3(0f, upOffset, 0f);
            try
            {
                ConsoleSystem.Run(ConsoleSystem.Option.Server.Quiet(), $"entity.spawn {shortName} {position.x:0.00},{position.y:0.00},{position.z:0.00}");
            }
            catch (Exception ex)
            {
                error = $"Failed to spawn entity. {ex.Message}";
                return false;
            }

            if (fuelAmount > 0)
            {
                TryGiveItem(player, "lowgradefuel", Mathf.Max(0, fuelAmount), out _);
            }

            ShowEffectUi(player, "Crowd Control", string.IsNullOrWhiteSpace(successMessage) ? BuildSpawnSuccessMessage(shortName) : successMessage);
            return true;
        }

        private string BuildSpawnSuccessMessage(string shortName)
        {
            if (string.IsNullOrWhiteSpace(shortName))
            {
                return "Spawned.";
            }

            var normalized = shortName.Replace('.', ' ').Replace('_', ' ').Trim();
            var parts = normalized.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            for (var i = 0; i < parts.Length; i++)
            {
                parts[i] = parts[i].Length == 1 ? parts[i].ToUpperInvariant() : char.ToUpperInvariant(parts[i][0]) + parts[i].Substring(1);
            }

            return $"{string.Join(" ", parts)} spawned.";
        }

        private bool TrySpawnOreNodes(BasePlayer player, string nodeType, int count, out string error)
        {
            error = string.Empty;
            var spawned = 0;
            for (var i = 0; i < Mathf.Clamp(count, 1, 8); i++)
            {
                if (!TryResolveNodeSpawnPosition(player, 6f, 16f, out var position))
                {
                    continue;
                }

                var prefabPath = GetOreNodePrefabPath(nodeType);
                var entity = GameManager.server.CreateEntity(prefabPath, position, Quaternion.identity, true);
                if (entity == null)
                {
                    continue;
                }

                entity.Spawn();
                spawned++;
            }

            if (spawned <= 0)
            {
                error = "Unable to spawn ore nodes at current location. Try moving to open terrain.";
                return false;
            }

            ShowEffectUi(player, "Crowd Control", $"Spawned {spawned} node(s).");
            return true;
        }

        private string GetOreNodePrefabPath(string nodeType)
        {
            switch ((nodeType ?? string.Empty).Trim().ToLowerInvariant())
            {
                case "stone":
                    return "assets/bundled/prefabs/autospawn/resource/ores/stone-ore.prefab";
                case "metal":
                    return "assets/bundled/prefabs/autospawn/resource/ores/metal-ore.prefab";
                case "sulfur":
                    return "assets/bundled/prefabs/autospawn/resource/ores/sulfur-ore.prefab";
                default:
                    var prefabs = new[]
                    {
                        "assets/bundled/prefabs/autospawn/resource/ores/stone-ore.prefab",
                        "assets/bundled/prefabs/autospawn/resource/ores/metal-ore.prefab",
                        "assets/bundled/prefabs/autospawn/resource/ores/sulfur-ore.prefab"
                    };
                    return prefabs[UnityEngine.Random.Range(0, prefabs.Length)];
            }
        }

        private bool TryResolveNodeSpawnPosition(BasePlayer player, float minDistance, float maxDistance, out Vector3 spawnPosition)
        {
            spawnPosition = Vector3.zero;
            var center = player.transform.position;
            for (var attempt = 0; attempt < 30; attempt++)
            {
                var angle = UnityEngine.Random.Range(0f, 360f);
                var direction = Quaternion.Euler(0f, angle, 0f) * Vector3.forward;
                var distance = UnityEngine.Random.Range(minDistance, maxDistance);
                var probe = center + direction * distance;
                var groundY = TerrainMeta.HeightMap.GetHeight(probe);
                var candidate = new Vector3(probe.x, groundY + 0.05f, probe.z);
                if (!Physics.Raycast(candidate + Vector3.up * 8f, Vector3.down, out var hit, 20f) || hit.normal.y < 0.65f || Mathf.Abs(hit.point.y - groundY) > 1.25f || IsNearStructure(candidate, 2.5f))
                {
                    continue;
                }

                spawnPosition = new Vector3(hit.point.x, hit.point.y + 0.05f, hit.point.z);
                return true;
            }

            return false;
        }

        private bool IsNearStructure(Vector3 position, float radius)
        {
            foreach (var collider in Physics.OverlapSphere(position + Vector3.up * 0.8f, Mathf.Max(0.5f, radius)))
            {
                if (collider == null)
                {
                    continue;
                }

                if (collider.GetComponentInParent<BuildingBlock>() != null)
                {
                    return true;
                }

                var entity = collider.GetComponentInParent<BaseEntity>();
                var prefabName = entity?.ShortPrefabName ?? string.Empty;
                if (prefabName.IndexOf("foundation", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    prefabName.IndexOf("wall", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    prefabName.IndexOf("floor", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    prefabName.IndexOf("roof", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return true;
                }
            }

            return false;
        }

        private bool TryForcePlaceSleepingBagAtPlayer(BasePlayer player, out string error)
        {
            error = string.Empty;
            if (!CanPlaceSleepingBagAtPlayerNow(player, out error))
            {
                return false;
            }

            const string sleepingBagPrefab = "assets/prefabs/deployable/sleeping bag/sleepingbag_leather_deployed.prefab";
            var playerPos = player.transform.position;
            var basePosition = playerPos;
            var yaw = player.eyes != null ? player.eyes.rotation.eulerAngles.y : player.transform.rotation.eulerAngles.y;
            var facing = Quaternion.Euler(0f, yaw, 0f) * Vector3.forward;
            var right = Quaternion.Euler(0f, yaw, 0f) * Vector3.right;
            var probes = new[]
            {
                playerPos + (facing * -0.75f),
                playerPos + (right * 0.65f),
                playerPos + (right * -0.65f),
                playerPos + (facing * 0.65f),
                playerPos
            };

            var found = false;
            foreach (var probe in probes)
            {
                if (!TryResolveSpawnGroundY(player, probe, out var groundY))
                {
                    continue;
                }

                var candidate = new Vector3(probe.x, groundY, probe.z);
                if (!Physics.Raycast(candidate + Vector3.up * 3f, Vector3.down, out var floorHit, 8f) || floorHit.normal.y < 0.55f)
                {
                    continue;
                }

                candidate.y = floorHit.point.y;
                if (Vector3.Distance(candidate, playerPos) < 0.55f)
                {
                    continue;
                }

                basePosition = candidate;
                found = true;
                break;
            }

            if (!found)
            {
                error = "Unable to place sleeping bag safely near the player.";
                return false;
            }

            var bagEntity = GameManager.server.CreateEntity(sleepingBagPrefab, basePosition + Vector3.up * 0.02f, Quaternion.Euler(0f, yaw, 0f), true);
            if (bagEntity == null)
            {
                error = "Failed to place sleeping bag.";
                return false;
            }

            bagEntity.OwnerID = player.userID;
            if (bagEntity is SleepingBag sleepingBag)
            {
                sleepingBag.deployerUserID = player.userID;
            }

            bagEntity.Spawn();
            ShowEffectUi(player, "Crowd Control", "Sleeping bag placed at your feet.");
            return true;
        }

        private bool CanPlaceSleepingBagAtPlayerNow(BasePlayer player, out string error)
        {
            error = string.Empty;
            if (player == null || !player.IsConnected || player.IsDead())
            {
                error = "Player must be alive and connected.";
                return false;
            }

            if (!IsPlayerGrounded(player))
            {
                error = "Player must be standing still on solid ground.";
                return false;
            }

            if (player.GetParentEntity() != null || IsPlayerMounted(player))
            {
                error = "Cannot place while mounted or driving.";
                return false;
            }

            if (IsPlayerInLikelyAirState(player))
            {
                error = "Cannot place while jumping, falling, or flying.";
                return false;
            }

            return true;
        }

        private bool IsPlayerGrounded(BasePlayer player)
        {
            try
            {
                var method = player.GetType().GetMethod("IsOnGround", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                if (method?.Invoke(player, null) is bool directOnGround)
                {
                    return directOnGround;
                }
            }
            catch
            {
            }

            var modelState = GetMemberObject(player, "modelState");
            if (modelState == null)
            {
                return false;
            }

            if (TryGetBoolMember(modelState, "onGround", out var stateOnGround))
            {
                return stateOnGround;
            }

            if (TryGetBoolMember(modelState, "onground", out var legacyOnGround))
            {
                return legacyOnGround;
            }

            return false;
        }

        private bool IsPlayerMounted(BasePlayer player)
        {
            try
            {
                var method = player.GetType().GetMethod("GetMounted", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                return method != null && method.GetParameters().Length == 0 && method.Invoke(player, null) != null;
            }
            catch
            {
                return false;
            }
        }

        private bool IsPlayerInLikelyAirState(BasePlayer player)
        {
            var modelState = GetMemberObject(player, "modelState");
            if (modelState == null)
            {
                return false;
            }

            if (TryGetBoolMember(modelState, "flying", out var flying) && flying)
            {
                return true;
            }

            return TryGetBoolMember(modelState, "swimming", out var swimming) && swimming;
        }

        private bool TryResolveEnemySpawnPosition(BasePlayer player, Vector3 centerPosition, float minRadius, float maxRadius, float minSeparation, List<Vector3> occupiedPositions, out Vector3 spawnPosition)
        {
            spawnPosition = Vector3.zero;
            for (var attempt = 0; attempt < 24; attempt++)
            {
                var angle = UnityEngine.Random.Range(0f, 360f);
                var dir = Quaternion.Euler(0f, angle, 0f) * Vector3.forward;
                var radius = UnityEngine.Random.Range(Mathf.Max(0f, minRadius), Mathf.Max(minRadius + 0.1f, maxRadius));
                var probe = centerPosition + (dir * radius);
                if (!TryResolveSpawnGroundY(player, probe, out var groundY))
                {
                    continue;
                }

                var candidate = new Vector3(probe.x, groundY, probe.z);
                if (!Physics.Raycast(candidate + Vector3.up * 3f, Vector3.down, out var floorHit, 8f) || floorHit.normal.y < 0.55f)
                {
                    continue;
                }

                candidate.y = floorHit.point.y;
                if (IsEnemySpawnPositionCrowded(candidate, Mathf.Max(0.5f, minSeparation), occupiedPositions))
                {
                    continue;
                }

                spawnPosition = candidate;
                return true;
            }

            return false;
        }

        private bool IsEnemySpawnPositionCrowded(Vector3 position, float minSeparation, List<Vector3> occupiedPositions)
        {
            if (occupiedPositions != null)
            {
                foreach (var occupied in occupiedPositions)
                {
                    if (Vector3.Distance(position, occupied) < minSeparation)
                    {
                        return true;
                    }
                }
            }

            foreach (var collider in Physics.OverlapSphere(position + Vector3.up * 0.5f, minSeparation))
            {
                var entity = collider?.GetComponentInParent<BaseEntity>();
                if (entity == null || entity.IsDestroyed || entity is BasePlayer)
                {
                    continue;
                }

                if (entity is BaseCombatEntity || entity.ShortPrefabName.IndexOf("scientist", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return true;
                }
            }

            return false;
        }

        private bool TryResolveGroundSpawnPosition(BasePlayer player, float preferredDistance, out Vector3 spawnPosition, out string error)
        {
            spawnPosition = Vector3.zero;
            error = string.Empty;
            if (player?.eyes == null)
            {
                error = "Unable to spawn at current location. Try again.";
                return false;
            }

            var eyes = player.eyes.position;
            var forward = player.eyes.HeadForward();
            forward.y = 0f;
            if (forward.sqrMagnitude < 0.001f)
            {
                forward = player.transform.forward;
                forward.y = 0f;
            }

            if (forward.sqrMagnitude < 0.001f)
            {
                error = "Unable to spawn at current location. Try again.";
                return false;
            }

            forward.Normalize();
            var desired = Mathf.Clamp(preferredDistance, 5f, 14f);
            var maxFromPlayer = Mathf.Clamp(preferredDistance + 4f, 8f, 18f);
            var right = Vector3.Cross(Vector3.up, forward).normalized;
            var anchor = eyes + (forward * desired);
            if (Physics.Raycast(eyes, forward, out var lookHit, 60f))
            {
                anchor = eyes + (forward * Mathf.Clamp(Vector3.Distance(eyes, lookHit.point), 4f, maxFromPlayer));
            }

            var forwardOffsets = new[] { 0f, 1.5f, -1.5f, 3f, -3f };
            var lateralOffsets = new[] { 0f, 1.5f, -1.5f, 3f, -3f };
            foreach (var forwardOffset in forwardOffsets)
            {
                foreach (var lateralOffset in lateralOffsets)
                {
                    var probe = anchor + (forward * forwardOffset) + (right * lateralOffset);
                    if (!TryResolveSpawnGroundY(player, probe, out var groundY))
                    {
                        continue;
                    }

                    var candidate = new Vector3(probe.x, groundY, probe.z);
                    if (!Physics.Raycast(candidate + Vector3.up * 3f, Vector3.down, out var floorHit, 8f) || floorHit.normal.y < 0.55f)
                    {
                        continue;
                    }

                    candidate.y = floorHit.point.y;
                    if (Vector3.Distance(player.transform.position, candidate) < 2f || Vector3.Distance(player.transform.position, candidate) > maxFromPlayer)
                    {
                        continue;
                    }

                    var toCandidate = candidate - player.transform.position;
                    toCandidate.y = 0f;
                    if (toCandidate.sqrMagnitude > 0.001f)
                    {
                        toCandidate.Normalize();
                        if (Vector3.Dot(forward, toCandidate) < 0.15f)
                        {
                            continue;
                        }
                    }

                    spawnPosition = candidate;
                    return true;
                }
            }

            error = "Unable to spawn at current location. Try again.";
            return false;
        }

        private bool TryResolveVehicleSpawnPosition(BasePlayer player, float preferredDistance, out Vector3 spawnPosition, out string error)
        {
            spawnPosition = Vector3.zero;
            error = string.Empty;
            if (!TryResolveGroundSpawnPosition(player, preferredDistance, out var candidate, out error))
            {
                return false;
            }

            if (IsNearStructure(candidate, 4f))
            {
                error = "Cannot spawn vehicles inside or too close to player-built structures.";
                return false;
            }

            if (Physics.OverlapSphere(candidate + Vector3.up * 0.75f, 2.5f).Length > 12)
            {
                error = "Not enough clear space to spawn a vehicle here.";
                return false;
            }

            spawnPosition = candidate;
            return true;
        }

        private bool TryResolveWaterSpawnPosition(BasePlayer player, float preferredDistance, out Vector3 spawnPosition, out string error)
        {
            spawnPosition = Vector3.zero;
            error = string.Empty;
            if (player == null || !player.IsConnected)
            {
                error = "Unable to find water near the player.";
                return false;
            }

            var center = player.transform.position;
            var preferredRadius = Mathf.Clamp(preferredDistance, 8f, 18f);
            var anchor = center;
            var forward = player.eyes != null ? player.eyes.HeadForward() : player.transform.forward;
            forward.y = 0f;
            if (forward.sqrMagnitude < 0.001f)
            {
                forward = player.transform.forward;
                forward.y = 0f;
            }

            if (forward.sqrMagnitude > 0.001f)
            {
                forward.Normalize();
                anchor = center + (forward * preferredRadius);

                if (player.eyes != null && Physics.Raycast(player.eyes.position, forward, out var lookHit, 40f))
                {
                    var lookDistance = Vector3.Distance(center, lookHit.point);
                    if (lookDistance >= 6f)
                    {
                        anchor = center + (forward * Mathf.Clamp(lookDistance, 6f, 26f));
                    }
                }
            }
            else
            {
                forward = Vector3.forward;
            }

            if (IsPlayerInOrVeryNearWater(player))
            {
                var closeRadii = new[] { 6f, 8f, 10f, 12f, 14f };
                var closeAngleOffsets = new[] { 0f, 20f, -20f, 40f, -40f, 60f, -60f, 90f, -90f, 120f, -120f, 160f, -160f, 180f };
                foreach (var radius in closeRadii)
                {
                    for (var i = 0; i < closeAngleOffsets.Length; i++)
                    {
                        var direction = Quaternion.Euler(0f, closeAngleOffsets[i], 0f) * forward;
                        var probe = center + (direction * radius);
                        if (!TryScoreBoatSpawnCandidate(player, center, anchor, forward, preferredRadius, probe, out var candidate, out _))
                        {
                            continue;
                        }

                        spawnPosition = candidate;
                        return true;
                    }
                }
            }

            foreach (var probe in EnumeratePreferredWaterProbes(center, anchor))
            {
                if (!TryScoreBoatSpawnCandidate(player, center, anchor, forward, preferredRadius, probe, out var candidate, out _))
                {
                    continue;
                }

                // Preferred probes are yielded in look-priority order, so accept the first good hit.
                spawnPosition = candidate;
                return true;
            }

            var searchRadii = new[] { 8f, 10f, 12f, 14f, 16f, 18f, 20f, 22f, 24f, 26f, 28f, 30f };
            var forwardAngleOffsets = new[] { 0f, 12f, -12f, 24f, -24f, 36f, -36f, 48f, -48f, 60f, -60f, 75f, -75f, 90f, -90f };
            foreach (var radius in searchRadii)
            {
                for (var i = 0; i < forwardAngleOffsets.Length; i++)
                {
                    var direction = Quaternion.Euler(0f, forwardAngleOffsets[i], 0f) * forward;
                    var probe = center + (direction * radius);
                    if (!TryScoreBoatSpawnCandidate(player, center, anchor, forward, preferredRadius, probe, out var candidate, out _))
                    {
                        continue;
                    }

                    spawnPosition = candidate;
                    return true;
                }
            }

            error = "You must be near open water to spawn a boat.";
            return false;
        }

        private IEnumerable<Vector3> EnumeratePreferredWaterProbes(Vector3 center, Vector3 anchor)
        {
            yield return anchor;

            var forward = anchor - center;
            forward.y = 0f;
            if (forward.sqrMagnitude < 0.001f)
            {
                forward = Vector3.forward;
            }

            forward.Normalize();
            var right = Vector3.Cross(Vector3.up, forward).normalized;
            var forwardOffsets = new[] { 0f, 2f, 4f, 6f, 8f, 10f, -2f, -4f };
            var lateralOffsets = new[] { 0f, 1.5f, -1.5f, 3f, -3f, 4.5f, -4.5f, 6f, -6f };
            for (var i = 0; i < forwardOffsets.Length; i++)
            {
                for (var j = 0; j < lateralOffsets.Length; j++)
                {
                    if (forwardOffsets[i] == 0f && lateralOffsets[j] == 0f)
                    {
                        continue;
                    }

                    yield return anchor + (forward * forwardOffsets[i]) + (right * lateralOffsets[j]);
                }
            }
        }

        private bool TryScoreBoatSpawnCandidate(BasePlayer player, Vector3 center, Vector3 anchor, Vector3 forward, float preferredRadius, Vector3 probe, out Vector3 candidate, out float score)
        {
            candidate = Vector3.zero;
            score = 0f;
            if (!TryGetWaterSurfaceY(probe, out var waterY))
            {
                return false;
            }

            candidate = new Vector3(probe.x, waterY, probe.z);
            var candidateDistance = Vector3.Distance(center, candidate);
            if (candidateDistance < 4f || candidateDistance > 32f)
            {
                return false;
            }

            if (IsNearStructure(candidate, 3f))
            {
                return false;
            }

            var toCandidate = candidate - center;
            toCandidate.y = 0f;
            if (toCandidate.sqrMagnitude > 0.001f)
            {
                toCandidate.Normalize();
                var facingDot = Vector3.Dot(forward, toCandidate);
                score += Mathf.Max(-0.5f, facingDot) * 30f;
            }

            var waterDepth = 0.35f;
            if (TryResolveSpawnGroundY(player, probe, out var groundY))
            {
                waterDepth = waterY - groundY;
                if (waterDepth < 0.15f)
                {
                    return false;
                }
            }

            var nearbyWaterSupport = CountNearbyWaterNeighbors(candidate, waterY);
            if (nearbyWaterSupport < 2)
            {
                return false;
            }

            var anchorDistance = Vector3.Distance(anchor, candidate);
            var depthScore = Mathf.Clamp(waterDepth, 0.15f, 3f) * 12f;
            var supportScore = nearbyWaterSupport * 10f;
            var anchorScore = 22f - Mathf.Min(22f, anchorDistance);
            var radiusScore = 18f - Mathf.Abs(candidateDistance - preferredRadius);
            score = depthScore + supportScore + anchorScore + radiusScore;
            return true;
        }

        private int CountNearbyWaterNeighbors(Vector3 candidate, float waterY)
        {
            var supports = 0;
            foreach (var offset in new[]
            {
                new Vector3(2f, 0f, 0f),
                new Vector3(-2f, 0f, 0f),
                new Vector3(0f, 0f, 2f),
                new Vector3(0f, 0f, -2f),
                new Vector3(1.5f, 0f, 1.5f),
                new Vector3(-1.5f, 0f, 1.5f),
                new Vector3(1.5f, 0f, -1.5f),
                new Vector3(-1.5f, 0f, -1.5f)
            })
            {
                if (!TryGetWaterSurfaceY(candidate + offset, out var nearbyWaterY))
                {
                    continue;
                }

                if (Mathf.Abs(nearbyWaterY - waterY) <= 1.5f)
                {
                    supports++;
                }
            }

            return supports;
        }

        private bool IsPlayerInOrVeryNearWater(BasePlayer player)
        {
            if (player == null)
            {
                return false;
            }

            var modelState = GetMemberObject(player, "modelState");
            if (TryGetBoolMember(modelState, "swimming", out var swimming) && swimming)
            {
                return true;
            }

            var position = player.transform.position;
            if (!TryGetWaterSurfaceY(position, out var waterY))
            {
                return false;
            }

            if (!TryResolveSpawnGroundY(player, position, out var groundY))
            {
                return Mathf.Abs(waterY - position.y) <= 1.5f;
            }

            return (waterY - groundY) >= 0.2f && Mathf.Abs(waterY - position.y) <= 2f;
        }

        private bool TryGetWaterSurfaceY(Vector3 position, out float waterY)
        {
            waterY = 0f;
            try
            {
                var flags = BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;
                var waterMap = typeof(TerrainMeta).GetProperty("WaterMap", flags)?.GetValue(null, null) ??
                    typeof(TerrainMeta).GetField("WaterMap", flags)?.GetValue(null);
                if (waterMap == null)
                {
                    return false;
                }

                foreach (var methodName in new[] { "GetHeight", "GetHeightFast" })
                {
                    var method = waterMap.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null, new[] { typeof(Vector3) }, null);
                    if (method == null)
                    {
                        continue;
                    }

                    var value = method.Invoke(waterMap, new object[] { position });
                    if (value is float directFloat)
                    {
                        waterY = directFloat;
                        return waterY > -1000f;
                    }

                    if (value is double directDouble)
                    {
                        waterY = (float)directDouble;
                        return waterY > -1000f;
                    }
                }
            }
            catch
            {
            }

            return false;
        }

        private bool TryResolveSpawnGroundY(BasePlayer player, Vector3 probe, out float groundY)
        {
            groundY = probe.y;
            var playerPos = player.transform.position;
            var playerY = playerPos.y;
            var terrainAtPlayer = TerrainMeta.HeightMap != null ? TerrainMeta.HeightMap.GetHeight(playerPos) : playerY;
            var isLikelyUnderground = playerY + 2f < terrainAtPlayer;

            foreach (var origin in new[] { new Vector3(probe.x, playerY + 2.5f, probe.z), new Vector3(probe.x, playerY + 6f, probe.z) })
            {
                if (Physics.Raycast(origin, Vector3.down, out var sameLevelHit, 18f) && sameLevelHit.normal.y >= 0.55f)
                {
                    groundY = sameLevelHit.point.y;
                    return true;
                }
            }

            if (Physics.Raycast(new Vector3(probe.x, playerY + 30f, probe.z), Vector3.down, out var broadHit, 80f) && broadHit.normal.y >= 0.55f)
            {
                groundY = broadHit.point.y;
                return true;
            }

            if (TerrainMeta.HeightMap != null && !isLikelyUnderground)
            {
                groundY = TerrainMeta.HeightMap.GetHeight(probe);
                return true;
            }

            return false;
        }

        private bool TryRunHypeTrainTestEffect(BasePlayer player, JObject requestPayload, JObject effectPayload, out string error)
        {
            error = string.Empty;
            if (player == null || !player.IsConnected || player.IsDead())
            {
                error = "Player must be alive and connected.";
                return false;
            }

            var direction = player.eyes != null ? player.eyes.rotation * Vector3.forward : player.transform.forward;
            direction.y = 0f;
            if (direction.sqrMagnitude < 0.01f)
            {
                direction = Vector3.forward;
            }
            direction.Normalize();

            var spawnProbe = player.transform.position + (direction * 10f);
            if (!TryResolveSpawnGroundY(player, spawnProbe, out var spawnY))
            {
                if (!TryResolveGroundSpawnPosition(player, 16f, out spawnProbe, out error))
                {
                    return false;
                }
                spawnY = spawnProbe.y;
            }

            var spawnPosition = new Vector3(spawnProbe.x, spawnY + 0.12f, spawnProbe.z);
            var spawnRotation = Quaternion.LookRotation(direction, Vector3.up);
            if (!TrySpawnHypeTrainEntity(spawnPosition, spawnRotation, out var trainEntity, out var trainLabel))
            {
                error = "Failed to spawn hype train test entity";
                return false;
            }

            var rideSeconds = 30f;
            Oxide.Plugins.Timer travelTick = null;
            if (trainEntity != null && !trainEntity.IsDestroyed)
            {
                travelTick = timer.Every(0.2f, () =>
                {
                    if (trainEntity == null || trainEntity.IsDestroyed)
                    {
                        travelTick?.Destroy();
                        return;
                    }

                    var nextPos = trainEntity.transform.position + (direction * (11.5f * 0.2f));
                    if (TryResolveSpawnGroundY(player, nextPos, out var nextY))
                    {
                        nextPos.y = nextY + 0.12f;
                    }

                    trainEntity.transform.position = nextPos;
                    trainEntity.SendNetworkUpdateImmediate();
                });
            }

            timer.Once(rideSeconds, () =>
            {
                travelTick?.Destroy();
                if (trainEntity != null && !trainEntity.IsDestroyed)
                {
                    trainEntity.Kill();
                }
            });

            var riders = GetHypeTrainTestRiderNames(requestPayload, effectPayload);
            ShowEffectUi(player, "Hype Train", $"Incoming ({trainLabel})!");
            TryPlayHypeTrainSound(player);
            TryRunHypeTrainWorldFx(player, spawnPosition);
            for (var i = 0; i < riders.Count; i++)
            {
                var localIndex = i;
                timer.Once(1.5f + (localIndex * 2f), () =>
                {
                    if (player != null && player.IsConnected)
                    {
                        ShowEffectUi(player, "Hype Train", $"Rider {localIndex + 1}: {riders[localIndex]}");
                    }
                });
            }

            return true;
        }

        private bool TrySpawnHypeTrainEntity(Vector3 position, Quaternion rotation, out BaseEntity entity, out string label)
        {
            entity = null;
            label = "train";
            var candidates = new[]
            {
                new { Prefab = "assets/content/vehicles/trains/locomotive/locomotive.entity.prefab", Name = "locomotive" },
                new { Prefab = "assets/content/vehicles/trains/workcart/workcart.entity.prefab", Name = "work cart" },
                new { Prefab = "assets/content/vehicles/trains/workcart/workcart_aboveground.entity.prefab", Name = "work cart" },
                new { Prefab = "assets/content/vehicles/trains/wagons/trainwagon_a.entity.prefab", Name = "wagon" }
            };

            foreach (var candidate in candidates)
            {
                try
                {
                    var created = GameManager.server.CreateEntity(candidate.Prefab, position, rotation, true);
                    if (created == null)
                    {
                        continue;
                    }

                    created.Spawn();
                    entity = created;
                    label = candidate.Name;
                    return true;
                }
                catch
                {
                }
            }

            foreach (var shortName in new[] { "locomotive", "workcart", "trainwagon_a" })
            {
                try
                {
                    ConsoleSystem.Run(ConsoleSystem.Option.Server.Quiet(), $"entity.spawn {shortName} {position.x:0.00},{position.y:0.00},{position.z:0.00}");
                    label = shortName;
                    return true;
                }
                catch
                {
                }
            }

            return false;
        }

        private List<string> GetHypeTrainTestRiderNames(JObject requestPayload, JObject effectPayload)
        {
            var riders = new List<string>();
            if (effectPayload != null)
            {
                AppendNamesFromToken(riders, effectPayload["hypeUsers"]);
                AppendNamesFromToken(riders, effectPayload["users"]);
                AppendNamesFromToken(riders, effectPayload["contributors"]);
            }

            if (requestPayload != null)
            {
                AppendNamesFromToken(riders, requestPayload["hypeUsers"]);
                AppendNamesFromToken(riders, requestPayload["users"]);
                AppendNamesFromToken(riders, requestPayload["contributors"]);
            }

            if (riders.Count == 0)
            {
                riders.AddRange(new[] { "Conductor_Jax", "SubTrainRider", "BitBomber", "GiftDropper" });
            }

            if (riders.Count > 8)
            {
                riders.RemoveRange(8, riders.Count - 8);
            }

            return riders;
        }

        private void AppendNamesFromToken(List<string> riders, JToken token)
        {
            if (!(token is JArray arr))
            {
                return;
            }

            foreach (var item in arr)
            {
                var name = item?.ToString()?.Trim();
                if (!string.IsNullOrEmpty(name))
                {
                    riders.Add(name);
                }
            }
        }

        private void TryPlayHypeTrainSound(BasePlayer player)
        {
            try
            {
                player.SendConsoleCommand("playsound", "assets/bundled/prefabs/fx/notice/item.select.fx.prefab");
                player.SendConsoleCommand("client.playsound", "assets/bundled/prefabs/fx/notice/item.select.fx.prefab");
            }
            catch
            {
            }
        }

        private void TryRunHypeTrainWorldFx(BasePlayer player, Vector3 position)
        {
            try
            {
                ConsoleSystem.Run(ConsoleSystem.Option.Server.Quiet(), $"effect.run assets/bundled/prefabs/fx/notice/item.select.fx.prefab {position.x:0.00},{position.y:0.00},{position.z:0.00}");
            }
            catch
            {
            }
        }

        private void ShowEffectUi(BasePlayer player, string title, string message)
        {
            if (player == null || !player.IsConnected)
            {
                return;
            }

            SendToast(player, 0, string.IsNullOrWhiteSpace(title) ? message : $"{title}: {message}");
        }

        private void ShowErrorUi(BasePlayer player, string message)
        {
            if (player == null || !player.IsConnected || string.IsNullOrWhiteSpace(message))
            {
                return;
            }

            SendToast(player, 1, message);
        }

        private void SendToast(BasePlayer player, int style, string text)
        {
            if (player != null && player.IsConnected && !string.IsNullOrWhiteSpace(text))
            {
                player.SendConsoleCommand("gametip.showtoast", style, text);
            }
        }

        private object GetMemberObject(object target, string memberName)
        {
            if (target == null || string.IsNullOrEmpty(memberName))
            {
                return null;
            }

            var flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
            return target.GetType().GetProperty(memberName, flags)?.GetValue(target, null) ??
                target.GetType().GetField(memberName, flags)?.GetValue(target);
        }

        private bool TryGetBoolMember(object target, string memberName, out bool value)
        {
            value = false;
            if (target == null || string.IsNullOrEmpty(memberName))
            {
                return false;
            }

            var flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
            var prop = target.GetType().GetProperty(memberName, flags);
            if (prop != null && prop.CanRead && prop.PropertyType == typeof(bool) && prop.GetValue(target, null) is bool propValue)
            {
                value = propValue;
                return true;
            }

            var field = target.GetType().GetField(memberName, flags);
            if (field != null && field.FieldType == typeof(bool) && field.GetValue(target) is bool fieldValue)
            {
                value = fieldValue;
                return true;
            }

            return false;
        }

        private bool TrySetBoolMember(object target, string memberName, bool value)
        {
            if (target == null || string.IsNullOrEmpty(memberName))
            {
                return false;
            }

            var flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
            var prop = target.GetType().GetProperty(memberName, flags);
            if (prop != null && prop.CanWrite && prop.PropertyType == typeof(bool))
            {
                prop.SetValue(target, value, null);
                return true;
            }

            var field = target.GetType().GetField(memberName, flags);
            if (field != null && field.FieldType == typeof(bool))
            {
                field.SetValue(target, value);
                return true;
            }

            return false;
        }

        private void LogVerbose(string message)
        {
            if (VerboseLogging)
            {
                Puts($"[Verbose] {message}");
            }
        }

        private int GetTimedEffectDurationSeconds(JObject effectPayload, string effectId)
        {
            var durationSeconds = effectPayload?.Value<int?>("duration") ?? 0;
            if (durationSeconds > 0)
            {
                return durationSeconds;
            }

            var durationToken = effectPayload?["duration"];
            if (durationToken is JObject durationObj)
            {
                durationSeconds = durationObj.Value<int?>("seconds") ?? 0;
                if (durationSeconds > 0)
                {
                    return durationSeconds;
                }

                var durationValue = durationObj.Value<string>("value");
                if (!string.IsNullOrWhiteSpace(durationValue) && TimeSpan.TryParse(durationValue, out var parsedDuration))
                {
                    return Mathf.Max(1, Mathf.RoundToInt((float)parsedDuration.TotalSeconds));
                }
            }

            switch (NormalizeEffectId(effectId))
            {
                case "player_handcuff":
                    return 12;
                case "player_damage_over_time":
                case "player_heal_over_time":
                    return 15;
                default:
                    return IsTimedEffect(effectId) ? 20 : 0;
            }
        }

        private BasePlayer FindPlayerBySteamId(string steamId)
        {
            if (!ulong.TryParse(steamId, out var id))
            {
                return null;
            }

            return BasePlayer.FindByID(id) ?? BasePlayer.FindSleeping(id);
        }
    }
}
