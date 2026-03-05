using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Oxide.Core;
using Oxide.Core.Plugins;
using UnityEngine;

namespace Oxide.Plugins
{
    [Info("CrowdControl", "jaku", "0.1.0")]
    [Description("Crowd Control integration for Rust with auth, PubSub, and permission-based access controls.")]
    public class CrowdControlRust : RustPlugin
    {
        private const string PermUse = "crowdcontrolrust.use";
        private const bool StopSessionOnDisconnect = true;
        private const int SocketKeepAliveMinutes = 5;
        private const bool StopSessionOnUnload = true;
        private const bool AutoStartSessionAfterAuth = true;
        private const float ReconnectDelaySeconds = 5f;
        private const int MaxAuthQueue = 50;
        private const string GamePackId = "RustServer";
        private const string PubSubWebSocketUrl = "wss://pubsub.crowdcontrol.live/";
        private const string OpenApiUrl = "https://openapi.crowdcontrol.live";
        private const string UserAgent = "CrowdControl/0.1.0";
        private const bool VerboseLogging = true;
        private const int CustomEffectsPerOperation = 20;
        private const string EffectPricingFileName = "CrowdControl-Effects.json";
        private const float ExternalEffectPendingTimeoutSeconds = 15f;
        private const string ExternalEffectHookName = "OnCrowdControlEffect";
        private static readonly List<string> Scopes = new List<string>
        {
            "profile:read",
            "session:write",
            "session:control",
            "custom-effects:read",
            "custom-effects:write"
        };
        private static readonly List<string> Packs = new List<string> { "RustServer" };

        private PluginConfig _config;
        private StoredData _data;

        private readonly object _socketSync = new object();
        private readonly SemaphoreSlim _sendSemaphore = new SemaphoreSlim(1, 1);
        private readonly Queue<string> _pendingAuthRequests = new Queue<string>();
        private readonly Dictionary<string, string> _authCodeToSteamId = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, TimedEffectState> _activeTimedEffects = new Dictionary<string, TimedEffectState>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, MovementFreezeState> _movementFreezeTimers = new Dictionary<string, MovementFreezeState>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, Oxide.Plugins.Timer> _activeHandcuffTimers = new Dictionary<string, Oxide.Plugins.Timer>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, Oxide.Plugins.Timer> _activePowerModeTimers = new Dictionary<string, Oxide.Plugins.Timer>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, Oxide.Plugins.Timer> _activeBurnTimers = new Dictionary<string, Oxide.Plugins.Timer>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _godModeSteamIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _flyModeSteamIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _sessionStartInProgress = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, DateTime> _lastCustomEffectsSyncUtc = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, Vector3> _lastDeathPositionBySteamId = new Dictionary<string, Vector3>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, DateTime> _recentHandledRequestIds = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, MovementModifierState> _activeMovementModifiers = new Dictionary<string, MovementModifierState>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, EffectRetryState> _activeEffectRetries = new Dictionary<string, EffectRetryState>(StringComparer.OrdinalIgnoreCase);
        private readonly object _requestIdSync = new object();
        private readonly object _externalEffectsSync = new object();
        private readonly Dictionary<string, ExternalEffectDefinition> _externalEffectsById = new Dictionary<string, ExternalEffectDefinition>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, HashSet<string>> _externalEffectIdsByProvider = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, ExternalEffectPendingRequest> _externalPendingRequests = new Dictionary<string, ExternalEffectPendingRequest>(StringComparer.OrdinalIgnoreCase);

        private ClientWebSocket _socket;
        private CancellationTokenSource _socketCts;
        private Oxide.Plugins.Timer _reconnectTimer;
        private Oxide.Plugins.Timer _heartbeatTimer;
        private bool _isSocketConnecting;
        private volatile bool _isUnloading;
        private DateTime _lastSocketConnectUtc = DateTime.MinValue;
        private DateTime _lastSessionDisconnectToastUtc = DateTime.MinValue;

        #region Models

        private sealed class PluginConfig
        {
            // Default IDs, you can get unique ones for your own server is you wish. Reach out to support@crowdcontrol.live for more info.
            [JsonProperty("app_id")]
            public string AppId { get; set; } = "ccaid-01kjfx91h9cf0mqa7j2z3tjmwx";

            [JsonProperty("app_secret")]
            public string AppSecret { get; set; } = "b9b187f3026b70aad6017dbe41a62445811acc4bb2d3b4c092e445b11ee72fa3";

            [JsonProperty("allow_all_users_without_permission")]
            public bool AllowAllUsersWithoutPermission { get; set; } = true;

            [JsonProperty("session_rules")]
            public SessionRulesConfig SessionRules { get; set; } = new SessionRulesConfig();

            [JsonProperty("retry_policy")]
            public RetryPolicyConfig RetryPolicy { get; set; } = new RetryPolicyConfig();
        }

        private sealed class SessionRulesConfig
        {
            [JsonProperty("enable_integration_triggers")]
            public bool EnableIntegrationTriggers { get; set; } = true;

            [JsonProperty("enable_price_change")]
            public bool EnablePriceChange { get; set; } = true;

            [JsonProperty("disable_test_effects")]
            public bool DisableTestEffects { get; set; } = false;

            [JsonProperty("disable_custom_effects_sync")]
            public bool DisableCustomEffectsSync { get; set; } = false;
        }

        private sealed class RetryPolicyConfig
        {
            [JsonProperty("enabled")]
            public bool Enabled { get; set; } = true;

            [JsonProperty("default")]
            public RetrySourcePolicyConfig Default { get; set; } = new RetrySourcePolicyConfig
            {
                MaxAttempts = 25,
                MaxDurationSeconds = 60,
                RetryIntervalSeconds = 2.4f
            };

            [JsonProperty("twitch")]
            public RetrySourcePolicyConfig Twitch { get; set; } = new RetrySourcePolicyConfig
            {
                MaxAttempts = 25,
                MaxDurationSeconds = 60,
                RetryIntervalSeconds = 2.4f
            };

            [JsonProperty("tiktok")]
            public RetrySourcePolicyConfig Tiktok { get; set; } = new RetrySourcePolicyConfig
            {
                MaxAttempts = 250,
                MaxDurationSeconds = 300,
                RetryIntervalSeconds = 1.2f
            };
        }

        private sealed class RetrySourcePolicyConfig
        {
            [JsonProperty("max_attempts")]
            public int MaxAttempts { get; set; } = 25;

            [JsonProperty("max_duration_seconds")]
            public int MaxDurationSeconds { get; set; } = 60;

            [JsonProperty("retry_interval_seconds")]
            public float RetryIntervalSeconds { get; set; } = 2.4f;
        }

        private sealed class EffectPricingScaleConfig
        {
            [JsonProperty("percent")]
            public float Percent { get; set; } = 1f;

            [JsonProperty("duration")]
            public float Duration { get; set; } = 1f;

            [JsonProperty("inactive")]
            public bool Inactive { get; set; } = true;
        }

        private sealed class EffectPricingEntry
        {
            [JsonProperty("effectID")]
            public string EffectId { get; set; }

            [JsonProperty("price")]
            public int Price { get; set; }

            [JsonProperty("sessionCooldown")]
            public int SessionCooldown { get; set; }

            [JsonProperty("userCooldown")]
            public int UserCooldown { get; set; }

            [JsonProperty("duration")]
            public JObject Duration { get; set; }

            [JsonProperty("inactive")]
            public bool Inactive { get; set; } = false;

            [JsonProperty("scale")]
            public EffectPricingScaleConfig Scale { get; set; } = new EffectPricingScaleConfig();
        }

        private sealed class ExternalEffectDefinition
        {
            public string ProviderName;
            public string EffectId;
            public JObject MenuEffect;
        }

        private sealed class ExternalEffectPendingRequest
        {
            public string RequestId;
            public string EffectId;
            public string ProviderName;
            public string SteamId;
            public string Token;
            public Oxide.Plugins.Timer TimeoutTimer;
        }

        private sealed class StoredData
        {
            [JsonProperty("player_sessions")]
            public Dictionary<string, PlayerSessionState> PlayerSessions { get; set; } = new Dictionary<string, PlayerSessionState>();

            [JsonProperty("session_rules_signature")]
            public string SessionRulesSignature { get; set; } = string.Empty;
        }

        private sealed class PlayerSessionState
        {
            [JsonProperty("steam_id")]
            public string SteamId { get; set; }

            [JsonProperty("token")]
            public string Token { get; set; }

            [JsonProperty("ccuid")]
            public string CcUid { get; set; }

            [JsonProperty("origin_id")]
            public string OriginId { get; set; }

            [JsonProperty("profile_type")]
            public string ProfileType { get; set; }

            [JsonProperty("display_name")]
            public string DisplayName { get; set; }

            [JsonProperty("token_expiry_unix")]
            public long TokenExpiryUnix { get; set; }

            [JsonProperty("game_session_id")]
            public string GameSessionId { get; set; }

            [JsonProperty("authenticated_at_unix")]
            public long AuthenticatedAtUnix { get; set; }
        }

        private sealed class TimedEffectState
        {
            public string RequestId;
            public string EffectId;
            public string SteamId;
            public string Token;
            public DateTime EndUtc;
            public bool IsPaused;
            public long RemainingMs;
            public Oxide.Plugins.Timer TickTimer;
        }

        private sealed class MovementFreezeState
        {
            public Vector3 AnchorPosition;
            public Oxide.Plugins.Timer EnforceTimer;
            public Oxide.Plugins.Timer EndTimer;
        }

        private sealed class MovementModifierState
        {
            public Oxide.Plugins.Timer EndTimer;
            public Oxide.Plugins.Timer TickTimer;
            public readonly Dictionary<string, float> PlayerOriginals = new Dictionary<string, float>(StringComparer.OrdinalIgnoreCase);
            public readonly Dictionary<string, float> MovementOriginals = new Dictionary<string, float>(StringComparer.OrdinalIgnoreCase);
            public string Label;
            public float? SpeedMultiplier;
            public float? GravityMultiplier;
            public float? JumpMultiplier;
            public Vector3 LastPosition;
            public bool LastGrounded;
            public bool UsingFallback;
        }

        private sealed class EffectRetryState
        {
            public string RequestId;
            public string EffectId;
            public string SteamId;
            public string Token;
            public JObject Payload;
            public JObject Effect;
            public int DurationSeconds;
            public string SourceType;
            public int AttemptsMade;
            public int MaxAttempts;
            public TimeSpan MaxDuration;
            public float RetryIntervalSeconds;
            public DateTime FirstFailureUtc;
            public string LastError;
            public Oxide.Plugins.Timer RetryTimer;
        }

        private sealed class DecodedJwt
        {
            public string CcUid;
            public string OriginId;
            public string ProfileType;
            public string Name;
            public long Exp;
        }

        #endregion

        #region Oxide Lifecycle

        private void Init()
        {
            _isUnloading = false;
            permission.RegisterPermission(PermUse, this);
            _data = Interface.Oxide.DataFileSystem.ReadObject<StoredData>(Name) ?? new StoredData();
            Puts("CrowdControlRust initialized.");
        }

        private void OnServerInitialized()
        {
            ValidateConfig();
            SaveConfig();
            LoadOrSeedEffectPricingEntries();
            FireAndForget(RefreshSessionsAfterReloadAsync(), "refresh sessions after reload");
            FireAndForget(ApplySessionRulesAsync(), "apply session rules");
            FireAndForget(EnsureSocketConnectedAsync(), "initial socket connect");
            StartSocketHeartbeat();
            FireAndForget(UpdateTeleportAvailabilityAsync(), "initial teleport availability report");
        }

        private void Unload()
        {
            _isUnloading = true;

            if (_config != null && StopSessionOnUnload)
            {
                foreach (var kvp in _data.PlayerSessions)
                {
                    var session = kvp.Value;
                    if (!string.IsNullOrEmpty(session?.Token))
                    {
                        FireAndForget(StopGameSessionAsync(session), "stop game session on unload");
                    }
                }
            }

            foreach (var kvp in _activeTimedEffects)
            {
                kvp.Value.TickTimer?.Destroy();
            }
            _activeTimedEffects.Clear();

            foreach (var kvp in _activeEffectRetries)
            {
                kvp.Value?.RetryTimer?.Destroy();
            }
            _activeEffectRetries.Clear();

            lock (_externalEffectsSync)
            {
                foreach (var kvp in _externalPendingRequests)
                {
                    kvp.Value?.TimeoutTimer?.Destroy();
                }
                _externalPendingRequests.Clear();
                _externalEffectsById.Clear();
                _externalEffectIdsByProvider.Clear();
            }

            foreach (var kvp in _movementFreezeTimers)
            {
                kvp.Value?.EnforceTimer?.Destroy();
                kvp.Value?.EndTimer?.Destroy();
            }
            _movementFreezeTimers.Clear();

            foreach (var kvp in _activeHandcuffTimers)
            {
                kvp.Value?.Destroy();
            }
            _activeHandcuffTimers.Clear();

            foreach (var kvp in _activePowerModeTimers)
            {
                kvp.Value?.Destroy();
            }
            _activePowerModeTimers.Clear();

            foreach (var kvp in _activeBurnTimers)
            {
                kvp.Value?.Destroy();
            }
            _activeBurnTimers.Clear();
            _godModeSteamIds.Clear();
            _flyModeSteamIds.Clear();

            foreach (var kvp in _activeMovementModifiers)
            {
                kvp.Value?.EndTimer?.Destroy();
                kvp.Value?.TickTimer?.Destroy();
            }
            _activeMovementModifiers.Clear();

            _reconnectTimer?.Destroy();
            _reconnectTimer = null;
            _heartbeatTimer?.Destroy();
            _heartbeatTimer = null;

            _socketCts?.Cancel();
            _socket?.Abort();
            _socket?.Dispose();
            _socket = null;
            _socketCts = null;

            SaveData();
        }

        private void OnServerSave()
        {
            SaveData();
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
            EndTemporaryPowerMode(player.UserIDString, restoreFly: false);
            EndMovementModifier(player.UserIDString, showUi: false);
            EndBurnEffect(player.UserIDString);

            if (StopSessionOnDisconnect &&
                _data.PlayerSessions.TryGetValue(player.UserIDString, out var session) &&
                !string.IsNullOrEmpty(session?.Token))
            {
                FireAndForget(StopGameSessionAsync(session, player.UserIDString), "stop session on disconnect");
            }

            FireAndForget(UpdateTeleportAvailabilityAsync(), "teleport availability on disconnect");
        }

        private void OnPlayerConnected(BasePlayer player)
        {
            if (player == null)
            {
                return;
            }
            FireAndForget(UpdateTeleportAvailabilityAsync(), "teleport availability on connect");
        }

        private void OnPluginUnloaded(Plugin plugin)
        {
            if (plugin == null || string.IsNullOrWhiteSpace(plugin.Name))
            {
                return;
            }

            CC_UnregisterEffects(plugin.Name);
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

            // Explicitly nullify all incoming damage while god mode is active.
            hitInfo.damageTypes?.ScaleAll(0f);
            return null;
        }

        #endregion

        #region Commands

        [ChatCommand("cc")]
        private void CommandCrowdControl(BasePlayer player, string command, string[] args)
        {
            HandleCrowdControlCommand(player, args);
        }

        [ChatCommand("crowdcontrol")]
        private void CommandCrowdControlLong(BasePlayer player, string command, string[] args)
        {
            HandleCrowdControlCommand(player, args);
        }

        private void HandleCrowdControlCommand(BasePlayer player, string[] args)
        {
            if (player == null)
            {
                return;
            }

            if (args.Length == 0)
            {
                SendHelp(player);
                return;
            }

            var sub = args[0].ToLowerInvariant();
            switch (sub)
            {
                case "help":
                    SendHelp(player);
                    break;
                case "link":
                case "auth":
                    HandleAuthCommand(player);
                    break;
                case "unlink":
                    HandleLogoutCommand(player);
                    break;
                case "status":
                    HandleStatusCommand(player);
                    break;
                case "restart":
                    HandleRestartSessionCommand(player);
                    break;
                case "logout":
                    HandleLogoutCommand(player);
                    break;
                default:
                    SendHelp(player);
                    break;
            }
        }

        private void SendHelp(BasePlayer player)
        {
            player.ConsoleMessage("[CrowdControl] Commands:");
            player.ConsoleMessage("[CrowdControl] /crowdcontrol link - Link your Crowd Control account");
            player.ConsoleMessage("[CrowdControl] /crowdcontrol unlink - Unlink your Crowd Control account");
            player.ConsoleMessage("[CrowdControl] /crowdcontrol status - Show auth/session status");
            player.ConsoleMessage("[CrowdControl] /crowdcontrol restart - Restart game session using saved token");
            player.ConsoleMessage("[CrowdControl] Alias: /cc <same_subcommand>");
            ShowEffectUi(player, "Crowd Control", "Help sent to F1 console.");
        }

        private void HandleAuthCommand(BasePlayer player)
        {
            if (!CanUsePlugin(player))
            {
                return;
            }

            if (_pendingAuthRequests.Count >= MaxAuthQueue)
            {
                ShowEffectUi(player, "Crowd Control", "Auth queue is currently full. Try again shortly.");
                return;
            }

            lock (_pendingAuthRequests)
            {
                _pendingAuthRequests.Enqueue(player.UserIDString);
            }

            FireAndForget(EnsureSocketConnectedAsync(), "auth command socket ensure");
            FireAndForget(SendGenerateAuthCodeAsync(), "send generate auth code");
            ShowEffectUi(player, "Crowd Control", "Check console (F1) for instructions.");
        }

        private void HandleStatusCommand(BasePlayer player)
        {
            if (!CanUsePlugin(player))
            {
                return;
            }

            if (!_data.PlayerSessions.TryGetValue(player.UserIDString, out var session) || string.IsNullOrEmpty(session.Token))
            {
                ShowEffectUi(player, "Crowd Control", "No Crowd Control session found. Run /cc link to connect.");
                return;
            }

            var expires = DateTimeOffset.FromUnixTimeSeconds(session.TokenExpiryUnix).UtcDateTime;
            ShowEffectUi(player, "Crowd Control", $"Authenticated as {session.DisplayName} ({session.CcUid}).");
            ShowEffectUi(player, "Crowd Control", $"Token expires (UTC): {expires:O}");
            ShowEffectUi(player, "Crowd Control", $"Game Session ID: {(string.IsNullOrEmpty(session.GameSessionId) ? "none" : session.GameSessionId)}");
        }

        private void HandleRestartSessionCommand(BasePlayer player)
        {
            if (!CanUsePlugin(player))
            {
                return;
            }

            if (!_data.PlayerSessions.TryGetValue(player.UserIDString, out var session) || string.IsNullOrEmpty(session?.Token))
            {
                ShowEffectUi(player, "Crowd Control", "No saved Crowd Control token. Run /cc link first.");
                return;
            }

            FireAndForget(RestartGameSessionAsync(session, player.UserIDString), "manual session restart");
            ShowEffectUi(player, "Crowd Control", "Restarting Crowd Control session...");
        }

        private void HandleLogoutCommand(BasePlayer player)
        {
            if (!CanUsePlugin(player))
            {
                return;
            }

            if (!_data.PlayerSessions.TryGetValue(player.UserIDString, out var session))
            {
                ShowEffectUi(player, "Crowd Control", "No active auth state to clear.");
                return;
            }

            FireAndForget(StopGameSessionAsync(session), "logout stop session");
            _data.PlayerSessions.Remove(player.UserIDString);
            SaveData();
            ShowEffectUi(player, "Crowd Control", "Crowd Control credentials removed.");
        }

        #endregion

        #region External Effect API

        public object CC_RegisterEffects(string providerName, object effectsPayload)
        {
            if (string.IsNullOrWhiteSpace(providerName))
            {
                return false;
            }

            var parsed = ParseExternalEffectsPayload(effectsPayload);
            if (parsed == null || parsed.Count == 0)
            {
                return false;
            }

            var normalizedProvider = providerName.Trim();
            var registered = 0;
            lock (_externalEffectsSync)
            {
                if (!_externalEffectIdsByProvider.TryGetValue(normalizedProvider, out var providerEffectIds))
                {
                    providerEffectIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    _externalEffectIdsByProvider[normalizedProvider] = providerEffectIds;
                }

                for (var i = 0; i < parsed.Count; i++)
                {
                    var item = parsed[i];
                    var effectId = item.Value<string>("effectID");
                    if (string.IsNullOrWhiteSpace(effectId))
                    {
                        continue;
                    }

                    effectId = NormalizeEffectId(effectId);
                    var menuObj = new JObject
                    {
                        ["name"] = item.Value<string>("name") ?? effectId,
                        ["description"] = item.Value<string>("description") ?? "External effect.",
                        ["price"] = Math.Max(0, item.Value<int?>("price") ?? 0)
                    };

                    var duration = item["duration"] as JObject;
                    if (duration != null)
                    {
                        menuObj["duration"] = (JObject)duration.DeepClone();
                    }

                    _externalEffectsById[effectId] = new ExternalEffectDefinition
                    {
                        ProviderName = normalizedProvider,
                        EffectId = effectId,
                        MenuEffect = menuObj
                    };
                    providerEffectIds.Add(effectId);
                    registered++;
                }
            }

            if (registered > 0)
            {
                LogVerbose($"Registered {registered} external effect(s) from provider={normalizedProvider}.");
                FireAndForget(SyncCustomEffectsForAllSessionsAsync(), "external effects register sync");
            }

            return registered > 0;
        }

        public object CC_UnregisterEffects(string providerName)
        {
            if (string.IsNullOrWhiteSpace(providerName))
            {
                return false;
            }

            var normalizedProvider = providerName.Trim();
            var removed = 0;
            lock (_externalEffectsSync)
            {
                if (!_externalEffectIdsByProvider.TryGetValue(normalizedProvider, out var ids))
                {
                    return false;
                }

                foreach (var effectId in ids)
                {
                    if (_externalEffectsById.Remove(effectId))
                    {
                        removed++;
                    }
                }

                _externalEffectIdsByProvider.Remove(normalizedProvider);
            }

            LogVerbose($"Unregistered {removed} external effect(s) from provider={normalizedProvider}.");
            if (removed > 0)
            {
                FireAndForget(SyncCustomEffectsForAllSessionsAsync(), "external effects unregister sync");
            }
            return removed > 0;
        }

        public object CC_CompleteEffect(string requestId, string status = "success", string reason = "", string playerMessage = "")
        {
            if (string.IsNullOrWhiteSpace(requestId))
            {
                return false;
            }

            ExternalEffectPendingRequest pending;
            lock (_externalEffectsSync)
            {
                if (!_externalPendingRequests.TryGetValue(requestId, out pending))
                {
                    return false;
                }

                pending.TimeoutTimer?.Destroy();
                _externalPendingRequests.Remove(requestId);
            }

            var normalizedStatus = NormalizeExternalCompletionStatus(status);
            var player = FindPlayerBySteamId(pending.SteamId);
            if (!string.IsNullOrWhiteSpace(playerMessage))
            {
                ShowExternalProviderMessage(player, normalizedStatus, playerMessage);
            }
            else
            {
                LogVerbose($"External provider={pending.ProviderName} completed requestID={requestId} without playerMessage (recommended).");
            }

            FireAndForget(
                SendEffectResponseAsync(pending.Token, pending.RequestId, normalizedStatus, reason ?? string.Empty),
                $"external completion {normalizedStatus}"
            );
            LogVerbose($"External effect completed requestID={requestId}, provider={pending.ProviderName}, status={normalizedStatus}.");
            return true;
        }

        private JArray ParseExternalEffectsPayload(object effectsPayload)
        {
            if (effectsPayload == null)
            {
                return null;
            }

            if (effectsPayload is JArray arr)
            {
                return arr;
            }

            if (effectsPayload is JObject obj && obj["effects"] is JArray effectsArray)
            {
                return effectsArray;
            }

            if (effectsPayload is string text)
            {
                text = text.Trim();
                if (string.IsNullOrEmpty(text))
                {
                    return null;
                }

                try
                {
                    if (text.StartsWith("["))
                    {
                        return JArray.Parse(text);
                    }

                    var parsedObj = JObject.Parse(text);
                    if (parsedObj["effects"] is JArray parsedEffects)
                    {
                        return parsedEffects;
                    }
                }
                catch
                {
                    return null;
                }
            }

            try
            {
                return JArray.FromObject(effectsPayload);
            }
            catch
            {
                return null;
            }
        }

        private string NormalizeExternalCompletionStatus(string status)
        {
            var normalized = (status ?? string.Empty).Trim();
            if (string.Equals(normalized, "success", StringComparison.OrdinalIgnoreCase))
            {
                return "success";
            }

            if (string.Equals(normalized, "failPermanent", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(normalized, "fail", StringComparison.OrdinalIgnoreCase))
            {
                return "failPermanent";
            }

            return "failTemporary";
        }

        private async Task SyncCustomEffectsForAllSessionsAsync()
        {
            foreach (var kvp in _data.PlayerSessions)
            {
                var steamId = kvp.Key;
                var session = kvp.Value;
                if (session == null || string.IsNullOrWhiteSpace(session.Token))
                {
                    continue;
                }

                try
                {
                    await SyncCustomEffectsAsync(session.Token, steamId);
                }
                catch (Exception ex)
                {
                    LogVerbose($"External effect sync refresh failed for {steamId}: {ex.Message}");
                }
            }
        }

        #endregion

        #region Access Control

        private bool CanUsePlugin(BasePlayer player)
        {
            if (player == null)
            {
                return false;
            }

            var allowAllUsers = _config?.AllowAllUsersWithoutPermission == true;
            if (!allowAllUsers && !permission.UserHasPermission(player.UserIDString, PermUse) && !player.IsAdmin)
            {
                ShowEffectUi(player, "Crowd Control", "You do not have permission to use Crowd Control.");
                return false;
            }

            return true;
        }

        #endregion

        #region WebSocket

        private async Task EnsureSocketConnectedAsync()
        {
            if (_isUnloading)
            {
                return;
            }

            if (_socket != null && _socket.State == WebSocketState.Open)
            {
                return;
            }

            lock (_socketSync)
            {
                if (_isUnloading || _isSocketConnecting)
                {
                    return;
                }
                _isSocketConnecting = true;
            }

            try
            {
                if (DateTime.UtcNow - _lastSocketConnectUtc < TimeSpan.FromSeconds(1))
                {
                    return;
                }

                if (_isUnloading)
                {
                    return;
                }

                _lastSocketConnectUtc = DateTime.UtcNow;

                _socketCts?.Cancel();
                _socket?.Abort();
                _socket?.Dispose();

                _socketCts = new CancellationTokenSource();
                _socket = new ClientWebSocket();
                _socket.Options.SetRequestHeader("User-Agent", UserAgent);
                _socket.Options.KeepAliveInterval = TimeSpan.FromMinutes(SocketKeepAliveMinutes);

                await _socket.ConnectAsync(new Uri(PubSubWebSocketUrl), _socketCts.Token);
                Puts("Connected to Crowd Control PubSub WebSocket.");

                FireAndForget(ReceiveLoopAsync(_socketCts.Token), "websocket receive loop");
                FireAndForget(ResubscribeAllSessionsAsync(), "resubscribe all sessions");
            }
            catch (Exception ex)
            {
                if (_isUnloading)
                {
                    return;
                }

                PrintError($"WebSocket connect failed: {ex.Message}");
                StartReconnectTimer();
            }
            finally
            {
                lock (_socketSync)
                {
                    _isSocketConnecting = false;
                }
            }
        }

        private async Task ReceiveLoopAsync(CancellationToken token)
        {
            var buffer = new byte[8192];
            var sb = new StringBuilder();

            try
            {
                while (_socket != null && _socket.State == WebSocketState.Open && !token.IsCancellationRequested && !_isUnloading)
                {
                    WebSocketReceiveResult result;
                    do
                    {
                        result = await _socket.ReceiveAsync(new ArraySegment<byte>(buffer), token);
                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            Puts("Crowd Control WebSocket closed by server.");
                            NotifyActiveCcPlayersSessionDisconnected("Crowd Control disconnected. Session may have ended.");
                            if (!_isUnloading)
                            {
                                StartReconnectTimer();
                            }
                            return;
                        }

                        sb.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));
                    }
                    while (!result.EndOfMessage);

                    var message = sb.ToString();
                    sb.Clear();
                    HandleSocketMessage(message);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                if (_isUnloading)
                {
                    return;
                }

                PrintError($"WebSocket receive error: {ex.Message}");
                NotifyActiveCcPlayersSessionDisconnected("Crowd Control connection error. Session may have ended.");
                StartReconnectTimer();
            }
        }

        private void HandleSocketMessage(string json)
        {
            if (_isUnloading)
            {
                return;
            }

            if (string.IsNullOrWhiteSpace(json))
            {
                return;
            }

            var trimmed = json.Trim();
            if (!trimmed.StartsWith("{") && !trimmed.StartsWith("["))
            {
                LogVerbose($"Ignoring non-JSON socket frame: {trimmed}");
                return;
            }

            JObject msg;
            try
            {
                msg = JObject.Parse(trimmed);
            }
            catch (Exception ex)
            {
                LogVerbose($"Failed parsing JSON socket frame: {ex.Message}");
                return;
            }

            var domain = msg.Value<string>("domain");
            var type = msg.Value<string>("type");
            if (string.IsNullOrEmpty(type))
            {
                return;
            }

            var payload = msg["payload"] as JObject ?? new JObject();

            switch (type)
            {
                case "application-auth-code":
                    HandleApplicationAuthCode(payload);
                    break;
                case "application-auth-code-error":
                    HandleApplicationAuthCodeError(payload);
                    break;
                case "application-auth-code-redeemed":
                    HandleApplicationAuthCodeRedeemed(payload);
                    break;
                case "subscription-result":
                    HandleSubscriptionResult(payload);
                    break;
                case "effect-request":
                    if (domain == "pub")
                    {
                        HandleEffectRequest(payload);
                    }
                    break;
                default:
                    break;
            }
        }

        private void HandleApplicationAuthCode(JObject payload)
        {
            string steamId = null;
            lock (_pendingAuthRequests)
            {
                if (_pendingAuthRequests.Count > 0)
                {
                    steamId = _pendingAuthRequests.Dequeue();
                }
            }

            if (string.IsNullOrEmpty(steamId))
            {
                PrintWarning("Received application-auth-code without any waiting player.");
                return;
            }

            var code = payload.Value<string>("code");

            if (!string.IsNullOrEmpty(code))
            {
                _authCodeToSteamId[code] = steamId;
            }

            var player = FindPlayerBySteamId(steamId);
            if (player != null)
            {
                if (!string.IsNullOrEmpty(code))
                {
                    player.ConsoleMessage($"[CrowdControl] Enter this auth code into your Crowd Control app: {code}");
                    player.ConsoleMessage("[CrowdControl] You have 3 minutes to enter this code before it expires.");
                    ShowEffectUi(player, "Crowd Control", "Check console (F1) for instructions.");
                }
                else
                {
                    ShowEffectUi(player, "Crowd Control", "Auth code generation failed. Please run /cc link again.");
                }
            }
        }

        private void HandleApplicationAuthCodeError(JObject payload)
        {
            string steamId = null;
            lock (_pendingAuthRequests)
            {
                if (_pendingAuthRequests.Count > 0)
                {
                    steamId = _pendingAuthRequests.Dequeue();
                }
            }

            var message = payload.Value<string>("message") ?? "Unknown Crowd Control auth error.";
            PrintWarning($"Crowd Control auth code error: {message}");

            var player = !string.IsNullOrEmpty(steamId) ? FindPlayerBySteamId(steamId) : null;
            if (player != null)
            {
                ShowEffectUi(player, "Crowd Control", $"Auth code failed: {message}");
            }
        }

        private void HandleApplicationAuthCodeRedeemed(JObject payload)
        {
            var code = payload.Value<string>("code");
            if (string.IsNullOrEmpty(code))
            {
                return;
            }

            if (!_authCodeToSteamId.TryGetValue(code, out var steamId))
            {
                PrintWarning($"Auth code redeemed but no matching player mapping for code {code}.");
                return;
            }

            _authCodeToSteamId.Remove(code);
            FireAndForget(ExchangeAuthCodeForTokenAsync(steamId, code), "exchange auth code");
        }

        private void HandleSubscriptionResult(JObject payload)
        {
            var success = payload["success"]?.ToString(Formatting.None) ?? "[]";
            var failure = payload["failure"]?.ToString(Formatting.None) ?? "[]";
            Puts($"Subscription result success={success}, failure={failure}");
        }

        private void StartReconnectTimer()
        {
            if (_isUnloading)
            {
                return;
            }

            if (_reconnectTimer != null && !_reconnectTimer.Destroyed)
            {
                return;
            }

            _reconnectTimer = timer.Once(ReconnectDelaySeconds, () =>
            {
                if (_isUnloading)
                {
                    _reconnectTimer = null;
                    return;
                }

                _reconnectTimer = null;
                FireAndForget(EnsureSocketConnectedAsync(), "reconnect socket");
            });
        }

        private void StartSocketHeartbeat()
        {
            if (_isUnloading)
            {
                return;
            }

            _heartbeatTimer?.Destroy();
            _heartbeatTimer = timer.Every(300f, () =>
            {
                if (_isUnloading)
                {
                    return;
                }

                // Keep a single shared websocket healthy; ClientWebSocket also has KeepAliveInterval set.
                FireAndForget(SendSocketHeartbeatAsync(), "socket heartbeat");
            });
        }

        private async Task SendSocketHeartbeatAsync()
        {
            if (_isUnloading)
            {
                return;
            }

            if (_socket == null || _socket.State != WebSocketState.Open)
            {
                return;
            }

            var payload = new JObject
            {
                ["action"] = "ping",
                ["data"] = new JObject
                {
                    ["stamp"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                }
            };
            await SendSocketMessageAsync(payload);
        }

        private async Task SendGenerateAuthCodeAsync()
        {
            var payload = new JObject
            {
                ["action"] = "generate-auth-code",
                ["data"] = new JObject
                {
                    ["appID"] = _config.AppId,
                    ["scopes"] = JArray.FromObject(Scopes),
                    ["packs"] = JArray.FromObject(Packs),
                    ["qrCode"] = false
                }
            };

            await SendSocketMessageAsync(payload);
        }

        private async Task ResubscribeAllSessionsAsync()
        {
            foreach (var kvp in _data.PlayerSessions)
            {
                var session = kvp.Value;
                if (session == null || string.IsNullOrEmpty(session.Token) || string.IsNullOrEmpty(session.CcUid))
                {
                    continue;
                }

                await SendSubscribeAsync(session.Token, session.CcUid);
            }
        }

        private async Task SendSubscribeAsync(string token, string ccUid)
        {
            var topic = $"pub/{ccUid}";
            var payload = new JObject
            {
                ["action"] = "subscribe",
                ["data"] = new JObject
                {
                    ["token"] = token,
                    ["topics"] = new JArray { topic }
                }
            };

            await SendSocketMessageAsync(payload);
        }

        private async Task SendSocketMessageAsync(JObject payload)
        {
            if (_isUnloading)
            {
                return;
            }

            if (_socket == null || _socket.State != WebSocketState.Open)
            {
                await EnsureSocketConnectedAsync();
            }

            if (_socket == null || _socket.State != WebSocketState.Open)
            {
                throw new InvalidOperationException("WebSocket is not connected.");
            }

            var json = payload.ToString(Formatting.None);
            var bytes = Encoding.UTF8.GetBytes(json);

            await _sendSemaphore.WaitAsync();
            try
            {
                await _socket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, _socketCts.Token);
            }
            finally
            {
                _sendSemaphore.Release();
            }
        }

        #endregion

        #region HTTP Auth + Session

        private async Task ExchangeAuthCodeForTokenAsync(string steamId, string code)
        {
            try
            {
                var endpoint = $"{OpenApiUrl.TrimEnd('/')}/auth/application/token";
                var body = new JObject
                {
                    ["appID"] = _config.AppId,
                    ["code"] = code,
                    ["secret"] = _config.AppSecret
                };

                var responseJson = await PostJsonAsync(endpoint, body, includeAuth: null);
                var token = responseJson.Value<string>("token");
                if (string.IsNullOrEmpty(token))
                {
                    PrintWarning("Token exchange succeeded but no token field was present.");
                    return;
                }

                var decoded = DecodeJwt(token);
                if (decoded == null || string.IsNullOrEmpty(decoded.CcUid))
                {
                    PrintWarning("Failed to decode Crowd Control JWT.");
                    return;
                }

                _data.PlayerSessions[steamId] = new PlayerSessionState
                {
                    SteamId = steamId,
                    Token = token,
                    CcUid = decoded.CcUid,
                    OriginId = decoded.OriginId,
                    ProfileType = decoded.ProfileType,
                    DisplayName = decoded.Name,
                    TokenExpiryUnix = decoded.Exp,
                    AuthenticatedAtUnix = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                };
                SaveData();

                var player = FindPlayerBySteamId(steamId);
                if (player != null)
                {
                    ShowEffectUi(player, "Crowd Control", "Crowd Control auth complete.");
                    ShowEffectUi(player, "Crowd Control", $"Connected profile: {decoded.Name} ({decoded.CcUid})");
                }

                await SendSubscribeAsync(token, decoded.CcUid);

                if (AutoStartSessionAfterAuth)
                {
                    await StartGameSessionAsync(_data.PlayerSessions[steamId], steamId);
                }

                await UpdateTeleportAvailabilityAsync();
            }
            catch (Exception ex)
            {
                PrintError($"Token exchange failed for {steamId}: {ex.Message}");
            }
        }

        private async Task StartGameSessionAsync(PlayerSessionState session, string notifySteamId = null)
        {
            if (session == null || string.IsNullOrEmpty(session.Token))
            {
                return;
            }

            var steamId = session.SteamId ?? notifySteamId ?? string.Empty;
            if (!TryBeginSessionStart(steamId))
            {
                LogVerbose($"Session start already in progress for {steamId}; skipping duplicate start.");
                return;
            }

            try
            {
                var endpoint = $"{OpenApiUrl.TrimEnd('/')}/game-session/start";
                var body = new JObject
                {
                    ["gamePackID"] = GamePackId,
                    ["effectReportArgs"] = new JArray()
                };

                var response = await PostJsonAsync(endpoint, body, includeAuth: session.Token);
                var gameSessionId = response.Value<string>("gameSessionID");
                if (!string.IsNullOrEmpty(gameSessionId))
                {
                    session.GameSessionId = gameSessionId;
                    SaveData();

                    var player = !string.IsNullOrEmpty(notifySteamId) ? FindPlayerBySteamId(notifySteamId) : null;
                    if (player != null)
                    {
                        ShowEffectUi(player, "Crowd Control", "Session started!");
                    }

                    if (_config?.SessionRules?.DisableCustomEffectsSync != true)
                    {
                        // Custom effects endpoint expects an active session context.
                        await Task.Delay(1000);
                        await SyncCustomEffectsAsync(session.Token, steamId);
                    }

                    if (_config?.SessionRules?.EnablePriceChange == false)
                    {
                        await SyncEffectPricingOverridesAsync(session.Token, steamId);
                    }
                }
            }
            catch (Exception ex)
            {
                PrintError($"Failed to start game session: {ex.Message}");
            }
            finally
            {
                EndSessionStart(steamId);
            }
        }

        private async Task RestartGameSessionAsync(PlayerSessionState session, string notifySteamId = null)
        {
            if (session == null || string.IsNullOrEmpty(session.Token))
            {
                return;
            }

            await StopGameSessionAsync(session, notifySteamId);
            await StartGameSessionAsync(session, notifySteamId);
        }

        private async Task SyncCustomEffectsAsync(string token, string steamId)
        {
            if (_config?.SessionRules?.DisableCustomEffectsSync == true)
            {
                LogVerbose("Custom effects sync is disabled by config; skipping.");
                return;
            }

            if (string.IsNullOrEmpty(token))
            {
                return;
            }

            if (!string.IsNullOrEmpty(steamId) &&
                _lastCustomEffectsSyncUtc.TryGetValue(steamId, out var lastSyncUtc) &&
                DateTime.UtcNow - lastSyncUtc < TimeSpan.FromSeconds(3))
            {
                LogVerbose($"Skipping duplicate custom-effects sync for {steamId}.");
                return;
            }

            try
            {
                var endpoint = GetCustomEffectsEndpoint();
                var body = BuildCustomEffectsSyncRequestBody();
                var operations = body["operations"] as JArray;
                if (operations == null || operations.Count == 0)
                {
                    LogVerbose("No custom effects configured for sync; skipping upload.");
                    return;
                }

                await PutJsonAsync(endpoint, body, includeAuth: token);
                if (!string.IsNullOrEmpty(steamId))
                {
                    _lastCustomEffectsSyncUtc[steamId] = DateTime.UtcNow;
                }
                LogVerbose("Custom effects synced to Crowd Control menu endpoint.");
            }
            catch (Exception ex)
            {
                PrintWarning($"Custom effects sync failed: {ex.Message}");
            }
        }

        private bool TryBeginSessionStart(string steamId)
        {
            if (string.IsNullOrEmpty(steamId))
            {
                return true;
            }

            lock (_sessionStartInProgress)
            {
                if (_sessionStartInProgress.Contains(steamId))
                {
                    return false;
                }

                _sessionStartInProgress.Add(steamId);
                return true;
            }
        }

        private void EndSessionStart(string steamId)
        {
            if (string.IsNullOrEmpty(steamId))
            {
                return;
            }

            lock (_sessionStartInProgress)
            {
                _sessionStartInProgress.Remove(steamId);
            }
        }

        private string GetCustomEffectsEndpoint()
        {
            return $"{OpenApiUrl.TrimEnd('/')}/menu/custom-effects";
        }

        private JObject BuildCustomEffectsSyncRequestBody()
        {
            var effects = BuildCustomEffectsPayload();

            var operations = BuildChunkedCustomEffectOperations(effects, CustomEffectsPerOperation);

            return new JObject
            {
                ["gamePackID"] = GamePackId,
                ["operations"] = operations
            };
        }

        private async Task SyncEffectPricingOverridesAsync(string token, string steamId)
        {
            if (string.IsNullOrEmpty(token))
            {
                return;
            }

            var entries = LoadOrSeedEffectPricingEntries();
            var effectNames = LoadEffectNamesFromBaseUpdated();
            if (entries.Count == 0)
            {
                LogVerbose("No effect pricing entries available; skipping pricing override sync.");
                return;
            }

            var effects = new JObject();
            for (var i = 0; i < entries.Count; i++)
            {
                var entry = entries[i];
                if (string.IsNullOrWhiteSpace(entry?.EffectId))
                {
                    continue;
                }

                var effectName = entry.EffectId;
                if (effectNames.TryGetValue(entry.EffectId, out var configuredName) && !string.IsNullOrWhiteSpace(configuredName))
                {
                    effectName = configuredName;
                }

                var effectObj = new JObject
                {
                    ["name"] = effectName,
                    ["price"] = Math.Max(0, entry.Price)
                };

                effects[entry.EffectId] = effectObj;
            }

            if (!effects.HasValues)
            {
                LogVerbose("No valid effect pricing overrides to sync.");
                return;
            }

            const int pricingOverrideTestLimit = 10;
            if (effects.Count > pricingOverrideTestLimit)
            {
                var limited = new JObject();
                var kept = 0;
                foreach (var prop in effects.Properties())
                {
                    limited[prop.Name] = prop.Value;
                    kept++;
                    if (kept >= pricingOverrideTestLimit)
                    {
                        break;
                    }
                }

                effects = limited;
                LogVerbose($"Pricing override test limit active: sending first {effects.Count} effect(s).");
            }

            var customEffects = BuildCustomEffectsPayload();
            var matchedIds = 0;
            foreach (var prop in effects.Properties())
            {
                if (customEffects[prop.Name] != null)
                {
                    matchedIds++;
                }
            }
            LogVerbose($"Pricing override coverage: matched={matchedIds}/{effects.Count}, customEffectsPublished={customEffects.Count}");

            var sampleEntries = new List<string>();
            var sampled = 0;
            foreach (var prop in effects.Properties())
            {
                if (!(prop.Value is JObject obj))
                {
                    continue;
                }

                var sampleName = obj.Value<string>("name") ?? string.Empty;
                var samplePrice = obj.Value<int?>("price") ?? 0;
                sampleEntries.Add($"{prop.Name}=>{{name:{sampleName},price:{samplePrice}}}");
                sampled++;
                if (sampled >= 2)
                {
                    break;
                }
            }
            LogVerbose($"Pricing override sample ({sampled}/{effects.Count}): {string.Join(" | ", sampleEntries)}");

            var body = new JObject
            {
                ["gamePackID"] = GamePackId,
                ["operations"] = new JArray
                {
                    new JObject
                    {
                        ["mode"] = "replace-partial",
                        ["effects"] = effects
                    }
                }
            };

            try
            {
                await PutJsonAsync(GetCustomEffectsEndpoint(), body, includeAuth: token);
                LogVerbose($"Pricing overrides synced via replace-partial for {effects.Count} effect(s), steamID={steamId}.");
            }
            catch (Exception ex)
            {
                PrintWarning($"Pricing override sync failed: {ex.Message}");
            }
        }

        private string GetEffectPricingFilePath()
        {
            return Path.Combine(Interface.Oxide.RootDirectory, "oxide", "plugins", EffectPricingFileName);
        }

        private List<EffectPricingEntry> LoadOrSeedEffectPricingEntries()
        {
            var path = GetEffectPricingFilePath();
            var defaults = BuildDefaultEffectPricingEntriesFromBaseUpdated();
            var byId = new Dictionary<string, EffectPricingEntry>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < defaults.Count; i++)
            {
                var entry = defaults[i];
                if (entry != null && !string.IsNullOrWhiteSpace(entry.EffectId))
                {
                    byId[entry.EffectId] = entry;
                }
            }

            var changed = false;
            try
            {
                if (File.Exists(path))
                {
                    var json = File.ReadAllText(path);
                    var loaded = JsonConvert.DeserializeObject<List<EffectPricingEntry>>(json) ?? new List<EffectPricingEntry>();
                    for (var i = 0; i < loaded.Count; i++)
                    {
                        var entry = loaded[i];
                        if (entry == null || string.IsNullOrWhiteSpace(entry.EffectId))
                        {
                            continue;
                        }

                        entry.Scale = entry.Scale ?? new EffectPricingScaleConfig();
                        byId[entry.EffectId] = entry;
                    }
                }
                else
                {
                    changed = true;
                }
            }
            catch (Exception ex)
            {
                PrintWarning($"Failed reading {EffectPricingFileName}, using defaults: {ex.Message}");
                changed = true;
            }

            foreach (var entry in defaults)
            {
                if (entry == null || string.IsNullOrWhiteSpace(entry.EffectId))
                {
                    continue;
                }

                if (!byId.ContainsKey(entry.EffectId))
                {
                    byId[entry.EffectId] = entry;
                    changed = true;
                }
            }

            var ordered = new List<EffectPricingEntry>(byId.Values);
            ordered.Sort((a, b) => string.Compare(a?.EffectId, b?.EffectId, StringComparison.OrdinalIgnoreCase));

            if (changed)
            {
                try
                {
                    var serialized = JsonConvert.SerializeObject(ordered, Formatting.Indented);
                    File.WriteAllText(path, serialized + Environment.NewLine, Encoding.UTF8);
                    LogVerbose($"Wrote {EffectPricingFileName} with {ordered.Count} effect pricing entries.");
                }
                catch (Exception ex)
                {
                    PrintWarning($"Failed writing {EffectPricingFileName}: {ex.Message}");
                }
            }

            return ordered;
        }

        private List<EffectPricingEntry> BuildDefaultEffectPricingEntriesFromBaseUpdated()
        {
            var results = new List<EffectPricingEntry>();
            var path = Path.Combine(Interface.Oxide.RootDirectory, "oxide", "plugins", "effects.json");
            if (!File.Exists(path))
            {
                PrintWarning("effects.json not found; cannot auto-seed effect pricing entries.");
                return results;
            }

            try
            {
                var root = JObject.Parse(File.ReadAllText(path));
                var effects = root["effects"]?["game"] as JObject;
                if (effects == null)
                {
                    return results;
                }

                foreach (var prop in effects.Properties())
                {
                    var effectObj = prop.Value as JObject;
                    var entry = new EffectPricingEntry
                    {
                        EffectId = prop.Name,
                        Price = effectObj?.Value<int?>("price") ?? 0,
                        SessionCooldown = effectObj?.Value<int?>("sessionCooldown") ?? 0,
                        UserCooldown = effectObj?.Value<int?>("userCooldown") ?? 0,
                        Inactive = effectObj?.Value<bool?>("inactive") ?? false,
                        Duration = effectObj?["duration"] as JObject != null
                            ? (JObject)((JObject)effectObj["duration"]).DeepClone()
                            : null,
                        Scale = new EffectPricingScaleConfig
                        {
                            Percent = effectObj?["scale"]?["percent"]?.Value<float?>() ?? 1f,
                            Duration = effectObj?["scale"]?["duration"]?.Value<float?>() ?? 1f,
                            Inactive = effectObj?["scale"]?["inactive"]?.Value<bool?>() ?? true
                        }
                    };

                    results.Add(entry);
                }
            }
            catch (Exception ex)
            {
                PrintWarning($"Failed building default effect pricing entries from effects.json: {ex.Message}");
            }

            return results;
        }

        private Dictionary<string, string> LoadEffectNamesFromBaseUpdated()
        {
            var results = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var path = Path.Combine(Interface.Oxide.RootDirectory, "oxide", "plugins", "effects.json");
            if (!File.Exists(path))
            {
                return results;
            }

            try
            {
                var root = JObject.Parse(File.ReadAllText(path));
                var effects = root["effects"]?["game"] as JObject;
                if (effects == null)
                {
                    return results;
                }

                foreach (var prop in effects.Properties())
                {
                    var effectObj = prop.Value as JObject;
                    var name = effectObj?.Value<string>("name");
                    if (!string.IsNullOrWhiteSpace(name))
                    {
                        results[prop.Name] = name;
                    }
                }
            }
            catch (Exception ex)
            {
                PrintWarning($"Failed loading effect names from effects.json: {ex.Message}");
            }

            return results;
        }

        private JArray BuildChunkedCustomEffectOperations(JObject effects, int perOperation)
        {
            var operations = new JArray();
            if (effects == null || !effects.HasValues)
            {
                return operations;
            }

            operations.Add(new JObject
            {
                ["mode"] = "replace-all",
                ["effects"] = effects
            });

            LogVerbose($"Prepared {effects.Count} custom effects across {operations.Count} replace-all operation(s).");
            return operations;
        }

        private JObject BuildCustomEffectsPayload()
        {

            /*
                Custom effects 

            */
            var effects = new JObject
            {
                ["player_kill"] = new JObject { ["name"] = "Player Kill", ["price"] = 300, ["description"] = "Instantly kill the player." },
                ["test_hype_train"] = new JObject { ["name"] = "TEST: Hype Train", ["price"] = 25, ["description"] = "Test effect: spawn a short-lived hype train with stub names and sound." },
                ["player_teleport_to_sleeping_bag"] = new JObject { ["name"] = "Teleport To Sleeping Bag", ["price"] = 220, ["description"] = "Teleport player to one of their sleeping bags." }
            };

            lock (_externalEffectsSync)
            {
                foreach (var kvp in _externalEffectsById)
                {
                    var def = kvp.Value;
                    if (def?.MenuEffect == null || string.IsNullOrWhiteSpace(def.EffectId))
                    {
                        continue;
                    }

                    effects[def.EffectId] = (JObject)def.MenuEffect.DeepClone();
                }
            }

            // Intentionally kept as reference (disabled).
            // Pricing enforcement is now handled by CrowdControl-Effects.json + replace-partial sync.
            /*
            if (_config?.SessionRules != null && !_config.SessionRules.EnablePriceChange)
            {
                foreach (var prop in effects.Properties())
                {
                    var effect = prop.Value as JObject;
                    effect?.Remove("price");
                }
            }
            */

            return effects;
        }

        private async Task StopGameSessionAsync(PlayerSessionState session, string notifySteamId = null)
        {
            if (session == null || string.IsNullOrEmpty(session.Token))
            {
                return;
            }

            if (string.IsNullOrEmpty(session.GameSessionId))
            {
                return;
            }

            try
            {
                var endpoint = $"{OpenApiUrl.TrimEnd('/')}/game-session/stop";
                var body = new JObject
                {
                    ["gameSessionID"] = session.GameSessionId
                };

                await PostJsonAsync(endpoint, body, includeAuth: session.Token);
                session.GameSessionId = null;
                SaveData();

                var player = !string.IsNullOrEmpty(notifySteamId) ? FindPlayerBySteamId(notifySteamId) : null;
                if (player != null)
                {
                    ShowErrorUi(player, "Crowd Control session ended or disconnected.");
                }

                await UpdateTeleportAvailabilityAsync();
            }
            catch (Exception ex)
            {
                PrintError($"Failed to stop game session: {ex.Message}");
            }
        }

        private Task<JObject> PostJsonAsync(string url, JObject payload, string includeAuth)
        {
            return SendJsonAsync(url, payload, includeAuth, Oxide.Core.Libraries.RequestMethod.POST);
        }

        private Task<JObject> PutJsonAsync(string url, JObject payload, string includeAuth)
        {
            return SendJsonAsync(url, payload, includeAuth, Oxide.Core.Libraries.RequestMethod.PUT);
        }

        private Task<JObject> SendJsonAsync(
            string url,
            JObject payload,
            string includeAuth,
            Oxide.Core.Libraries.RequestMethod method
        )
        {
            var tcs = new TaskCompletionSource<JObject>();
            var headers = new Dictionary<string, string>
            {
                ["Content-Type"] = "application/json",
                ["Accept"] = "application/json",
                ["User-Agent"] = UserAgent
            };

            if (!string.IsNullOrEmpty(includeAuth))
            {
                headers["Authorization"] = $"cc-auth-token {includeAuth}";
            }
            if (method == Oxide.Core.Libraries.RequestMethod.PUT)
            {
                // Compatibility fallback for environments that tunnel PUT behind POST handlers.
                headers["X-HTTP-Method-Override"] = "PUT";
            }

            webrequest.Enqueue(
                url,
                payload.ToString(Formatting.None),
                (code, response) =>
                {
                    if (code < 200 || code >= 300)
                    {
                        tcs.TrySetException(new Exception($"HTTP {code}: {response ?? string.Empty}"));
                        return;
                    }

                    if (string.IsNullOrEmpty(response))
                    {
                        tcs.TrySetResult(new JObject());
                        return;
                    }

                    try
                    {
                        tcs.TrySetResult(JObject.Parse(response));
                    }
                    catch
                    {
                        tcs.TrySetResult(new JObject());
                    }
                },
                this,
                method,
                headers,
                20f);

            return tcs.Task;
        }


        #endregion

        #region Effect Lifecycle

        private void HandleEffectRequest(JObject payload)
        {
            HandleEffectRequest(payload, false);
        }

        private void HandleEffectRequest(JObject payload, bool isRetryAttempt)
        {
            if (_isUnloading)
            {
                return;
            }

            var requestId = payload.Value<string>("requestID");
            var effect = payload["effect"] as JObject;
            if (string.IsNullOrEmpty(requestId) || effect == null)
            {
                LogVerbose("Ignoring effect-request with missing requestID or effect payload.");
                return;
            }

            var effectId = effect.Value<string>("effectID") ?? string.Empty;

            var effectType = effect.Value<string>("type")
                ?? effect.Value<string>("effectType")
                ?? payload.Value<string>("type")
                ?? payload.Value<string>("effectType")
                ?? string.Empty;
            if (!IsSupportedEffectType(effectType, effectId))
            {
                //LogVerbose($"Ignoring unsupported effect requestID={requestId}, effectID={effectId}, effectType={effectType}.");
                ClearEffectRetryState(requestId);
                return;
            }

            if (!isRetryAttempt && !TryRegisterRequestId(requestId))
            {
                LogVerbose($"Ignoring duplicate effect requestID={requestId}.");
                return;
            }

            if (!isRetryAttempt && _activeTimedEffects.ContainsKey(requestId))
            {
                LogVerbose($"Ignoring duplicate effect requestID={requestId}.");
                return;
            }

            var durationSeconds = effect.Value<int?>("duration") ?? 0;
            LogVerbose($"Effect request received requestID={requestId}, effectID={effectId}, duration={durationSeconds}s.");

            var targetCcUid = payload["target"]?["ccUID"]?.ToString();
            var steamId = FindSteamIdByCcUid(targetCcUid);
            if (string.IsNullOrEmpty(steamId) &&
                _activeEffectRetries.TryGetValue(requestId, out var retryState) &&
                !string.IsNullOrEmpty(retryState?.SteamId))
            {
                steamId = retryState.SteamId;
            }
            if (string.IsNullOrEmpty(steamId))
            {
                PrintWarning($"Could not map effect request {requestId} target ccUID={targetCcUid} to a SteamID.");
                return;
            }

            var responseToken = string.Empty;
            if (!_data.PlayerSessions.TryGetValue(steamId, out var session) || string.IsNullOrEmpty(session.Token))
            {
                if (_activeEffectRetries.TryGetValue(requestId, out var missingSessionRetryState) &&
                    !string.IsNullOrEmpty(missingSessionRetryState?.Token))
                {
                    responseToken = missingSessionRetryState.Token;
                    const string missingSessionReason = "Target session is not currently available.";
                    if (TryScheduleEffectRetry(requestId, payload, effect, effectId, durationSeconds, steamId, responseToken, missingSessionReason))
                    {
                        return;
                    }

                    FireAndForget(SendEffectResponseAsync(responseToken, requestId, "failTemporary", missingSessionReason), "effect failTemporary missing session");
                    return;
                }

                PrintWarning($"No active token for SteamID {steamId}; cannot answer effect request {requestId}.");
                return;
            }
            responseToken = session.Token;

            var isDisabledTestEffect =
                _config?.SessionRules?.DisableTestEffects == true
                && IsTestEffectRequest(payload, effect);

            if (IsEffectBlockedBySessionRules(payload, effect, out var blockedReason))
            {
                LogVerbose($"Effect request blocked by session rules requestID={requestId}, effectID={effectId}, reason={blockedReason}");
                ClearEffectRetryState(requestId);
                var blockedMessage = isDisabledTestEffect ? string.Empty : blockedReason;
                FireAndForget(SendEffectResponseAsync(responseToken, requestId, "failTemporary", blockedMessage), "effect failTemporary blocked by rules");
                return;
            }

            var player = FindPlayerBySteamId(steamId);
            if (player == null || !player.IsConnected)
            {
                const string offlineReason = "Target player is currently offline.";
                LogVerbose($"Effect requestID={requestId} target SteamID={steamId} is offline; evaluating retry.");
                if (TryScheduleEffectRetry(requestId, payload, effect, effectId, durationSeconds, steamId, responseToken, offlineReason))
                {
                    return;
                }

                FireAndForget(SendEffectResponseAsync(responseToken, requestId, "failTemporary", offlineReason), "effect failTemporary offline");
                return;
            }

            LogVerbose($"Effect requestID={requestId} resolved to player={player.displayName} ({steamId}).");
            var normalizedEffectId = NormalizeEffectId(effectId);
            var effectiveDurationSeconds = durationSeconds;
            if (effectiveDurationSeconds <= 0 &&
                (normalizedEffectId == "player_damage_over_time" || normalizedEffectId == "player_heal_over_time"))
            {
                effectiveDurationSeconds = 20;
                LogVerbose($"Timed effect {normalizedEffectId} had no duration; defaulting to {effectiveDurationSeconds}s.");
            }

            if (TryDispatchExternalEffect(
                    requestId,
                    effectId,
                    responseToken,
                    player,
                    payload,
                    effect,
                    out var externalStatus,
                    out var externalReason,
                    out var externalPlayerMessage,
                    out var pendingExternal))
            {
                if (!string.IsNullOrWhiteSpace(externalPlayerMessage))
                {
                    ShowExternalProviderMessage(player, externalStatus, externalPlayerMessage);
                }
                else
                {
                    LogVerbose($"External provider result requestID={requestId}, effectID={effectId} omitted playerMessage (recommended).");
                }

                if (pendingExternal)
                {
                    return;
                }

                ClearEffectRetryState(requestId);
                FireAndForget(
                    SendEffectResponseAsync(responseToken, requestId, externalStatus, externalReason ?? string.Empty),
                    $"external effect {externalStatus}"
                );
                return;
            }

            if (effectiveDurationSeconds > 0)
            {
                if (TryStartTimedEffect(player, session, requestId, effectId, effectiveDurationSeconds, out var failMessage))
                {
                    ClearEffectRetryState(requestId);
                    LogVerbose($"Timed effect accepted requestID={requestId}, effectID={effectId}.");
                    return;
                }

                LogVerbose($"Timed effect rejected requestID={requestId}, effectID={effectId}, reason={failMessage}");
                if (TryScheduleEffectRetry(requestId, payload, effect, effectId, durationSeconds, steamId, responseToken, failMessage))
                {
                    return;
                }

                FireAndForget(SendEffectResponseAsync(responseToken, requestId, "failTemporary", failMessage), "effect failTemporary timed");
                return;
            }

            if (TryApplyInstantEffect(player, effectId, payload, effect, out var instantError))
            {
                ClearEffectRetryState(requestId);
                LogVerbose($"Instant effect succeeded requestID={requestId}, effectID={effectId}.");
                FireAndForget(SendEffectResponseAsync(responseToken, requestId, "success", string.Empty), "effect success instant");
            }
            else
            {
                var failureStatus = "failTemporary";
                LogVerbose($"Instant effect failed requestID={requestId}, effectID={effectId}, reason={instantError}");
                if (TryScheduleEffectRetry(requestId, payload, effect, effectId, durationSeconds, steamId, responseToken, instantError))
                {
                    return;
                }

                FireAndForget(
                    SendEffectResponseAsync(
                        responseToken,
                        requestId,
                        failureStatus,
                        instantError
                    ),
                    $"effect {failureStatus} instant"
                );
            }
        }

        private bool TryScheduleEffectRetry(
            string requestId,
            JObject payload,
            JObject effect,
            string effectId,
            int durationSeconds,
            string steamId,
            string token,
            string failureReason
        )
        {
            if (string.IsNullOrEmpty(requestId) || payload == null || effect == null)
            {
                return false;
            }

            if (!IsRetryableFailure(effectId, failureReason))
            {
                LogVerbose(
                    $"Retry skipped requestID={requestId}, effectID={effectId}, reason={failureReason ?? string.Empty}"
                );
                ClearEffectRetryState(requestId);
                return false;
            }

            var sourceType = GetRetrySourceType(payload, effect);
            var policy = GetRetryPolicy(sourceType);
            var now = DateTime.UtcNow;

            if (!_activeEffectRetries.TryGetValue(requestId, out var state))
            {
                state = new EffectRetryState
                {
                    RequestId = requestId,
                    EffectId = effectId,
                    SteamId = steamId,
                    Token = token,
                    Payload = (JObject)payload.DeepClone(),
                    Effect = (JObject)effect.DeepClone(),
                    DurationSeconds = durationSeconds,
                    SourceType = sourceType,
                    AttemptsMade = 1, // first failed attempt has already happened
                    MaxAttempts = policy.maxAttempts,
                    MaxDuration = TimeSpan.FromSeconds(policy.maxDurationSeconds),
                    RetryIntervalSeconds = policy.retryIntervalSeconds,
                    FirstFailureUtc = now,
                    LastError = failureReason ?? string.Empty
                };
                _activeEffectRetries[requestId] = state;
            }
            else
            {
                state.SteamId = steamId;
                state.Token = token;
                state.LastError = failureReason ?? string.Empty;
                state.AttemptsMade++;
            }

            var retryLifetime = now - state.FirstFailureUtc;
            if (state.AttemptsMade >= state.MaxAttempts || retryLifetime >= state.MaxDuration)
            {
                LogVerbose(
                    $"Retry exhausted requestID={requestId}, effectID={effectId}, source={state.SourceType}, attempts={state.AttemptsMade}/{state.MaxAttempts}, elapsed={retryLifetime.TotalSeconds:0.0}s, reason={state.LastError}"
                );
                ClearEffectRetryState(requestId);
                return false;
            }

            state.RetryTimer?.Destroy();
            state.RetryTimer = timer.Once(state.RetryIntervalSeconds, () => ExecuteEffectRetry(requestId));

            var remainingAttempts = Math.Max(0, state.MaxAttempts - state.AttemptsMade);
            var remainingSeconds = Math.Max(0.0, (state.MaxDuration - retryLifetime).TotalSeconds);
            LogVerbose(
                $"Queued retry requestID={requestId}, effectID={effectId}, source={state.SourceType}, attempt={state.AttemptsMade}/{state.MaxAttempts}, nextAttempt={Math.Min(state.AttemptsMade + 1, state.MaxAttempts)}/{state.MaxAttempts}, elapsed={retryLifetime.TotalSeconds:0.0}s/{state.MaxDuration.TotalSeconds:0.0}s, nextIn={state.RetryIntervalSeconds:0.0}s, remainingAttempts={remainingAttempts}, remainingSeconds={remainingSeconds:0.0}, reason={state.LastError}"
            );
            return true;
        }

        private void ExecuteEffectRetry(string requestId)
        {
            if (_isUnloading || string.IsNullOrEmpty(requestId))
            {
                return;
            }

            if (!_activeEffectRetries.TryGetValue(requestId, out var state))
            {
                return;
            }

            if (state.Payload == null)
            {
                ClearEffectRetryState(requestId);
                return;
            }

            try
            {
                var retryLifetime = DateTime.UtcNow - state.FirstFailureUtc;
                var nextAttempt = Math.Min(state.AttemptsMade + 1, state.MaxAttempts);
                var remainingSeconds = Math.Max(0.0, (state.MaxDuration - retryLifetime).TotalSeconds);
                LogVerbose(
                    $"Executing retry requestID={requestId}, effectID={state.EffectId}, source={state.SourceType}, attempt={nextAttempt}/{state.MaxAttempts}, elapsed={retryLifetime.TotalSeconds:0.0}s/{state.MaxDuration.TotalSeconds:0.0}s, remainingSeconds={remainingSeconds:0.0}"
                );
                HandleEffectRequest((JObject)state.Payload.DeepClone(), true);
            }
            catch (Exception ex)
            {
                LogVerbose($"Retry execution threw for requestID={requestId}: {ex.Message}");
                if (!TryScheduleEffectRetry(
                        state.RequestId,
                        state.Payload,
                        state.Effect ?? new JObject(),
                        state.EffectId ?? string.Empty,
                        state.DurationSeconds,
                        state.SteamId ?? string.Empty,
                        state.Token ?? string.Empty,
                        ex.Message))
                {
                    var finalReason = string.IsNullOrWhiteSpace(state.LastError) ? "Effect retry exhausted." : state.LastError;
                    FireAndForget(SendEffectResponseAsync(state.Token, state.RequestId, "failTemporary", finalReason), "effect failTemporary retry exhausted");
                }
            }
        }

        private void ClearEffectRetryState(string requestId)
        {
            if (string.IsNullOrEmpty(requestId))
            {
                return;
            }

            if (_activeEffectRetries.TryGetValue(requestId, out var state))
            {
                state?.RetryTimer?.Destroy();
                _activeEffectRetries.Remove(requestId);
            }
        }

        private bool IsRetryableFailure(string effectId, string failureReason)
        {
            var normalized = NormalizeEffectId(effectId);
            var reason = (failureReason ?? string.Empty).Trim();
            if (string.IsNullOrEmpty(normalized))
            {
                return false;
            }

            if (reason.IndexOf("Unknown instant effectID", StringComparison.OrdinalIgnoreCase) >= 0 ||
                reason.IndexOf("Unknown timed effectID", StringComparison.OrdinalIgnoreCase) >= 0 ||
                reason.IndexOf("currently disabled by server settings", StringComparison.OrdinalIgnoreCase) >= 0 ||
                reason.IndexOf("blocked by server settings", StringComparison.OrdinalIgnoreCase) >= 0 ||
                reason.IndexOf("Metabolism state is unavailable", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return false;
            }

            return true;
        }

        private string GetRetrySourceType(JObject payload, JObject effect)
        {
            var joined = BuildSearchableJsonText(payload, effect);
            if (ContainsAny(joined, "tiktok"))
            {
                return "tiktok";
            }

            if (ContainsAny(joined, "twitch"))
            {
                return "twitch";
            }

            return "default";
        }

        private (int maxAttempts, int maxDurationSeconds, float retryIntervalSeconds) GetRetryPolicy(string sourceType)
        {
            var cfg = _config?.RetryPolicy;
            if (cfg == null || !cfg.Enabled)
            {
                return (1, 1, 0.5f);
            }

            RetrySourcePolicyConfig selected;
            if (string.Equals(sourceType, "tiktok", StringComparison.OrdinalIgnoreCase))
            {
                selected = cfg.Tiktok ?? cfg.Default;
            }
            else if (string.Equals(sourceType, "twitch", StringComparison.OrdinalIgnoreCase))
            {
                selected = cfg.Twitch ?? cfg.Default;
            }
            else
            {
                selected = cfg.Default;
            }

            selected = selected ?? new RetrySourcePolicyConfig();

            var attempts = Mathf.Clamp(selected.MaxAttempts, 1, 1000);
            var durationSeconds = Mathf.Clamp(selected.MaxDurationSeconds, 1, 3600);
            var interval = selected.RetryIntervalSeconds > 0f
                ? selected.RetryIntervalSeconds
                : durationSeconds / (float)attempts;
            interval = Mathf.Clamp(interval, 0.25f, 10f);

            return (attempts, durationSeconds, interval);
        }

        private bool ShouldUseTemporaryFailure(string effectId, string error)
        {
            var normalized = NormalizeEffectId(effectId);
            var message = error ?? string.Empty;

            if (message.IndexOf("Unable to spawn at current location", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return true;
            }

            if (message.IndexOf("Failed to spawn", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return true;
            }

            switch (normalized)
            {
                case "spawn_minicopter":
                case "spawn_attack_helicopter":
                case "spawn_supply_drop":
                case "spawn_horse":
                    return true;
                default:
                    return normalized.StartsWith("spawn_", StringComparison.Ordinal);
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
                    if (_config?.SessionRules?.DisableTestEffects == true && IsTestEffectRequest(requestPayload, effectPayload))
                    {
                        error = "Test effects are currently disabled by server settings.";
                        LogVerbose("KillPlayer hard-blocked at execution guard because request matched test criteria.");
                        return false;
                    }

                    LogVerbose("Executing player_kill effect.");
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
                    return TrySetTimeOfDay(player, setDay: true, out error);
                case "world_set_night":
                    return TrySetTimeOfDay(player, setDay: false, out error);
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
                    return TryDropHotbarItem(player, out error);
                case "player_drop_some":
                    return TryDropInventorySome(player, out error);
                case "player_drop_all":
                    return TryDropInventoryAll(player, out error);
                case "player_unload_ammo":
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
                case "player_drop_hotbar_item":
                    return TryDropHotbarItem(player, out error);
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
                case "player_teleport_swap_cc_player":
                    return TrySwapTeleportWithCcPlayer(player, out error);
                case "player_reload_active_weapon":
                    return TryReloadActiveWeapon(player, out error);
                case "player_drain_active_weapon_ammo":
                    return TryDrainActiveWeaponAmmo(player, out error);
                case "player_bleed":
                    return TryBleedPlayer(player, GetEffectAmount(effectPayload, 20), out error);
                case "player_fracture":
                    return TryFracturePlayer(player, out error);
                case "player_freeze_short":
                    return TryFreezeMovement(player, GetEffectAmount(effectPayload, 8), out error);
                case "player_god_mode_15s":
                    return TryActivateTemporaryPowerMode(player, 15, enableFly: false, out error);
                case "player_fly_mode_15s":
                    return TryActivateTemporaryPowerMode(player, 15, enableFly: true, out error);
                case "player_admin_power_15s":
                    return TryActivateTemporaryPowerMode(player, 15, enableFly: true, out error);
                case "player_revive":
                    return TryRevivePlayer(player, out error);
                default:
                    error = $"Unknown instant effectID '{effectId}'.";
                    return false;
            }
        }

        private string NormalizeEffectId(string effectId)
        {
            return (effectId ?? string.Empty).Trim().ToLowerInvariant();
        }

        private bool IsSupportedEffectType(string effectType, string effectId)
        {
            if (string.Equals(effectType, "game", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (string.Equals(NormalizeEffectId(effectId), "test_hype_train", StringComparison.Ordinal))
            {
                return true;
            }

            if (IsExternalEffectRegistered(effectId))
            {
                return true;
            }

            return false;
        }

        private bool IsExternalEffectRegistered(string effectId)
        {
            var normalized = NormalizeEffectId(effectId);
            if (string.IsNullOrEmpty(normalized))
            {
                return false;
            }

            lock (_externalEffectsSync)
            {
                return _externalEffectsById.ContainsKey(normalized);
            }
        }

        private bool TryDispatchExternalEffect(
            string requestId,
            string effectId,
            string responseToken,
            BasePlayer player,
            JObject payload,
            JObject effectPayload,
            out string status,
            out string reason,
            out string playerMessage,
            out bool pending)
        {
            status = string.Empty;
            reason = string.Empty;
            playerMessage = string.Empty;
            pending = false;

            var normalizedEffectId = NormalizeEffectId(effectId);
            ExternalEffectDefinition definition = null;
            lock (_externalEffectsSync)
            {
                if (!_externalEffectsById.TryGetValue(normalizedEffectId, out definition))
                {
                    return false;
                }
            }

            if (definition == null || string.IsNullOrWhiteSpace(definition.ProviderName))
            {
                status = "failTemporary";
                reason = "External effect provider is not configured.";
                return true;
            }

            var providerPlugin = plugins.Find(definition.ProviderName);
            if (providerPlugin == null)
            {
                status = "failTemporary";
                reason = $"External effect provider '{definition.ProviderName}' is unavailable.";
                return true;
            }

            var context = new JObject
            {
                ["requestID"] = requestId,
                ["effectID"] = effectId,
                ["provider"] = definition.ProviderName,
                ["playerSteamID"] = player?.UserIDString ?? string.Empty,
                ["playerName"] = player?.displayName ?? string.Empty,
                ["effect"] = effectPayload != null ? (JObject)effectPayload.DeepClone() : new JObject(),
                ["payload"] = payload != null ? (JObject)payload.DeepClone() : new JObject()
            };

            object providerResult;
            try
            {
                providerResult = providerPlugin.Call(ExternalEffectHookName, context);
            }
            catch (Exception ex)
            {
                status = "failTemporary";
                reason = $"External effect provider threw an exception: {ex.Message}";
                return true;
            }

            ParseExternalProviderResult(providerResult, out status, out reason, out playerMessage, out pending);

            if (!pending)
            {
                status = NormalizeExternalCompletionStatus(status);
                return true;
            }

            var pendingState = new ExternalEffectPendingRequest
            {
                RequestId = requestId,
                EffectId = effectId,
                ProviderName = definition.ProviderName,
                SteamId = player?.UserIDString ?? string.Empty,
                Token = responseToken
            };
            pendingState.TimeoutTimer = timer.Once(ExternalEffectPendingTimeoutSeconds, () => HandleExternalEffectTimeout(requestId));

            lock (_externalEffectsSync)
            {
                if (_externalPendingRequests.TryGetValue(requestId, out var existingPending))
                {
                    existingPending?.TimeoutTimer?.Destroy();
                }
                _externalPendingRequests[requestId] = pendingState;
            }

            LogVerbose($"External effect pending requestID={requestId}, effectID={effectId}, provider={definition.ProviderName}.");
            return true;
        }

        private void ParseExternalProviderResult(object providerResult, out string status, out string reason, out string playerMessage, out bool pending)
        {
            status = "success";
            reason = string.Empty;
            playerMessage = string.Empty;
            pending = false;

            if (providerResult == null)
            {
                return;
            }

            if (providerResult is bool ok)
            {
                status = ok ? "success" : "failTemporary";
                reason = ok ? string.Empty : "External effect provider returned failure.";
                return;
            }

            if (providerResult is string statusText)
            {
                var normalized = (statusText ?? string.Empty).Trim();
                if (string.Equals(normalized, "pending", StringComparison.OrdinalIgnoreCase))
                {
                    pending = true;
                    status = "failTemporary";
                    reason = "Pending external completion.";
                    return;
                }

                status = normalized;
                return;
            }

            if (providerResult is JObject obj)
            {
                status = obj.Value<string>("status") ?? "success";
                reason = obj.Value<string>("reason") ?? string.Empty;
                playerMessage = obj.Value<string>("playerMessage")
                    ?? obj.Value<string>("message")
                    ?? string.Empty;
                pending = string.Equals(status, "pending", StringComparison.OrdinalIgnoreCase) ||
                    obj.Value<bool?>("pending") == true;
                return;
            }
        }

        private void HandleExternalEffectTimeout(string requestId)
        {
            if (string.IsNullOrWhiteSpace(requestId))
            {
                return;
            }

            ExternalEffectPendingRequest pendingState;
            lock (_externalEffectsSync)
            {
                if (!_externalPendingRequests.TryGetValue(requestId, out pendingState))
                {
                    return;
                }
                _externalPendingRequests.Remove(requestId);
            }

            pendingState?.TimeoutTimer?.Destroy();
            FireAndForget(
                SendEffectResponseAsync(pendingState?.Token, requestId, "failTemporary", "External effect timed out waiting for provider completion."),
                "external effect timeout"
            );
            LogVerbose($"External effect timed out requestID={requestId}, provider={pendingState?.ProviderName}.");
        }

        private void ShowExternalProviderMessage(BasePlayer player, string status, string playerMessage)
        {
            if (player == null || !player.IsConnected || string.IsNullOrWhiteSpace(playerMessage))
            {
                return;
            }

            if (string.Equals((status ?? string.Empty).Trim(), "pending", StringComparison.OrdinalIgnoreCase))
            {
                ShowEffectUi(player, "Crowd Control", playerMessage);
                return;
            }

            var normalizedStatus = NormalizeExternalCompletionStatus(status);
            if (string.Equals(normalizedStatus, "success", StringComparison.OrdinalIgnoreCase))
            {
                ShowEffectUi(player, "Crowd Control", playerMessage);
                return;
            }

            ShowErrorUi(player, playerMessage);
        }

        private bool IsGenericSpawnEffect(string effectId)
        {
            if (string.IsNullOrEmpty(effectId))
            {
                return false;
            }

            if (!effectId.StartsWith("spawn_", StringComparison.Ordinal))
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
                case "rhib":
                    forwardDistance = 12f;
                    upOffset = 0.5f;
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

            var caloriesThreshold = caloriesFull * refillThresholdPct;
            var hydrationThreshold = hydrationFull * refillThresholdPct;
            var caloriesValue = player.metabolism.calories.value;
            var hydrationValue = player.metabolism.hydration.value;

            // Only allow refill when at least one meter is below 80% so the effect is not wasted.
            if (caloriesValue >= caloriesThreshold && hydrationValue >= hydrationThreshold)
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
            if (player == null || player.IsDead())
            {
                error = "Full Heal requires a living player.";
                return false;
            }

            var healThreshold = player.MaxHealth() * 0.9f;
            if (player.health >= healThreshold)
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
            var qty = Mathf.Clamp(amount, 1, 1000);
            if (!TryGiveItem(player, "lowgradefuel", qty, out error))
            {
                return false;
            }

            return true;
        }

        private bool TryGiveHazmatSuit(BasePlayer player, out string error)
        {
            error = string.Empty;
            if (!TryGiveWearItem(player, "hazmatsuit", out error))
            {
                return false;
            }

            return true;
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
                return false;
            }

            return true;
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

            var items = wear.itemList.ToArray();
            foreach (var item in items)
            {
                if (item == null)
                {
                    continue;
                }

                item.RemoveFromContainer();
                item.Drop(player.GetDropPosition(), player.GetDropVelocity());
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

            var breakableFound = false;
            var changed = 0;
            foreach (var item in wear.itemList)
            {
                if (item == null)
                {
                    continue;
                }

                if (item.condition <= 0f)
                {
                    continue;
                }

                breakableFound = true;
                item.LoseCondition(item.condition + 9999f);
                changed++;
            }

            if (!breakableFound || changed <= 0)
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
            if (sky == null || sky.Cycle == null)
            {
                error = "Time cycle system is unavailable.";
                return false;
            }

            var hour = sky.Cycle.Hour;
            var isDay = hour >= 8f && hour < 18f;
            var isNight = hour >= 20f || hour < 5f;

            if (setDay && isDay)
            {
                error = $"It is already daytime ({hour:0.0}h).";
                return false;
            }

            if (!setDay && isNight)
            {
                error = $"It is already nighttime ({hour:0.0}h).";
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
                if (item == null || item.parent == null)
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
            for (var i = 0; i < items.Count; i++)
            {
                var item = items[i];
                if (item == null || item.parent == null)
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
            var result = new List<Item>();
            if (player?.inventory == null)
            {
                return result;
            }

            CollectItemsFromContainer(player.inventory.containerMain, result);
            CollectItemsFromContainer(player.inventory.containerBelt, result);
            CollectItemsFromContainer(player.inventory.containerWear, result);
            return result;
        }

        private void CollectItemsFromContainer(ItemContainer container, List<Item> target)
        {
            if (container?.itemList == null || target == null)
            {
                return;
            }

            var snapshot = container.itemList.ToArray();
            for (var i = 0; i < snapshot.Length; i++)
            {
                var item = snapshot[i];
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

            var removed = 0;
            removed += TryTakeAmount(player, "bandage", countPerType);
            removed += TryTakeAmount(player, "syringe.medical", countPerType);
            removed += TryTakeAmount(player, "largemedkit", countPerType);

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
            if (definition == null)
            {
                return 0;
            }

            return player.inventory.Take(null, definition.itemid, Mathf.Max(1, amount));
        }

        private bool TrySpawnEntityAtPlayer(BasePlayer player, string prefabPath, float distance, out string error)
        {
            error = string.Empty;
            if (!TryResolveGroundSpawnPosition(player, distance, out var position, out error))
            {
                return false;
            }

            var entity = GameManager.server.CreateEntity(prefabPath, position, Quaternion.identity, true);
            if (entity == null)
            {
                error = $"Failed to spawn entity '{prefabPath}'.";
                return false;
            }

            entity.Spawn();
            return true;
        }

        private bool TrySpawnPatrolHelicopterAtPlayer(BasePlayer player, out string error)
        {
            error = string.Empty;
            const string patrolHeliPrefab = "assets/prefabs/npc/patrol helicopter/patrolhelicopter.prefab";
            if (!TryResolveGroundSpawnPosition(player, 20f, out var basePosition, out error))
            {
                return false;
            }

            var position = basePosition + new Vector3(0f, 25f, 0f);
            var heli = GameManager.server.CreateEntity(patrolHeliPrefab, position, Quaternion.identity, true);
            if (heli == null)
            {
                error = "Failed to spawn patrol helicopter.";
                return false;
            }

            heli.Spawn();
            ShowEffectUi(player, "Crowd Control", "Patrol helicopter inbound.");
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

        private bool TryGiveMiniWithFuel(BasePlayer player, out string error)
        {
            if (!TrySpawnByShortnameAtPlayer(player, "minicopter.entity", 10f, 2f, "Minicopter spawned.", out error))
            {
                return false;
            }

            // Give fuel to player inventory as a practical cross-build fallback.
            TryGiveItem(player, "lowgradefuel", 50, out _);
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
            var desired = Mathf.Clamp(count, 1, 8);
            var occupiedPositions = new List<Vector3>();
            for (var i = 0; i < desired; i++)
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
            var desired = Mathf.Clamp(count, 1, 8);
            var occupiedPositions = new List<Vector3>();
            for (var i = 0; i < desired; i++)
            {
                if (!TryResolveEnemySpawnPosition(player, centerPosition, 4f, 8f, 2.5f, occupiedPositions, out var position))
                {
                    continue;
                }
                var cmd = $"entity.spawn scarecrow {position.x:0.00},{position.y:0.00},{position.z:0.00}";

                try
                {
                    ConsoleSystem.Run(ConsoleSystem.Option.Server.Quiet(), cmd);
                    occupiedPositions.Add(position);
                    spawned++;
                }
                catch
                {
                    // Ignore individual spawn failures and continue trying the rest.
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
            if (string.IsNullOrWhiteSpace(shortName))
            {
                error = "Failed to spawn enemy.";
                return false;
            }

            if (!TryResolveGroundSpawnPosition(player, forwardDistance, out var centerPosition, out error))
            {
                return false;
            }

            if (!TryResolveEnemySpawnPosition(player, centerPosition, 0.5f, 4f, 2.5f, null, out var spawnPosition))
            {
                error = "Unable to find safe enemy spawn ground.";
                return false;
            }

            var cmd = $"entity.spawn {shortName} {spawnPosition.x:0.00},{spawnPosition.y:0.00},{spawnPosition.z:0.00}";
            try
            {
                ConsoleSystem.Run(ConsoleSystem.Option.Server.Quiet(), cmd);
                ShowEffectUi(player, "Crowd Control", BuildSpawnSuccessMessage(shortName));
                return true;
            }
            catch (Exception ex)
            {
                error = $"Failed to spawn enemy. {ex.Message}";
                return false;
            }
        }

        private bool TryForcePlaceSleepingBagAtPlayer(BasePlayer player, out string error)
        {
            error = string.Empty;
            if (player == null || !player.IsConnected)
            {
                error = "Player is not connected.";
                return false;
            }
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
                playerPos + (facing * -0.75f), // Try feet/behind first to avoid colliding with player capsule.
                playerPos + (right * 0.65f),
                playerPos + (right * -0.65f),
                playerPos + (facing * 0.65f),
                playerPos
            };

            var found = false;
            for (var i = 0; i < probes.Length; i++)
            {
                var probe = probes[i];
                if (!TryResolveSpawnGroundY(player, probe, out var groundY))
                {
                    continue;
                }

                var candidate = new Vector3(probe.x, groundY, probe.z);
                var floorCheckOrigin = candidate + Vector3.up * 3f;
                if (!Physics.Raycast(floorCheckOrigin, Vector3.down, out var floorHit, 8f))
                {
                    continue;
                }

                if (floorHit.normal.y < 0.55f)
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

            var spawnRotation = Quaternion.Euler(0f, yaw, 0f);
            var bagEntity = GameManager.server.CreateEntity(sleepingBagPrefab, basePosition + Vector3.up * 0.02f, spawnRotation, true);
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

        private bool TryRunHypeTrainTestEffect(BasePlayer player, JObject requestPayload, JObject effectPayload, out string error)
        {
            error = string.Empty;
            if (player == null || !player.IsConnected || player.IsDead())
            {
                error = "Player must be alive and connected.";
                return false;
            }

            var playerPos = player.transform.position;
            var direction = player.eyes != null
                ? (player.eyes.rotation * Vector3.forward)
                : player.transform.forward;
            direction.y = 0f;
            if (direction.sqrMagnitude < 0.01f)
            {
                direction = Vector3.forward;
            }
            direction.Normalize();

            var spawnProbe = playerPos + (direction * 10f);
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
            var speed = 11.5f;
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

                    var nextPos = trainEntity.transform.position + (direction * (speed * 0.2f));
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
                    if (player == null || !player.IsConnected)
                    {
                        return;
                    }

                    ShowEffectUi(player, "Hype Train", $"Rider {localIndex + 1}: {riders[localIndex]}");
                });
            }

            timer.Once(rideSeconds - 1f, () =>
            {
                if (player == null || !player.IsConnected)
                {
                    return;
                }
                ShowEffectUi(player, "Hype Train", "Hype train departed.");
            });

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

            for (var i = 0; i < candidates.Length; i++)
            {
                try
                {
                    var created = GameManager.server.CreateEntity(candidates[i].Prefab, position, rotation, true);
                    if (created == null)
                    {
                        LogVerbose($"HypeTrain candidate returned null prefab={candidates[i].Prefab}");
                        continue;
                    }

                    created.Spawn();
                    entity = created;
                    label = candidates[i].Name;
                    LogVerbose($"HypeTrain spawned prefab={candidates[i].Prefab} label={label} pos={position}");
                    return true;
                }
                catch (Exception ex)
                {
                    LogVerbose($"HypeTrain spawn failed prefab={candidates[i].Prefab} reason={ex.Message}");
                }
            }

            // Fallback: try entity shortnames via console for server-build compatibility.
            var shortNameCandidates = new[] { "locomotive", "workcart", "trainwagon_a" };
            for (var i = 0; i < shortNameCandidates.Length; i++)
            {
                try
                {
                    var cmd = $"entity.spawn {shortNameCandidates[i]} {position.x:0.00},{position.y:0.00},{position.z:0.00}";
                    ConsoleSystem.Run(ConsoleSystem.Option.Server.Quiet(), cmd);
                    label = shortNameCandidates[i];
                    LogVerbose($"HypeTrain fallback command executed shortname={shortNameCandidates[i]} pos={position}");
                    return true;
                }
                catch
                {
                    // Try the next spawn fallback.
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
                riders.Add("Conductor_Jax");
                riders.Add("SubTrainRider");
                riders.Add("BitBomber");
                riders.Add("GiftDropper");
            }

            if (riders.Count > 8)
            {
                riders.RemoveRange(8, riders.Count - 8);
            }

            return riders;
        }

        private void AppendNamesFromToken(List<string> riders, JToken token)
        {
            if (riders == null || !(token is JArray arr))
            {
                return;
            }

            for (var i = 0; i < arr.Count; i++)
            {
                var name = arr[i]?.ToString()?.Trim();
                if (!string.IsNullOrEmpty(name))
                {
                    riders.Add(name);
                }
            }
        }

        private void TryPlayHypeTrainSound(BasePlayer player)
        {
            if (player == null || !player.IsConnected)
            {
                return;
            }

            try
            {
                player.SendConsoleCommand("playsound", "assets/bundled/prefabs/fx/notice/item.select.fx.prefab");
                player.SendConsoleCommand("client.playsound", "assets/bundled/prefabs/fx/notice/item.select.fx.prefab");
            }
            catch (Exception ex)
            {
                LogVerbose($"Hype train sound failed for {player.displayName}: {ex.Message}");
            }
        }

        private void TryRunHypeTrainWorldFx(BasePlayer player, Vector3 position)
        {
            if (player == null || !player.IsConnected)
            {
                return;
            }

            try
            {
                var cmd = $"effect.run assets/bundled/prefabs/fx/notice/item.select.fx.prefab {position.x:0.00},{position.y:0.00},{position.z:0.00}";
                ConsoleSystem.Run(ConsoleSystem.Option.Server.Quiet(), cmd);
            }
            catch (Exception ex)
            {
                LogVerbose($"Hype train world FX failed: {ex.Message}");
            }
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

        private bool IsPlayerMounted(BasePlayer player)
        {
            if (player == null)
            {
                return false;
            }

            var flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
            try
            {
                var method = player.GetType().GetMethod("GetMounted", flags);
                if (method != null && method.GetParameters().Length == 0)
                {
                    return method.Invoke(player, null) != null;
                }
            }
            catch
            {
            }

            return false;
        }

        private bool IsPlayerInLikelyAirState(BasePlayer player)
        {
            if (player == null)
            {
                return true;
            }

            var modelState = GetMemberObject(player, "modelState");
            if (modelState != null)
            {
                if (TryGetBoolMember(modelState, "flying", out var flying) && flying)
                {
                    return true;
                }
                if (TryGetBoolMember(modelState, "swimming", out var swimming) && swimming)
                {
                    return true;
                }
            }

            return false;
        }

        private bool TryResolveEnemySpawnPosition(
            BasePlayer player,
            Vector3 centerPosition,
            float minRadius,
            float maxRadius,
            float minSeparation,
            List<Vector3> occupiedPositions,
            out Vector3 spawnPosition)
        {
            spawnPosition = Vector3.zero;
            var minR = Mathf.Max(0f, minRadius);
            var maxR = Mathf.Max(minR + 0.1f, maxRadius);
            var separation = Mathf.Max(0.5f, minSeparation);

            for (var attempt = 0; attempt < 24; attempt++)
            {
                var angle = UnityEngine.Random.Range(0f, 360f);
                var dir = Quaternion.Euler(0f, angle, 0f) * Vector3.forward;
                var radius = UnityEngine.Random.Range(minR, maxR);
                var probe = centerPosition + (dir * radius);

                if (!TryResolveSpawnGroundY(player, probe, out var groundY))
                {
                    continue;
                }

                var candidate = new Vector3(probe.x, groundY, probe.z);
                var floorCheckOrigin = candidate + Vector3.up * 3f;
                if (!Physics.Raycast(floorCheckOrigin, Vector3.down, out var floorHit, 8f))
                {
                    continue;
                }

                if (floorHit.normal.y < 0.55f)
                {
                    continue;
                }

                candidate.y = floorHit.point.y;
                if (IsEnemySpawnPositionCrowded(candidate, separation, occupiedPositions))
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
                for (var i = 0; i < occupiedPositions.Count; i++)
                {
                    if (Vector3.Distance(position, occupiedPositions[i]) < minSeparation)
                    {
                        return true;
                    }
                }
            }

            var nearby = Physics.OverlapSphere(position + Vector3.up * 0.5f, minSeparation);
            for (var i = 0; i < nearby.Length; i++)
            {
                var collider = nearby[i];
                if (collider == null)
                {
                    continue;
                }

                var entity = collider.GetComponentInParent<BaseEntity>();
                if (entity == null || entity.IsDestroyed)
                {
                    continue;
                }

                if (entity is BasePlayer)
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

        private bool TrySpawnOreNodes(BasePlayer player, string nodeType, int count, out string error)
        {
            error = string.Empty;
            if (player == null || !player.IsConnected)
            {
                error = "Player is not connected.";
                return false;
            }

            var desired = Mathf.Clamp(count, 1, 8);
            var spawned = 0;
            for (var i = 0; i < desired; i++)
            {
                if (!TryResolveNodeSpawnPosition(player, 6f, 16f, out var position))
                {
                    continue;
                }

                var prefabPath = GetOreNodePrefabPath(nodeType);
                if (string.IsNullOrEmpty(prefabPath))
                {
                    error = "Unknown node type.";
                    return false;
                }

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
                case "random":
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
            if (player == null || player.transform == null || TerrainMeta.HeightMap == null)
            {
                return false;
            }

            var center = player.transform.position;
            for (var attempt = 0; attempt < 30; attempt++)
            {
                var angle = UnityEngine.Random.Range(0f, 360f);
                var direction = Quaternion.Euler(0f, angle, 0f) * Vector3.forward;
                var distance = UnityEngine.Random.Range(minDistance, maxDistance);
                var probe = center + direction * distance;

                var groundY = TerrainMeta.HeightMap.GetHeight(probe);
                var candidate = new Vector3(probe.x, groundY + 0.05f, probe.z);

                // Must be actual ground (not roofs/foundations) and not inside nearby structures.
                if (!Physics.Raycast(candidate + Vector3.up * 8f, Vector3.down, out var hit, 20f))
                {
                    continue;
                }

                if (hit.normal.y < 0.65f)
                {
                    continue;
                }

                if (Mathf.Abs(hit.point.y - groundY) > 1.25f)
                {
                    continue;
                }

                if (IsNearStructure(candidate, 2.5f))
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
            var colliders = Physics.OverlapSphere(position + Vector3.up * 0.8f, Mathf.Max(0.5f, radius));
            for (var i = 0; i < colliders.Length; i++)
            {
                var collider = colliders[i];
                if (collider == null)
                {
                    continue;
                }

                var block = collider.GetComponentInParent<BuildingBlock>();
                if (block != null)
                {
                    return true;
                }

                var entity = collider.GetComponentInParent<BaseEntity>();
                if (entity == null)
                {
                    continue;
                }

                var prefabName = entity.ShortPrefabName ?? string.Empty;
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

        private bool TrySpawnVehicleAtPlayer(
            BasePlayer player,
            string prefabPath,
            float forwardDistance,
            float upOffset,
            string successMessage,
            int fuelAmount,
            out string error
        )
        {
            error = string.Empty;
            if (player == null || !player.IsConnected)
            {
                error = "Player is not connected.";
                return false;
            }

            if (!TryResolveGroundSpawnPosition(player, forwardDistance, out var basePosition, out error))
            {
                return false;
            }

            var position = basePosition + new Vector3(0f, upOffset, 0f);
            var vehicle = GameManager.server.CreateEntity(prefabPath, position, Quaternion.identity, true);
            if (vehicle == null)
            {
                error = $"Failed to spawn vehicle '{prefabPath}'.";
                return false;
            }

            vehicle.Spawn();

            // Always provide fuel to the targeted player so spawned vehicles are usable immediately.
            var giveFuelResult = TryGiveItem(player, "lowgradefuel", Mathf.Max(0, fuelAmount), out var fuelError);
            if (!giveFuelResult && fuelAmount > 0)
            {
                LogVerbose($"Fuel grant failed after vehicle spawn: {fuelError}");
            }

            ShowEffectUi(player, "Crowd Control", successMessage);
            return true;
        }

        private bool TryResolveGroundSpawnPosition(BasePlayer player, float preferredDistance, out Vector3 spawnPosition, out string error)
        {
            spawnPosition = Vector3.zero;
            error = string.Empty;
            if (player == null || player.eyes == null)
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

            // Anchor around where the player is looking, but allow nearby alternatives.
            var anchor = eyes + (forward * desired);
            if (Physics.Raycast(eyes, forward, out var lookHit, 60f))
            {
                var lookDistance = Mathf.Clamp(Vector3.Distance(eyes, lookHit.point), 4f, maxFromPlayer);
                anchor = eyes + (forward * lookDistance);
            }

            // Prefer slightly-in-front positions first, then broader nearby options.
            var forwardOffsets = new[] { 0f, 1.5f, -1.5f, 3f, -3f };
            var lateralOffsets = new[] { 0f, 1.5f, -1.5f, 3f, -3f };

            for (var i = 0; i < forwardOffsets.Length; i++)
            {
                for (var j = 0; j < lateralOffsets.Length; j++)
                {
                    var probe = anchor + (forward * forwardOffsets[i]) + (right * lateralOffsets[j]);
                    if (!TryResolveSpawnGroundY(player, probe, out var groundY))
                    {
                        continue;
                    }
                    var candidate = new Vector3(probe.x, groundY, probe.z);

                    // Confirm a real floor directly under the candidate so we don't spawn floating.
                    var floorCheckOrigin = candidate + Vector3.up * 3f;
                    if (!Physics.Raycast(floorCheckOrigin, Vector3.down, out var floorHit, 8f))
                    {
                        continue;
                    }

                    if (floorHit.normal.y < 0.55f)
                    {
                        continue;
                    }

                    candidate.y = floorHit.point.y;

                    // Avoid spawning too close to the player.
                    if (Vector3.Distance(player.transform.position, candidate) < 2f)
                    {
                        continue;
                    }

                    if (Vector3.Distance(player.transform.position, candidate) > maxFromPlayer)
                    {
                        continue;
                    }

                    var toCandidate = (candidate - player.transform.position);
                    toCandidate.y = 0f;
                    if (toCandidate.sqrMagnitude > 0.001f)
                    {
                        toCandidate.Normalize();
                        // Keep spawn generally in front of the player.
                        if (Vector3.Dot(forward, toCandidate) < 0.15f)
                        {
                            continue;
                        }
                    }

                    // Keep a little breathing room around the spawn point; use player-facing local axes.
                    var checkOrigin = candidate + Vector3.up * 0.8f;
                    if (Physics.Raycast(checkOrigin, forward, 1.0f) ||
                        Physics.Raycast(checkOrigin, -forward, 1.0f) ||
                        Physics.Raycast(checkOrigin, right, 1.0f) ||
                        Physics.Raycast(checkOrigin, -right, 1.0f))
                    {
                        continue;
                    }

                    spawnPosition = candidate;
                    return true;
                }
            }

            error = "Unable to spawn at current location. Try again.";
            return false;
        }

        private bool TryResolveSpawnGroundY(BasePlayer player, Vector3 probe, out float groundY)
        {
            groundY = probe.y;
            if (player == null || player.transform == null)
            {
                return false;
            }

            var playerPos = player.transform.position;
            var playerY = playerPos.y;
            var terrainAtPlayer = TerrainMeta.HeightMap != null ? TerrainMeta.HeightMap.GetHeight(playerPos) : playerY;
            var isLikelyUnderground = playerY + 2f < terrainAtPlayer;

            // Prefer surfaces near the player's current elevation first (important for caves/underground).
            var sameLevelOrigins = new[]
            {
                new Vector3(probe.x, playerY + 2.5f, probe.z),
                new Vector3(probe.x, playerY + 6f, probe.z)
            };

            for (var i = 0; i < sameLevelOrigins.Length; i++)
            {
                if (Physics.Raycast(sameLevelOrigins[i], Vector3.down, out var sameLevelHit, 18f))
                {
                    if (sameLevelHit.normal.y < 0.55f)
                    {
                        continue;
                    }

                    groundY = sameLevelHit.point.y;
                    return true;
                }
            }

            // General fallback: get a real collider-backed floor under the probe.
            var broadOrigin = new Vector3(probe.x, playerY + 30f, probe.z);
            if (Physics.Raycast(broadOrigin, Vector3.down, out var broadHit, 80f))
            {
                if (broadHit.normal.y >= 0.55f)
                {
                    groundY = broadHit.point.y;
                    return true;
                }
            }

            // Last resort: terrain map, but only when player isn't clearly underground.
            if (TerrainMeta.HeightMap != null)
            {
                if (isLikelyUnderground)
                {
                    return false;
                }

                groundY = TerrainMeta.HeightMap.GetHeight(probe);
                return true;
            }

            return false;
        }

        private bool TrySpawnByShortnameAtPlayer(
            BasePlayer player,
            string shortName,
            float forwardDistance,
            float upOffset,
            string successMessage,
            out string error
        )
        {
            return TrySpawnByShortnameAtPlayer(player, shortName, forwardDistance, upOffset, successMessage, 0, out error);
        }

        private bool TrySpawnByShortnameAtPlayer(
            BasePlayer player,
            string shortName,
            float forwardDistance,
            float upOffset,
            string successMessage,
            int fuelAmount,
            out string error
        )
        {
            error = string.Empty;
            if (string.IsNullOrWhiteSpace(shortName))
            {
                error = "Failed to spawn entity.";
                return false;
            }

            if (!TryResolveGroundSpawnPosition(player, forwardDistance, out var basePosition, out error))
            {
                return false;
            }

            var position = basePosition + new Vector3(0f, upOffset, 0f);
            var cmd = $"entity.spawn {shortName} {position.x:0.00},{position.y:0.00},{position.z:0.00}";
            try
            {
                ConsoleSystem.Run(ConsoleSystem.Option.Server.Quiet(), cmd);
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

            var baseMessage = string.IsNullOrWhiteSpace(successMessage)
                ? BuildSpawnSuccessMessage(shortName)
                : successMessage;
            ShowEffectUi(player, "Crowd Control", baseMessage);
            return true;
        }

        private string BuildSpawnSuccessMessage(string shortName)
        {
            if (string.IsNullOrWhiteSpace(shortName))
            {
                return "Spawned.";
            }

            var normalized = shortName.Replace('.', ' ').Replace('_', ' ').Trim();
            if (string.IsNullOrEmpty(normalized))
            {
                return "Spawned.";
            }

            var parts = normalized.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            for (var i = 0; i < parts.Length; i++)
            {
                var part = parts[i];
                parts[i] = part.Length == 1
                    ? part.ToUpperInvariant()
                    : char.ToUpperInvariant(part[0]) + part.Substring(1);
            }

            return $"{string.Join(" ", parts)} spawned.";
        }

        private bool TryRegisterRequestId(string requestId)
        {
            if (string.IsNullOrEmpty(requestId))
            {
                return true;
            }

            lock (_requestIdSync)
            {
                if (_recentHandledRequestIds.TryGetValue(requestId, out var seenAt))
                {
                    if (DateTime.UtcNow - seenAt < TimeSpan.FromMinutes(5))
                    {
                        return false;
                    }
                }

                _recentHandledRequestIds[requestId] = DateTime.UtcNow;
                if (_recentHandledRequestIds.Count <= 2048)
                {
                    return true;
                }

                var cutoff = DateTime.UtcNow.AddMinutes(-10);
                var toRemove = new List<string>();
                foreach (var kvp in _recentHandledRequestIds)
                {
                    if (kvp.Value < cutoff)
                    {
                        toRemove.Add(kvp.Key);
                    }
                }

                foreach (var key in toRemove)
                {
                    _recentHandledRequestIds.Remove(key);
                }
            }

            return true;
        }

        private bool TrySpawnModularCarAtPlayer(
            BasePlayer player,
            string prefabPath,
            float forwardDistance,
            float upOffset,
            string successMessage,
            int fuelAmount,
            int engineKits,
            out string error
        )
        {
            if (!TrySpawnVehicleAtPlayer(player, prefabPath, forwardDistance, upOffset, successMessage, fuelAmount, out error))
            {
                return false;
            }

            if (!TryGiveModularCarEngineParts(player, Mathf.Max(1, engineKits), out var partsError))
            {
                // Vehicle still spawned, so keep effect successful and only log add-on grant failures.
                LogVerbose($"Car parts grant issue after spawn: {partsError}");
                return true;
            }

            return true;
        }

        private bool TrySpawnModularCarByShortnameAtPlayer(
            BasePlayer player,
            string shortName,
            float forwardDistance,
            float upOffset,
            string successMessage,
            int fuelAmount,
            int engineKits,
            out string error
        )
        {
            if (!TrySpawnByShortnameAtPlayer(player, shortName, forwardDistance, upOffset, successMessage, fuelAmount, out error))
            {
                return false;
            }

            if (!TryGiveModularCarEngineParts(player, Mathf.Max(1, engineKits), out var partsError))
            {
                LogVerbose($"Car parts grant issue after spawn: {partsError}");
                return true;
            }

            return true;
        }

        private bool TryGiveModularCarEngineParts(BasePlayer player, int engineKits, out string error)
        {
            error = string.Empty;
            var kits = Mathf.Clamp(engineKits, 1, 3);
            var ok = true;

            for (var i = 0; i < kits; i++)
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
                return false;
            }

            return true;
        }

        private bool TrySwapTeleportWithCcPlayer(BasePlayer player, out string error)
        {
            error = string.Empty;
            var candidates = GetActiveCcPlayers();
            candidates.RemoveAll(x => x.UserIDString == player.UserIDString);
            if (candidates.Count == 0)
            {
                error = "No other active Crowd Control player is available for swap teleport.";
                return false;
            }

            var target = candidates[UnityEngine.Random.Range(0, candidates.Count)];
            var sourcePos = player.transform.position;
            var targetPos = target.transform.position;

            player.Teleport(targetPos + new Vector3(0f, 0f, 1.5f));
            target.Teleport(sourcePos + new Vector3(0f, 0f, 1.5f));
            return true;
        }

        private bool TryTeleportToSleepingBag(BasePlayer player, out string error)
        {
            error = string.Empty;
            if (player == null || !player.IsConnected)
            {
                error = "Player is not connected.";
                return false;
            }

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
                if (bag == null || bag.IsDestroyed)
                {
                    continue;
                }

                if (bag.deployerUserID != player.userID)
                {
                    continue;
                }

                results.Add(bag);
            }

            return results;
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
            if (player.metabolism == null || player.metabolism.bleeding == null)
            {
                error = "Bleeding metabolism is not available.";
                return false;
            }

            var delta = Mathf.Clamp(amount, 1, 100);
            player.metabolism.bleeding.value = Mathf.Clamp(player.metabolism.bleeding.value + delta, 0f, 200f);
            player.metabolism.SendChangesToClient();
            return true;
        }

        private bool TryFracturePlayer(BasePlayer player, out string error)
        {
            error = string.Empty;
            // Rust does not provide a stable public "set fractured" API across builds.
            // Apply a fracture-like penalty: damage + bleed + brief movement freeze.
            player.Hurt(15f);
            if (player.metabolism?.bleeding != null)
            {
                player.metabolism.bleeding.value = Mathf.Clamp(player.metabolism.bleeding.value + 10f, 0f, 200f);
                player.metabolism.SendChangesToClient();
            }

            return TryFreezeMovement(player, 6, out error);
        }

        private bool TryFreezeMovement(BasePlayer player, int seconds, out string error)
        {
            error = string.Empty;
            var duration = Mathf.Clamp(seconds, 1, 30);
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

            // Keep forcing the same position briefly to simulate movement freeze across Rust builds.
            freezeState.EnforceTimer = timer.Every(0.1f, () =>
            {
                var current = FindPlayerBySteamId(steamId);
                if (current == null || !current.IsConnected)
                {
                    return;
                }

                current.Teleport(freezeState.AnchorPosition);
            });

            freezeState.EndTimer = timer.Once(duration, () =>
            {
                if (_movementFreezeTimers.TryGetValue(steamId, out var currentState))
                {
                    currentState.EnforceTimer?.Destroy();
                    currentState.EndTimer?.Destroy();
                    _movementFreezeTimers.Remove(steamId);
                }
            });

            _movementFreezeTimers[steamId] = freezeState;
            return true;
        }

        private bool TryHandcuffPlayer(BasePlayer player, int seconds, out string error)
        {
            error = string.Empty;
            if (player == null || !player.IsConnected)
            {
                error = "Player is not connected.";
                return false;
            }

            var duration = Mathf.Clamp(seconds, 3, 30);
            if (!TrySetRestrainedStatus(player, true, out error))
            {
                return false;
            }

            // Ensure we are using native restraint behavior only (no freeze fallback).
            ClearMovementFreeze(player);

            var steamId = player.UserIDString;
            if (_activeHandcuffTimers.TryGetValue(steamId, out var existingEndTimer))
            {
                existingEndTimer?.Destroy();
                _activeHandcuffTimers.Remove(steamId);
            }

            _activeHandcuffTimers[steamId] = timer.Once(duration, () => EndHandcuffEffect(steamId));
            ShowEffectUi(player, "Crowd Control", $"Handcuffed for {duration}s.");
            return true;
        }

        private void EndHandcuffEffect(string steamId)
        {
            if (string.IsNullOrEmpty(steamId))
            {
                return;
            }

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
            if (player == null)
            {
                error = "Player is not valid.";
                return false;
            }

            try
            {
                // Prefer dedicated restraint APIs when present.
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

                MethodInfo setFlagMethod = null;
                foreach (var method in player.GetType().GetMethods(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
                {
                    if (!string.Equals(method.Name, "SetPlayerFlag", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    var parameters = method.GetParameters();
                    if (parameters.Length != 2 || parameters[1].ParameterType != typeof(bool) || !parameters[0].ParameterType.IsEnum)
                    {
                        continue;
                    }

                    var enumTypeName = parameters[0].ParameterType.Name ?? string.Empty;
                    if (enumTypeName.IndexOf("PlayerFlags", StringComparison.OrdinalIgnoreCase) < 0)
                    {
                        continue;
                    }

                    setFlagMethod = method;
                    break;
                }

                if (setFlagMethod == null)
                {
                    error = "Handcuff status API is unavailable on this server build.";
                    return false;
                }

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

                if (restrainedFlag == null)
                {
                    error = "Handcuff flag is unavailable on this server build.";
                    return false;
                }

                setFlagMethod.Invoke(player, new[] { restrainedFlag, (object)restrained });
                player.SendNetworkUpdateImmediate();
                return true;
            }
            catch (Exception ex)
            {
                error = $"Unable to update handcuff status: {ex.Message}";
                return false;
            }
        }

        private bool TrySetPlayerOnFire(BasePlayer player, int seconds, out string error)
        {
            error = string.Empty;
            if (player == null || !player.IsConnected)
            {
                error = "Player is not connected.";
                return false;
            }

            var duration = Mathf.Clamp(seconds, 2, 12);
            var steamId = player.UserIDString;
            EndBurnEffect(steamId);

            // Try to trigger native burn visuals when available; fallback damage still applies.
            TryIgnitePlayer(player, duration);

            var remainingTicks = duration;
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

            ShowEffectUi(player, "Crowd Control", $"Set you on fire!.");
            return true;
        }

        private void EndBurnEffect(string steamId)
        {
            if (string.IsNullOrEmpty(steamId))
            {
                return;
            }

            if (_activeBurnTimers.TryGetValue(steamId, out var timerInstance))
            {
                timerInstance?.Destroy();
                _activeBurnTimers.Remove(steamId);
            }
        }

        private void TryIgnitePlayer(BasePlayer player, int seconds)
        {
            try
            {
                var method = player.GetType().GetMethod("Ignite", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                if (method == null)
                {
                    return;
                }

                var args = method.GetParameters().Length == 0
                    ? Array.Empty<object>()
                    : new object[] { Mathf.Max(1f, seconds) };
                method.Invoke(player, args);
            }
            catch
            {
                // Burn visuals are best-effort only.
            }
        }

        private void TrySpawnFireAroundPlayer(BasePlayer player, int count)
        {
            if (player == null || !player.IsConnected)
            {
                return;
            }

            var center = player.transform.position;
            var amount = Mathf.Clamp(count, 1, 4);
            for (var i = 0; i < amount; i++)
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
                    fireEntity = GameManager.server.CreateEntity("assets/bundled/prefabs/oilfireballsmall.prefab", position, Quaternion.identity, true);
                    if (fireEntity == null)
                    {
                        fireEntity = GameManager.server.CreateEntity("assets/bundled/prefabs/fireball_small.prefab", position, Quaternion.identity, true);
                    }

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
            if (player == null || !player.IsConnected)
            {
                error = "Player is not connected.";
                return false;
            }

            var steamId = player.UserIDString;
            EndTemporaryPowerMode(steamId, restoreFly: enableFly, showUi: false);

            _godModeSteamIds.Add(steamId);
            player.health = player.MaxHealth();
            if (player.metabolism?.bleeding != null)
            {
                player.metabolism.bleeding.value = 0f;
                player.metabolism.SendChangesToClient();
            }

            if (enableFly)
            {
                if (!_flyModeSteamIds.Contains(steamId))
                {
                    TryTogglePlayerNoClip(player);
                    _flyModeSteamIds.Add(steamId);
                }
            }

            ShowEffectUi(player, "Crowd Control", enableFly ? "Admin Power active for 15s" : "God Mode active for 15s");
            _activePowerModeTimers[steamId] = timer.Once(Mathf.Max(1, seconds), () =>
            {
                EndTemporaryPowerMode(steamId, restoreFly: enableFly, showUi: true);
            });

            return true;
        }

        private void EndTemporaryPowerMode(string steamId, bool restoreFly, bool showUi = false)
        {
            if (string.IsNullOrEmpty(steamId))
            {
                return;
            }

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

        private bool TryHealPlayer(BasePlayer player, float amount, out string error)
        {
            error = string.Empty;
            if (player == null || player.IsDead())
            {
                error = "Cannot heal while dead.";
                return false;
            }

            var maxHealth = player.MaxHealth();
            var healThreshold = maxHealth * 0.91f;
            if (player.health >= healThreshold)
            {
                error = "Player is healed enough.";
                return false;
            }

            player.health = Mathf.Min(maxHealth, player.health + Mathf.Max(1f, amount));
            player.SendNetworkUpdateImmediate();
            return true;
        }

        private bool TryActivateMovementModifier(
            BasePlayer player,
            int seconds,
            float? speedMultiplier,
            float? gravityMultiplier,
            float? jumpMultiplier,
            string label,
            out string error
        )
        {
            error = string.Empty;
            if (player == null || !player.IsConnected || player.IsDead())
            {
                error = "Player must be alive and connected.";
                return false;
            }

            var steamId = player.UserIDString;
            EndMovementModifier(steamId, showUi: false);

            var state = new MovementModifierState { Label = label };
            state.SpeedMultiplier = speedMultiplier;
            state.GravityMultiplier = gravityMultiplier;
            state.JumpMultiplier = jumpMultiplier;
            var movementObj = GetMemberObject(player, "movement");
            var changed = false;

            if (speedMultiplier.HasValue)
            {
                changed |= ScaleNumericMembers(movementObj, state.MovementOriginals, speedMultiplier.Value, new[]
                {
                    "maxVelocity", "running", "runSpeed", "sprinting", "sprintSpeed", "ducking", "duckSpeed", "swimming", "swimSpeed"
                });
            }

            if (gravityMultiplier.HasValue)
            {
                changed |= ScaleNumericMembers(movementObj, state.MovementOriginals, gravityMultiplier.Value, new[]
                {
                    "gravityMultiplier", "gravityScale", "gravity"
                });
            }

            if (jumpMultiplier.HasValue)
            {
                changed |= ScaleNumericMembers(movementObj, state.MovementOriginals, jumpMultiplier.Value, new[]
                {
                    "jumpHeight", "jumpTime", "jumpVelocity", "jumpPower"
                });
            }

            if (!changed)
            {
                if (!TryStartMovementFallback(player, state, out error))
                {
                    return false;
                }
                changed = true;
            }

            state.EndTimer = timer.Once(Mathf.Max(1, seconds), () => EndMovementModifier(steamId, showUi: true));
            _activeMovementModifiers[steamId] = state;
            player.SendNetworkUpdateImmediate();
            ShowEffectUi(player, "Crowd Control", $"{label} active for {seconds}s");
            return true;
        }

        private void EndMovementModifier(string steamId, bool showUi)
        {
            if (string.IsNullOrEmpty(steamId))
            {
                return;
            }

            if (!_activeMovementModifiers.TryGetValue(steamId, out var state))
            {
                return;
            }

            state.EndTimer?.Destroy();
            state.TickTimer?.Destroy();
            _activeMovementModifiers.Remove(steamId);

            var player = FindPlayerBySteamId(steamId);
            if (player == null)
            {
                return;
            }

            var movementObj = GetMemberObject(player, "movement");
            RestoreNumericMembers(movementObj, state.MovementOriginals);
            RestoreNumericMembers(player, state.PlayerOriginals);
            player.SendNetworkUpdateImmediate();

            if (showUi)
            {
                ShowEffectUi(player, "Crowd Control", $"{state.Label} ended");
            }
        }

        private bool TryStartMovementFallback(BasePlayer player, MovementModifierState state, out string error)
        {
            error = string.Empty;
            if (player == null || state == null)
            {
                error = "Movement fallback is unavailable.";
                return false;
            }

            var steamId = player.UserIDString;
            state.UsingFallback = true;
            state.LastPosition = player.transform.position;
            state.LastGrounded = IsPlayerGrounded(player);
            state.TickTimer = timer.Every(0.1f, () =>
            {
                var current = FindPlayerBySteamId(steamId);
                if (current == null || !current.IsConnected || current.IsDead())
                {
                    return;
                }

                var currentPos = current.transform.position;
                var delta = currentPos - state.LastPosition;
                var adjustedPos = currentPos;

                // Speed fallback: scale actual horizontal displacement each tick.
                if (state.SpeedMultiplier.HasValue)
                {
                    var mult = Mathf.Clamp(state.SpeedMultiplier.Value, 0.2f, 3.0f);
                    var horiz = new Vector3(delta.x, 0f, delta.z);
                    if (horiz.sqrMagnitude > 0.0004f)
                    {
                        var scaled = horiz * mult;
                        adjustedPos = new Vector3(
                            state.LastPosition.x + scaled.x,
                            adjustedPos.y,
                            state.LastPosition.z + scaled.z
                        );
                    }
                }

                var grounded = IsPlayerGrounded(current);

                // Jump fallback: add a one-time upward boost when leaving ground.
                if (state.JumpMultiplier.HasValue && !grounded && state.LastGrounded && delta.y > 0.01f)
                {
                    var mult = Mathf.Clamp(state.JumpMultiplier.Value, 1f, 3f);
                    var boost = Mathf.Clamp((mult - 1f) * 0.75f, 0f, 2.2f);
                    adjustedPos += new Vector3(0f, boost, 0f);
                }

                // Gravity fallback: bias vertical movement while airborne.
                if (state.GravityMultiplier.HasValue && !grounded)
                {
                    var mult = Mathf.Clamp(state.GravityMultiplier.Value, 0.4f, 2.5f);
                    if (mult > 1f)
                    {
                        adjustedPos += new Vector3(0f, -(mult - 1f) * 0.25f, 0f);
                    }
                    else if (mult < 1f)
                    {
                        adjustedPos += new Vector3(0f, (1f - mult) * 0.15f, 0f);
                    }
                }

                if ((adjustedPos - currentPos).sqrMagnitude > 0.0001f)
                {
                    current.Teleport(adjustedPos);
                    currentPos = adjustedPos;
                }

                state.LastGrounded = grounded;
                state.LastPosition = currentPos;
            });

            LogVerbose($"Using movement fallback for {player.displayName} ({steamId}) label={state.Label}");
            return true;
        }

        private bool IsPlayerGrounded(BasePlayer player)
        {
            if (player == null)
            {
                return false;
            }

            try
            {
                var method = player.GetType().GetMethod("IsOnGround", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                if (method != null)
                {
                    var result = method.Invoke(player, null);
                    if (result is bool onGround)
                    {
                        return onGround;
                    }
                }
            }
            catch
            {
            }

            var modelState = GetMemberObject(player, "modelState");
            if (modelState != null)
            {
                if (TryGetBoolMember(modelState, "onGround", out var onGround))
                {
                    return onGround;
                }
                if (TryGetBoolMember(modelState, "onground", out onGround))
                {
                    return onGround;
                }
            }

            return false;
        }

        private object GetMemberObject(object target, string memberName)
        {
            if (target == null || string.IsNullOrEmpty(memberName))
            {
                return null;
            }

            var type = target.GetType();
            var flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
            var prop = type.GetProperty(memberName, flags);
            if (prop != null)
            {
                return prop.GetValue(target, null);
            }

            var field = type.GetField(memberName, flags);
            if (field != null)
            {
                return field.GetValue(target);
            }

            return null;
        }

        private bool ScaleNumericMembers(object target, Dictionary<string, float> originalStore, float multiplier, IEnumerable<string> memberNames)
        {
            if (target == null || originalStore == null || memberNames == null)
            {
                return false;
            }

            var changed = false;
            foreach (var name in memberNames)
            {
                if (!TryGetNumericMember(target, name, out var value))
                {
                    continue;
                }

                if (!originalStore.ContainsKey(name))
                {
                    originalStore[name] = value;
                }

                var scaled = value * multiplier;
                if (TrySetNumericMember(target, name, scaled))
                {
                    changed = true;
                }
            }

            return changed;
        }

        private void RestoreNumericMembers(object target, Dictionary<string, float> originals)
        {
            if (target == null || originals == null)
            {
                return;
            }

            foreach (var kvp in originals)
            {
                TrySetNumericMember(target, kvp.Key, kvp.Value);
            }
        }

        private bool TryGetNumericMember(object target, string memberName, out float value)
        {
            value = 0f;
            if (target == null || string.IsNullOrEmpty(memberName))
            {
                return false;
            }

            var type = target.GetType();
            var flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

            var prop = type.GetProperty(memberName, flags);
            if (prop != null && prop.CanRead)
            {
                var obj = prop.GetValue(target, null);
                return TryConvertToFloat(obj, out value);
            }

            var field = type.GetField(memberName, flags);
            if (field != null)
            {
                var obj = field.GetValue(target);
                return TryConvertToFloat(obj, out value);
            }

            return false;
        }

        private bool TrySetNumericMember(object target, string memberName, float value)
        {
            if (target == null || string.IsNullOrEmpty(memberName))
            {
                return false;
            }

            var type = target.GetType();
            var flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

            var prop = type.GetProperty(memberName, flags);
            if (prop != null && prop.CanWrite)
            {
                var converted = ConvertFloatToType(value, prop.PropertyType, out var boxed);
                if (!converted)
                {
                    return false;
                }

                prop.SetValue(target, boxed, null);
                return true;
            }

            var field = type.GetField(memberName, flags);
            if (field != null)
            {
                var converted = ConvertFloatToType(value, field.FieldType, out var boxed);
                if (!converted)
                {
                    return false;
                }

                field.SetValue(target, boxed);
                return true;
            }

            return false;
        }

        private bool TryConvertToFloat(object obj, out float value)
        {
            value = 0f;
            if (obj == null)
            {
                return false;
            }

            switch (obj)
            {
                case float f:
                    value = f;
                    return true;
                case double d:
                    value = (float)d;
                    return true;
                case int i:
                    value = i;
                    return true;
                case long l:
                    value = l;
                    return true;
                default:
                    return false;
            }
        }

        private bool ConvertFloatToType(float value, Type targetType, out object boxed)
        {
            boxed = null;
            if (targetType == typeof(float))
            {
                boxed = value;
                return true;
            }

            if (targetType == typeof(double))
            {
                boxed = (double)value;
                return true;
            }

            if (targetType == typeof(int))
            {
                boxed = Mathf.RoundToInt(value);
                return true;
            }

            if (targetType == typeof(long))
            {
                boxed = (long)Mathf.RoundToInt(value);
                return true;
            }

            return false;
        }

        private bool TryGetBoolMember(object target, string memberName, out bool value)
        {
            value = false;
            if (target == null || string.IsNullOrEmpty(memberName))
            {
                return false;
            }

            var type = target.GetType();
            var flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
            var prop = type.GetProperty(memberName, flags);
            if (prop != null && prop.CanRead && prop.PropertyType == typeof(bool))
            {
                var obj = prop.GetValue(target, null);
                if (obj is bool b)
                {
                    value = b;
                    return true;
                }
            }

            var field = type.GetField(memberName, flags);
            if (field != null && field.FieldType == typeof(bool))
            {
                var obj = field.GetValue(target);
                if (obj is bool b)
                {
                    value = b;
                    return true;
                }
            }

            return false;
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

            var steamId = player.UserIDString;
            if (!_lastDeathPositionBySteamId.TryGetValue(steamId, out var deathPos))
            {
                error = "No recorded death position for this player yet.";
                return false;
            }

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

        private void ShowEffectUi(BasePlayer player, string title, string message)
        {
            if (player == null || !player.IsConnected)
            {
                return;
            }

            var toastText = string.IsNullOrWhiteSpace(title)
                ? message
                : $"{title}: {message}";
            SendToast(player, 0, toastText);
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
            if (player == null || !player.IsConnected || string.IsNullOrWhiteSpace(text))
            {
                return;
            }

            player.SendConsoleCommand("gametip.showtoast", style, text);
        }

        private void NotifyActiveCcPlayersSessionDisconnected(string message)
        {
            if (_isUnloading || string.IsNullOrWhiteSpace(message))
            {
                return;
            }

            if (DateTime.UtcNow - _lastSessionDisconnectToastUtc < TimeSpan.FromSeconds(15))
            {
                return;
            }

            _lastSessionDisconnectToastUtc = DateTime.UtcNow;
            foreach (var kvp in _data.PlayerSessions)
            {
                var session = kvp.Value;
                if (session == null || string.IsNullOrEmpty(session.Token))
                {
                    continue;
                }

                var player = FindPlayerBySteamId(kvp.Key);
                if (player == null || !player.IsConnected)
                {
                    continue;
                }

                ShowErrorUi(player, message);
            }
        }

        private int GetEffectAmount(JObject effectPayload, int fallback)
        {
            if (effectPayload == null)
            {
                return fallback;
            }

            var direct =
                effectPayload.Value<int?>("amount") ??
                effectPayload.Value<int?>("quantity") ??
                effectPayload.Value<int?>("value");
            if (direct.HasValue && direct.Value > 0)
            {
                return direct.Value;
            }

            var options = effectPayload["options"] as JObject;
            if (options != null)
            {
                var optionValue =
                    options.Value<int?>("amount") ??
                    options.Value<int?>("quantity") ??
                    options.Value<int?>("value");
                if (optionValue.HasValue && optionValue.Value > 0)
                {
                    return optionValue.Value;
                }
            }

            return fallback;
        }

        private bool TryStartTimedEffect(BasePlayer player, PlayerSessionState session, string requestId, string effectId, int durationSeconds, out string error)
        {
            error = string.Empty;
            var normalized = NormalizeEffectId(effectId);
            if (normalized != "player_damage_over_time" && normalized != "player_heal_over_time")
            {
                error = $"Unknown timed effectID '{effectId}'.";
                return false;
            }

            var totalMs = durationSeconds * 1000L;
            var state = new TimedEffectState
            {
                RequestId = requestId,
                EffectId = normalized,
                SteamId = player.UserIDString,
                Token = session.Token,
                EndUtc = DateTime.UtcNow.AddMilliseconds(totalMs),
                RemainingMs = totalMs,
                IsPaused = false
            };

            _activeTimedEffects[requestId] = state;
            RestartTimedEffectTicker(state);
            FireAndForget(SendTimedResponseAsync(state.Token, requestId, "timedBegin", totalMs, string.Empty), "timedBegin response");
            return true;
        }

        private void RestartTimedEffectTicker(TimedEffectState state)
        {
            state.TickTimer?.Destroy();

            state.TickTimer = timer.Every(1f, () =>
            {
                if (state.IsPaused)
                {
                    return;
                }

                state.RemainingMs = Math.Max(0, (long)(state.EndUtc - DateTime.UtcNow).TotalMilliseconds);
                var player = FindPlayerBySteamId(state.SteamId);
                if (player == null || !player.IsConnected)
                {
                    return;
                }

                switch (state.EffectId)
                {
                    case "player_damage_over_time":
                        player.Hurt(5f);
                        break;
                    case "player_heal_over_time":
                        player.health = Mathf.Min(player.MaxHealth(), player.health + 5f);
                        player.SendNetworkUpdateImmediate();
                        break;
                }

                if (state.RemainingMs <= 0)
                {
                    EndTimedEffect(state.RequestId, "timedEnd");
                }
            });
        }

        private void EndTimedEffect(string requestId, string status)
        {
            if (!_activeTimedEffects.TryGetValue(requestId, out var state))
            {
                return;
            }

            state.TickTimer?.Destroy();
            _activeTimedEffects.Remove(requestId);
            FireAndForget(SendTimedResponseAsync(state.Token, requestId, status, null, string.Empty), $"{status} response");
        }

        private void StopTimedEffectsForSteamId(string steamId, string reason)
        {
            var toStop = new List<string>();
            foreach (var kvp in _activeTimedEffects)
            {
                var state = kvp.Value;
                if (state.SteamId != steamId)
                {
                    continue;
                }
                toStop.Add(kvp.Key);
            }

            foreach (var requestId in toStop)
            {
                if (_activeTimedEffects.TryGetValue(requestId, out var state))
                {
                    state.TickTimer?.Destroy();
                    _activeTimedEffects.Remove(requestId);
                    FireAndForget(SendTimedResponseAsync(state.Token, requestId, "timedEnd", null, reason), "timedEnd on disconnect");
                }
            }
        }

        private async Task SendEffectResponseAsync(string token, string requestId, string status, string message)
        {
            var arg = new JObject
            {
                ["id"] = Guid.NewGuid().ToString(),
                ["request"] = requestId,
                ["status"] = status,
                ["message"] = message ?? string.Empty,
                ["stamp"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            };

            await SendRpcAsync(token, "effectResponse", new JArray { arg });
        }

        private async Task SendTimedResponseAsync(string token, string requestId, string status, long? timeRemainingMs, string message)
        {
            var arg = new JObject
            {
                ["id"] = Guid.NewGuid().ToString(),
                ["request"] = requestId,
                ["status"] = status,
                ["message"] = message ?? string.Empty,
                ["stamp"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            };

            if (timeRemainingMs.HasValue && status != "timedEnd")
            {
                arg["timeRemaining"] = timeRemainingMs.Value;
            }

            await SendRpcAsync(token, "effectResponse", new JArray { arg });
        }

        private async Task SendRpcAsync(string token, string method, JArray args)
        {
            var payload = new JObject
            {
                ["action"] = "rpc",
                ["data"] = new JObject
                {
                    ["token"] = token,
                    ["call"] = new JObject
                    {
                        ["type"] = "call",
                        ["id"] = Guid.NewGuid().ToString(),
                        ["method"] = method,
                        ["args"] = args
                    }
                }
            };

            await SendSocketMessageAsync(payload);
        }

        #endregion

        #region Utility

        private BasePlayer FindPlayerBySteamId(string steamId)
        {
            if (!ulong.TryParse(steamId, out var id))
            {
                return null;
            }

            return BasePlayer.FindByID(id) ?? BasePlayer.FindSleeping(id);
        }

        private string FindSteamIdByCcUid(string ccUid)
        {
            if (string.IsNullOrEmpty(ccUid))
            {
                return null;
            }

            foreach (var kvp in _data.PlayerSessions)
            {
                if (string.Equals(kvp.Value?.CcUid, ccUid, StringComparison.OrdinalIgnoreCase))
                {
                    return kvp.Key;
                }
            }
            return null;
        }

        private DecodedJwt DecodeJwt(string jwt)
        {
            if (string.IsNullOrEmpty(jwt))
            {
                return null;
            }

            var parts = jwt.Split('.');
            if (parts.Length < 2)
            {
                return null;
            }

            try
            {
                var payload = parts[1]
                    .Replace('-', '+')
                    .Replace('_', '/');
                switch (payload.Length % 4)
                {
                    case 2:
                        payload += "==";
                        break;
                    case 3:
                        payload += "=";
                        break;
                }

                var bytes = Convert.FromBase64String(payload);
                var json = Encoding.UTF8.GetString(bytes);
                var obj = JObject.Parse(json);

                return new DecodedJwt
                {
                    CcUid = obj.Value<string>("ccUID"),
                    OriginId = obj.Value<string>("originID"),
                    ProfileType = obj.Value<string>("profileType"),
                    Name = obj.Value<string>("name"),
                    Exp = obj.Value<long?>("exp") ?? 0
                };
            }
            catch
            {
                return null;
            }
        }

        private void FireAndForget(Task task, string operationName)
        {
            task.ContinueWith(t =>
            {
                if (t.Exception != null)
                {
                    PrintError($"{operationName} failed: {t.Exception.Flatten().InnerException?.Message ?? t.Exception.Message}");
                }
            }, TaskContinuationOptions.OnlyOnFaulted);
        }

        private void LogVerbose(string message)
        {
            if (!VerboseLogging)
            {
                return;
            }

            Puts($"[Verbose] {message}");
        }

        private void ValidateConfig()
        {
            if (_config == null)
            {
                return;
            }

            if (_config.SessionRules == null)
            {
                _config.SessionRules = new SessionRulesConfig();
            }

            if (_config.RetryPolicy == null)
            {
                _config.RetryPolicy = new RetryPolicyConfig();
            }

            if (_config.RetryPolicy.Default == null)
            {
                _config.RetryPolicy.Default = new RetrySourcePolicyConfig
                {
                    MaxAttempts = 25,
                    MaxDurationSeconds = 60,
                    RetryIntervalSeconds = 2.4f
                };
            }

            if (_config.RetryPolicy.Twitch == null)
            {
                _config.RetryPolicy.Twitch = new RetrySourcePolicyConfig
                {
                    MaxAttempts = 25,
                    MaxDurationSeconds = 60,
                    RetryIntervalSeconds = 2.4f
                };
            }

            if (_config.RetryPolicy.Tiktok == null)
            {
                _config.RetryPolicy.Tiktok = new RetrySourcePolicyConfig
                {
                    MaxAttempts = 250,
                    MaxDurationSeconds = 300,
                    RetryIntervalSeconds = 1.2f
                };
            }

            if (string.IsNullOrWhiteSpace(_config.AppId) || _config.AppId == "INSERT_APP_ID")
            {
                PrintWarning("Set app_id in CrowdControl.json before using auth flow.");
            }

            if (string.IsNullOrWhiteSpace(_config.AppSecret) || _config.AppSecret == "INSERT_APP_SECRET")
            {
                PrintWarning("Set app_secret in CrowdControl.json before using auth flow.");
            }
        }

        protected override void LoadDefaultConfig()
        {
            _config = new PluginConfig();
            SaveConfig();
        }

        protected override void LoadConfig()
        {
            base.LoadConfig();
            try
            {
                _config = Config.ReadObject<PluginConfig>();
                if (_config == null)
                {
                    throw new Exception("Config deserialized null.");
                }

                if (_config.SessionRules == null)
                {
                    _config.SessionRules = new SessionRulesConfig();
                }
            }
            catch (Exception ex)
            {
                PrintError($"Config load failed, using defaults: {ex.Message}");
                LoadDefaultConfig();
            }
        }

        protected override void SaveConfig()
        {
            Config.WriteObject(_config, true);
        }

        private void SaveData()
        {
            Interface.Oxide.DataFileSystem.WriteObject(Name, _data);
        }

        private async Task RefreshSessionsAfterReloadAsync()
        {
            if (_data?.PlayerSessions == null || _data.PlayerSessions.Count == 0)
            {
                return;
            }

            var refreshed = 0;
            var snapshot = new List<KeyValuePair<string, PlayerSessionState>>(_data.PlayerSessions);
            foreach (var kvp in snapshot)
            {
                var session = kvp.Value;
                if (session == null || string.IsNullOrEmpty(session.Token))
                {
                    continue;
                }

                // Force a clean session cycle on plugin init so old lingering sessions
                // from previous plugin instances do not keep receiving effect requests.
                await StopGameSessionAsync(session);
                await StartGameSessionAsync(session);
                refreshed++;
            }

            if (refreshed > 0)
            {
                Puts($"Reload refresh completed for {refreshed} Crowd Control session(s).");
            }
        }

        private async Task ApplySessionRulesAsync()
        {
            var currentSignature = GetSessionRulesSignature();
            var previousSignature = _data?.SessionRulesSignature ?? string.Empty;

            if (string.Equals(currentSignature, previousSignature, StringComparison.Ordinal))
            {
                return;
            }

            _data.SessionRulesSignature = currentSignature;
            SaveData();

            if (string.IsNullOrEmpty(previousSignature))
            {
                LogVerbose("Stored initial session rules signature.");
                return;
            }

            var restarted = 0;
            foreach (var kvp in _data.PlayerSessions)
            {
                var steamId = kvp.Key;
                var session = kvp.Value;
                if (session == null || string.IsNullOrEmpty(session.Token))
                {
                    continue;
                }

                await RestartGameSessionAsync(session, steamId);
                restarted++;
            }

            Puts($"Session rules changed; restarted {restarted} Crowd Control session(s).");
        }

        private string GetSessionRulesSignature()
        {
            var rules = _config?.SessionRules ?? new SessionRulesConfig();
            return string.Join("|",
                rules.EnableIntegrationTriggers ? "1" : "0",
                rules.EnablePriceChange ? "1" : "0",
                rules.DisableTestEffects ? "1" : "0",
                rules.DisableCustomEffectsSync ? "1" : "0");
        }

        private bool IsEffectBlockedBySessionRules(JObject payload, JObject effect, out string reason)
        {
            reason = string.Empty;
            var rules = _config?.SessionRules;
            if (rules == null)
            {
                return false;
            }

            if (!rules.EnableIntegrationTriggers && IsIntegrationTriggeredRequest(payload, effect))
            {
                reason = "Integration-triggered effects are currently disabled by server settings.";
                return true;
            }

            if (rules.DisableTestEffects && IsTestEffectRequest(payload, effect))
            {
                reason = "Test effects are currently disabled by server settings.";
                return true;
            }

            if (!rules.EnablePriceChange && IsPriceChangeRequest(payload, effect))
            {
                reason = "Price-change effects are currently disabled by server settings.";
                return true;
            }

            return false;
        }

        private bool IsIntegrationTriggeredRequest(JObject payload, JObject effect)
        {
            var joined = BuildSearchableJsonText(payload, effect);
            return ContainsAny(joined, "tiktok", "twitch", "pulsoid", "gift", "reward", "integration");
        }

        private bool IsTestEffectRequest(JObject payload, JObject effect)
        {
            var requesterCcUid = payload?["requester"]?["ccUID"]?.ToString();
            var targetCcUid = payload?["target"]?["ccUID"]?.ToString();
            var sourceType = payload?["sourceDetails"]?["type"]?.ToString();

    
            if (!string.IsNullOrEmpty(requesterCcUid) &&
                !string.IsNullOrEmpty(targetCcUid) &&
                string.Equals(requesterCcUid, targetCcUid, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }


            return false;
        }

        private bool IsPriceChangeRequest(JObject payload, JObject effect)
        {
            if ((payload?.Value<bool?>("priceChanged") ?? false) || (effect?.Value<bool?>("priceChanged") ?? false))
            {
                return true;
            }

            var requestedPrice = effect?.Value<int?>("price") ?? payload?.Value<int?>("price");
            var basePrice = effect?.Value<int?>("defaultPrice") ??
                            effect?.Value<int?>("menuPrice") ??
                            payload?.Value<int?>("defaultPrice") ??
                            payload?.Value<int?>("menuPrice");
            if (requestedPrice.HasValue && basePrice.HasValue && requestedPrice.Value != basePrice.Value)
            {
                return true;
            }

            var joined = BuildSearchableJsonText(payload, effect);
            return ContainsAny(joined, "pricechange", "price_changed", "price changed", "overrideprice");
        }

        private string BuildSearchableJsonText(params JToken[] tokens)
        {
            var parts = new List<string>();
            foreach (var token in tokens)
            {
                if (token == null)
                {
                    continue;
                }

                var text = token.ToString(Formatting.None);
                if (!string.IsNullOrEmpty(text))
                {
                    parts.Add(text);
                }
            }

            return string.Join(" ", parts).ToLowerInvariant();
        }

        private bool ContainsAny(string haystack, params string[] needles)
        {
            if (string.IsNullOrEmpty(haystack) || needles == null)
            {
                return false;
            }

            foreach (var needle in needles)
            {
                if (string.IsNullOrEmpty(needle))
                {
                    continue;
                }

                if (haystack.IndexOf(needle, StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return true;
                }
            }

            return false;
        }

        private List<BasePlayer> GetActiveCcPlayers()
        {
            var list = new List<BasePlayer>();
            foreach (var kvp in _data.PlayerSessions)
            {
                if (kvp.Value == null || string.IsNullOrEmpty(kvp.Value.Token) || string.IsNullOrEmpty(kvp.Value.GameSessionId))
                {
                    continue;
                }

                var player = FindPlayerBySteamId(kvp.Key);
                if (player != null && player.IsConnected)
                {
                    list.Add(player);
                }
            }

            return list;
        }

        private async Task UpdateTeleportAvailabilityAsync()
        {
            var activeCount = GetActiveCcPlayers().Count;
            var status = activeCount > 1 ? "menuAvailable" : "menuUnavailable";

            foreach (var kvp in _data.PlayerSessions)
            {
                var session = kvp.Value;
                if (session == null || string.IsNullOrEmpty(session.Token))
                {
                    continue;
                }

                var arg = new JObject
                {
                    ["id"] = Guid.NewGuid().ToString(),
                    ["identifierType"] = "effect",
                    ["ids"] = new JArray { "player_teleport_swap_cc_player" },
                    ["status"] = status,
                    ["stamp"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                };

                await SendRpcAsync(session.Token, "effectReport", new JArray { arg });
            }
        }

        #endregion
    }
}
