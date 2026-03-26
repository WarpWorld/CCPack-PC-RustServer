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
    [Info("CrowdControl", "Warp World", "1.0.2")]
    [Description("Crowd Control integration for Rust with auth, PubSub, and permission-based access controls.")]
    public class CrowdControl : RustPlugin
    {
        private const string PermUse = "crowdcontrol.use";
        private const string PermAdmin = "crowdcontrol.admin";
        private const string PermIgnore = "crowdcontrol.ignore";
        private const bool StopSessionOnDisconnect = true;
        private const int SocketKeepAliveMinutes = 5;
        private const bool StopSessionOnUnload = true;
        private const bool AutoStartSessionAfterAuth = true;
        private const float ReconnectDelaySeconds = 5f;
        private const int MaxAuthQueue = 100;
        private const string GamePackId = "RustServer";
        private const string PubSubWebSocketUrl = "wss://pubsub.crowdcontrol.live";
        private const string OpenApiUrl = "https://openapi.crowdcontrol.live";
        private const string UserAgent = "CrowdControl/1.0.2";
        private const bool VerboseLogging = true;
        private const int CustomEffectsPerOperation = 20;
        private const string DefaultEffectsFileName = "CrowdControl-DefaultEffects.json";
        private const string CustomEffectsFilePrefix = "CrowdControl-CustomEffects-";
        private const float ExternalEffectPendingTimeoutSeconds = 15f;
        private const string ExternalEffectHookName = "OnCrowdControlEffect";
        private const string BuiltInEffectsPluginName = "CrowdControlEffects";
        private static readonly List<string> Scopes = new List<string>
        {
            "profile:read",
            "session:write",
            "session:control",
            "custom-effects:read",
            "custom-effects:write",
            "default-effect:write",
            "instance:write"
        };
        private static readonly List<string> Packs = new List<string> { "RustServer" };

        private PluginConfig _config;
        private StoredData _data;

        private readonly object _socketSync = new object();
        private readonly SemaphoreSlim _sendSemaphore = new SemaphoreSlim(1, 1);
        private readonly Queue<string> _pendingAuthRequests = new Queue<string>();
        private readonly Dictionary<string, string> _authCodeToSteamId = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, ActiveAuthCodeState> _activeAuthCodesBySteamId = new Dictionary<string, ActiveAuthCodeState>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, CommandReplyMode> _authReplyModeBySteamId = new Dictionary<string, CommandReplyMode>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _sessionStartInProgress = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, DateTime> _lastCustomEffectsSyncUtc = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, DateTime> _recentHandledRequestIds = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, EffectRetryState> _activeEffectRetries = new Dictionary<string, EffectRetryState>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, CrowdControlEnforcementState> _crowdControlEnforcementBySteamId = new Dictionary<string, CrowdControlEnforcementState>(StringComparer.OrdinalIgnoreCase);
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
            // These defaults can be overridden in CrowdControl.json.
            // Server owners reach out to support@crowdcontrol.live for your own app_id and app_secret if you wish.
            [JsonProperty("app_id")]
            public string AppId { get; set; } = "ccaid-01kjfx91h9cf0mqa7j2z3tjmwx";

            [JsonProperty("app_secret")]
            public string AppSecret { get; set; } = "b9b187f3026b70aad6017dbe41a62445811acc4bb2d3b4c092e445b11ee72fa3";

            [JsonProperty("allow_all_users_without_permission")]
            public bool AllowAllUsersWithoutPermission { get; set; } = false;

            [JsonProperty("session_rules")]
            public SessionRulesConfig SessionRules { get; set; } = new SessionRulesConfig();

            [JsonProperty("retry_policy")]
            public RetryPolicyConfig RetryPolicy { get; set; } = new RetryPolicyConfig();

            [JsonProperty("enforce_crowd_control")]
            public EnforceCrowdControlConfig EnforceCrowdControl { get; set; } = new EnforceCrowdControlConfig();
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

        private sealed class EnforceCrowdControlConfig
        {
            [JsonProperty("enabled")]
            public bool Enabled { get; set; } = false;

            [JsonProperty("enforce_time_seconds")]
            public int EnforceTimeSeconds { get; set; } = 120;

            [JsonProperty("restrict_movement")]
            public bool RestrictMovement { get; set; } = false;
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

            [JsonProperty("name")]
            public string Name { get; set; }

            [JsonProperty("description")]
            public string Description { get; set; }

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
            public bool LocalOnly;
            public JObject DefaultMenuEffect;
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

        private enum CommandReplyMode
        {
            Chat,
            Console
        }

        private enum StoredSessionStatus
        {
            Missing,
            Valid,
            Expired,
            Invalid
        }

        private sealed class ActiveAuthCodeState
        {
            public string Code;
            public DateTime ExpiresUtc;
        }

        private sealed class StoredData
        {
            [JsonProperty("player_sessions")]
            public Dictionary<string, PlayerSessionState> PlayerSessions { get; set; } = new Dictionary<string, PlayerSessionState>();

            [JsonProperty("session_rules_signature")]
            public string SessionRulesSignature { get; set; } = string.Empty;

            [JsonProperty("application_instance_id")]
            public string ApplicationInstanceId { get; set; } = string.Empty;

            [JsonProperty("application_instance_app_id")]
            public string ApplicationInstanceAppId { get; set; } = string.Empty;
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

        private sealed class CrowdControlEnforcementState
        {
            public string SteamId;
            public bool IsEnforced;
            public Vector3 AnchorPosition;
            public Oxide.Plugins.Timer GraceTimer;
            public Oxide.Plugins.Timer MovementTimer;
            public Oxide.Plugins.Timer ReminderTimer;
            public DateTime LastBlockedNoticeUtc = DateTime.MinValue;
        }

        private sealed class DecodedJwt
        {
            public string CcUid;
            public string OriginId;
            public string ProfileType;
            public string Name;
            public long Exp;
            public string ScopeSummary;
        }

        #endregion

        #region Oxide Lifecycle

        private void Init()
        {
            _isUnloading = false;
            permission.RegisterPermission(PermUse, this);
            permission.RegisterPermission(PermAdmin, this);
            permission.RegisterPermission(PermIgnore, this);
            _data = Interface.Oxide.DataFileSystem.ReadObject<StoredData>(Name) ?? new StoredData();
            Puts("CrowdControl initialized.");
        }

        private void OnServerInitialized()
        {
            ValidateConfig();
            SaveConfig();
            FireAndForget(RefreshSessionsAfterReloadAsync(), "refresh sessions after reload");
            FireAndForget(ApplySessionRulesAsync(), "apply session rules");
            FireAndForget(EnsureSocketConnectedAsync(), "initial socket connect");
            StartSocketHeartbeat();
            WarnIfBuiltInEffectsPluginMissing("server initialization", null, false);
            NotifyCrowdControlProvidersSessionStateChanged();
            RefreshCrowdControlEnforcementForAllPlayers();
        }

        private void Unload()
        {
            _isUnloading = true;

            if (_config != null && StopSessionOnUnload)
            {
                foreach (var kvp in _data.PlayerSessions)
                {
                    var session = kvp.Value;
                    if (IsSessionTokenUsable(session))
                    {
                        FireAndForget(StopGameSessionAsync(session), "stop game session on unload");
                    }
                }
            }

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

            if (StopSessionOnDisconnect &&
                _data.PlayerSessions.TryGetValue(player.UserIDString, out var session) &&
                IsSessionTokenUsable(session))
            {
                FireAndForget(StopGameSessionAsync(session, player.UserIDString), "stop session on disconnect");
            }

            FireAndForget(EnsureSocketConnectedAsync(), "socket state check on disconnect");
            NotifyCrowdControlProvidersSessionStateChanged();
            ClearCrowdControlEnforcement(player.UserIDString, showReleasedMessage: false);
        }

        private void OnPlayerConnected(BasePlayer player)
        {
            if (player == null)
            {
                return;
            }

            PruneStoredSessionIfInvalid(player.UserIDString, CommandReplyMode.Console, notifyPlayer: false);
            MaybeShowCrowdControlJoinInstructions(player);
            RefreshCrowdControlEnforcement(player);

            FireAndForget(EnsureSocketConnectedAsync(), "socket state check on connect");
            if (TryGetAuthenticatedSession(player.UserIDString, out var session, pruneInvalid: false) &&
                string.IsNullOrEmpty(session.GameSessionId))
            {
                FireAndForget(RestartGameSessionAsync(session, player.UserIDString), "start session on connect");
            }

            NotifyCrowdControlProvidersSessionStateChanged();
        }

        private void OnPluginLoaded(Plugin plugin)
        {
            if (plugin == null || string.IsNullOrWhiteSpace(plugin.Name))
            {
                return;
            }

            if (string.Equals(plugin.Name, BuiltInEffectsPluginName, StringComparison.OrdinalIgnoreCase))
            {
                Puts("CrowdControlEffects loaded; built-in effect provider is now available.");
                NotifyCrowdControlProvidersSessionStateChanged();
            }
        }

        private void OnPluginUnloaded(Plugin plugin)
        {
            if (plugin == null || string.IsNullOrWhiteSpace(plugin.Name))
            {
                return;
            }

            CC_UnregisterEffects(plugin.Name);
            if (string.Equals(plugin.Name, BuiltInEffectsPluginName, StringComparison.OrdinalIgnoreCase))
            {
                PrintWarning("CrowdControlEffects unloaded; built-in effects will be unavailable until it is loaded again.");
            }
        }

        private object OnPlayerAttack(BasePlayer attacker, HitInfo hitInfo)
        {
            if (!IsCrowdControlEnforced(attacker))
            {
                return null;
            }

            NotifyCrowdControlEnforcementBlocked(attacker);
            return true;
        }

        private object CanDropActiveItem(BasePlayer player)
        {
            if (!IsCrowdControlEnforced(player))
            {
                return null;
            }

            NotifyCrowdControlEnforcementBlocked(player);
            return false;
        }

        private object CanLootEntity(BasePlayer player, BaseEntity entity)
        {
            if (!IsCrowdControlEnforced(player))
            {
                return null;
            }

            NotifyCrowdControlEnforcementBlocked(player);
            TryClosePlayerInventory(player);
            return false;
        }

        private object CanLootPlayer(BasePlayer target, BasePlayer looter)
        {
            if (!IsCrowdControlEnforced(looter))
            {
                return null;
            }

            NotifyCrowdControlEnforcementBlocked(looter);
            TryClosePlayerInventory(looter);
            return false;
        }

        #endregion

        #region Commands

        [ChatCommand("cc")]
        private void CommandCrowdControl(BasePlayer player, string command, string[] args)
        {
            HandleCrowdControlCommand(player, args, CommandReplyMode.Chat);
        }

        [ChatCommand("crowdcontrol")]
        private void CommandCrowdControlLong(BasePlayer player, string command, string[] args)
        {
            HandleCrowdControlCommand(player, args, CommandReplyMode.Chat);
        }

        [ConsoleCommand("cc")]
        private void ConsoleCommandCrowdControlShort(ConsoleSystem.Arg arg)
        {
            HandleCrowdControlConsoleCommand(arg);
        }

        [ConsoleCommand("crowdcontrol")]
        private void ConsoleCommandCrowdControlLong(ConsoleSystem.Arg arg)
        {
            HandleCrowdControlConsoleCommand(arg);
        }

        private void HandleCrowdControlConsoleCommand(ConsoleSystem.Arg arg)
        {
            var player = arg?.Player();
            if (player == null)
            {
                return;
            }

            HandleCrowdControlCommand(player, arg.Args ?? Array.Empty<string>(), CommandReplyMode.Console);
        }

        private void HandleCrowdControlCommand(BasePlayer player, string[] args, CommandReplyMode replyMode)
        {
            if (player == null)
            {
                return;
            }

            if (args.Length == 0)
            {
                if (!CanUsePlugin(player, replyMode))
                {
                    return;
                }

                SendHelp(player, replyMode);
                return;
            }

            var sub = args[0].ToLowerInvariant();
            if (string.Equals(sub, "reload", StringComparison.Ordinal))
            {
                HandleReloadCommand(player, replyMode);
                return;
            }

            if (!CanUsePlugin(player, replyMode))
            {
                return;
            }

            switch (sub)
            {
                case "help":
                    SendHelp(player, replyMode);
                    break;
                case "link":
                case "auth":
                    HandleAuthCommand(player, replyMode);
                    break;
                case "unlink":
                    HandleLogoutCommand(player, replyMode);
                    break;
                case "status":
                    HandleStatusCommand(player, replyMode);
                    break;
                case "settings":
                    HandleSettingsCommand(player, replyMode);
                    break;
                case "restart":
                    HandleRestartSessionCommand(player, replyMode);
                    break;
                case "logout":
                    HandleLogoutCommand(player, replyMode);
                    break;
                default:
                    SendHelp(player, replyMode);
                    break;
            }
        }

        private void SendHelp(BasePlayer player, CommandReplyMode replyMode)
        {
            if (replyMode == CommandReplyMode.Console)
            {
                SendCommandReplies(
                    player,
                    replyMode,
                    "Commands:",
                    "Console commands use no leading slash.",
                    "cc link - Link your Crowd Control account",
                    "cc unlink - Unlink your Crowd Control account",
                    "cc status - Show connection status and plugin version",
                    "cc settings - Show current server Crowd Control settings",
                    $"cc reload - Reload Crowd Control config/data and refresh sessions ({PermAdmin})",
                    "cc restart - Restart game session using saved token",
                    "Alias: crowdcontrol <same_subcommand>",
                    $"Auth/enforcement bypass permission: {PermIgnore}",
                    "Chat commands still use /cc <same_subcommand>."
                );
                return;
            }

            SendCommandReplies(
                player,
                replyMode,
                "Commands:",
                "/cc link - Link your Crowd Control account",
                "/cc unlink - Unlink your Crowd Control account",
                "/cc status - Show connection status and plugin version",
                "/cc settings - Show current server Crowd Control settings",
                $"/cc reload - Reload Crowd Control config/data and refresh sessions ({PermAdmin})",
                "/cc restart - Restart game session using saved token",
                "Console aliases are cc <same_subcommand> and crowdcontrol <same_subcommand>.",
                $"Auth/enforcement bypass permission: {PermIgnore}"
            );
        }

        private void HandleAuthCommand(BasePlayer player, CommandReplyMode replyMode)
        {
            var authReplyMode = CommandReplyMode.Console;
            var requestedFromChat = replyMode == CommandReplyMode.Chat;
            string existingCode = null;
            var alreadyPending = false;
            var queueFull = false;

            if (!CanUsePlugin(player, authReplyMode))
            {
                return;
            }

            PruneStoredSessionIfInvalid(player.UserIDString, authReplyMode, notifyPlayer: false);
            if (TryGetAuthenticatedSession(player.UserIDString, out _))
            {
                SendCommandReply(
                    player,
                    authReplyMode,
                    "You are already authenticated. Use cc status, cc restart, or cc unlink."
                );
                return;
            }

            lock (_pendingAuthRequests)
            {
                if (TryGetActiveAuthCodeLocked(player.UserIDString, out existingCode))
                {
                }
                else if (_pendingAuthRequests.Contains(player.UserIDString))
                {
                    alreadyPending = true;
                }
                else if (_pendingAuthRequests.Count >= MaxAuthQueue)
                {
                    queueFull = true;
                }
                else
                {
                    _pendingAuthRequests.Enqueue(player.UserIDString);
                    _authReplyModeBySteamId[player.UserIDString] = authReplyMode;
                }
            }

            if (!string.IsNullOrWhiteSpace(existingCode))
            {
                SendCommandReply(player, authReplyMode, $"You already have an active auth code: {existingCode}");
                SendCommandReply(player, authReplyMode, "You have 3 minutes to enter this code before it expires.");
                if (requestedFromChat)
                {
                    SendCommandReply(player, CommandReplyMode.Chat, "You already have a valid Crowd Control code. Press F1 to view it in console.");
                }
                return;
            }

            if (alreadyPending)
            {
                SendCommandReply(player, authReplyMode, "An auth code request is already pending for you.");
                if (requestedFromChat)
                {
                    SendCommandReply(player, CommandReplyMode.Chat, "Auth is already in progress. Press F1 to watch for your code in console.");
                }
                return;
            }

            if (queueFull)
            {
                SendCommandReply(player, authReplyMode, "Auth queue is currently full. Try again shortly.");
                if (requestedFromChat)
                {
                    SendCommandReply(player, CommandReplyMode.Chat, "Crowd Control auth queue is full right now. Try again shortly.");
                }
                return;
            }

            FireAndForget(RequestAuthCodeAsync(player.UserIDString, authReplyMode, requestedFromChat), "request auth code");
        }

        private void HandleStatusCommand(BasePlayer player, CommandReplyMode replyMode)
        {
            if (!CanUsePlugin(player, replyMode))
            {
                return;
            }

            PruneStoredSessionIfInvalid(player.UserIDString, replyMode, notifyPlayer: true);
            TryGetAuthenticatedSession(player.UserIDString, out var session, pruneInvalid: false);
            var isConnected = HasActiveGameSession(player.UserIDString, session);
            SendCommandReply(player, replyMode, $"Status: {(isConnected ? "Connected" : "Not connected")}");
            SendCommandReply(player, replyMode, $"Version: {Version}");
        }

        private void HandleSettingsCommand(BasePlayer player, CommandReplyMode replyMode)
        {
            if (!CanUsePlugin(player, replyMode))
            {
                return;
            }

            var rules = _config?.SessionRules ?? new SessionRulesConfig();
            var retry = _config?.RetryPolicy ?? new RetryPolicyConfig();
            var retryDefault = retry.Default ?? new RetrySourcePolicyConfig();
            var retryTwitch = retry.Twitch ?? retryDefault;
            var retryTiktok = retry.Tiktok ?? retryDefault;
            var enforce = _config?.EnforceCrowdControl ?? new EnforceCrowdControlConfig();

            SendCommandReplies(
                player,
                replyMode,
                "Active server settings:",
                $"Access: {(_config?.AllowAllUsersWithoutPermission ?? true ? "all players may use Crowd Control" : $"permission required ({PermUse})")}",
                $"Auth bypass permission: {PermIgnore}",
                $"Session rules: integration triggers={(rules.EnableIntegrationTriggers ? "enabled" : "disabled")}, price changes={(rules.EnablePriceChange ? "enabled" : "disabled")}, test effects={(rules.DisableTestEffects ? "disabled" : "enabled")}, custom effect sync={(rules.DisableCustomEffectsSync ? "disabled" : "enabled")}",
                $"Enforcement: {(enforce.Enabled ? "enabled" : "disabled")}, grace={Math.Max(30, enforce.EnforceTimeSeconds)}s, restrict movement={(enforce.RestrictMovement ? "enabled" : "disabled")}",
                $"Retry policy: {(retry.Enabled ? "enabled" : "disabled")}",
                $"Retry default: attempts={retryDefault.MaxAttempts}, duration={retryDefault.MaxDurationSeconds}s, interval={retryDefault.RetryIntervalSeconds:0.##}s",
                $"Retry Twitch: attempts={retryTwitch.MaxAttempts}, duration={retryTwitch.MaxDurationSeconds}s, interval={retryTwitch.RetryIntervalSeconds:0.##}s",
                $"Retry TikTok: attempts={retryTiktok.MaxAttempts}, duration={retryTiktok.MaxDurationSeconds}s, interval={retryTiktok.RetryIntervalSeconds:0.##}s"
            );
        }

        private void HandleReloadCommand(BasePlayer player, CommandReplyMode replyMode)
        {
            if (!CanUseAdminCommand(player, replyMode))
            {
                return;
            }

            LoadConfig();
            ValidateConfig();
            SaveConfig();
            _data = Interface.Oxide.DataFileSystem.ReadObject<StoredData>(Name) ?? new StoredData();
            RefreshRegisteredProviderEffectConfigs();
            FireAndForget(RefreshSessionsAfterReloadAsync(), "admin reload refresh sessions");
            FireAndForget(ApplySessionRulesAsync(), "admin reload apply session rules");
            FireAndForget(EnsureSocketConnectedAsync(), "admin reload socket ensure");
            NotifyCrowdControlProvidersSessionStateChanged();
            RefreshCrowdControlEnforcementForAllPlayers();
            SendCommandReply(player, replyMode, "Reloaded config/data and refreshing Crowd Control sessions.");
        }

        private void HandleRestartSessionCommand(BasePlayer player, CommandReplyMode replyMode)
        {
            if (!CanUsePlugin(player, replyMode))
            {
                return;
            }

            var sessionStatus = PruneStoredSessionIfInvalid(player.UserIDString, replyMode, notifyPlayer: true);
            if (!TryGetAuthenticatedSession(player.UserIDString, out var session, pruneInvalid: false))
            {
                if (sessionStatus == StoredSessionStatus.Missing)
                {
                    SendCommandReply(
                        player,
                        replyMode,
                        replyMode == CommandReplyMode.Console
                            ? "No saved Crowd Control token. Run cc link first."
                            : "No saved Crowd Control token. Run /cc link first."
                    );
                }
                return;
            }

            FireAndForget(RestartGameSessionAsync(session, player.UserIDString), "manual session restart");
            SendCommandReply(player, replyMode, "Restarting Crowd Control session...");
        }

        private void HandleLogoutCommand(BasePlayer player, CommandReplyMode replyMode)
        {
            if (!CanUsePlugin(player, replyMode))
            {
                return;
            }

            if (!ClearPlayerAuthState(player.UserIDString, stopActiveSession: true))
            {
                SendCommandReply(player, replyMode, "No active auth state to clear.");
                return;
            }

            SendCommandReply(player, replyMode, "Crowd Control credentials removed.");
        }

        #endregion

        #region External Effect API

        [HookMethod("CC_RegisterEffects")]
        public object CC_RegisterEffects(string providerName, object effectsPayload)
        {
            return RegisterEffectsInternal(providerName, effectsPayload, localOnly: false);
        }

        [HookMethod("CC_RegisterLocalEffects")]
        public object CC_RegisterLocalEffects(string providerName, object effectsPayload)
        {
            return RegisterEffectsInternal(providerName, effectsPayload, localOnly: true);
        }

        [HookMethod("CC_UnregisterEffects")]
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

        private bool RegisterEffectsInternal(string providerName, object effectsPayload, bool localOnly)
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
            var defaultsById = BuildProviderEffectEntries(parsed, localOnly);
            var configuredById = LoadOrSeedProviderEffectEntries(normalizedProvider, defaultsById, localOnly);
            var registered = 0;
            var registeredMenuEffects = 0;
            lock (_externalEffectsSync)
            {
                if (!_externalEffectIdsByProvider.TryGetValue(normalizedProvider, out var providerEffectIds))
                {
                    providerEffectIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    _externalEffectIdsByProvider[normalizedProvider] = providerEffectIds;
                }
                else
                {
                    foreach (var existingEffectId in providerEffectIds)
                    {
                        _externalEffectsById.Remove(existingEffectId);
                    }

                    providerEffectIds.Clear();
                }

                for (var i = 0; i < parsed.Count; i++)
                {
                    if (!(parsed[i] is JObject item))
                    {
                        continue;
                    }

                    var effectId = item.Value<string>("effectID");
                    if (string.IsNullOrWhiteSpace(effectId))
                    {
                        continue;
                    }

                    effectId = NormalizeEffectId(effectId);
                    defaultsById.TryGetValue(effectId, out var defaultEntry);
                    configuredById.TryGetValue(effectId, out var configuredEntry);
                    var menuObj = BuildConfiguredEffectMenu(item, configuredEntry ?? defaultEntry, localOnly);

                    _externalEffectsById[effectId] = new ExternalEffectDefinition
                    {
                        ProviderName = normalizedProvider,
                        EffectId = effectId,
                        MenuEffect = menuObj,
                        LocalOnly = localOnly,
                        DefaultMenuEffect = (JObject)item.DeepClone()
                    };
                    providerEffectIds.Add(effectId);
                    registered++;
                    if (!localOnly)
                    {
                        registeredMenuEffects++;
                    }
                }
            }

            if (registered <= 0)
            {
                return false;
            }

            LogVerbose(
                $"Registered {registered} {(localOnly ? "local-only" : "external")} effect(s) from provider={normalizedProvider} using {Path.GetFileName(GetProviderEffectFilePath(normalizedProvider, localOnly))}.");
            if (registeredMenuEffects > 0)
            {
                FireAndForget(SyncCustomEffectsForAllSessionsAsync(), "external effects register sync");
            }

            return true;
        }

        private Dictionary<string, EffectPricingEntry> BuildProviderEffectEntries(JArray parsed, bool localOnly)
        {
            var results = new Dictionary<string, EffectPricingEntry>(StringComparer.OrdinalIgnoreCase);
            if (parsed == null)
            {
                return results;
            }

            for (var i = 0; i < parsed.Count; i++)
            {
                if (!(parsed[i] is JObject item))
                {
                    continue;
                }

                var effectId = NormalizeEffectId(item.Value<string>("effectID"));
                if (string.IsNullOrWhiteSpace(effectId))
                {
                    continue;
                }

                results[effectId] = new EffectPricingEntry
                {
                    EffectId = effectId,
                    Name = item.Value<string>("name") ?? effectId,
                    Description = item.Value<string>("description") ?? "External effect.",
                    Price = Math.Max(0, item.Value<int?>("price") ?? 0),
                    SessionCooldown = Math.Max(0, item.Value<int?>("sessionCooldown") ?? 0),
                    UserCooldown = Math.Max(0, item.Value<int?>("userCooldown") ?? 0),
                    Duration = item["duration"] as JObject != null ? (JObject)((JObject)item["duration"]).DeepClone() : null,
                    Inactive = item.Value<bool?>("inactive") ?? false,
                    Scale = item["scale"] as JObject != null
                        ? new EffectPricingScaleConfig
                        {
                            Percent = item["scale"]?["percent"]?.Value<float?>() ?? 1f,
                            Duration = item["scale"]?["duration"]?.Value<float?>() ?? 1f,
                            Inactive = item["scale"]?["inactive"]?.Value<bool?>() ?? true
                        }
                        : new EffectPricingScaleConfig(),
                };
            }

            return results;
        }

        private Dictionary<string, EffectPricingEntry> LoadOrSeedProviderEffectEntries(string providerName, Dictionary<string, EffectPricingEntry> defaultsById, bool localOnly)
        {
            var path = GetProviderEffectFilePath(providerName, localOnly);
            var byId = new Dictionary<string, EffectPricingEntry>(StringComparer.OrdinalIgnoreCase);
            foreach (var kvp in defaultsById)
            {
                byId[kvp.Key] = CloneEffectPricingEntry(kvp.Value);
            }

            var changed = false;
            try
            {
                if (File.Exists(path))
                {
                    var json = File.ReadAllText(path);
                    var loadedArray = JArray.Parse(json);
                    for (var i = 0; i < loadedArray.Count; i++)
                    {
                        if (!(loadedArray[i] is JObject entryObj))
                        {
                            continue;
                        }

                        if (entryObj.Property("syncMenu") != null)
                        {
                            changed = true;
                        }

                        if (localOnly &&
                            (entryObj.Property("name") != null ||
                             entryObj.Property("description") != null))
                        {
                            changed = true;
                        }

                        var entry = entryObj.ToObject<EffectPricingEntry>();
                        if (entry == null || string.IsNullOrWhiteSpace(entry.EffectId))
                        {
                            continue;
                        }

                        entry.EffectId = NormalizeEffectId(entry.EffectId);
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
                PrintWarning($"Failed reading {Path.GetFileName(path)}, using defaults: {ex.Message}");
                changed = true;
            }

            foreach (var kvp in defaultsById)
            {
                if (!byId.ContainsKey(kvp.Key))
                {
                    byId[kvp.Key] = CloneEffectPricingEntry(kvp.Value);
                    changed = true;
                }
            }

            if (changed)
            {
                WriteProviderEffectEntries(path, byId, localOnly);
            }

            return byId;
        }

        private void WriteProviderEffectEntries(string path, Dictionary<string, EffectPricingEntry> byId, bool localOnly)
        {
            try
            {
                var ordered = new List<EffectPricingEntry>(byId.Values);
                ordered.Sort((a, b) => string.Compare(a?.EffectId, b?.EffectId, StringComparison.OrdinalIgnoreCase));
                var serializedEntries = new JArray();
                for (var i = 0; i < ordered.Count; i++)
                {
                    var entry = ordered[i];
                    if (entry == null || string.IsNullOrWhiteSpace(entry.EffectId))
                    {
                        continue;
                    }

                    serializedEntries.Add(SerializeEffectPricingEntry(entry, localOnly));
                }

                var serialized = JsonConvert.SerializeObject(serializedEntries, Formatting.Indented);
                File.WriteAllText(path, serialized + Environment.NewLine, Encoding.UTF8);
                LogVerbose($"Wrote {Path.GetFileName(path)} with {ordered.Count} effect entries.");
            }
            catch (Exception ex)
            {
                PrintWarning($"Failed writing {Path.GetFileName(path)}: {ex.Message}");
            }
        }

        private JObject SerializeEffectPricingEntry(EffectPricingEntry entry, bool localOnly)
        {
            var obj = new JObject
            {
                ["effectID"] = entry.EffectId,
                ["price"] = Math.Max(0, entry.Price),
                ["sessionCooldown"] = Math.Max(0, entry.SessionCooldown),
                ["userCooldown"] = Math.Max(0, entry.UserCooldown),
                ["inactive"] = entry.Inactive
            };

            if (!localOnly)
            {
                obj["name"] = entry.Name ?? entry.EffectId;
                obj["description"] = entry.Description ?? "External effect.";
            }

            if (entry.Duration != null)
            {
                obj["duration"] = (JObject)entry.Duration.DeepClone();
            }

            if (entry.Scale != null)
            {
                obj["scale"] = new JObject
                {
                    ["percent"] = entry.Scale.Percent,
                    ["duration"] = entry.Scale.Duration,
                    ["inactive"] = entry.Scale.Inactive
                };
            }

            return obj;
        }

        private EffectPricingEntry CloneEffectPricingEntry(EffectPricingEntry source)
        {
            if (source == null)
            {
                return null;
            }

            return new EffectPricingEntry
            {
                EffectId = source.EffectId,
                Name = source.Name,
                Description = source.Description,
                Price = source.Price,
                SessionCooldown = source.SessionCooldown,
                UserCooldown = source.UserCooldown,
                Duration = source.Duration != null ? (JObject)source.Duration.DeepClone() : null,
                Inactive = source.Inactive,
                Scale = new EffectPricingScaleConfig
                {
                    Percent = source.Scale?.Percent ?? 1f,
                    Duration = source.Scale?.Duration ?? 1f,
                    Inactive = source.Scale?.Inactive ?? true
                }
            };
        }

        private JObject BuildConfiguredEffectMenu(JObject defaultMenu, EffectPricingEntry entry, bool localOnly)
        {
            var menu = defaultMenu != null ? (JObject)defaultMenu.DeepClone() : new JObject();
            menu["name"] = entry?.Name ?? menu.Value<string>("name") ?? menu.Value<string>("effectID") ?? "Unnamed Effect";
            menu["description"] = entry?.Description ?? menu.Value<string>("description") ?? "External effect.";
            menu["price"] = Math.Max(0, entry?.Price ?? menu.Value<int?>("price") ?? 0);
            menu["sessionCooldown"] = Math.Max(0, entry?.SessionCooldown ?? menu.Value<int?>("sessionCooldown") ?? 0);
            menu["userCooldown"] = Math.Max(0, entry?.UserCooldown ?? menu.Value<int?>("userCooldown") ?? 0);
            menu["inactive"] = entry?.Inactive ?? menu.Value<bool?>("inactive") ?? false;

            if (entry?.Duration != null)
            {
                menu["duration"] = (JObject)entry.Duration.DeepClone();
            }

            if (entry?.Scale != null && !entry.Scale.Inactive)
            {
                menu["scale"] = new JObject
                {
                    ["percent"] = entry.Scale.Percent,
                    ["duration"] = entry.Scale.Duration
                };
            }
            else
            {
                menu.Remove("scale");
            }
            return menu;
        }

        private string GetProviderEffectFilePath(string providerName, bool localOnly)
        {
            if (localOnly && string.Equals(providerName, BuiltInEffectsPluginName, StringComparison.OrdinalIgnoreCase))
            {
                return Path.Combine(Interface.Oxide.RootDirectory, "oxide", "plugins", DefaultEffectsFileName);
            }

            var safeProviderName = SanitizeFileSegment(providerName);
            var fileName = $"{CustomEffectsFilePrefix}{safeProviderName}.json";
            return Path.Combine(Interface.Oxide.RootDirectory, "oxide", "plugins", fileName);
        }

        private string SanitizeFileSegment(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return "UnknownProvider";
            }

            var builder = new StringBuilder(value.Length);
            foreach (var ch in value)
            {
                builder.Append(char.IsLetterOrDigit(ch) || ch == '-' || ch == '_' ? ch : '_');
            }

            return builder.ToString().Trim('_');
        }

        private Dictionary<string, EffectPricingEntry> BuildRegisteredProviderDefaults(string providerName)
        {
            var results = new Dictionary<string, EffectPricingEntry>(StringComparer.OrdinalIgnoreCase);
            lock (_externalEffectsSync)
            {
                foreach (var kvp in _externalEffectsById)
                {
                    var def = kvp.Value;
                    if (def == null || !string.Equals(def.ProviderName, providerName, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    var entry = BuildProviderEffectEntries(new JArray { def.DefaultMenuEffect ?? def.MenuEffect }, def.LocalOnly);
                    if (entry.TryGetValue(kvp.Key, out var builtEntry))
                    {
                        results[kvp.Key] = builtEntry;
                    }
                }
            }

            return results;
        }

        private void RefreshRegisteredProviderEffectConfigs()
        {
            var providers = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
            lock (_externalEffectsSync)
            {
                foreach (var def in _externalEffectsById.Values)
                {
                    if (def == null || string.IsNullOrWhiteSpace(def.ProviderName))
                    {
                        continue;
                    }

                    providers[def.ProviderName] = def.LocalOnly;
                }
            }

            foreach (var kvp in providers)
            {
                var defaultsById = BuildRegisteredProviderDefaults(kvp.Key);
                var configuredById = LoadOrSeedProviderEffectEntries(kvp.Key, defaultsById, kvp.Value);
                lock (_externalEffectsSync)
                {
                    foreach (var def in _externalEffectsById.Values)
                    {
                        if (def == null || !string.Equals(def.ProviderName, kvp.Key, StringComparison.OrdinalIgnoreCase))
                        {
                            continue;
                        }

                        configuredById.TryGetValue(def.EffectId, out var entry);
                        def.MenuEffect = BuildConfiguredEffectMenu(def.DefaultMenuEffect ?? def.MenuEffect, entry, def.LocalOnly);
                    }
                }
            }
        }

        [HookMethod("CC_CompleteEffect")]
        public object CC_CompleteEffect(string requestId, string status = "success", string reason = "", string playerMessage = "")
        {
            return CC_SendEffectResponse(requestId, status, reason, playerMessage, null);
        }

        [HookMethod("CC_GetActiveCcPlayerSteamIds")]
        public object CC_GetActiveCcPlayerSteamIds(string excludeSteamId = "")
        {
            var results = new JArray();
            foreach (var kvp in _data.PlayerSessions)
            {
                if (!string.IsNullOrWhiteSpace(excludeSteamId) &&
                    string.Equals(kvp.Key, excludeSteamId, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var session = kvp.Value;
                if (!HasActiveGameSession(kvp.Key, session))
                {
                    continue;
                }

                results.Add(kvp.Key);
            }

            return results;
        }

        [HookMethod("CC_ReportEffectAvailability")]
        public object CC_ReportEffectAvailability(object effectIdsPayload, string status)
        {
            var effectIds = ParseEffectIdsPayload(effectIdsPayload);
            if (effectIds.Count == 0 || string.IsNullOrWhiteSpace(status))
            {
                return false;
            }

            FireAndForget(ReportEffectAvailabilityAsync(effectIds, status.Trim()), "report effect availability");
            return true;
        }

        [HookMethod("CC_SendEffectResponse")]
        public object CC_SendEffectResponse(string requestId, string status, string reason = "", string playerMessage = "", object timeRemainingMs = null)
        {
            if (string.IsNullOrWhiteSpace(requestId))
            {
                return false;
            }

            var normalizedStatus = (status ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(normalizedStatus))
            {
                return false;
            }

            var isFinal = IsFinalExternalResponseStatus(normalizedStatus);
            var isTimed = IsTimedResponseStatus(normalizedStatus);
            if (!isFinal && !string.Equals(normalizedStatus, "pending", StringComparison.OrdinalIgnoreCase) && !isTimed)
            {
                normalizedStatus = NormalizeExternalCompletionStatus(normalizedStatus);
                isFinal = true;
            }

            if (string.Equals(normalizedStatus, "pending", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            var parsedTimeRemainingMs = TryGetLongValue(timeRemainingMs);
            ExternalEffectPendingRequest pending;
            lock (_externalEffectsSync)
            {
                if (!_externalPendingRequests.TryGetValue(requestId, out pending))
                {
                    return false;
                }

                pending.TimeoutTimer?.Destroy();
                pending.TimeoutTimer = null;

                if (!isFinal)
                {
                    pending.TimeoutTimer = timer.Once(
                        GetExternalEffectTimeoutSeconds(normalizedStatus, parsedTimeRemainingMs),
                        () => HandleExternalEffectTimeout(requestId));
                    _externalPendingRequests[requestId] = pending;
                }
                else
                {
                    _externalPendingRequests.Remove(requestId);
                }
            }

            var player = FindPlayerBySteamId(pending.SteamId);
            if (!string.IsNullOrWhiteSpace(playerMessage))
            {
                ShowExternalProviderMessage(player, normalizedStatus, playerMessage);
            }
            else
            {
                LogVerbose($"External provider={pending.ProviderName} sent requestID={requestId} status={normalizedStatus} without playerMessage (recommended).");
            }

            if (isTimed)
            {
                FireAndForget(
                    SendTimedResponseAsync(pending.Token, pending.RequestId, normalizedStatus, parsedTimeRemainingMs, reason ?? string.Empty),
                    $"external timed response {normalizedStatus}"
                );
            }
            else
            {
                FireAndForget(
                    SendEffectResponseAsync(pending.Token, pending.RequestId, normalizedStatus, reason ?? string.Empty),
                    $"external completion {normalizedStatus}"
                );
            }

            LogVerbose($"External effect response requestID={requestId}, provider={pending.ProviderName}, status={normalizedStatus}.");
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

        private bool IsFinalExternalResponseStatus(string status)
        {
            var normalized = (status ?? string.Empty).Trim();
            return string.Equals(normalized, "success", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(normalized, "failTemporary", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(normalized, "failPermanent", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(normalized, "fail", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(normalized, "timedEnd", StringComparison.OrdinalIgnoreCase);
        }

        private bool IsTimedResponseStatus(string status)
        {
            var normalized = (status ?? string.Empty).Trim();
            return string.Equals(normalized, "timedBegin", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(normalized, "timedEnd", StringComparison.OrdinalIgnoreCase);
        }

        private float GetExternalEffectTimeoutSeconds(string status, long? timeRemainingMs)
        {
            var timeoutSeconds = ExternalEffectPendingTimeoutSeconds;
            if (string.Equals((status ?? string.Empty).Trim(), "timedBegin", StringComparison.OrdinalIgnoreCase) && timeRemainingMs.HasValue)
            {
                timeoutSeconds = Math.Max(
                    ExternalEffectPendingTimeoutSeconds,
                    (float)Math.Ceiling(timeRemainingMs.Value / 1000d) + 5f);
            }

            return timeoutSeconds;
        }

        private long? TryGetLongValue(object value)
        {
            if (value == null)
            {
                return null;
            }

            if (value is long longValue)
            {
                return longValue >= 0 ? longValue : (long?)null;
            }

            if (value is int intValue)
            {
                return intValue >= 0 ? intValue : (long?)null;
            }

            if (value is short shortValue)
            {
                return shortValue >= 0 ? shortValue : (long?)null;
            }

            if (value is uint uintValue)
            {
                return uintValue;
            }

            if (value is ulong ulongValue)
            {
                return ulongValue <= long.MaxValue ? (long)ulongValue : (long?)null;
            }

            if (value is JValue tokenValue)
            {
                return TryGetLongValue(tokenValue.Value);
            }

            if (long.TryParse(value.ToString(), out var parsed) && parsed >= 0)
            {
                return parsed;
            }

            return null;
        }

        private JArray ParseEffectIdsPayload(object effectIdsPayload)
        {
            if (effectIdsPayload == null)
            {
                return new JArray();
            }

            if (effectIdsPayload is JArray arr)
            {
                return arr;
            }

            if (effectIdsPayload is string text)
            {
                text = text.Trim();
                if (string.IsNullOrEmpty(text))
                {
                    return new JArray();
                }

                try
                {
                    if (text.StartsWith("["))
                    {
                        return JArray.Parse(text);
                    }
                }
                catch
                {
                    return new JArray();
                }

                return new JArray { text };
            }

            try
            {
                return JArray.FromObject(effectIdsPayload);
            }
            catch
            {
                return new JArray();
            }
        }

        private async Task ReportEffectAvailabilityAsync(JArray effectIds, string status)
        {
            foreach (var kvp in _data.PlayerSessions)
            {
                var session = kvp.Value;
                if (!HasActiveGameSession(kvp.Key, session))
                {
                    continue;
                }

                var arg = new JObject
                {
                    ["id"] = Guid.NewGuid().ToString(),
                    ["identifierType"] = "effect",
                    ["ids"] = (JArray)effectIds.DeepClone(),
                    ["status"] = status,
                    ["stamp"] = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                };

                await SendRpcAsync(session.Token, "effectReport", new JArray { arg });
            }
        }

        private void NotifyCrowdControlProvidersSessionStateChanged()
        {
            Interface.CallHook("OnCrowdControlSessionsChanged");
        }

        private async Task SyncCustomEffectsForAllSessionsAsync()
        {
            if (!HasAnyActiveGameSession())
            {
                LogVerbose("No active Crowd Control sessions available; skipping custom effect sync refresh.");
                return;
            }

            foreach (var kvp in _data.PlayerSessions)
            {
                var steamId = kvp.Key;
                var session = kvp.Value;
                if (!HasActiveGameSession(steamId, session))
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

        private void SendCommandReply(BasePlayer player, CommandReplyMode replyMode, string message)
        {
            if (player == null || string.IsNullOrWhiteSpace(message))
            {
                return;
            }

            if (replyMode == CommandReplyMode.Console)
            {
                player.ConsoleMessage($"[CrowdControl] {message}");
                return;
            }

            player.ChatMessage($"[CrowdControl] {message}");
        }

        private void SendCommandReplies(BasePlayer player, CommandReplyMode replyMode, params string[] messages)
        {
            if (messages == null)
            {
                return;
            }

            for (var i = 0; i < messages.Length; i++)
            {
                SendCommandReply(player, replyMode, messages[i]);
            }
        }

        private CommandReplyMode GetAuthReplyMode(string steamId)
        {
            lock (_pendingAuthRequests)
            {
                if (!string.IsNullOrWhiteSpace(steamId) && _authReplyModeBySteamId.TryGetValue(steamId, out var replyMode))
                {
                    return replyMode;
                }
            }

            return CommandReplyMode.Console;
        }

        private bool TryGetActiveAuthCode(string steamId, out string code)
        {
            lock (_pendingAuthRequests)
            {
                return TryGetActiveAuthCodeLocked(steamId, out code);
            }
        }

        private bool TryGetActiveAuthCodeLocked(string steamId, out string code)
        {
            code = null;
            if (string.IsNullOrWhiteSpace(steamId) || !_activeAuthCodesBySteamId.TryGetValue(steamId, out var state) || state == null)
            {
                return false;
            }

            if (string.IsNullOrWhiteSpace(state.Code) || state.ExpiresUtc <= DateTime.UtcNow)
            {
                if (!string.IsNullOrWhiteSpace(state.Code))
                {
                    _authCodeToSteamId.Remove(state.Code);
                }

                _activeAuthCodesBySteamId.Remove(steamId);
                return false;
            }

            code = state.Code;
            return true;
        }

        private void SetActiveAuthCode(string steamId, string code, DateTime expiresUtc)
        {
            lock (_pendingAuthRequests)
            {
                if (string.IsNullOrWhiteSpace(steamId) || string.IsNullOrWhiteSpace(code))
                {
                    return;
                }

                if (_activeAuthCodesBySteamId.TryGetValue(steamId, out var existingState) && !string.IsNullOrWhiteSpace(existingState?.Code))
                {
                    _authCodeToSteamId.Remove(existingState.Code);
                }

                _activeAuthCodesBySteamId[steamId] = new ActiveAuthCodeState
                {
                    Code = code,
                    ExpiresUtc = expiresUtc
                };
                _authCodeToSteamId[code] = steamId;
            }
        }

        private void ClearActiveAuthCode(string steamId, string code = null)
        {
            lock (_pendingAuthRequests)
            {
                if (string.IsNullOrWhiteSpace(steamId))
                {
                    return;
                }

                if (_activeAuthCodesBySteamId.TryGetValue(steamId, out var state))
                {
                    if (string.IsNullOrWhiteSpace(code) || string.Equals(state?.Code, code, StringComparison.OrdinalIgnoreCase))
                    {
                        if (!string.IsNullOrWhiteSpace(state?.Code))
                        {
                            _authCodeToSteamId.Remove(state.Code);
                        }

                        _activeAuthCodesBySteamId.Remove(steamId);
                    }
                }
                else if (!string.IsNullOrWhiteSpace(code))
                {
                    _authCodeToSteamId.Remove(code);
                }
            }
        }

        private void ClearAuthReplyMode(string steamId)
        {
            lock (_pendingAuthRequests)
            {
                if (!string.IsNullOrWhiteSpace(steamId))
                {
                    _authReplyModeBySteamId.Remove(steamId);
                }
            }
        }

        private void RemovePendingAuthRequest(string steamId)
        {
            lock (_pendingAuthRequests)
            {
                if (string.IsNullOrWhiteSpace(steamId) || _pendingAuthRequests.Count == 0)
                {
                    return;
                }

                var retainedRequests = new Queue<string>();
                while (_pendingAuthRequests.Count > 0)
                {
                    var queuedSteamId = _pendingAuthRequests.Dequeue();
                    if (!string.Equals(queuedSteamId, steamId, StringComparison.OrdinalIgnoreCase))
                    {
                        retainedRequests.Enqueue(queuedSteamId);
                    }
                }

                while (retainedRequests.Count > 0)
                {
                    _pendingAuthRequests.Enqueue(retainedRequests.Dequeue());
                }
            }
        }

        private bool HasPendingAuthState(string steamId)
        {
            lock (_pendingAuthRequests)
            {
                return !string.IsNullOrWhiteSpace(steamId) &&
                    (_pendingAuthRequests.Contains(steamId) ||
                     _activeAuthCodesBySteamId.ContainsKey(steamId) ||
                     _authReplyModeBySteamId.ContainsKey(steamId));
            }
        }

        private bool ClearPlayerAuthState(string steamId, bool stopActiveSession, bool refreshSocketState = true)
        {
            if (string.IsNullOrWhiteSpace(steamId))
            {
                return false;
            }

            PlayerSessionState session = null;
            var hadSession = _data?.PlayerSessions != null &&
                _data.PlayerSessions.TryGetValue(steamId, out session) &&
                session != null;
            var hadPendingState = HasPendingAuthState(steamId);

            if (stopActiveSession &&
                hadSession &&
                IsSessionTokenUsable(session) &&
                !string.IsNullOrEmpty(session.GameSessionId))
            {
                FireAndForget(StopGameSessionAsync(session), "clear auth stop session");
            }

            if (hadSession)
            {
                _data.PlayerSessions.Remove(steamId);
            }

            RemovePendingAuthRequest(steamId);
            ClearActiveAuthCode(steamId);
            ClearAuthReplyMode(steamId);

            if (hadSession)
            {
                SaveData();
                NotifyCrowdControlProvidersSessionStateChanged();
            }

            var player = FindPlayerBySteamId(steamId);
            if (player != null)
            {
                RefreshCrowdControlEnforcement(player);
            }

            if (refreshSocketState && (hadSession || hadPendingState))
            {
                FireAndForget(EnsureSocketConnectedAsync(), "socket state check after auth clear");
            }

            return hadSession || hadPendingState;
        }

        private async Task RequestAuthCodeAsync(string steamId, CommandReplyMode replyMode, bool requestedFromChat)
        {
            try
            {
                await EnsureSocketConnectedAsync();
                await SendGenerateAuthCodeAsync();

                var player = FindPlayerBySteamId(steamId);
                if (player == null)
                {
                    return;
                }

                SendCommandReply(player, replyMode, "Auth code request submitted. Watch this console for your code.");
                if (requestedFromChat)
                {
                    SendCommandReply(player, CommandReplyMode.Chat, "Auth started. Press F1 to view your Crowd Control code in console.");
                }
            }
            catch (Exception ex)
            {
                RemovePendingAuthRequest(steamId);
                ClearAuthReplyMode(steamId);

                var player = FindPlayerBySteamId(steamId);
                if (player != null)
                {
                    SendCommandReply(player, replyMode, "Failed to submit Crowd Control auth request. Please try again.");
                    if (requestedFromChat)
                    {
                        SendCommandReply(player, CommandReplyMode.Chat, "Crowd Control auth could not start. Press F1 for details, then try again.");
                    }
                }

                throw new InvalidOperationException($"Auth request submission failed for SteamID {steamId}: {ex.Message}", ex);
            }
        }

        private bool CanUsePlugin(BasePlayer player, CommandReplyMode? replyMode = null)
        {
            if (player == null)
            {
                return false;
            }

            if (!IsPlayerAllowedForCrowdControl(player))
            {
                if (replyMode.HasValue)
                {
                    SendCommandReply(player, replyMode.Value, $"You do not have permission to use Crowd Control. Required permission: {PermUse}");
                }
                else
                {
                    ShowEffectUi(player, "Crowd Control", "You do not have permission to use Crowd Control.");
                    player.ConsoleMessage($"[CrowdControl] You do not have permission to use Crowd Control. Required permission: {PermUse}");
                }
                return false;
            }

            return true;
        }

        private bool CanUseAdminCommand(BasePlayer player, CommandReplyMode? replyMode = null)
        {
            if (player == null)
            {
                return false;
            }

            if (!permission.UserHasPermission(player.UserIDString, PermAdmin))
            {
                if (replyMode.HasValue)
                {
                    SendCommandReply(player, replyMode.Value, $"You need the {PermAdmin} permission to use this command.");
                }
                else
                {
                    ShowErrorUi(player, $"You need the {PermAdmin} permission to use this command.");
                }
                return false;
            }

            return true;
        }

        private bool HasCrowdControlAuth(string steamId)
        {
            return TryGetAuthenticatedSession(steamId, out _);
        }

        private bool IsPlayerIgnoredForCrowdControlAuth(BasePlayer player)
        {
            return player != null && permission.UserHasPermission(player.UserIDString, PermIgnore);
        }

        private bool ShouldPromptCrowdControlLink(BasePlayer player)
        {
            return player != null &&
                IsPlayerAllowedForCrowdControl(player) &&
                !IsPlayerIgnoredForCrowdControlAuth(player) &&
                !HasCrowdControlAuth(player.UserIDString);
        }

        private void MaybeShowCrowdControlJoinInstructions(BasePlayer player)
        {
            if (!ShouldPromptCrowdControlLink(player))
            {
                return;
            }

            player.ChatMessage("Crowd Control is available on this server. Type /cc link in chat or use cc link in F1 console to connect.");
            ShowEffectUi(player, "Crowd Control", "Press F1 and type cc link to connect.");
            player.ConsoleMessage("[CrowdControl] Crowd Control is available on this server.");
            player.ConsoleMessage("[CrowdControl] Run: cc link");
            player.ConsoleMessage("[CrowdControl] Other useful commands: cc status, cc settings");

            if (!_crowdControlEnforcementBySteamId.TryGetValue(player.UserIDString, out var state))
            {
                state = new CrowdControlEnforcementState { SteamId = player.UserIDString };
                _crowdControlEnforcementBySteamId[player.UserIDString] = state;
            }

            state.ReminderTimer?.Destroy();
            state.ReminderTimer = timer.Once(8f, () =>
            {
                state.ReminderTimer = null;
                var current = FindPlayerBySteamId(player.UserIDString);
                if (ShouldPromptCrowdControlLink(current))
                {
                    ShowEffectUi(current, "Crowd Control", "Reminder: press F1 and type cc link to connect.");
                }
            });
        }

        private void RefreshCrowdControlEnforcementForAllPlayers()
        {
            foreach (var player in BasePlayer.activePlayerList)
            {
                if (player != null && player.IsConnected)
                {
                    RefreshCrowdControlEnforcement(player);
                }
            }
        }

        private void RefreshCrowdControlEnforcement(BasePlayer player)
        {
            if (player == null)
            {
                return;
            }

            if (!ShouldPromptCrowdControlLink(player) || !(_config?.EnforceCrowdControl?.Enabled ?? false))
            {
                ClearCrowdControlEnforcement(player.UserIDString, showReleasedMessage: false);
                return;
            }

            if (!_crowdControlEnforcementBySteamId.TryGetValue(player.UserIDString, out var state))
            {
                state = new CrowdControlEnforcementState
                {
                    SteamId = player.UserIDString
                };
                _crowdControlEnforcementBySteamId[player.UserIDString] = state;
            }

            state.IsEnforced = false;
            state.AnchorPosition = player.transform.position;
            state.GraceTimer?.Destroy();
            state.MovementTimer?.Destroy();
            state.MovementTimer = null;

            var graceSeconds = Math.Max(30, _config?.EnforceCrowdControl?.EnforceTimeSeconds ?? 120);
            state.GraceTimer = timer.Once(graceSeconds, () =>
            {
                state.GraceTimer = null;
                ActivateCrowdControlEnforcement(player.UserIDString);
            });
        }

        private void ActivateCrowdControlEnforcement(string steamId)
        {
            var player = FindPlayerBySteamId(steamId);
            if (player == null || !ShouldPromptCrowdControlLink(player))
            {
                ClearCrowdControlEnforcement(steamId, showReleasedMessage: false);
                return;
            }

            if (!_crowdControlEnforcementBySteamId.TryGetValue(steamId, out var state))
            {
                state = new CrowdControlEnforcementState
                {
                    SteamId = steamId
                };
                _crowdControlEnforcementBySteamId[steamId] = state;
            }

            state.IsEnforced = true;
            state.AnchorPosition = player.transform.position;
            ShowErrorUi(player, "Crowd Control linking is required. Press F1 and type cc link now.");
            player.ConsoleMessage("[CrowdControl] Crowd Control linking is required on this server.");
            player.ConsoleMessage("[CrowdControl] Run: cc link");
            TryClosePlayerInventory(player);

            if (_config?.EnforceCrowdControl?.RestrictMovement == true)
            {
                state.MovementTimer?.Destroy();
                state.MovementTimer = timer.Every(0.1f, () =>
                {
                    var current = FindPlayerBySteamId(steamId);
                    if (current == null || !ShouldPromptCrowdControlLink(current))
                    {
                        ClearCrowdControlEnforcement(steamId, showReleasedMessage: false);
                        return;
                    }

                    current.Teleport(state.AnchorPosition);
                });
            }
        }

        private void ClearCrowdControlEnforcement(string steamId, bool showReleasedMessage)
        {
            if (string.IsNullOrWhiteSpace(steamId) || !_crowdControlEnforcementBySteamId.TryGetValue(steamId, out var state))
            {
                return;
            }

            var wasEnforced = state.IsEnforced;
            state.GraceTimer?.Destroy();
            state.ReminderTimer?.Destroy();
            state.MovementTimer?.Destroy();
            _crowdControlEnforcementBySteamId.Remove(steamId);

            var player = FindPlayerBySteamId(steamId);
            if (showReleasedMessage && wasEnforced && player != null && player.IsConnected)
            {
                ShowEffectUi(player, "Crowd Control", "Crowd Control link detected. Restrictions lifted.");
            }
        }

        private bool IsCrowdControlEnforced(BasePlayer player)
        {
            return player != null &&
                _crowdControlEnforcementBySteamId.TryGetValue(player.UserIDString, out var state) &&
                state != null &&
                state.IsEnforced;
        }

        private void NotifyCrowdControlEnforcementBlocked(BasePlayer player)
        {
            if (player == null || !player.IsConnected || !_crowdControlEnforcementBySteamId.TryGetValue(player.UserIDString, out var state))
            {
                return;
            }

            if ((DateTime.UtcNow - state.LastBlockedNoticeUtc).TotalSeconds < 5)
            {
                return;
            }

            state.LastBlockedNoticeUtc = DateTime.UtcNow;
            ShowErrorUi(player, "Link Crowd Control with cc link in F1 or /cc link in chat before playing.");
        }

        private bool IsPlayerAllowedForCrowdControl(BasePlayer player)
        {
            if (player == null)
            {
                return false;
            }

            if (_config?.AllowAllUsersWithoutPermission == true)
            {
                return true;
            }

            return player.IsAdmin || permission.UserHasPermission(player.UserIDString, PermUse);
        }

        #endregion

        #region WebSocket

        private async Task EnsureSocketConnectedAsync()
        {
            if (_isUnloading)
            {
                return;
            }

            if (!ShouldMaintainSocketConnection())
            {
                CloseSocketConnection();
                return;
            }

            if (_socket != null && _socket.State == WebSocketState.Open)
            {
                return;
            }

            if (await WaitForSocketConnectionAsync())
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

                if (!ShouldMaintainSocketConnection())
                {
                    CloseSocketConnection();
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

        private async Task<bool> WaitForSocketConnectionAsync()
        {
            while (true)
            {
                if (_socket != null && _socket.State == WebSocketState.Open)
                {
                    return true;
                }

                var isConnecting = false;
                lock (_socketSync)
                {
                    isConnecting = _isSocketConnecting;
                }

                if (!isConnecting)
                {
                    return _socket != null && _socket.State == WebSocketState.Open;
                }

                await Task.Delay(100);
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
                            ShowSessionDisconnectToastToActivePlayers("Crowd Control disconnected. Session may have ended.");
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
                ShowSessionDisconnectToastToActivePlayers("Crowd Control connection error. Session may have ended.");
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
                //LogVerbose($"Ignoring non-JSON socket frame: {trimmed}");
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
            var replyMode = CommandReplyMode.Console;
            lock (_pendingAuthRequests)
            {
                if (_pendingAuthRequests.Count > 0)
                {
                    steamId = _pendingAuthRequests.Dequeue();
                    if (!string.IsNullOrWhiteSpace(steamId) && _authReplyModeBySteamId.TryGetValue(steamId, out var queuedReplyMode))
                    {
                        replyMode = queuedReplyMode;
                    }
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
                SetActiveAuthCode(steamId, code, DateTime.UtcNow.AddMinutes(3));
            }

            var player = FindPlayerBySteamId(steamId);
            if (player != null)
            {
                if (!string.IsNullOrEmpty(code))
                {
                    SendCommandReply(player, replyMode, $"Enter this auth code into your Crowd Control app: {code}");
                    SendCommandReply(player, replyMode, "You have 3 minutes to enter this code before it expires.");
                }
                else
                {
                    SendCommandReply(
                        player,
                        replyMode,
                        replyMode == CommandReplyMode.Console
                            ? "Auth code generation failed. Please run cc link again."
                            : "Auth code generation failed. Please run /cc link again."
                    );
                    ClearAuthReplyMode(steamId);
                }
            }

            if (string.IsNullOrEmpty(code))
            {
                ClearAuthReplyMode(steamId);
            }
        }

        private void HandleApplicationAuthCodeError(JObject payload)
        {
            string steamId = null;
            var replyMode = CommandReplyMode.Console;
            lock (_pendingAuthRequests)
            {
                if (_pendingAuthRequests.Count > 0)
                {
                    steamId = _pendingAuthRequests.Dequeue();
                    if (!string.IsNullOrWhiteSpace(steamId) && _authReplyModeBySteamId.TryGetValue(steamId, out var queuedReplyMode))
                    {
                        replyMode = queuedReplyMode;
                    }
                }
            }

            var message = payload.Value<string>("message") ?? "Unknown Crowd Control auth error.";
            PrintWarning($"Crowd Control auth code error: {message}");

            var player = !string.IsNullOrEmpty(steamId) ? FindPlayerBySteamId(steamId) : null;
            if (player != null)
            {
                SendCommandReply(player, replyMode, $"Auth code failed: {message}");
            }

            ClearAuthReplyMode(steamId);
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

            ClearActiveAuthCode(steamId, code);
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
            PruneInvalidStoredSessions();
            foreach (var kvp in _data.PlayerSessions)
            {
                var session = kvp.Value;
                if (session == null ||
                    !IsSessionTokenUsable(session) ||
                    string.IsNullOrEmpty(session.CcUid) ||
                    !IsSteamPlayerOnline(kvp.Key))
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
            var replyMode = GetAuthReplyMode(steamId);
            try
            {
                LogVerbose(
                    $"Auth token exchange config appID={_config?.AppId ?? "(null)"}, scopes={string.Join(",", Scopes)}, secretFingerprint={BuildSecretFingerprint(_config?.AppSecret)}");
                var endpoint = $"{OpenApiUrl}/auth/application/token";
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
                LogVerbose(
                    $"Auth token decoded ccUID={decoded.CcUid}, profileType={decoded.ProfileType}, tokenScopes={decoded.ScopeSummary ?? "(missing)"}");

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

                if (string.IsNullOrEmpty(_data.ApplicationInstanceId))
                {
                    await EnsureApplicationInstanceIdAsync();
                }

                var player = FindPlayerBySteamId(steamId);
                if (player != null)
                {
                    SendCommandReply(player, replyMode, "Crowd Control auth complete.");
                    SendCommandReply(player, replyMode, $"Connected profile: {decoded.Name} ({decoded.CcUid})");
                }
                ClearCrowdControlEnforcement(steamId, showReleasedMessage: true);
                WarnIfBuiltInEffectsPluginMissing("auth completion", player);

                if (IsSteamPlayerOnline(steamId))
                {
                    await SendSubscribeAsync(token, decoded.CcUid);
                }

                if (AutoStartSessionAfterAuth && IsSteamPlayerOnline(steamId))
                {
                    await StartGameSessionAsync(_data.PlayerSessions[steamId], steamId);
                }

                NotifyCrowdControlProvidersSessionStateChanged();
            }
            catch (Exception ex)
            {
                PrintError($"Token exchange failed for {steamId}: {ex.Message}");
                var player = FindPlayerBySteamId(steamId);
                if (player != null)
                {
                    SendCommandReply(player, replyMode, $"Auth token exchange failed: {ex.Message}");
                }
            }
            finally
            {
                ClearActiveAuthCode(steamId);
                ClearAuthReplyMode(steamId);
            }
        }

        private async Task StartGameSessionAsync(PlayerSessionState session, string notifySteamId = null)
        {
            if (!IsSessionTokenUsable(session))
            {
                return;
            }

            var steamId = session.SteamId ?? notifySteamId ?? string.Empty;
            if (string.IsNullOrEmpty(steamId) || !IsSteamPlayerOnline(steamId))
            {
                LogVerbose($"Skipping game-session start because player is offline (SteamID={steamId}).");
                return;
            }

            if (!TryBeginSessionStart(steamId))
            {
                LogVerbose($"Session start already in progress for {steamId}; skipping duplicate start.");
                return;
            }

            try
            {
                var instanceId = await EnsureApplicationInstanceIdAsync();
                if (_config?.SessionRules?.EnablePriceChange == false)
                {
                    await SyncDefaultGameEffectOverridesAsync(session.Token, steamId, instanceId);
                    // Sync call may refresh stale instance IDs; use latest cached value for session start.
                    instanceId = _data?.ApplicationInstanceId ?? instanceId;
                }

                var endpoint = $"{OpenApiUrl}/game-session/start";
                var body = new JObject
                {
                    ["gamePackID"] = GamePackId,
                    ["effectReportArgs"] = new JArray(),
                    ["sessionRules"] = new JObject
                    {
                        ["enableIntegrationTriggers"] = _config?.SessionRules?.EnableIntegrationTriggers ?? true,
                        ["enablePriceChange"] = _config?.SessionRules?.EnablePriceChange ?? true,
                        ["disableTestEffects"] = _config?.SessionRules?.DisableTestEffects ?? false,
                        ["disableCustomEffectsSync"] = _config?.SessionRules?.DisableCustomEffectsSync ?? false
                    }
                };
                if (!string.IsNullOrEmpty(_config?.AppId))
                {
                    var gameSessionProperty = new JObject
                    {
                        ["appID"] = _config.AppId
                    };
                    if (!string.IsNullOrEmpty(instanceId))
                    {
                        gameSessionProperty["instanceID"] = instanceId;
                    }
                    body["gameSessionProperty"] = gameSessionProperty;
                }

                var response = await PostJsonAsync(endpoint, body, includeAuth: session.Token);
                var gameSessionId = response.Value<string>("gameSessionID");
                if (!string.IsNullOrEmpty(gameSessionId))
                {
                    session.GameSessionId = gameSessionId;
                    SaveData();
                    NotifyCrowdControlProvidersSessionStateChanged();

                    var player = !string.IsNullOrEmpty(notifySteamId) ? FindPlayerBySteamId(notifySteamId) : null;
                    if (player != null)
                    {
                        ShowEffectUi(player, "Crowd Control", "Session started!");
                    }
                    WarnIfBuiltInEffectsPluginMissing("game session start", player);

                    if (_config?.SessionRules?.DisableCustomEffectsSync != true)
                    {
                        // Custom effects endpoint expects an active session context.
                        await Task.Delay(1000);
                        await SyncCustomEffectsAsync(session.Token, steamId);
                    }

                }
            }
            catch (Exception ex)
            {
                if (IsInstanceOwnershipError(ex.Message) || IsHttpUnauthorized(ex.Message))
                {
                    //LogVerbose("Session start instance ownership mismatch detected; refreshing application instance ID and retrying once.");
                    _data.ApplicationInstanceId = string.Empty;
                    _data.ApplicationInstanceAppId = string.Empty;
                    SaveData();

                    try
                    {
                        var refreshedInstanceId = await EnsureApplicationInstanceIdAsync();
                        var retryEndpoint = $"{OpenApiUrl}/game-session/start";
                        var retryBody = new JObject
                        {
                            ["gamePackID"] = GamePackId,
                            ["effectReportArgs"] = new JArray(),
                            ["sessionRules"] = new JObject
                            {
                                ["enableIntegrationTriggers"] = _config?.SessionRules?.EnableIntegrationTriggers ?? true,
                                ["enablePriceChange"] = _config?.SessionRules?.EnablePriceChange ?? true,
                                ["disableTestEffects"] = _config?.SessionRules?.DisableTestEffects ?? false,
                                ["disableCustomEffectsSync"] = _config?.SessionRules?.DisableCustomEffectsSync ?? false
                            }
                        };

                        if (!string.IsNullOrEmpty(_config?.AppId))
                        {
                            var retryGameSessionProperty = new JObject
                            {
                                ["appID"] = _config.AppId
                            };
                            if (!string.IsNullOrEmpty(refreshedInstanceId))
                            {
                                retryGameSessionProperty["instanceID"] = refreshedInstanceId;
                            }
                            retryBody["gameSessionProperty"] = retryGameSessionProperty;
                        }

                        var retryResponse = await PostJsonAsync(retryEndpoint, retryBody, includeAuth: session.Token);
                        var retryGameSessionId = retryResponse.Value<string>("gameSessionID");
                        if (!string.IsNullOrEmpty(retryGameSessionId))
                        {
                            session.GameSessionId = retryGameSessionId;
                            SaveData();
                            NotifyCrowdControlProvidersSessionStateChanged();
                            return;
                        }
                    }
                    catch (Exception retryEx)
                    {
                        PrintWarning("Retry start game session after instance refresh failed: {0}", retryEx.Message);
                    }
                }

                PrintError($"Failed to start game session: {ex.Message}");
            }
            finally
            {
                EndSessionStart(steamId);
            }
        }

        private async Task<string> EnsureApplicationInstanceIdAsync()
        {
            var configuredAppId = (_config?.AppId ?? string.Empty).Trim();
            if (!string.IsNullOrEmpty(_data?.ApplicationInstanceId) &&
                string.Equals(_data?.ApplicationInstanceAppId ?? string.Empty, configuredAppId, StringComparison.Ordinal))
            {
                LogVerbose($"Using cached application instance ID: {_data.ApplicationInstanceId}");
                return _data.ApplicationInstanceId;
            }

            if (string.IsNullOrWhiteSpace(_config?.AppId) || string.IsNullOrWhiteSpace(_config?.AppSecret))
            {
                LogVerbose("App credentials missing; skipping application instance ID generation.");
                return string.Empty;
            }

            try
            {
                var endpoint = $"{OpenApiUrl}/auth/application/instance";
                var body = new JObject
                {
                    ["appID"] = _config.AppId,
                    ["secret"] = _config.AppSecret
                };

                var response = await PostJsonAsync(endpoint, body, includeAuth: null);
                var instanceId = response.Value<string>("instanceID") ?? string.Empty;
                if (string.IsNullOrEmpty(instanceId))
                {
                    PrintWarning("Application instance generation succeeded but no instanceID was returned.");
                    return string.Empty;
                }

                _data.ApplicationInstanceId = instanceId;
                _data.ApplicationInstanceAppId = configuredAppId;
                SaveData();
                LogVerbose($"Application instance ID ready: {instanceId}");
                return instanceId;
            }
            catch (Exception ex)
            {
                PrintWarning("Application instance ID generation failed: {0}", ex.Message);
                return string.Empty;
            }
        }

        private async Task RestartGameSessionAsync(PlayerSessionState session, string notifySteamId = null)
        {
            if (!IsSessionTokenUsable(session))
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
                (!_data.PlayerSessions.TryGetValue(steamId, out var session) || !HasActiveGameSession(steamId, session)))
            {
                LogVerbose($"Skipping custom-effects sync for {steamId}: no active Crowd Control session.");
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
            return $"{OpenApiUrl}/menu/custom-effects";
        }

        private string GetDefaultGameEffectsEndpoint()
        {
            return $"{OpenApiUrl}/menu/effects/default";
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

        private async Task SyncDefaultGameEffectOverridesAsync(string token, string steamId, string instanceId)
        {
            if (string.IsNullOrEmpty(token))
            {
                return;
            }

            if (string.IsNullOrEmpty(instanceId))
            {
                LogVerbose("No application instance ID available; skipping default game effect override sync.");
                return;
            }

            var defaultsById = BuildRegisteredProviderDefaults(BuiltInEffectsPluginName);
            var entriesById = LoadOrSeedProviderEffectEntries(BuiltInEffectsPluginName, defaultsById, localOnly: true);
            if (entriesById.Count == 0)
            {
                LogVerbose("No effect pricing entries available; skipping default game effect override sync.");
                return;
            }

            var effectOverrides = new JArray();
            var entries = new List<EffectPricingEntry>(entriesById.Values);
            entries.Sort((a, b) => string.Compare(a?.EffectId, b?.EffectId, StringComparison.OrdinalIgnoreCase));
            for (var i = 0; i < entries.Count; i++)
            {
                var entry = entries[i];
                if (string.IsNullOrWhiteSpace(entry?.EffectId))
                {
                    continue;
                }

                // The default-effect override endpoint only accepts default game pack effects.
                // Exclude local-only test/demo effects that do not exist as default pack effects.
                if (string.Equals(entry.EffectId, "test_hype_train", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                try
                {
                    var scalePercent = 0f;
                    var scaleDuration = 0f;
                    if (entry.Scale != null && !entry.Scale.Inactive)
                    {
                        scalePercent = Math.Max(0f, entry.Scale.Percent);
                        scaleDuration = Math.Max(0f, entry.Scale.Duration);
                    }

                    var durationValue = 0f;
                    var durationImmutable = false;
                    if (entry.Duration != null)
                    {
                        durationValue = Math.Max(0f, ReadTokenAsFloat(entry.Duration["value"], 0f));
                        durationImmutable = ReadTokenAsBool(entry.Duration["immutable"], false);
                    }

                    var overrideObj = new JObject
                    {
                        ["effectID"] = entry.EffectId,
                        ["type"] = "game",
                        ["inactive"] = entry.Inactive,
                        ["sessionCooldown"] = Math.Max(0, entry.SessionCooldown),
                        ["userCooldown"] = Math.Max(0, entry.UserCooldown),
                        ["scale"] = new JObject
                        {
                            ["percent"] = scalePercent,
                            ["duration"] = scaleDuration
                        }
                    };

                    // Default-effect payload requires price >= 1.
                    // If local pricing isn't set, force minimum 1.
                    overrideObj["price"] = Math.Max(1, entry.Price);

                    // Only include duration when it is valid (>0).
                    if (durationValue > 0f)
                    {
                        overrideObj["duration"] = new JObject
                        {
                            ["value"] = durationValue,
                            ["immutable"] = durationImmutable
                        };
                    }

                    effectOverrides.Add(overrideObj);
                }
                catch (Exception ex)
                {
                    PrintWarning("Skipping default game effect override for {0}: {1}", entry.EffectId, ex.Message);
                }
            }

            if (effectOverrides.Count == 0)
            {
                LogVerbose("No valid default game effect overrides to sync.");
                return;
            }

            var sampleEntries = new List<string>();
            var sampled = 0;
            foreach (var tokenEntry in effectOverrides)
            {
                if (!(tokenEntry is JObject obj))
                {
                    continue;
                }

                var sampleEffectId = obj.Value<string>("effectID") ?? string.Empty;
                var samplePrice = obj.Value<int?>("price") ?? 0;
                sampleEntries.Add($"{sampleEffectId}=>{{price:{samplePrice}}}");
                sampled++;
                if (sampled >= 2)
                {
                    break;
                }
            }
            LogVerbose($"Default effect override sample ({sampled}/{effectOverrides.Count}): {string.Join(" | ", sampleEntries)}");

            var body = new JObject
            {
                ["gamePackID"] = GamePackId,
                ["instanceID"] = instanceId,
                ["effectOverrides"] = effectOverrides
            };

            try
            {
                var decoded = DecodeJwt(token);
                LogVerbose(
                    $"Posting default-effect overrides instanceID={instanceId}, effects={effectOverrides.Count}, tokenScopes={decoded?.ScopeSummary ?? "(missing)"}");
                await PostJsonAsync(GetDefaultGameEffectsEndpoint(), body, includeAuth: token);
                LogVerbose($"Default game effect overrides synced for {effectOverrides.Count} effect(s), steamID={steamId}.");
            }
            catch (Exception ex)
            {
                if (IsInstanceOwnershipError(ex.Message) || IsHttpUnauthorized(ex.Message))
                {
                    LogVerbose("Instance ownership mismatch detected; regenerating application instance ID and retrying once.");
                    _data.ApplicationInstanceId = string.Empty;
                    _data.ApplicationInstanceAppId = string.Empty;
                    SaveData();

                    var refreshedInstanceId = await EnsureApplicationInstanceIdAsync();
                    if (!string.IsNullOrEmpty(refreshedInstanceId))
                    {
                        body["instanceID"] = refreshedInstanceId;
                        try
                        {
                            await PostJsonAsync(GetDefaultGameEffectsEndpoint(), body, includeAuth: token);
                            LogVerbose($"Default game effect overrides synced after instance refresh for {effectOverrides.Count} effect(s), steamID={steamId}.");
                            return;
                        }
                        catch (Exception retryEx)
                        {
                            PrintWarning("Default game effect override retry failed: {0}", retryEx.Message);
                            return;
                        }
                    }
                }

                // PrintWarning uses format placeholders; pass message as an argument to avoid
                // format exceptions when HTTP responses include JSON braces.
                PrintWarning("Default game effect override sync failed: {0}", ex.Message);
            }
        }

        private bool IsInstanceOwnershipError(string message)
        {
            if (string.IsNullOrEmpty(message))
            {
                return false;
            }

            return message.IndexOf("not owned by application", StringComparison.OrdinalIgnoreCase) >= 0;
        }

        private bool IsHttpUnauthorized(string message)
        {
            if (string.IsNullOrEmpty(message))
            {
                return false;
            }

            return message.IndexOf("HTTP 401", StringComparison.OrdinalIgnoreCase) >= 0
                || message.IndexOf("\"code\":\"UNAUTHORIZED\"", StringComparison.OrdinalIgnoreCase) >= 0;
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
            var effects = new JObject();

            lock (_externalEffectsSync)
            {
                foreach (var kvp in _externalEffectsById)
                {
                    var def = kvp.Value;
                    if (def?.MenuEffect == null || string.IsNullOrWhiteSpace(def.EffectId) || def.LocalOnly)
                    {
                        continue;
                    }

                    var menuEffect = (JObject)def.MenuEffect.DeepClone();
                    menuEffect.Remove("effectID");
                    effects[def.EffectId] = menuEffect;
                }
            }

            return effects;
        }

        private async Task StopGameSessionAsync(PlayerSessionState session, string notifySteamId = null)
        {
            if (!IsSessionTokenUsable(session))
            {
                return;
            }

            if (string.IsNullOrEmpty(session.GameSessionId))
            {
                return;
            }

            try
            {
                var endpoint = $"{OpenApiUrl}/game-session/stop";
                var body = new JObject
                {
                    ["gameSessionID"] = session.GameSessionId
                };

                await PostJsonAsync(endpoint, body, includeAuth: session.Token);
                session.GameSessionId = null;

                var hasActiveSessions = false;
                foreach (var playerSession in _data.PlayerSessions.Values)
                {
                    if (!string.IsNullOrEmpty(playerSession?.GameSessionId))
                    {
                        hasActiveSessions = true;
                        break;
                    }
                }

                if (!hasActiveSessions)
                {
                    _data.ApplicationInstanceId = string.Empty;
                    _data.ApplicationInstanceAppId = string.Empty;
                }

                SaveData();

                var player = !string.IsNullOrEmpty(notifySteamId) ? FindPlayerBySteamId(notifySteamId) : null;
                if (player != null)
                {
                    ShowErrorUi(player, "Crowd Control session ended or disconnected.");
                }

                NotifyCrowdControlProvidersSessionStateChanged();
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
            if (!TryGetAuthenticatedSession(steamId, out var session))
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

                    FireAndForget(SendEffectResponseAsync(responseToken, requestId, "failTemporary", missingSessionReason), "effect missing session");
                    return;
                }

                PrintWarning($"No active token for SteamID {steamId}; cannot answer effect request {requestId}.");
                return;
            }
            responseToken = session.Token;

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

            if (IsEffectBlockedBySessionRules(payload, effect, out var blockedReason))
            {
                LogVerbose($"Effect request blocked by session rules requestID={requestId}, effectID={effectId}, reason={blockedReason}");
                if ((_config?.SessionRules?.DisableTestEffects ?? false) && IsTestEffectRequest(payload, effect))
                {
                    ShowEffectUi(player, "Crowd Control", "Test effect received, but test effects are disabled and it will not activate.");
                }

                ClearEffectRetryState(requestId);
                FireAndForget(SendEffectResponseAsync(responseToken, requestId, "failTemporary", blockedReason), "effect blocked by rules");
                return;
            }

            LogVerbose($"Effect requestID={requestId} resolved to player={player.displayName} ({steamId}).");
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
                    out var pendingExternal,
                    out var externalTimeRemainingMs))
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
                    if (string.Equals(externalStatus, "timedBegin", StringComparison.OrdinalIgnoreCase))
                    {
                        FireAndForget(
                            SendTimedResponseAsync(responseToken, requestId, "timedBegin", externalTimeRemainingMs, externalReason ?? string.Empty),
                            "external effect timedBegin"
                        );
                    }
                    return;
                }

                ClearEffectRetryState(requestId);
                FireAndForget(
                    SendEffectResponseAsync(responseToken, requestId, externalStatus, externalReason ?? string.Empty),
                    $"external effect {externalStatus}"
                );
                return;
            }

            const string unhandledReason = "No registered provider handled this effect.";
            LogVerbose($"Effect request requestID={requestId}, effectID={effectId} had no registered provider.");
            ClearEffectRetryState(requestId);
            FireAndForget(
                SendEffectResponseAsync(responseToken, requestId, "failPermanent", unhandledReason),
                "effect failPermanent unhandled"
            );
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
            out bool pending,
            out long? timeRemainingMs)
        {
            status = string.Empty;
            reason = string.Empty;
            playerMessage = string.Empty;
            pending = false;
            timeRemainingMs = null;

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

            ParseExternalProviderResult(providerResult, out status, out reason, out playerMessage, out pending, out timeRemainingMs);

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
            pendingState.TimeoutTimer = timer.Once(
                GetExternalEffectTimeoutSeconds(status, timeRemainingMs),
                () => HandleExternalEffectTimeout(requestId));

            lock (_externalEffectsSync)
            {
                if (_externalPendingRequests.TryGetValue(requestId, out var existingPending))
                {
                    existingPending?.TimeoutTimer?.Destroy();
                }
                _externalPendingRequests[requestId] = pendingState;
            }

            LogVerbose($"External effect pending requestID={requestId}, effectID={effectId}, provider={definition.ProviderName}, status={status}.");
            return true;
        }

        private void ParseExternalProviderResult(object providerResult, out string status, out string reason, out string playerMessage, out bool pending, out long? timeRemainingMs)
        {
            status = "success";
            reason = string.Empty;
            playerMessage = string.Empty;
            pending = false;
            timeRemainingMs = null;

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
                    status = "pending";
                    reason = "Pending external completion.";
                    return;
                }

                if (string.Equals(normalized, "timedBegin", StringComparison.OrdinalIgnoreCase))
                {
                    pending = true;
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
                    string.Equals(status, "timedBegin", StringComparison.OrdinalIgnoreCase) ||
                    obj.Value<bool?>("pending") == true;
                timeRemainingMs = obj.Value<long?>("timeRemainingMs")
                    ?? obj.Value<long?>("timeRemaining")
                    ?? obj.Value<long?>("durationMs");
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

            var normalized = (status ?? string.Empty).Trim();
            if (string.Equals(normalized, "pending", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(normalized, "timedBegin", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(normalized, "timedEnd", StringComparison.OrdinalIgnoreCase))
            {
                ShowEffectUi(player, "Crowd Control", playerMessage);
                return;
            }

            var normalizedStatus = NormalizeExternalCompletionStatus(normalized);
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

        private bool IsSteamPlayerOnline(string steamId)
        {
            var player = FindPlayerBySteamId(steamId);
            return player != null && player.IsConnected;
        }

        private bool HasAnyOnlinePlayerWithToken()
        {
            if (_data?.PlayerSessions == null || _data.PlayerSessions.Count == 0)
            {
                return false;
            }

            foreach (var kvp in _data.PlayerSessions)
            {
                var session = kvp.Value;
                if (!IsSessionTokenUsable(session))
                {
                    continue;
                }

                if (IsSteamPlayerOnline(kvp.Key))
                {
                    return true;
                }
            }

            return false;
        }

        private bool HasAnyActiveGameSession()
        {
            if (_data?.PlayerSessions == null || _data.PlayerSessions.Count == 0)
            {
                return false;
            }

            foreach (var kvp in _data.PlayerSessions)
            {
                if (HasActiveGameSession(kvp.Key, kvp.Value))
                {
                    return true;
                }
            }

            return false;
        }

        private bool HasActiveGameSession(string steamId, PlayerSessionState session)
        {
            return session != null &&
                IsSessionTokenUsable(session) &&
                !string.IsNullOrWhiteSpace(session.GameSessionId) &&
                IsSteamPlayerOnline(steamId);
        }

        private bool HasPendingAuthRequests()
        {
            lock (_pendingAuthRequests)
            {
                return _pendingAuthRequests.Count > 0;
            }
        }

        private bool ShouldMaintainSocketConnection()
        {
            PruneInvalidStoredSessions();
            return HasAnyOnlinePlayerWithToken() || HasPendingAuthRequests();
        }

        private void CloseSocketConnection()
        {
            _reconnectTimer?.Destroy();
            _reconnectTimer = null;
            _socketCts?.Cancel();
            _socket?.Abort();
            _socket?.Dispose();
            _socket = null;
            _socketCts = null;
        }

        private string FindSteamIdByCcUid(string ccUid)
        {
            if (string.IsNullOrEmpty(ccUid))
            {
                return null;
            }

            foreach (var kvp in _data.PlayerSessions)
            {
                if (IsSessionTokenUsable(kvp.Value) &&
                    string.Equals(kvp.Value?.CcUid, ccUid, StringComparison.OrdinalIgnoreCase))
                {
                    return kvp.Key;
                }
            }
            return null;
        }

        private bool TryGetSessionTokenExpiryUnix(PlayerSessionState session, out long expiryUnix)
        {
            expiryUnix = 0;
            if (session == null || string.IsNullOrWhiteSpace(session.Token))
            {
                return false;
            }

            if (session.TokenExpiryUnix > 0)
            {
                expiryUnix = session.TokenExpiryUnix;
                return true;
            }

            var decoded = DecodeJwt(session.Token);
            if (decoded == null || decoded.Exp <= 0)
            {
                return false;
            }

            session.TokenExpiryUnix = decoded.Exp;
            if (string.IsNullOrWhiteSpace(session.CcUid) && !string.IsNullOrWhiteSpace(decoded.CcUid))
            {
                session.CcUid = decoded.CcUid;
            }

            if (string.IsNullOrWhiteSpace(session.OriginId) && !string.IsNullOrWhiteSpace(decoded.OriginId))
            {
                session.OriginId = decoded.OriginId;
            }

            if (string.IsNullOrWhiteSpace(session.ProfileType) && !string.IsNullOrWhiteSpace(decoded.ProfileType))
            {
                session.ProfileType = decoded.ProfileType;
            }

            if (string.IsNullOrWhiteSpace(session.DisplayName) && !string.IsNullOrWhiteSpace(decoded.Name))
            {
                session.DisplayName = decoded.Name;
            }

            expiryUnix = session.TokenExpiryUnix;
            return true;
        }

        private bool IsSessionTokenUsable(PlayerSessionState session)
        {
            return TryGetSessionTokenExpiryUnix(session, out var expiryUnix) &&
                expiryUnix > DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        }

        private StoredSessionStatus GetStoredSessionStatus(string steamId, out PlayerSessionState session)
        {
            session = null;
            if (string.IsNullOrWhiteSpace(steamId) ||
                _data?.PlayerSessions == null ||
                !_data.PlayerSessions.TryGetValue(steamId, out session) ||
                session == null ||
                string.IsNullOrWhiteSpace(session.Token))
            {
                session = null;
                return StoredSessionStatus.Missing;
            }

            if (TryGetSessionTokenExpiryUnix(session, out var expiryUnix))
            {
                return expiryUnix > DateTimeOffset.UtcNow.ToUnixTimeSeconds()
                    ? StoredSessionStatus.Valid
                    : StoredSessionStatus.Expired;
            }

            return StoredSessionStatus.Invalid;
        }

        private bool TryGetAuthenticatedSession(string steamId, out PlayerSessionState session, bool pruneInvalid = true)
        {
            var status = GetStoredSessionStatus(steamId, out session);
            if (status == StoredSessionStatus.Valid)
            {
                return true;
            }

            if (pruneInvalid && (status == StoredSessionStatus.Expired || status == StoredSessionStatus.Invalid))
            {
                ClearPlayerAuthState(steamId, stopActiveSession: false);
            }

            session = null;
            return false;
        }

        private string GetTokenReauthMessage(CommandReplyMode replyMode, bool invalidToken)
        {
            if (invalidToken)
            {
                return replyMode == CommandReplyMode.Console
                    ? "Saved Crowd Control token is invalid. Run cc link to reauthenticate."
                    : "Saved Crowd Control token is invalid. Run /cc link to reauthenticate.";
            }

            return replyMode == CommandReplyMode.Console
                ? "Saved Crowd Control token expired and was removed. Run cc link to reauthenticate."
                : "Saved Crowd Control token expired and was removed. Run /cc link to reauthenticate.";
        }

        private StoredSessionStatus PruneStoredSessionIfInvalid(string steamId, CommandReplyMode replyMode, bool notifyPlayer)
        {
            var status = GetStoredSessionStatus(steamId, out _);
            if (status != StoredSessionStatus.Expired && status != StoredSessionStatus.Invalid)
            {
                return status;
            }

            ClearPlayerAuthState(steamId, stopActiveSession: false);
            if (notifyPlayer)
            {
                var player = FindPlayerBySteamId(steamId);
                if (player != null && player.IsConnected)
                {
                    SendCommandReply(player, replyMode, GetTokenReauthMessage(replyMode, status == StoredSessionStatus.Invalid));
                }
            }

            return status;
        }

        private void PruneInvalidStoredSessions()
        {
            if (_data?.PlayerSessions == null || _data.PlayerSessions.Count == 0)
            {
                return;
            }

            var toRemove = new List<string>();
            foreach (var kvp in _data.PlayerSessions)
            {
                var status = GetStoredSessionStatus(kvp.Key, out _);
                if (status == StoredSessionStatus.Expired || status == StoredSessionStatus.Invalid)
                {
                    toRemove.Add(kvp.Key);
                }
            }

            foreach (var steamId in toRemove)
            {
                ClearPlayerAuthState(steamId, stopActiveSession: false, refreshSocketState: false);
            }
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
                    Exp = obj.Value<long?>("exp") ?? 0,
                    ScopeSummary = GetScopeSummaryFromClaims(obj["scope"] ?? obj["scopes"])
                };
            }
            catch
            {
                return null;
            }
        }

        private string GetScopeSummaryFromClaims(JToken token)
        {
            if (token == null)
            {
                return null;
            }

            if (token.Type == JTokenType.Array)
            {
                var scopes = new List<string>();
                foreach (var item in token as JArray)
                {
                    var value = item?.ToString();
                    if (!string.IsNullOrEmpty(value))
                    {
                        scopes.Add(value);
                    }
                }
                return scopes.Count > 0 ? string.Join(",", scopes) : null;
            }

            var text = token.ToString();
            return string.IsNullOrWhiteSpace(text) ? null : text;
        }

        private string BuildSecretFingerprint(string secret)
        {
            if (string.IsNullOrEmpty(secret))
            {
                return "(missing)";
            }

            if (secret.Length <= 8)
            {
                return $"{secret[0]}***{secret[secret.Length - 1]}";
            }

            return $"{secret.Substring(0, 4)}...{secret.Substring(secret.Length - 4)}";
        }

        private float ReadTokenAsFloat(JToken token, float fallback)
        {
            if (token == null)
            {
                return fallback;
            }

            try
            {
                var direct = token.Value<float?>();
                if (direct.HasValue)
                {
                    return direct.Value;
                }
            }
            catch { }

            var text = token.ToString();
            if (float.TryParse(text, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var parsed))
            {
                return parsed;
            }

            return fallback;
        }

        private bool ReadTokenAsBool(JToken token, bool fallback)
        {
            if (token == null)
            {
                return fallback;
            }

            try
            {
                var direct = token.Value<bool?>();
                if (direct.HasValue)
                {
                    return direct.Value;
                }
            }
            catch { }

            var text = token.ToString();
            if (bool.TryParse(text, out var parsed))
            {
                return parsed;
            }

            return fallback;
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

            if (_config.EnforceCrowdControl == null)
            {
                _config.EnforceCrowdControl = new EnforceCrowdControlConfig();
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

            _config.EnforceCrowdControl.EnforceTimeSeconds = Math.Max(30, _config.EnforceCrowdControl.EnforceTimeSeconds);

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
                var defaultConfig = new PluginConfig();
                _config = Config.ReadObject<PluginConfig>();
                if (_config == null)
                {
                    throw new Exception("Config deserialized null.");
                }

                if (string.IsNullOrWhiteSpace(_config.AppId) || _config.AppId == "INSERT_APP_ID")
                {
                    _config.AppId = defaultConfig.AppId;
                }

                if (string.IsNullOrWhiteSpace(_config.AppSecret) || _config.AppSecret == "INSERT_APP_SECRET")
                {
                    _config.AppSecret = defaultConfig.AppSecret;
                }

                if (_config.SessionRules == null)
                {
                    _config.SessionRules = new SessionRulesConfig();
                }

                if (_config.EnforceCrowdControl == null)
                {
                    _config.EnforceCrowdControl = new EnforceCrowdControlConfig();
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
            PruneInvalidStoredSessions();
            if (_data?.PlayerSessions == null || _data.PlayerSessions.Count == 0)
            {
                return;
            }

            var refreshed = 0;
            var snapshot = new List<KeyValuePair<string, PlayerSessionState>>(_data.PlayerSessions);
            foreach (var kvp in snapshot)
            {
                var session = kvp.Value;
                if (!IsSessionTokenUsable(session))
                {
                    continue;
                }

                if (!IsSteamPlayerOnline(kvp.Key))
                {
                    if (!string.IsNullOrEmpty(session.GameSessionId))
                    {
                        await StopGameSessionAsync(session);
                    }
                    continue;
                }

                // Force a clean session cycle on plugin init so old lingering sessions
                // from previous plugin instances do not keep receiving effect requests.
                await StopGameSessionAsync(session);
                await StartGameSessionAsync(session, kvp.Key);
                refreshed++;
            }

            if (refreshed > 0)
            {
                NotifyCrowdControlProvidersSessionStateChanged();
                Puts($"Reload refresh completed for {refreshed} Crowd Control session(s).");
            }
        }

        private async Task ApplySessionRulesAsync()
        {
            PruneInvalidStoredSessions();
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
                if (!IsSessionTokenUsable(session))
                {
                    continue;
                }

                if (!IsSteamPlayerOnline(steamId))
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

            if (!rules.EnablePriceChange && IsPriceChangeRequest(payload, effect))
            {
                reason = "Price-change effects are currently disabled by server settings.";
                return true;
            }

            if (rules.DisableTestEffects && IsTestEffectRequest(payload, effect))
            {
                reason = "Test effects are currently disabled by server settings.";
                return true;
            }

            return false;
        }

        private bool IsIntegrationTriggeredRequest(JObject payload, JObject effect)
        {
            var joined = BuildSearchableJsonText(payload, effect);
            return ContainsAny(joined, "tiktok", "twitch", "pulsoid", "gift", "reward", "integration");
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

        private bool IsTestEffectRequest(JObject payload, JObject effect)
        {
            if ((payload?.Value<bool?>("isTest") ?? false) ||
                (effect?.Value<bool?>("isTest") ?? false))
            {
                return true;
            }

            var requesterCcUid = payload?["requester"]?["ccUID"]?.ToString() ??
                                 payload?["requester"]?["ccUid"]?.ToString() ??
                                 string.Empty;
            var targetCcUid = payload?["target"]?["ccUID"]?.ToString() ??
                              payload?["target"]?["ccUid"]?.ToString() ??
                              string.Empty;
            return !string.IsNullOrWhiteSpace(requesterCcUid) &&
                   !string.IsNullOrWhiteSpace(targetCcUid) &&
                   string.Equals(requesterCcUid, targetCcUid, StringComparison.OrdinalIgnoreCase);
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

        private void TryClosePlayerInventory(BasePlayer player)
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
                player.SendConsoleCommand("inventory.close");
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

        private void ShowSessionDisconnectToastToActivePlayers(string message)
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
                if (!IsSessionTokenUsable(session))
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

        private bool IsBuiltInEffectsPluginLoaded()
        {
            return plugins.Find(BuiltInEffectsPluginName) != null;
        }

        private void WarnIfBuiltInEffectsPluginMissing(string context, BasePlayer player = null, bool showWarning = true)
        {
            if (IsBuiltInEffectsPluginLoaded())
            {
                return;
            }

            var warning = $"CrowdControlEffects is not loaded during {context}; built-in Rust effects will be unavailable until that plugin loads.";
            if (showWarning)
            {
                PrintWarning(warning);
            }
            else
            {
                Puts(warning);
            }
            if (player != null && player.IsConnected)
            {
                ShowErrorUi(player, "CrowdControlEffects is not loaded. Built-in Rust effects are currently unavailable.");
            }
        }

        #endregion
    }
}
