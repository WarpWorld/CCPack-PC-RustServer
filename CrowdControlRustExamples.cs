using System;
using Newtonsoft.Json.Linq;
using Oxide.Core.Plugins;

namespace Oxide.Plugins
{
    /// <summary>
    /// Sample external provider for <c>CC_RegisterEffects</c> demonstrating common <c>OnCrowdControlEffect</c> result shapes.
    /// </summary>
    /// <remarks>
    /// <para><b>Illustrated in this plugin:</b> instant <c>success</c>, <c>pending</c> + <c>CC_CompleteEffect</c> success,
    /// <c>failTemporary</c>, <c>timedBegin</c> + <c>CC_CompleteEffect</c> <c>timedEnd</c>, <c>failPermanent</c> (unknown id),
    /// registration flag <c>worldEffect</c> (skips <c>broadcast_effects_to_all_players</c> fan-out), and fan-out context
    /// <c>crowdControlFanout</c> / <c>originalRequestID</c> on the hook <c>context</c>.</para>
    /// <para><b>Other public Crowd Control APIs</b> (call from your own plugin as needed): <c>CC_ParseEffectDurationSeconds</c>,
    /// <c>CC_GetActiveCcPlayerSteamIds</c>, <c>CC_ReportEffectAvailability</c>, <c>CC_SendEffectResponse</c> (full signature with
    /// <c>timeRemainingMs</c> when not using <c>CC_CompleteEffect</c>).</para>
    /// </remarks>
    [Info("CrowdControlRustExamples", "Warp World", "1.0.2")]
    [Description("Example external Crowd Control provider: instant, async, timed, fail, and world-effect patterns.")]
    public class CrowdControlRustExamples : RustPlugin
    {
        #region Fields

        [PluginReference]
        private Plugin CrowdControl;

        private const string ProviderName = "CrowdControlRustExamples";

        #endregion

        #region Oxide lifecycle

        private void OnServerInitialized()
        {
            RegisterExampleEffects();
        }

        private void Unload()
        {
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
                RegisterExampleEffects();
            }
        }

        #endregion

        #region Registration and hook

        private void RegisterExampleEffects()
        {
            if (CrowdControl == null)
            {
                PrintWarning("CrowdControl plugin not loaded; cannot register example effects yet.");
                return;
            }

            var effects = new JArray
            {
                new JObject
                {
                    ["effectID"] = "example_instant_success",
                    ["name"] = "EXAMPLE: Instant Success",
                    ["description"] = "Runs immediately and returns success.",
                    ["price"] = 25
                },
                new JObject
                {
                    ["effectID"] = "example_pending_async",
                    ["name"] = "EXAMPLE: Pending Async",
                    ["description"] = "Returns pending, then completes after a delay.",
                    ["price"] = 40
                },
                new JObject
                {
                    ["effectID"] = "example_temporary_fail",
                    ["name"] = "EXAMPLE: Temporary Fail",
                    ["description"] = "Returns failTemporary to demonstrate retry behavior.",
                    ["price"] = 20
                },
                new JObject
                {
                    ["effectID"] = "example_timed_begin",
                    ["name"] = "EXAMPLE: Timed Begin/End",
                    ["description"] = "Returns timedBegin, then completes with timedEnd after a few seconds.",
                    ["price"] = 35,
                    ["duration"] = new JObject { ["value"] = "0:0:4.0" }
                },
                new JObject
                {
                    ["effectID"] = "example_world_effect",
                    ["name"] = "EXAMPLE: World Effect",
                    ["description"] = "Marked worldEffect so broadcast-to-all-players does not fan this out per player.",
                    ["price"] = 30,
                    ["worldEffect"] = true
                }
            };

            var registerResult = CrowdControl.Call("CC_RegisterEffects", ProviderName, effects);
            var ok = registerResult is bool registered && registered;
            Puts($"Example effect registration result: {(ok ? "success" : "failed")}.");
        }

        // Hook called by CrowdControl for registered external effects.
        [HookMethod("OnCrowdControlEffect")]
        private object OnCrowdControlEffect(JObject context)
        {
            var effectId = (context?.Value<string>("effectID") ?? string.Empty).Trim().ToLowerInvariant();
            var requestId = context?.Value<string>("requestID") ?? string.Empty;
            var playerName = context?.Value<string>("playerName") ?? "player";

            if (context?.Value<bool?>("crowdControlFanout") == true)
            {
                var originalReq = context.Value<string>("originalRequestID") ?? string.Empty;
                Puts($"[Examples] Fan-out invocation effectID={effectId} player={playerName} originalRequestID={originalReq}");
            }

            switch (effectId)
            {
                case "example_instant_success":
                    Puts($"[Examples] Instant success effect handled for {playerName}.");
                    return new JObject
                    {
                        ["status"] = "success",
                        ["playerMessage"] = "Example instant effect succeeded."
                    };

                case "example_pending_async":
                    if (string.IsNullOrWhiteSpace(requestId))
                    {
                        return new JObject
                        {
                            ["status"] = "failTemporary",
                            ["reason"] = "Missing requestID for async completion."
                        };
                    }

                    Puts($"[Examples] Pending async effect started for {playerName}, requestID={requestId}.");
                    timer.Once(3f, () =>
                    {
                        if (CrowdControl == null)
                        {
                            return;
                        }

                        CrowdControl.Call(
                            "CC_CompleteEffect",
                            requestId,
                            "success",
                            "Async work complete.",
                            "Example async effect completed."
                        );
                    });

                    return new JObject
                    {
                        ["status"] = "pending",
                        ["playerMessage"] = "Example async effect started..."
                    };

                case "example_temporary_fail":
                    Puts($"[Examples] Temporary fail effect triggered for {playerName}.");
                    return new JObject
                    {
                        ["status"] = "failTemporary",
                        ["reason"] = "Example temporary failure from provider plugin.",
                        ["playerMessage"] = "Example temporary failure. Try again shortly."
                    };

                case "example_timed_begin":
                    if (string.IsNullOrWhiteSpace(requestId))
                    {
                        return new JObject
                        {
                            ["status"] = "failTemporary",
                            ["reason"] = "Missing requestID for timed completion."
                        };
                    }

                    var effectPayload = context?["effect"] as JObject;
                    var durationSeconds = 4;
                    var parsedObj = CrowdControl?.Call("CC_ParseEffectDurationSeconds", effectPayload);
                    if (parsedObj is int parsed && parsed > 0)
                    {
                        durationSeconds = parsed;
                    }

                    var timeRemainingMs = durationSeconds * 1000L;
                    Puts($"[Examples] Timed effect started for {playerName}, requestID={requestId}, ~{durationSeconds}s.");
                    timer.Once(durationSeconds, () =>
                    {
                        if (CrowdControl == null)
                        {
                            return;
                        }

                        CrowdControl.Call("CC_CompleteEffect", requestId, "timedEnd", string.Empty, string.Empty);
                    });

                    return new JObject
                    {
                        ["status"] = "timedBegin",
                        ["timeRemainingMs"] = timeRemainingMs,
                        ["playerMessage"] = "Example timed effect running..."
                    };

                case "example_world_effect":
                    Puts($"[Examples] World-scoped example for {playerName} (no broadcast fan-out).");
                    return new JObject
                    {
                        ["status"] = "success",
                        ["playerMessage"] = "Example world effect succeeded once for the server."
                    };
            }

            return new JObject
            {
                ["status"] = "failPermanent",
                ["reason"] = $"Unknown external example effect '{effectId}'."
            };
        }

        #endregion
    }
}
