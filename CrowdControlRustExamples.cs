using System;
using Newtonsoft.Json.Linq;
using Oxide.Core.Plugins;

namespace Oxide.Plugins
{
    [Info("CrowdControlRustExamples", "jaku", "0.1.0")]
    [Description("Example external Crowd Control provider with 3 effect patterns.")]
    public class CrowdControlRustExamples : RustPlugin
    {
        [PluginReference]
        private Plugin CrowdControl;

        private const string ProviderName = "CrowdControlRustExamples";

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
                }
            };

            var registerResult = CrowdControl.Call("CC_RegisterEffects", ProviderName, effects);
            var ok = registerResult is bool registered && registered;
            Puts($"Example effect registration result: {(ok ? "success" : "failed")}.");
        }

        // Hook called by CrowdControlRust for registered external effects.
        [HookMethod("OnCrowdControlEffect")]
        private object OnCrowdControlEffect(JObject context)
        {
            var effectId = (context?.Value<string>("effectID") ?? string.Empty).Trim().ToLowerInvariant();
            var requestId = context?.Value<string>("requestID") ?? string.Empty;
            var playerName = context?.Value<string>("playerName") ?? "player";

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
            }

            return new JObject
            {
                ["status"] = "failPermanent",
                ["reason"] = $"Unknown external example effect '{effectId}'."
            };
        }
    }
}
