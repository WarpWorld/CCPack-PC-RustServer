# Crowd Control Rust Server Guide

This document explains how the Rust Crowd Control server plugins are structured, how server admins should configure them, and how other plugin developers can attach their own effects to the base plugin.

## Plugin layout

This setup is split into three main pieces:

- `CrowdControl.cs`
  - Base plugin.
  - Handles auth, session start/stop, websocket/pubsub communication, effect routing, retries, rule enforcement, UI toasts, and plugin-to-plugin APIs.
- `CrowdControlEffects.cs`
  - Built-in Rust gameplay effect provider.
  - Registers built-in effects locally and handles actual gameplay logic like healing, giving items, teleporting, timed effects, etc.
- `CrowdControl-DefaultEffects.json`
  - Generated admin-editable metadata file for the built-in/local Rust provider.
  - Stores built-in prices, durations, cooldowns, inactive flags, and other effect metadata.
- `CrowdControl-CustomEffects-<ProviderName>.json`
  - Generated admin-editable metadata file for each external/custom provider.
  - New effects are added automatically without overwriting existing admin-edited values.
- `CrowdControlRustExamples.cs`
  - Example plugin for external/custom effects.
  - Good reference if you want to build your own provider plugin.

## High-level flow

1. A player links their Crowd Control account through `CrowdControl.cs`.
2. The base plugin starts a Crowd Control game session for that player.
3. Crowd Control sends effect requests to the base plugin.
4. The base plugin resolves the target player and applies server rules.
5. The base plugin dispatches the effect to a registered provider plugin.
6. The provider plugin returns a result immediately, asynchronously, or as a timed effect.
7. The base plugin sends the effect response back to Crowd Control.

## Built-in effects

Built-in Rust effects are handled by `CrowdControlEffects.cs`.

Important distinction:

- Built-in effects use `CC_RegisterLocalEffects(...)`
  - They are registered for local dispatch only.
  - They are not pushed as custom effects to Crowd Control.
- External/custom provider effects use `CC_RegisterEffects(...)`
  - They are registered for dispatch.
  - They can also be synced to the Crowd Control custom effects menu.

Built-in effect defaults come from `CrowdControlEffects.cs`, and the base plugin seeds/admin-manages them through `CrowdControl-DefaultEffects.json`.

Examples:

- built-in effects: price, duration, inactive, cooldowns, scale
- custom/provider effects: name, description, price, duration, inactive, cooldowns, scale

If you want to change built-in effect metadata as an admin, update `CrowdControl-DefaultEffects.json`.

If you want to change custom/provider effect metadata as an admin, update `CrowdControl-CustomEffects-<ProviderName>.json`.

If you want to change what the effect actually does, update `CrowdControlEffects.cs`.

## Generated effect files

The base plugin generates and updates provider metadata files automatically when effects are registered.

Current file model:

- built-in local Rust effects
  - `CrowdControl-DefaultEffects.json`
- external/custom provider effects
  - `CrowdControl-CustomEffects-<ProviderName>.json`

Behavior:

- if the file does not exist, it is created from the provider's registered defaults
- if the file already exists, existing admin-edited values are preserved
- if a provider adds new effects in a plugin update, only the new effects are added
- existing entries are not overwritten by plugin updates

## Commands

Player chat commands:

- `/cc link`
- `/cc auth`
- `/cc unlink`
- `/cc logout`
- `/cc status`
- `/cc settings`
- `/cc restart`

Admin command:

- `/cc reload`

Aliases:

- chat alias `/crowdcontrol ...`
- console command `crowdcontrol ...`
- console alias `cc ...`

Use `/` only in chat. In F1 console, run `cc ...` or `crowdcontrol ...` without a slash.

## Permissions

Current permissions:

- `crowdcontrol.use`
  - Allows the player to use normal Crowd Control commands and features.
- `crowdcontrol.admin`
  - Required for `/cc reload`.
- `crowdcontrol.ignore`
  - Bypasses auth reminders and Crowd Control enforcement.
  - Useful for staff, spectators, bots, or users you do not want forced to link.

Recommended usage:

- grant `crowdcontrol.use` to players who should be allowed to use Crowd Control
- grant `crowdcontrol.admin` only to trusted admins
- grant `crowdcontrol.ignore` to users who should not be prompted or forced to authenticate

## Config overview

The config file is `oxide/config/CrowdControl.json`.

Main fields:

```json
{
  "app_id": "your-app-id",
  "app_secret": "your-app-secret",
  "allow_all_users_without_permission": true,
  "session_rules": {
    "enable_integration_triggers": true,
    "enable_price_change": true,
    "disable_test_effects": false,
    "disable_custom_effects_sync": false
  },
  "retry_policy": {
    "enabled": true,
    "default": {
      "max_attempts": 25,
      "max_duration_seconds": 60,
      "retry_interval_seconds": 2.4
    },
    "twitch": {
      "max_attempts": 25,
      "max_duration_seconds": 60,
      "retry_interval_seconds": 2.4
    },
    "tiktok": {
      "max_attempts": 250,
      "max_duration_seconds": 300,
      "retry_interval_seconds": 1.2
    }
  },
  "enforce_crowd_control": {
    "enabled": false,
    "enforce_time_seconds": 120,
    "restrict_movement": false
  }
}
```

### Session rules

- `enable_integration_triggers`
  - Allows or blocks integration-triggered effects.
- `enable_price_change`
  - Allows or blocks price-change style effects.
- `disable_test_effects`
  - Rejects test effects locally and shows a toast to the player when one is received but blocked.
- `disable_custom_effects_sync`
  - Prevents custom effects from being synced to Crowd Control.

### Retry policy

The retry policy is used for cases where an effect cannot be applied immediately, for example if the player is offline or their session is temporarily unavailable.

### Enforce Crowd Control

`enforce_crowd_control` controls whether players are required to link Crowd Control.

- `enabled`
  - Turns enforcement on or off.
- `enforce_time_seconds`
  - Grace period before enforcement begins.
  - The plugin clamps this to at least 30 seconds.
- `restrict_movement`
  - If enabled, the player is kept in place after the grace timer expires until they link.

When enforcement is enabled:

- eligible unauthenticated players get a join reminder
- they get a toast reminder after 8 seconds
- after the grace timer expires, they are restricted until they link
- players with `crowdcontrol.ignore` are exempt

## Join reminders and enforcement behavior

Only players who are allowed to use Crowd Control are shown onboarding reminders.

The plugin currently does all of the following for eligible unauthenticated players:

- chat message on join
- toast on join
- F1 console instructions
- 8 second reminder toast

Players who cannot use Crowd Control do not get these reminders.

## Admin reload

`/cc reload` is intended for admins with `crowdcontrol.admin`.

It reloads:

- config
- stored data
- generated provider effect metadata
- session rules
- current Crowd Control sessions
- socket/session state notifications for providers

This is useful after editing config or changing effect metadata without fully restarting the server.

## How effect routing works

When the base plugin receives an effect request:

1. It validates the request type and target player.
2. It checks session rules.
3. It finds a registered provider for the effect ID.
4. It calls that provider's `OnCrowdControlEffect` hook.
5. It interprets the returned status and reports back to Crowd Control.

## Provider plugin API

The base plugin exposes these hook-callable APIs:

- `CC_RegisterEffects(string providerName, object effectsPayload)`
- `CC_RegisterLocalEffects(string providerName, object effectsPayload)`
- `CC_UnregisterEffects(string providerName)`
- `CC_CompleteEffect(string requestId, string status = "success", string reason = "", string playerMessage = "")`
- `CC_GetActiveCcPlayerSteamIds(string excludeSteamId = "")`
- `CC_ReportEffectAvailability(object effectIdsPayload, string status)`
- `CC_SendEffectResponse(string requestId, string status, string reason = "", string playerMessage = "", object timeRemainingMs = null)`

### Which register method should I use?

Use `CC_RegisterEffects(...)` if:

- your plugin provides real external/custom effects
- you want those effects to sync to Crowd Control as custom effects
- you want the base plugin to generate a per-provider custom effect metadata file

Use `CC_RegisterLocalEffects(...)` if:

- your plugin only wants local dispatch
- the effects already exist in the server/game pack
- you do not want them synced as custom menu effects

## Effect registration payload

Providers register effects with a `JArray` of objects like this:

```json
[
  {
    "effectID": "example_instant_success",
    "name": "EXAMPLE: Instant Success",
    "description": "Runs immediately and returns success.",
    "price": 25
  }
]
```

Optional fields:

- `duration`
- `sessionCooldown`
- `userCooldown`
- `inactive`
- `scale`

## Provider hook

Provider plugins handle effects through:

```csharp
[HookMethod("OnCrowdControlEffect")]
private object OnCrowdControlEffect(JObject context)
```

The context includes values such as:

- `requestID`
- `effectID`
- `provider`
- `playerSteamID`
- `playerName`
- `effect`
- `payload`

## Valid provider responses

A provider can return a `JObject` containing a `status`.

Common statuses:

- `success`
  - Effect completed successfully.
- `failTemporary`
  - Effect failed for a retryable reason.
- `failPermanent`
  - Effect failed permanently.
- `pending`
  - Provider accepted the effect and will complete it later.
- `timedBegin`
  - Timed effect started.
- `timedEnd`
  - Timed effect ended.

Optional response fields:

- `reason`
- `playerMessage`
- `timeRemainingMs`

## Asynchronous effects

If your effect needs time to finish:

1. Return `pending`.
2. Later call:

```csharp
CrowdControl.Call("CC_CompleteEffect", requestId, "success", "done", "Effect completed.");
```

Or use the more general response API:

```csharp
CrowdControl.Call("CC_SendEffectResponse", requestId, "success", "done", "Effect completed.", null);
```

## Timed effects

Timed effects should:

1. Start the effect
2. Return `timedBegin`
3. End the effect later
4. Call `CC_SendEffectResponse(..., "timedEnd", ...)`

The built-in provider in `CrowdControlEffects.cs` is the main reference for how this is done.

## Reporting dynamic availability

Provider plugins can hide or disable effects dynamically by reporting availability:

```csharp
CrowdControl.Call(
    "CC_ReportEffectAvailability",
    new JArray { "player_teleport_swap_cc_player", "player_teleport_to_cc_player" },
    "menuAvailable"
);
```

Typical statuses:

- `menuAvailable`
- `menuUnavailable`

This is useful when an effect depends on conditions like:

- at least two active Crowd Control players
- an online target
- world or event state

## Getting active Crowd Control players

Providers can query active linked players with:

```csharp
var ids = CrowdControl.Call("CC_GetActiveCcPlayerSteamIds", excludeSteamId) as JArray;
```

This is useful for effects like:

- teleport to another Crowd Control player
- swap positions with another Crowd Control player
- multi-player targeting logic

## Example provider plugin

`CrowdControlRustExamples.cs` shows three basic patterns:

- instant success
- async `pending` then later `CC_CompleteEffect(...)`
- temporary failure

That file is the best minimal example for building your own provider.

## Load order

The plugins are designed to tolerate load order issues.

- provider plugins retry registration when needed
- providers can register in `OnServerInitialized()`
- providers can retry in `OnPluginLoaded()` when `CrowdControl` loads later
- the base plugin unregisters provider effects when a provider unloads

In short, `CrowdControl.cs` should ideally load first, but the current setup is resilient enough to recover when that does not happen.

## Recommendations for plugin developers

- keep gameplay logic in your provider plugin, not in `CrowdControl.cs`
- do not use  `CC_RegisterLocalEffects(...)` this is for the Crowd Control team and our default effects
- use `CC_RegisterEffects(...)` for true custom effects
- let the base plugin own generated effect metadata files
- prefer returning structured `JObject` responses
- include `playerMessage` only when the player should actually see a message
- use `pending` or timed responses instead of blocking long operations
- unregister your effects in `Unload()`

## Recommendations for server admins

- keep `CrowdControl.cs` loaded at all times
- keep `CrowdControlEffects.cs` loaded if you want the built-in Rust effects
- use `/cc settings` to verify current rules
- use `/cc reload` after config changes
- grant `crowdcontrol.ignore` to players who should never be forced to link
- check `CrowdControl-DefaultEffects.json` for built-in prices, durations, cooldowns, inactive flags, and scale settings
- check `CrowdControl-CustomEffects-<ProviderName>.json` for custom/provider effect metadata

## Quick troubleshooting

### "No registered provider handled this effect"

Usually means:

- the provider plugin is not loaded
- the effect ID is not registered
- the provider registration failed

Check:

- `CrowdControl.cs` loaded
- `CrowdControlEffects.cs` loaded
- provider log output after reload

### Built-in effect is the wrong price or duration

Check `CrowdControl-DefaultEffects.json`.

### Gameplay behavior is wrong but the effect exists

Check `CrowdControlEffects.cs`.

### Custom provider effect exists but is not running

Check:

- your plugin called `CC_RegisterEffects(...)` or `CC_RegisterLocalEffects(...)`
- your plugin has `[HookMethod("OnCrowdControlEffect")]`
- your provider returns a valid status object

## File summary

- `CrowdControl.cs`
  - base plugin, auth/session/routing/admin rules
- `CrowdControlEffects.cs`
  - built-in Rust effect behavior
- `CrowdControlRustExamples.cs`
  - example provider plugin
- `CrowdControl-DefaultEffects.json`
  - built-in/local provider metadata
- `CrowdControl-CustomEffects-<ProviderName>.json`
  - external/custom provider metadata