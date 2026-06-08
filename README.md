# LightningEver Eclair Plugins

This repository contains two essential Eclair plugins developed to operate the **LightningEver LSP** (Liquidity Service Provider) node. Both plugins implement the official ACINQ Eclair plugin SPI (`fr.acinq.eclair.Plugin`) and are loaded dynamically during Eclair startup via the classpath or JVM arguments, e.g., `java -cp "lib/*" fr.acinq.eclair.Boot <plugin1.jar> <plugin2.jar>`.

## System Architecture

**LightningEver** is a custom, closed-loop Lightning Network stack built on top of the **BitEver (BEC)** L1 blockchain. BitEver is a customized Bitcoin fork featuring Taproot, MuSig2, P2TR, and a custom `chainHash` (blockchain identifier). 

Within this architecture, the Eclair LSP functions as the central routing hub and primary liquidity provider for all client nodes (e.g., Phoenix Android mobile wallets, also known as the LightningEver App). Since mobile wallets do not open direct channels between themselves, all transactions are routed via the LSP node.

### Included Plugins

| Plugin | Role & Description |
|---|---|
| **channel-funding** | A **funding interceptor** plugin that intercepts incoming dual-funded channel open requests from mobile clients and responds with the LSP-side liquidity contribution (default: `50,000,000 sat`). |
| **fcm-push** | A **wake-up notification** plugin utilizing Firebase Cloud Messaging (FCM) to wake up offline mobile clients. This makes **offline BOLT12 offer payments** and **automatic swap-in deposit alerts** possible. |

---

## Directory Structure

```
.
├── channel-funding/       # Intercepts channel opens and promises LSP-side liquidity
│   ├── pom.xml
│   └── src/main/...
├── fcm-push/              # Custom wake-up push plugin for offline clients
│   ├── pom.xml
│   ├── src/main/resources/reference.conf
│   └── src/main/scala/fr/acinq/eclair/plugins/fcmpush/
│       ├── FcmPushPlugin.scala
│       ├── FcmPushConfig.scala
│       ├── FcmTokenRegistry.scala
│       ├── FcmOAuth2.scala
│       ├── FcmSender.scala
│       └── FcmPushActor.scala
└── pom.xml                # Parent Reactor POM (modules: channel-funding, fcm-push)
```

> [!NOTE]
> The directories `historical-gossip`, `offline-commands`, and `custom-offer` are legacy plugins referencing an older 0.13.0-SNAPSHOT POM. They are currently excluded from the parent POM's `<modules>` section and are not compiled in the active reactor build.

---

## Building the Plugins

### Prerequisites

- **Java Development Kit (JDK) 21**: Recommended OpenJDK 21 distribution.
- **Local Eclair Dependency**: Because these plugins are built against the custom LightningEver Eclair fork APIs, the compiled core jar must reside in your local Maven cache (`~/.m2/repository`). 

First, clone and install the customized **LightningEver Eclair fork** (branch `260517_FIN`):
```bash
# Inside the LightningEver-bitever-eclair repository:
./mvnw clean install -DskipTests
```

### Compile and Package

Once the Eclair dependency is installed locally, build this plugin repository by running:
```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Package both plugins (Parent Reactor)
./mvnw clean package -Dmaven.test.skip=true
```

You can also package a specific plugin individually:
```bash
./mvnw package -pl fcm-push        -am -Dmaven.test.skip=true
./mvnw package -pl channel-funding -am -Dmaven.test.skip=true
```

**Build Artifacts**:
- `channel-funding/target/channel-funding-plugin-0.13.1.jar`
- `fcm-push/target/fcm-push-plugin-0.13.1.jar`

---

## Deployment

Copy the packaged plugin jars to Eclair's plugins directory:
```bash
cp channel-funding/target/channel-funding-plugin-0.13.1.jar /root/.eclair/plugins/
cp fcm-push/target/fcm-push-plugin-0.13.1.jar               /root/.eclair/plugins/
```

Launch the Eclair node, specifying both jar paths:
```bash
cd /root/bitever-eclair-dist/eclair-node-0.13.1-93cc2ab
nohup java -cp "lib/*" fr.acinq.eclair.Boot \
  /root/.eclair/plugins/channel-funding-plugin-0.13.1.jar \
  /root/.eclair/plugins/fcm-push-plugin-0.13.1.jar \
  > /root/.eclair/eclair-stdout.log 2>&1 &
```

Confirm that Eclair successfully loads both plugins during boot:
```
fr.acinq.eclair.Boot - loaded plugin ChannelFundingPlugin
fr.acinq.eclair.Boot - loaded plugin FcmPushPlugin
```

---

## FCM Push Plugin Configuration

By default, the plugin uses configuration defaults stored in its jar (`reference.conf`). You can override these variables by creating or editing `~/.eclair/fcm_push.conf`:

```hocon
fcm-push {
  enabled              = true                                              # Enable token tracking and push requests
  service-account-file = ${user.home}"/.eclair/fcm-service-account.json"   # Path to Firebase Credentials (600 recommended)
  project-id           = "lightningever"                                   # Firebase project_id
  fcm-endpoint         = "https://fcm.googleapis.com"
  access-token-refresh-margin-seconds = 300
  http-timeout-seconds = 10
  android-priority     = "high"
  android-ttl-seconds  = 7200
}
```

> [!WARNING]
> The `service-account-file` must be the private key JSON file obtained via Firebase Console -> Project Settings -> Service Accounts -> "Generate New Private Key". Secure this file with strict read permissions (e.g., `chmod 600`). **NEVER commit this JSON file or push it to any public repository.**

---

## Technical Mechanism (fcm-push)

The wake-up push mechanism operates in the following sequential flow:

```
Phone (Offline B)                 Eclair LSP (Plugin)                   Phone (Active A)
      │                                   │                                   │
      │── 1. FCMToken (Tag 35017) ───────▶│                                   │
      │   (Initial connection)            │                                   │
      │                                   │◀── 2. Request BOLT12 Offer ───────│
      │                                   │    (Mobile B is currently offline)│
      │                                   │                                   │
      │                                   │── 3. PeerReadyNotifier triggered  │
      │                                   │    (Publish WakeUpPeerRequested)  │
      │                                   │                                   │
      │◀── 4. Send FCM Push Notification ─│                                   │
      │    (OAuth2 HTTP v1 POST)          │                                   │
      │                                   │                                   │
      │── 5. Wake up & Reconnect ────────▶│                                   │
      │                                   │── 6. Deliver Onion payload ──────▶│
      │                                   │                                   │
      │◀── 7. Issue & send Invoice ───────│                                   │
      │                                   │── 8. Forward Invoice ────────────▶│
      │                                   │                                   │
      │                                   │◀── 9. HTLC Payment Fulfill ───────│
      │◀── 10. HTLC Payment Fulfill ──────│                                   │
```

1. **Token Exchange**: When a Phoenix mobile client connects to the LSP, it sends its registration token using a custom Lightning protocol message tag **`35017`**.
2. **Event Publication**: Eclair LSP's `Peer.scala` parses this custom message and publishes `FcmTokenRegistered(nodeId, token, platform)` to the Akka `EventStream`.
3. **Registry Storage**: The plugin's `FcmPushActor` subscribes to this event and registers the token inside an in-memory `peer_nodeId -> token` lookup map.
4. **Offline Relay Detection**: When an active client (Phone A) attempts a payment or onion route relay to Phone B, LSP's `MessageRelay` triggers a `PeerReadyNotifier` if B is offline.
5. **Wake-up Trigger**: Inside `PeerReadyNotifier.waitForPeerConnected`, the LSP publishes a `WakeUpPeerRequested(nodeId, reason)` event to the `EventStream`.
6. **Push Outbound**: The plugin receives this event, fetches Phone B's FCM token, requests an OAuth2 token using the service account, and POSTs a high-priority push notification payload to the Google Firebase HTTP v1 endpoint.
7. **Reconnection & Settle**: B's background system service wakes up upon receiving the push, establishes a TCP socket connection back to Eclair LSP, receives the pending invoice request, generates the invoice response, and settles the HTLC payment.

---

## Safety & Operational Notes

- **Outbound-Only Traffic**: The FCM push plugin only makes outbound HTTPS requests to Google's FCM v1 endpoint. No extra ports are bound or exposed.
- **In-Memory token storage**: Tokens are currently cached in memory. If Eclair restarts, push notifications cannot be triggered for clients until they reconnect once and automatically re-register their tokens. (Persistent SQLite storage is a target for future updates).

---

## Branch Status & Features

### 260521OFFBOLT12 — Offline BOLT12 Settle Verified
Successfully tested and verified the full end-to-end offline payment scenario using physical devices. Mobile wallets can receive incoming Bolt12 payments even while the application is closed or the device screen is locked.

### 260522_OFFSWAPIN — Auto-SwapIn Deposit Alerts
Adds support for monitoring BitEver L1 on-chain transactions and alerting offline users of incoming swap-in deposits.

- **SwapInAddressRegistry**: An index maps peer node IDs to registered L1 swap-in addresses (`publicKeyScript`).
- **L1 Transaction Matcher**: The plugin subscribes to Eclair's `NewTransaction` stream. If a transaction matches a registered swap-in script, the plugin triggers a wake-up push notification (reason `SwapInDeposit`) containing the TXID and deposit amount.
- **Deduplication**: Push notifications are rate-limited with a 30-minute deduplication cache per TXID.

#### Current Operational Warning
Both `SwapInAddressesRegistered` and `NewTransaction` subscriptions are **currently disabled (commented out)** in the Actor's `preStart()` hook:
```scala
// context.system.eventStream.subscribe(self, classOf[SwapInAddressesRegistered])
context.system.eventStream.subscribe(self, classOf[PaymentReceived])
context.system.eventStream.subscribe(self, classOf[WakeUpPeerRequested])
// context.system.eventStream.subscribe(self, classOf[NewTransaction])
```
*Reason: Simultaneous execution of BOLT12 payments and Swap-in notifications could occasionally trigger a channel reserve violation force-close. The code remains fully written and functional; to reactivate the automated alerts, simply uncomment those two event subscriptions.*

---

### Modifications (2026-06-08)
- **Scaled Inbound Liquidity Provisioning**: The auto-generated channel capacity has been modified from a hardcoded ~50M sat to a dynamically scaled capacity based on the peer's actual deposit amount (scaled at 4x the peer's funding amount, with a minimum of 1M satoshis).

## License

Licensed under the Apache License, Version 2.0.
