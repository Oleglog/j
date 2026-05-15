<div align="center">

<img src="asset/logo.png" width="150">

![License](https://img.shields.io/badge/license-ZARAZAEX%20ANY%20DO-0D1117?style=flat-square&logo=open-source-initiative&logoColor=green&labelColor=0D1117)
![Golang](https://img.shields.io/badge/-Golang-0D1117?style=flat-square&logo=go&logoColor=00A7D0)

</div>

## j

Низкоуровневая Go-библиотека для программного подключения к **Jitsi Meet** звонкам.
Делает XMPP-сигнализацию, заходит в MUC, ловит Jingle session-initiate, парсит
SDP/ICE/SSRC, открывает bridge channel (colibri-ws) и интегрируется с
[pion/webrtc](https://github.com/pion/webrtc) для медиа.

Создана для проекта [olcRTC](https://github.com/openlibrecommunity/olcrtc).

Адреса/комнаты не захардкожены — всё передаётся пользователем.

## Что умеет

- Anonymous SASL → XMPP MUC join → focus → Jingle session-initiate
- Jingle XML ↔ SDP полный конвертер (BUNDLE, RTP payload-types, RTCP-FB, SSRC, fingerprint, ICE candidates, rtcp-mux фильтрация)
- Привязка `*webrtc.PeerConnection` (pion) к Jingle сессии: автоматический session-accept, **trickle ICE**, **reconnect** при `session-terminate moving`
- **Отправка sendonly видео-трека** (попадает в session-accept SDP, Jicofo раздаёт другим участникам)
- **Приём видео**: `RequestVideo(ctx, maxHeight)` — отправляет `ReceiverVideoConstraints` через bridge channel (без этого JVB не форвардит видео!)
- **Plan B detection**: `Negotiator.Accept` определяет Plan B offer, `peer.IsPlanBError(err)` для обработки
- `source-add` / `source-remove` / `transport-info` / `session-terminate` хелперы
- colibri-ws (modern Jitsi data channel) — broadcast/unicast `EndpointMessage`, **сырые байты** через base64
- Чат groupchat, raise/lower hand, leave
- Низкоуровневый XMPP API (`Send(rawXML)`, `SendJingle`, `NextID`, `LastJingleStanza`)
- CLI с 6 режимами для тестирования и benchmark

## Throughput colibri-ws (через JVB)

На `meet.cryptopro.ru` (одиночный bridge):

| payload | rx steady | tx side | заметки |
|---|---|---|---|
| 8 KB | **~135 Mbit/s** | ~220 Mbit/s | стабильно, без потерь |
| 16 KB | 80–160 Mbit/s | ~195 Mbit/s | плавает |
| 64 KB | — | — | bridge закрывает соединение (max-message-size) |

Достижимо до ~1 Gbit/s при близкой геолокации к JVB и нескольких параллельных endpoints.

## Структура

```
j/
├── j.go                     # public API: Join, JoinMUC, Session, Negotiator()
├── internal/
│   ├── xmpp/                # WS + ANONYMOUS SASL + bind + MUC + focus + Stream Mgmt + raw Jingle/IQ helpers
│   ├── jingle/              # Jingle XML ↔ SDP (BUNDLE, RTP, RTCP-FB, SSRC, fingerprint, candidates)
│   ├── colibri/             # bridge channel WebSocket — JVB protocol (EndpointMessage, LastN, …)
│   └── peer/                # pion PeerConnection ↔ Jingle bridge (Accept, trickle ICE, source-add)
├── cmd/cli/                 # CLI: jingle | chat | dc | dc-raw | media (+send-video) | bench
└── readme.md
```

## Протокол

```
WebSocket wss://host/xmpp-websocket?room=ROOM   (subprotocol: xmpp)
   │
   ├─ ANONYMOUS SASL → bind → session → Stream Management (XEP-0198)
   ├─ extdisco:2 → TURN/STUN credentials
   ├─ focus.host conference allocation
   ├─ MUC join (presence + codecList + SourceInfo + nick + caps)
   ├─ ← Jingle session-initiate (SDP-as-XML, ICE candidates, colibri-ws URL)
   ├─ → Jingle session-accept (с pion-сгенерированным SDP→Jingle)
   ├─ → Jingle transport-info (trickle late candidates)
   ├─ → Jingle source-add / source-remove (для late tracks)
   ├─ ← Jingle session-terminate (reason="moving" → reconnect)
   │
   └─ ─── colibri-ws (bridge channel WebSocket) ──→ JVB
                ├─ ClientHello / ServerHello
                ├─ EndpointMessage (broadcast или unicast — произвольный JSON payload)
                ├─ EndpointStats / DominantSpeaker / LastN / VideoType / …
                └─ raw bytes via base64 в EndpointMessage
```

## Использование

### Чат / MUC

`j.JoinMUC` — только XMPP, без Jingle (не ждёт session-initiate).

```go
sess, _ := j.JoinMUC(ctx, j.Config{Host: "meet.example.com", Room: "myroom", Nick: "thejproject"})
defer sess.Close()

sess.Chat("hello")
sess.RaiseHand()
sess.LowerHand()

for m := range sess.Messages() {
    fmt.Printf("<%s> %s\n", m.From, m.Body)
}
```

### Bridge channel (data-канал JVB)

```go
sess, _ := j.Join(ctx, j.Config{...})           // ждёт Jingle (нужен ≥1 другой участник)
defer sess.Close()

sess.OpenBridge(ctx)

// сырые байты — broadcast всем
sess.BridgeSendRaw("", []byte{0xDE, 0xAD, 0xBE, 0xEF})
// unicast конкретному endpoint
sess.BridgeSendRaw("2968719f", payload)

// JSON EndpointMessage с произвольными полями (бридж не парсит, релеит как есть)
sess.BridgeSendMessage("", map[string]any{"type": "chat", "text": "hi"})

for m := range sess.BridgeMessages() {
    if raw := colibri.DecodeRaw(m); raw != nil {
        // получили сырые байты от собеседника
    }
}
```

Низкоуровневый bridge:
```go
br := sess.Bridge()
br.SendLastN(8)
br.SendVideoType("camera")        // "camera" | "desktop" | "none"
br.SendEndpointStats(map[string]any{"bitrate": 1234, "jvbRTT": 12})
br.SendReceiverAudioSubscription("Include", []string{"alice-a0"})
br.SendReceiverVideoConstraints(map[string]any{ /* … */ })
br.SendJSON(anyJSONserialisable)
```

### pion интеграция: приём + отправка медиа

```go
import "github.com/pion/webrtc/v4"

pc, _ := webrtc.NewPeerConnection(sess.IceConfig())

// приём аудио
pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
    webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})

// отправка видео — pion положит ssrc/cname/msid в SDP, наш Negotiator
// автоматически переведёт это в <source> внутри session-accept,
// Jicofo раздаст другим участникам
videoTrack, _ := webrtc.NewTrackLocalStaticSample(
    webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
    "myvideo", "mystream")
pc.AddTrack(videoTrack)

pc.OnTrack(func(t *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
    buf := make([]byte, 1500)
    for {
        _, _, err := t.Read(buf)
        if err != nil { return }
        // RTP пакет от удалённого участника — обрабатывай как нужно
    }
})

neg := sess.Negotiator()
neg.PC = pc
if err := neg.Accept(ctx); err != nil { panic(err) }
defer neg.Terminate("success")

// ВАЖНО: без этого JVB НЕ будет форвардить видео!
sess.RequestVideo(ctx, 720)

// ICE candidates обнаруженные ПОСЛЕ session-accept автоматически
// уйдут через transport-info (trickle ICE)

// шли VP8 кадры
go func() {
    for { videoTrack.WriteSample(media.Sample{Data: vp8Frame, Duration: 33*time.Millisecond}) }
}()
```

### Получение видео (ReceiverVideoConstraints)

**Критически важно**: JVB не форвардит видео пока получатель не отправит
`ReceiverVideoConstraints` через bridge channel. Без этого `OnTrack` никогда не сработает.

```go
// Простой вариант — запросить всё видео в 720p:
sess.RequestVideo(ctx, 720)

// Или вручную через bridge для тонкой настройки:
sess.OpenBridge(ctx)
sess.Bridge().SendJSON(map[string]any{
    "colibriClass":       "ReceiverVideoConstraints",
    "lastN":              3,                              // макс 3 видеопотока
    "onStageSources":     []string{"alice-v0"},           // приоритетный источник
    "defaultConstraints": map[string]any{"maxHeight": 180},
    "constraints": map[string]any{
        "alice-v0": map[string]any{"maxHeight": 720},     // alice в HD
    },
})
```

### Plan B (несколько участников с видео)

Когда в комнате уже есть участники с видео, Jicofo шлёт offer в **Plan B** формате
(несколько SSRC в одном `m=video`). pion по умолчанию ожидает Unified Plan и упадёт.

```go
neg := sess.Negotiator()
neg.PC = pc
err := neg.Accept(ctx)
if peer.IsPlanBError(err) {
    // Пересоздать PC с Plan B семантикой
    pc.Close()
    cfg := sess.IceConfig()
    cfg.SDPSemantics = webrtc.SDPSemanticsPlanB
    pc, _ = webrtc.NewPeerConnection(cfg)
    // ... добавить transceivers/tracks заново ...
    neg = sess.Negotiator()
    neg.PC = pc
    neg.Accept(ctx)
}
```

CLI `-media` делает это автоматически.

### Reconnect loop (session-terminate moving)

Jicofo иногда переключает на другой bridge (`session-terminate reason="moving"`).
`Session.WaitJingleReinitiate(ctx)` блокируется до следующего `session-initiate`:

```go
for {
    pc, _ := webrtc.NewPeerConnection(sess.IceConfig())
    // … add tracks/transceivers …
    neg := sess.Negotiator()
    neg.PC = pc
    neg.Accept(ctx)

    // Жди пока pc.OnConnectionStateChange попадёт в Failed/Closed
    waitForFailed(pc)
    pc.Close()
    neg.Terminate("success")

    if _, err := sess.WaitJingleReinitiate(ctx); err != nil { return }
    // цикл — следующий session-initiate
}
```

CLI `-media` уже делает это автоматически.

### Late tracks: source-add

Если добавляешь трек **после** session-accept:

```go
pc.AddTrack(newTrack)
sdp := pc.LocalDescription().SDP   // pion перегенерит с новым SSRC
neg.SendSourceAddFromSDP(sdp)      // → <jingle action="source-add"> Jicofo'у
```

Если трек добавлен **до** Accept — он попадает в session-accept SDP автоматически, source-add не нужен (иначе Jicofo вернёт `SSRC is already used`).

### Низкоуровневый XMPP

```go
xc := sess.LowLevel()                // *xmpp.Conn
xc.Send(`<message …>…</message>`)    // любая стэнза с xmlns="jabber:client"
xc.SendJingle(to, "transport-info", sid, initiator, innerXML)
id := xc.NextID()                    // monotonic id для IQ
stanza := xc.LastJingleStanza()      // raw <iq><jingle action="session-initiate"…/></iq>
```

## CLI

```sh
go build -o jcli ./cmd/cli
```

| Режим | Что делает |
|---|---|
| (без флага) | Дождаться Jingle и вывести JSON c SDP/ICE/SSRC/colibriWS |
| `-chat` | MUC chat: stdin → groupchat. Команды `/raise`, `/lower`, `/quit` |
| `-dc` | Bridge channel: stdin (текст) → broadcast `EndpointMessage{text:line}` |
| `-dc-raw` | Bridge channel raw: pipe сырых байт между двумя CLI через JVB |
| `-media` | pion + session-accept + reconnect loop. Принимает RTP с других треков |
| `-media -send-video` | то же + sendonly VP8 трек (dummy keyframe loop) с авто-объявлением SSRC |
| `-bench` | colibri-ws throughput benchmark (`-bench-size`, `-bench-secs`) |

```sh
# чат
./jcli -host meet.example.com -room myroom -nick alice -chat

# pipe сырых байт между двумя CLI через JVB
./jcli -host meet.example.com -room myroom -nick alice -dc-raw <input.bin
./jcli -host meet.example.com -room myroom -nick bob   -dc-raw >output.bin

# приём + отправка видео в комнату (нужен ≥1 другой участник в комнате)
./jcli -host meet.example.com -room myroom -nick mediabot -media -send-video

# benchmark пропускной способности bridge channel
./jcli -host meet.example.com -room myroom -nick recv -bench -bench-secs 30  &
./jcli -host meet.example.com -room myroom -nick send -bench -bench-size 8192 -bench-secs 20

# общие флаги
-host           Jitsi-сервер
-room           имя комнаты
-nick           отображаемое имя
-debug          подробный лог XMPP/WS
-timeout 5m     сколько ждать Jingle session-initiate
```

## Зависимости

- Go 1.21+
- `github.com/coder/websocket`
- `github.com/pion/webrtc/v4` (для медиа)

## Сборка

```sh
git clone https://github.com/zarazaex69/j
cd j
go build ./...
```

## Что осталось (зачем не нам)

- Адаптер `provider.Provider` для olcRTC (это часть olcRTC, не `j`)
- `vp8channel` / `seichannel` / `videochannel` транспорты (тоже olcRTC, мы только даём sendable VideoTrack)
- TLS fingerprint Chrome / XHR-телеметрия для маскировки от ТСПУ — задача более высокого уровня (utls, обёртка соединений)

<div align="center">

---

### Контакты

Telegram: [zarazaex](https://t.me/zarazaexe)
<br>
Email: [zarazaex@tuta.io](mailto:zarazaex@tuta.io)

</div>
