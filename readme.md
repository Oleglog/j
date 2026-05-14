<div align="center">

<img src="asset/logo.png" width="150">

![License](https://img.shields.io/badge/license-ZARAZAEX%20ANY%20DO-0D1117?style=flat-square&logo=open-source-initiative&logoColor=green&labelColor=0D1117)
![Golang](https://img.shields.io/badge/-Golang-0D1117?style=flat-square&logo=go&logoColor=00A7D0)

</div>

## j

Go-библиотека для программного подключения к Jitsi Meet звонкам. Делает XMPP-сигнализацию, заходит в MUC, ловит Jingle session-initiate, парсит SDP/ICE/SSRC, открывает bridge channel (colibri-ws) и даёт API чтобы:

- сидеть в чате (groupchat сообщения, поднимать/опускать руку)
- слать текст всем участникам через bridge channel
- слать **сырые байты** через bridge channel (broadcast или конкретному endpoint)
- получать всё это в обратную сторону
- отдавать SDP/ICE/SSRC во внешний WebRTC-стек (например [pion/webrtc](https://github.com/pion/webrtc)) для медиа

Создана для проекта [olcRTC](https://github.com/openlibrecommunity/olcrtc).

Адреса/комнаты не захардкожены — всё передаётся пользователем.

## Что внутри

```
j/
├── j.go                       # public API: Join, JoinMUC, Session, ICEServer, Message
├── internal/
│   ├── xmpp/                  # WebSocket + ANONYMOUS SASL + bind + MUC + focus + SM (XEP-0198)
│   ├── jingle/                # Jingle session-initiate ↔ SDP конвертер (BUNDLE, RTP, RTCP-FB, SSRC)
│   ├── colibri/               # bridge channel WebSocket — JVB protocol (EndpointMessage, LastN, …)
│   └── peer/                  # pion *PeerConnection ↔ Jingle bridge (Accept, transport-info, source-add)
├── cmd/cli/                   # CLI: 5 режимов (jingle, chat, dc, dc-raw, media)
```

## Протокол

```
WebSocket wss://host/xmpp-websocket?room=ROOM   (subprotocol: xmpp)
   │
   ├─ ANONYMOUS SASL → bind → session → Stream Management (XEP-0198)
   ├─ extdisco:2 → TURN/STUN credentials
   ├─ focus.host conference allocation
   ├─ MUC join (presence, codecList, SourceInfo, nick, caps)
   ├─ ← Jingle session-initiate (SDP-as-XML, ICE candidates, colibri-ws URL)
   ├─ → session-accept (опционально, для медиа)
   ├─ groupchat / raise-hand / leave
   │
   └─ ─── colibri-ws (bridge channel WebSocket) ──→ JVB
                ├─ ClientHello / ServerHello
                ├─ EndpointMessage  (broadcast или unicast — произвольный payload)
                ├─ EndpointStats / DominantSpeaker / LastN / …
                └─ raw bytes via base64 в EndpointMessage
```

## Использование

```go
import (
    "context"
    j "github.com/zarazaex69/j"
    "github.com/zarazaex69/j/internal/colibri"
)

ctx := context.Background()
sess, err := j.Join(ctx, j.Config{
    Host: "meet.cryptopro.ru",
    Room: "myroom",
    Nick: "thejproject",
})
if err != nil { panic(err) }
defer sess.Close()
```

`j.Join` ждёт session-initiate от Jicofo (нужен ≥1 другой участник в комнате). Если хватает только захода в чат без медиа — используй `j.JoinMUC`.

### Чат / MUC

```go
sess.Chat("привет всем")
sess.RaiseHand()
sess.LowerHand()

for m := range sess.Messages() {
    fmt.Printf("<%s> %s\n", m.From, m.Body)
}
```

### Bridge channel (DataChannel)

В современном Jitsi классический SCTP DataChannel deprecated с 2020 — бридж раздаёт WebSocket URL в Jingle (`<web-socket url=…/>`). `j` сам его извлекает.

```go
// поднять colibri-ws к JVB
sess.OpenBridge(ctx)

// сырые байты — broadcast всем
sess.BridgeSendRaw("", []byte{0xDE, 0xAD, 0xBE, 0xEF})

// сырые байты — конкретному endpoint
sess.BridgeSendRaw("2968719f", payload)

// JSON EndpointMessage с произвольными полями (бридж не парсит, релеит как есть)
sess.BridgeSendMessage("", map[string]any{
    "type": "chat",
    "text": "hi",
})

// приём
for m := range sess.BridgeMessages() {
    switch m.Class {
    case "EndpointMessage":
        if raw := colibri.DecodeRaw(m); raw != nil {
            // получили сырые байты от собеседника
        } else {
            // m.Fields содержит весь JSON
        }
    case "DominantSpeakerMessage":
        // m.Fields["dominantSpeakerEndpoint"]
    }
}
```

### Низкоуровневый bridge API

```go
br := sess.Bridge()
br.SendLastN(8)
br.SendVideoType("camera")        // "camera" | "desktop" | "none"
br.SendSourceVideoType("alice-v0", "desktop")
br.SendEndpointStats(map[string]any{"bitrate": 1234, "jvbRTT": 12})
br.SendReceiverAudioSubscription("Include", []string{"alice-a0"})
br.SendReceiverVideoConstraints(map[string]any{ /* … */ })
br.SendJSON(anyJSONserialisable)
```

### WebRTC данные для pion

```go
sess.SDP          // remote SDP (offer от Jicofo)
sess.ICEServers   // STUN/TURN с расширенными creds (extdisco:2)
sess.Candidates   // ICE candidates
sess.AudioSSRC    // SSRC аудио
sess.VideoSSRC    // SSRC видео
sess.DataChannel  // SCTP DC параметры (если бридж его прислал)
sess.ColibriWS    // bridge WS URL
```

### Полный pion-цикл: pion + Jingle session-accept

```go
import "github.com/pion/webrtc/v4"

pc, _ := webrtc.NewPeerConnection(sess.IceConfig())
pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})

pc.OnTrack(func(t *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
    // принимаем пакеты — это RTP от участников
    buf := make([]byte, 1500)
    for { _, _, err := t.Read(buf); if err != nil { return } }
})

neg := sess.Negotiator()
neg.PC = pc
if err := neg.Accept(ctx); err != nil { panic(err) }     // SDP↔Jingle, шлёт session-accept
defer neg.Terminate("success")

// trickle ICE / source updates:
neg.SendTransportInfo("video", "candidate:… typ host …")
neg.SendSourceAdd(`<content name="audio">…</content>`)
neg.SendSourceRemove(`<content name="audio">…</content>`)
```

### Низкоуровневый XMPP / Jingle

```go
xc := sess.LowLevel()                // *xmpp.Conn
xc.Send(`<message …>…</message>`)    // любая стэнза
xc.SendJingle(to, "transport-info", sid, initiator, innerXML)
id := xc.NextID()
stanza := xc.LastJingleStanza()      // raw session-initiate
```

## CLI

```sh
# просто получить SDP/ICE/SSRC и выйти
go run ./cmd/cli -host meet.example.com -room myroom

# чат: stdin → groupchat, /raise, /lower, /quit
go run ./cmd/cli -host meet.example.com -room myroom -nick thejproject -chat

# bridge channel: stdin (текст) → broadcast EndpointMessage
go run ./cmd/cli -host meet.example.com -room myroom -nick thejproject -dc

# bridge channel raw: pipe сырых байт между двумя CLI через JVB
go run ./cmd/cli -host meet.example.com -room myroom -nick alice -dc-raw <input.bin
go run ./cmd/cli -host meet.example.com -room myroom -nick bob   -dc-raw >output.bin

# media: pion + session-accept + приём RTP
go run ./cmd/cli -host meet.example.com -room myroom -nick thejproject -media

# флаги
-host           Jitsi-сервер (например meet.example.com)
-room           имя комнаты
-nick           отображаемое имя (по умолчанию thejproject)
-debug          подробный лог XMPP/WS
-timeout 5m    сколько ждать Jingle session-initiate
-chat | -dc | -dc-raw | -media   режим (по умолчанию — режим Jingle: вывести данные сессии)
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

## Что дальше

- session-accept с реальным SDP-ответом от pion (для приёма медиа)
- transport-info / source-add / source-remove обработка
- авто-переподключение при разрыве WebSocket

<div align="center">

---

### Контакты

Telegram: [zarazaex](https://t.me/zarazaexe)
<br>
Email: [zarazaex@tuta.io](mailto:zarazaex@tuta.io)

</div>
