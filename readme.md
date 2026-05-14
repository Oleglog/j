<div align="center">

<img src="asset/logo.png" width="150">

![License](https://img.shields.io/badge/license-ZARAZAEX%20ANY%20DO-0D1117?style=flat-square&logo=open-source-initiative&logoColor=green&labelColor=0D1117)
![Golang](https://img.shields.io/badge/-Golang-0D1117?style=flat-square&logo=go&logoColor=00A7D0)




</div>

## j

Go-библиотека для программного подключения к Jitsi Meet звонкам. Выполняет XMPP-сигнализацию, получает Jingle session и возвращает готовые данные для подключения к DataChannel, Audio, Video и Chat через [pion/webrtc](https://github.com/pion/webrtc).

Создана для проекта [olcRTC](https://github.com/openlibrecommunity/olcrtc).

## Возможности

- Подключение к любому Jitsi Meet серверу (адрес передаётся пользователем)
- ANONYMOUS SASL аутентификация (гостевой вход)
- XMPP over WebSocket сигнализация
- Получение Jingle session-initiate (SDP, ICE candidates, DTLS fingerprint)
- Извлечение TURN/STUN credentials
- Отправка session-accept
- Участие в MUC (чат, presence, SourceInfo)
- Выдача структурированных данных для pion: SDP, ICE, DataChannel параметры

## Протокол

```
WebSocket (wss://host/xmpp-websocket?room=ROOM, subprotocol: xmpp)
    │
    ├─ ANONYMOUS SASL → bind → session → Stream Management (XEP-0198)
    ├─ extdisco:2 → TURN/STUN credentials
    ├─ focus allocation → conference ready
    ├─ MUC join (presence + codecList + SourceInfo + nick)
    ├─ ← Jingle session-initiate (SDP as XML, ICE candidates)
    ├─ → Jingle session-accept
    └─ Chat: groupchat messages
```

## Использование

```go
import "github.com/zarazaex69/j"

session, err := j.Join(j.Config{
    Host:     "meet.example.com",
    Room:     "myroom",
    Nick:     "thejproject",
})

// session.SDP        — remote SDP offer
// session.ICE        — ICE candidates + TURN/STUN creds
// session.DataChannel — DataChannel parameters
// session.Offer()    — send session-accept
// session.Chat("msg") — send groupchat message
```

## CLI

```sh
go run ./cmd/cli -host meet.example.com -room myroom -nick thejproject
```

Подключается к звонку с указанным именем и выводит данные сессии.

<div align="center">

---

### Контакты

Telegram: [zarazaex](https://t.me/zarazaexe)
<br>
Email: [zarazaex@tuta.io](mailto:zarazaex@tuta.io)

</div>
