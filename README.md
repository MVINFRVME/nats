# `nats-server` с включенным JetStream.

## Структура папок и что в них хранится

- `~/apps/nats-broker` — папка проекта с `docker-compose.yml`, `README.md`, `config/nats.conf`
- `~/apps/nats-broker/config/nats.conf` — конфигурация сервера NATS (порты, JetStream, лимиты)
- `~/apps/nats-data/jetstream` — данные JetStream на хосте (сообщения stream'ов, метаданные, состояние)


## Curl проверки

База: `http://127.0.0.1:8222` (из VPN: `http://<IP_СЕРВЕРА>:8222`)

```bash
# Проверка, что NATS жив
curl -s http://127.0.0.1:8222/healthz
# Состояние JetStream
curl -s http://127.0.0.1:8222/jsz
# Общая статистика сервера
curl -s http://127.0.0.1:8222/varz
# Текущие клиентские подключения
curl -s http://127.0.0.1:8222/connz
# Маршруты/кластерные соединения (актуально для кластера)
curl -s http://127.0.0.1:8222/routez
```

## 7) Следующие шаги для прода

1. Добавить пользователей и права в `config/nats.conf`
2. Ограничить firewall на порты `4222` и `8222`
3. Включить TLS для клиентских подключений
