# Брокер сообщений `nats` с включенным JetStream.

## Структура папок и что в них хранится

- `~/apps/nats-broker` — папка проекта с `docker-compose.yml`, `README.md`, `config/nats.conf`
- `~/apps/nats-broker/config/nats.conf` — конфигурация сервера NATS (порты, JetStream, лимиты)
- `~/apps/nats-broker/config/auth.conf` — логин/пароль `nats_admin` (только на сервере, не в git)
- `~/apps/nats-data/jetstream` — данные JetStream на хосте (сообщения stream'ов, метаданные, состояние)

## Авторизация (nats_admin)

Подключение приложений:

```text
nats://nats_admin:<ПАРОЛЬ>@<IP_СЕРВЕРА>:4222
```

Мониторинг `8222` (`/healthz`, `/varz`…) по умолчанию **без** логина — только клиентский порт `4222` требует `nats_admin`.

В JSON событий всё равно указывайте `source` (`airflow`, `db_manager`…), чтобы видеть, какой сервис отправил сообщение.

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

## Предполагаемая архитектура сабджектов

В NATS адрес сообщения — **subject** (аналог «топика»). Имена не создают в репозитории брокера: их задают **приложения** при `publish` / `subscribe`. Схема: **тип события → источник**.

```text
<тип>.<источник>[.<деталь>]

Примеры:
  incidents.airflow
  incidents.postgres
  incidents.db_manager
  stats.airflow
  stats.clickhouse
  business.minio.created
  data.sku.unmarked
```

### Паблишеры (кто шлёт)

| Источник | Subjects (пример) | Примечание |
|----------|-------------------|------------|
| Airflow | `incidents.airflow`, `stats.airflow` | инциденты DAG, DQ, успешные запуски |
| MinIO | `business.minio.*` | создание / обновление / удаление / скачивание (часто через bridge) |
| Streamlit | `stats.streamlit`, `incidents.streamlit` | активность пользователей, инциденты, логи |
| Postgres | `incidents.postgres`, `stats.postgres` | инциденты, ошибки, pg_stat_statements (часто через сервис-адаптер) |
| ClickHouse | `incidents.clickhouse`, `stats.clickhouse` | инциденты, ошибки, статистика запросов |
| Prometheus / Node Exporter | `stats.infra` | сводки/алерты, не сырой scrape |
| db_manager | `incidents.db_manager`, `stats.db_manager` | инциденты, ошибки, запросы с 4xx/5xx |
| Postgres / db_manager / Airflow | `data.sku.unmarked` | неразмеченные SKU |

В теле JSON всегда поле **`source`** (`airflow`, `db_manager`, …) — один логин `nats_admin` на всех, источник виден по payload.

### Подписчики (кто читает)

| Сервис | Что читает | Куда дальше |
|--------|------------|-------------|
| Бот Битрикс | `incidents.>` | уведомления в Битрикс |
| Grafana Aggregator | `stats.>`, при необходимости `incidents.>` | батч раз в N минут → Grafana |
| (позже) воркер SKU | `data.sku.>` | обработка неразмеченных SKU |

Grafana и Битрикс **не подключаются к NATS напрямую** — только ваши сервисы с `nats://nats_admin:...`.

### JetStream: stream’ы (хранение на диске)

Создаются отдельно (CLI/скрипт), не в `nats.conf`. Один stream — шаблон subjects + свои лимиты retention.

| Stream | Subjects | Назначение |
|--------|----------|------------|
| `INCIDENTS` | `incidents.>` | инциденты, дольше хранить |
| `STATS` | `stats.>` | метрики/статистика, короче retention |
| `BUSINESS` | `business.>` | события MinIO |
| `DATA` | `data.>` | SKU и прочие data-события |

Без stream сообщение всё равно уходит по subject, но **не персистится** (как «эфир»). Для очередей и «дочитать после рестарта» нужен stream + consumer.

### Схема потока

```text
[Airflow, db_manager, MinIO-bridge, …]
        │ publish (subject + JSON)
        ▼
   NATS JetStream (stream по шаблону)
        │
        ├──► consumer: bitrix-bot      (incidents.>)
        └──► consumer: grafana-agg     (stats.> → Grafana)
```

### JetStream: subjects и stream’ы — кто и как создаёт

| Сущность | Создаётся заранее? | Как |
|----------|-------------------|-----|
| **Subject** | Нет | Появляется, когда приложение делает `publish("stats.airflow", ...)` |
| **Stream** | Да, один раз | Вручную: CLI, API или скрипт на сервере |
| **Consumer** | Да, под задачу | Вручную или из кода подписчика (бот, aggregator) |

**Subject** в репозитории брокера не прописывают — только в коде паблишеров.  
**Stream** — «коробка на диске»: «сохранять всё, что приходит на `incidents.>`». Без stream JetStream не запишет сообщение, даже если subject верный.

Пример один раз на сервере (нужен `nats` CLI, например контейнер `natsio/nats-box`):

```bash
nats stream add INCIDENTS \
  --subjects "incidents.>" \
  --storage file \
  --retention limits \
  --max-age 30d \
  --defaults \
  -s "nats://nats_admin:<ПАРОЛЬ>@127.0.0.1:4222"
```

Аналогично: `STATS` на `stats.>`, `BUSINESS` на `business.>`.  
Проверка: `nats stream ls`, `nats stream info INCIDENTS`.

**Consumer** — кто читает из stream (Битрикс, Aggregator): `nats consumer add INCIDENTS bitrix-bot --filter "incidents.>" --ack explicit --defaults`.

Итого: subjects — из приложений; stream’ы и consumer’ы — **настроить один раз руками/скриптом**, потом только публиковать.