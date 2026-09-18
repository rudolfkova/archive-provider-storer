# Timing stats

Опциональный режим замера latency вокруг запросов с таймаутом в `archive-provider-storer`.
Бизнес-логика не меняется: только логи и файлы статистики.

## Флаги

| Флаг | По умолчанию | Описание |
|------|--------------|----------|
| `-timing-stats` | `false` | Включить замеры |
| `-timing-dir` | `timings` | Каталог для JSONL/summary |

Tx **не** отключаются автоматически. Для безопасного прогона добавь `-no-tx`.

### Примеры

Через Taskfile (из корня репы `archive-provider-storer`, нужен `config.json` или `TIMING_CONFIG`):

```bash
task timing:start          # фон, -timing-stats -no-tx
task timing:status
task timing:logs           # tail -f лога
task timing:summary        # список summary-*.json
task timing:stop           # выключить
```

Переменные окружения / task vars:

```bash
TIMING_CONFIG=/path/to/config.json task timing:start
TIMING_DIR=./my-timings task timing:start
TIMING_EXTRA='-limit-bags 5' task timing:start
```

Вручную:

```bash
# из каталога, где лежит бинарь / откуда запускаешь сервис
./service -timing-stats -no-tx

# свой каталог для отчётов
./service -timing-stats -timing-dir=/var/log/archiver-timings -no-tx

# вместе с остальным конфигом
./service -config=config.json -timing-stats -timing-dir=./timings -no-tx
```

Каталог из `-timing-dir` создаётся автоматически при Flush.

## Что пишется в лог

На **каждый** замер — строка `timed op`:

```text
timed op op=request_storage_info_connect latency_ms=1200 budget_ms=10000 timed_out=false bag=... provider=...
timed op op=request_storage_info_query latency_ms=8421 budget_ms=10000 timed_out=false bag=... provider=...
```

Полезные поля:

- `op` — имя операции (см. таблицу ниже)
- `latency_ms` — сколько реально занял вызов
- `budget_ms` — лимит контекста/таймаута
- `timed_out` — `true`, если ошибка unwrap'ится в `context.DeadlineExceeded`
- `bag` / `provider` / `attempt` — контекст, если есть
- `error` — текст ошибки (если была)

В конце loop (и отдельно после startup `network_config`) — строка `timing stats flushed` с путями к файлам и агрегатами `ops`.

## Файлы в `-timing-dir`

После старта:

- `loop-startup-<unix>.jsonl`
- `summary-startup-<unix>.json`

После каждого `doLoop`:

- `loop-<unix>-<master_seqno>.jsonl` — по одной JSON-строке на замер
- `summary-<unix>-<master_seqno>.json` — агрегаты по `op`

### JSONL (сырые замеры)

Пример строки:

```json
{"op":"request_storage_info_connect","latency_ms":1200,"budget_ms":10000,"timed_out":false,"bag":"abc...","provider":"deadbeef...","at_unix_ms":1710000000123}
{"op":"request_storage_info_query","latency_ms":8421,"budget_ms":10000,"timed_out":false,"bag":"abc...","provider":"deadbeef...","at_unix_ms":1710000000456}
```

Быстрый просмотр:

```bash
# все таймауты
grep '"timed_out":true' timings/loop-*.jsonl

# только connect/query storage info
grep '"op":"request_storage_info_' timings/loop-*.jsonl | jq .

# p95 руками не нужен — смотри summary; сырые latency:
jq 'select(.op|startswith("request_storage_info_")) | .latency_ms' timings/loop-*.jsonl
```

### Summary (агрегаты)

Пример:

```json
{
  "loop_id": "1710000000-12345678",
  "loop_started_at": "2026-08-27T14:00:00.000Z",
  "loop_finished_at": "2026-08-27T14:05:12.000Z",
  "ops": {
    "request_storage_info_connect": {
      "count": 120,
      "timeouts": 2,
      "errors": 3,
      "avg_ms": 400,
      "min_ms": 40,
      "max_ms": 10012,
      "p50_ms": 180,
      "p95_ms": 2100,
      "p99_ms": 5000
    },
    "request_storage_info_query": {
      "count": 118,
      "timeouts": 7,
      "errors": 11,
      "avg_ms": 1100,
      "min_ms": 40,
      "max_ms": 10012,
      "p50_ms": 340,
      "p95_ms": 8200,
      "p99_ms": 10001
    }
  }
}
```

- `timeouts` — число событий с `timed_out=true`
- `errors` — число событий с непустым `error` (включая таймауты и обычные ошибки)
- `p50` / `p95` / `p99` — приближённые перцентили по сэмплам loop'а

```bash
jq '.ops' timings/summary-*.json
jq '.ops.request_storage_info_connect' timings/summary-*.json | head
jq '.ops.request_storage_info_query' timings/summary-*.json | head
```

## Операции (`op`)

| op | Что измеряется | Типичный budget |
|----|----------------|-----------------|
| `network_config` | `GetConfigFromUrl` при старте | 10s |
| `http_index` | HTTP GET индекса bag'ов | 10s |
| `http_providers` | HTTP POST списка провайдеров | 10s |
| `get_providers_stats` | `GetProvidersV1` (фаза статистики), каждая attempt | 10s |
| `get_providers_decide` | `GetProvidersV1` (фаза решений) | 10s |
| `request_storage_info_connect` | DHT + ADNL connect к провайдеру | 10s |
| `request_storage_info_query` | RLDP storage info запрос | 10s |
| `get_storage_rates` | rates кандидата | 15s |
| `notify_storage_info_connect` | post-tx notify: connect | 10s |
| `notify_storage_info_query` | post-tx notify: storage info query | 10s |
| `wait_master` | ожидание masterchain-блока | без жёсткого budget / внешний ctx |
| `send_many_wait` | отправка tx и ожидание подтверждения | до ~180s |

Для `request_storage_info_*` / `notify_storage_info_*` connect и query измеряются отдельно (по 10s budget на фазу). Для `get_storage_rates` в `latency_ms` входит весь путь через клиент (DHT connect + RLDP).

## Замечания

- Без `-timing-stats` overhead почти нулевой (`timing == nil`).
- Если в loop ушли notify в горутине, при включённом timing Flush ждёт их завершения (после `Sleep(20s)` + сами запросы), чтобы попали в тот же summary.
- Режим не меняет значения таймаутов — только измеряет фактическое время относительно текущего budget.
