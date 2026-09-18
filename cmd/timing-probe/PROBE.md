# Timing probe (v1)

Отдельный бинарник для замера latency к storage-провайдерам. Не запускает storer-loop, wallet и tx.

## Локальный `tonutils-go`

Сборка по умолчанию идёт через копию `v1.17.2` в [`third_party/tonutils-go`](../../third_party/tonutils-go) (`replace` в корневом `go.mod`) — с хеджированием в `FindValue`.

Baseline (чистый upstream без hedge): [`go.baseline.mod`](../../go.baseline.mod) — тот же `require`, без `replace`.

## A/B: baseline → hedge

Один командой: сначала прогон на upstream, потом на локальном hedge, затем сравнение.

```bash
task probe:ab
LIMIT_PROVIDERS=30 task probe:ab
```

Результат:

- `timings/ab-<unix>/baseline/` — jsonl + summary без hedge
- `timings/ab-<unix>/hedge/` — jsonl + summary с hedge
- `timings/ab-<unix>/compare.txt` — стена, avg/p50/p95 connect, buckets 3/6/9 с

## Операции (`op`)

| op | Что измеряется | Budget |
|----|----------------|--------|
| `http_providers` | POST списка провайдеров | 10s |
| `storage_rates_connect` | DHT lookup + ADNL connect | `-phase-budget` (10s) |
| `storage_rates_query` | RLDP `StorageRatesRequest` | `-phase-budget` (10s) |

При ошибке connect фаза query **не выполняется** (нет ложных `not connected` в summary).

## Конфиг

Скопируй `config.probe.example.json` → `config.probe.json` (gitignored).

```json
{
  "network_config_url": "https://ton.org/global.config.json",
  "providers_url": "https://mytonprovider.org/api/v1/providers/search",
  "limit_providers": 50,
  "bag_size_bytes": 1073741824
}
```

## Флаги

| Флаг | Default | Описание |
|------|---------|----------|
| `-config` | `config.probe.json` | JSON-конфиг |
| `-output-dir` | `timings` | Каталог отчётов |
| `-phase-budget` | `10s` | Budget connect и query |
| `-once` | false | Один run и exit |
| `-interval` | `15m` | Между runs |
| `-samples` | `1` | Повторов на provider за run |
| `-label` | `""` | Метка в `loop_id` / summary (`baseline` / `hedge`) |
| `-limit-providers` | `0` | Override `limit_providers` из конфига, если > 0 |

## Taskfile

```bash
task probe:build
task probe:run                    # foreground, -once (hedge via replace)
LIMIT_PROVIDERS=30 task probe:run
task probe:ab                     # baseline затем hedge + compare.txt
LIMIT_PROVIDERS=30 task probe:ab
task probe:start                  # background daemon
task probe:status
task probe:logs
task probe:stop
```

## Выходные файлы

- `timings/loop-probe-<unix>.jsonl` / `summary-probe-*.json` — одиночный `probe:run`
- `timings/ab-<unix>/…` — пара A/B

## Примеры анализа

```bash
jq '.ops' timings/summary-probe-*.json | tail -1
jq 'select(.op|startswith("storage_rates_"))' timings/loop-probe-*.jsonl | head
grep '"timed_out":true' timings/loop-probe-*.jsonl
cat timings/ab-*/compare.txt | tail -20
```
