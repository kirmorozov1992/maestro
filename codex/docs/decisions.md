# Технические решения Maestro

Статус: принятые решения для MVP `v0.1`.

Дата проверки актуальности crates: 2026-09-19. Точные patch-версии должны
фиксироваться `Cargo.lock` в момент добавления зависимостей. В документации
фиксируются совместимые major/minor-семейства, а не обещание навсегда использовать
конкретную patch-версию.

## ADR-0001. Базовый async и HTTP stack

### Контекст

Maestro должен одновременно обслуживать HTTP API, запускать scheduler loop,
принимать heartbeat, выполнять polling agents и управлять локальными процессами.
Для MVP нужен один понятный async stack с хорошей совместимостью между server и
client частями.

### Решение

Использовать:

| Назначение | Crate | Семейство | Первоначальные feature flags |
| --- | --- | --- | --- |
| async runtime | `tokio` | `1.x` | `macros`, `rt-multi-thread`, `net`, `time`, `sync`, `process`, `signal` |
| HTTP server/router | `axum` | `0.8.x` | минимальный набор по умолчанию; дополнительные включать по необходимости |
| HTTP client | `reqwest` | `0.13.x` | `json`; использовать async client на Tokio |
| serialization traits | `serde` | `1.x` | `derive` |
| JSON | `serde_json` | `1.x` | default features, если не появится конкретная причина ограничивать их |

Tokio выбран как общий runtime для server, agent и CLI HTTP client. Axum
используется только в transport layer; domain и application layers не должны
зависеть от Axum. Reqwest используется для исходящих запросов agent-а и CLI,
поскольку Axum является server/router библиотекой, а не client library.

### Почему

- Tokio предоставляет runtime, timers, async networking, synchronization и
  process/signal primitives, которые нужны заявленному MVP.
- Axum даёт routing, extractors, JSON handlers и Tower middleware без
  необходимости писать HTTP protocol code вручную.
- Serde позволяет описывать typed DTO через `Serialize`/`Deserialize`, поэтому
  JSON-контракты проверяются компилятором и тестами.
- Reqwest скрывает низкоуровневую работу HTTP client, но сохраняет async API и
  переиспользуемый connection pool.
- Подходящие версии совместимы с локальным Rust `1.92.0`: проверенная текущая
  версия Axum требует Rust не ниже 1.80, а Tokio поддерживает более старый MSRV.

### Альтернативы

- `async-std` или `smol`: не выбираются для MVP, чтобы не смешивать runtime и
  не терять интеграцию с выбранными Tokio-oriented crates.
- `hyper` напрямую: оставляется за границами MVP, поскольку потребует больше
  ручного кода для routing, extraction и error responses.
- `warp`/`actix-web`: рабочие варианты, но Axum лучше совпадает с выбранным
  Tower/Tokio stack и уже покрывает нужный минимальный HTTP surface.
- `ureq` или прямой `hyper` client: не выбираются, поскольку agent уже будет
  асинхронным и ему нужны таймауты, JSON и переиспользование client.
- ручной JSON parsing: запрещается для API DTO, кроме специальных случаев;
  typed Serde-модели проще валидировать и тестировать.

### Ограничения

- Не включать `tokio = { features = ["full"] }` без необходимости: выбирать
  используемые features явно.
- Не добавлять Axum-типы в domain-модели.
- Не считать наличие HTTP/TLS stack authentication или security boundary:
  auth и TLS остаются за границами MVP.

## ADR-0002. CLI

### Решение

Использовать `clap` семейства `4.x` с derive API. Команды MVP:
`server`, `agent`, `submit`, `status`, `stop`.

Конфигурация читается в порядке `CLI > environment > defaults`. Parsing и
валидация CLI выполняются до запуска runtime или сетевых циклов.

### Значения конфигурации M1.4

| Настройка | CLI | Environment | Default |
| --- | --- | --- | --- |
| Адрес control plane для agent и клиентских команд | `--server-addr` | `MAESTRO_SERVER_ADDR` | `127.0.0.1:8080` |
| Адрес bind для server | `--bind-addr` | `MAESTRO_BIND_ADDR` | `127.0.0.1:8080` |
| Heartbeat interval, секунды | `--heartbeat-interval-secs` | `MAESTRO_HEARTBEAT_INTERVAL_SECS` | `5` |
| Polling interval, секунды | `--poll-interval-secs` | `MAESTRO_POLL_INTERVAL_SECS` | `1` |
| Liveness timeout, секунды | `--liveness-timeout-secs` | `MAESTRO_LIVENESS_TIMEOUT_SECS` | `30` |
| Рабочий каталог agent-а | `--working-dir` | `MAESTRO_WORKING_DIR` | `.` |

Адреса разбираются как `SocketAddr`; интервалы задаются положительным числом
секунд и преобразуются в `Duration`. Рабочий каталог проверяется до запуска
режима: существующий путь должен быть директорией, отсутствующая директория
создаётся рекурсивно, а итоговый путь нормализуется через canonicalize.
Адрес bind по умолчанию ограничен loopback, поскольку MVP не включает
аутентификацию. Значения интервалов хранятся в секундах, чтобы не вводить
дополнительный форматтер duration или новую зависимость.

### Почему

Typed derive API сокращает ручной parsing, автоматически формирует help и
позволяет тестировать CLI через `try_parse_from` без запуска процесса.

### Альтернативы

- `std::env::args` вручную: слишком много повторяющейся обработки ошибок и
  плохая поддерживаемость subcommands.
- `structopt`: функциональность вошла в Clap 3/4, отдельная зависимость не
  нужна.

## ADR-0003. Structured logging и diagnostics

### Решение

Использовать `tracing` семейства `0.1.x` и `tracing-subscriber` семейства
`0.3.x`.

Минимальная конфигурация:

- `tracing` для событий и spans;
- `tracing-subscriber` с `env-filter` и `fmt`;
- structured fields `job_id`, `allocation_id`, `agent_id`, `event`;
- вывод в stderr;
- уровень по умолчанию `info`, переопределяемый через environment;
- JSON output подготовить как опцию, если он понадобится для машинного
  сбора логов; обязательные поля не должны зависеть от текстового формата.

В логах запрещены credentials, tokens, полная карта environment и
неограниченный stdout/stderr job.

### Почему

`tracing` моделирует события и spans, что подходит для async-контекста и
корреляции событий job/allocation/agent. `tracing-subscriber` позволяет
изменить формат и фильтрацию без изменения бизнес-логики.

### Альтернативы

- `log` + `env_logger`: проще для маленького CLI, но хуже выражает spans и
  контекст параллельных async операций.
- `println!`/`eprintln!`: не использовать для runtime diagnostics; они не дают
  стабильных полей и уровней событий.
- OpenTelemetry: оставить на post-MVP, пока нет требования к distributed
  tracing backend.

## ADR-0004. Идентификаторы сущностей

### Решение

Использовать `uuid` семейства `1.x` с features `v4` и `serde`. Ввести отдельные
newtype-обёртки `JobId`, `AgentId` и `AllocationId`, даже если внутри каждой
обёртки находится `Uuid`.

В MVP использовать UUID v4 для случайных уникальных идентификаторов. Порядок
очереди определяется отдельным submit sequence/timestamp, а не сортировкой ID.
`new()` и `Default` создают новый UUID v4. `Display`/`FromStr` используют
каноническое представление UUID, а Serde прозрачно сериализует ID как UUID
строку. Между типами ID не добавляются автоматические преобразования.

### Почему

UUID не требует центрального allocator-а, легко передаётся через JSON и удобен
для распределённого server/agent протокола. Newtypes защищают от смешивания
идентификаторов разных сущностей на уровне Rust type system.

### Альтернативы

- UUID v7: полезен для сортируемых database keys, но persistence и сортировка
  по ID не входят в MVP; переход возможен при добавлении durable storage.
- ULID/KSUID: дают сортируемость, но добавляют отдельную семантику, которая пока
  не нужна.
- автоинкрементный integer: не подходит для нескольких agents/processes и
  усложняет будущую persistence/migration модель.

## ADR-0005. Версии, лицензии и порядок добавления зависимостей

### Проверка на 2026-09-19

Актуальные страницы crates показывали следующие версии-кандидаты:

- Tokio `1.53.1`;
- Axum `0.8.9`;
- Serde `1.0.229`, `serde_json` `1.0.151`;
- Clap `4.6.7`;
- Reqwest `0.13.5`;
- Tracing `0.1.44`, `tracing-subscriber` `0.3.23`;
- UUID `1.26.1`.

Это ориентир для первого `cargo add`, а не ручная фиксация всех patch-версий
в документации. Перед добавлением зависимостей нужно повторно проверить
совместимость с toolchain и сохранить разрешённые версии в `Cargo.lock`.

Выбранные crates используют permissive MIT и/или Apache-2.0 лицензии, совместимые
с текущей политикой проекта. Перед релизом добавить автоматическую проверку
лицензий и advisories; на этапе M0.3 отдельный audit tool не вводится, чтобы не
расширять MVP.

### Порядок добавления

1. Добавлять dependency только вместе с задачей, которая её использует.
2. Включать минимальные feature flags.
3. После каждого изменения Cargo manifest запускать полный quality workflow.
4. Не добавлять persistence, container runtime, auth или observability backend
   до соответствующей post-MVP задачи.

### Не зафиксировано намеренно

- explicit project MSRV: пока используется установленный Rust `1.92.0`, а
  минимальная поддерживаемая версия будет отдельным решением перед релизом;
- database client и migration tool: persistence не входит в v0.1;
- OpenTelemetry exporter: distributed tracing не входит в v0.1;
- TLS/auth crates: security layer не входит в v0.1.

## ADR-0006. Контракт `JobSpec`

### Решение

`JobSpec` содержит только пользовательский ввод: обязательную строку `command`,
упорядоченный список `args`, optional объект `env` и optional строку
`working_dir`. Если `args` отсутствует, он считается пустым. Поля runtime job
(`job_id`, статус, timestamps и результат) остаются за пределами `JobSpec`.

Ключи `env` должны быть уникальны. Повторяющийся ключ отклоняется при
десериализации; политика «последнее значение побеждает» не используется.
Окружение хранится в `BTreeMap`, поэтому порядок ключей не является частью
контракта. `working_dir` хранится как UTF-8 строка; преобразование к системному
пути и проверка самого пути выполняются на agent-е.

Лимиты измеряются в байтах UTF-8:

| Поле | Максимум | Правило подсчёта |
| --- | ---: | --- |
| `command` | 4 KiB | длина строки |
| `args` | 64 KiB | сумма длины каждой строки и 1 байта разделителя на аргумент |
| `env` | 64 KiB | сумма длины каждого ключа и значения плюс 2 байта на пару |
| `working_dir` | 4 KiB | длина строки |

Разделители в агрегатных лимитах также ограничивают число пустых аргументов и
пар окружения. Ограничение размера всего API body задаётся отдельно на
transport-границе. Конструктор и Serde-десериализация `JobSpec` применяют одни и
те же доменные лимиты; неизвестные поля отклоняются. Ошибки сообщают поле и
размер, но не содержимое команды, аргументов или окружения.

### Почему

- Раздельная модель пользовательского ввода не позволяет сериализовать runtime
  состояние как часть запроса создания job.
- Отклонение повторов устраняет неоднозначность между разными JSON-парсерами и
  платформами.
- Строка `working_dir` оставляет domain модель независимой от правил путей ОС;
  agent проверит и преобразует её перед запуском процесса.
- Лимиты в байтах задают воспроизводимую верхнюю границу для Unicode строк без
  зависимости от количества графем.

### Альтернативы

- Принимать последнее значение duplicate `env` key: отклонено, так как это
  скрывает ошибку отправителя и может менять смысл запроса между парсерами.
- Хранить `working_dir` как `PathBuf`: отклонено в domain, поскольку кодировка и
  правила интерпретации пути принадлежат платформе agent-а.
- Считать только длину элементов `args` и `env`: не выбрано, потому что большие
  количества пустых элементов обходили бы лимит; фиксированный учёт разделителя
  ограничивает и их.

### Последствия

Модель не добавляет зависимости и не содержит синхронного или async-состояния.
Проверка размера выполняется при создании/deserialization, до передачи
спецификации scheduler-у. Проверка пустой команды и допустимости пути относится
к последующей задаче доменной/agent-валидации.

## ADR-0007. Runtime-сущности, snapshots и lifecycle events

### Решение

- Runtime-сущности `Job`, `Agent` и `Allocation` имеют закрытые поля и
  конструкторы, принимающие ID, значения и timestamps от вызывающего слоя.
  Domain не читает системные часы и не генерирует время автоматически.
- `Timestamp` хранит Unix epoch milliseconds в `u64` и в JSON представлен
  целым числом. `u64::MAX` сохраняется без преобразования или потери точности.
- Начальное состояние `Job` — `pending`: `updated_at` равен `submitted_at`, а
  `started_at`, `finished_at` и terminal result отсутствуют. `Agent` создаётся
  `healthy` и `idle`, его начальный heartbeat равен времени регистрации, а
  address остаётся непрозрачной строкой. `Allocation` создаётся `assigned` с
  явно заданным `assigned_at`; `attempt` имеет тип `NonZeroU32`; start/finish
  timestamps отсутствуют.
- Runtime-сущности не задают внешний формат через Serde. Ответы сериализуются
  отдельными `Serialize`-only типами `JobSnapshot`, `AgentSnapshot` и
  `AllocationSnapshot`, создаваемыми из ссылок на сущности.
- `JobEvent` — tagged payload с discriminator `type` и snake_case вариантами.
  Вложенный `ProcessResult` использует такой же discriminator. Events содержат
  typed IDs и явный `occurred_at`; `stopped` допускает оба context ID либо ни
  одного, а десериализация отклоняет половинчатую пару.
- Domain types и payloads описывают состояние и факты. Проверка переходов,
  retry policy, обработка stale/duplicate events и согласование terminal
  состояний остаются в последующих задачах M2.4 и M2.6.

### Почему

Явные timestamps делают domain-логику детерминированной и позволяют верхним
слоям передавать часы через параметры. Разделение сущностей и snapshots не даёт
внешнему контракту случайно унаследовать внутреннее представление runtime
состояния. Typed events сохраняют ID, время, результат и причину в одном
проверяемом формате без привязки domain к HTTP или обработке процессов.

### Последствия

Модель не добавляет зависимостей и не выполняет I/O. Address остаётся
непроверенным до transport boundary. Конструкторы задают только начальные
состояния; изменения статусов и проверка их согласованности централизуются в
последующих задачах roadmap.

## Источники проверки

- [Tokio documentation](https://tokio.rs/tokio/tutorial) и
  [Tokio crate](https://docs.rs/crate/tokio/latest)
- [Axum crate](https://docs.rs/crate/axum/latest)
- [Serde documentation](https://serde.rs/) и
  [serde_json crate](https://docs.rs/crate/serde_json/latest)
- [Clap crate](https://docs.rs/crate/clap/latest)
- [Reqwest crate](https://docs.rs/crate/reqwest/latest)
- [Tracing crate](https://docs.rs/crate/tracing/latest) и
  [tracing-subscriber crate](https://docs.rs/crate/tracing-subscriber/latest)
- [UUID crate](https://docs.rs/crate/uuid/latest)
