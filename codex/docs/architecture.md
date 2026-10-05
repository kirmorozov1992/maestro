# Maestro: архитектура базового оркестратора

Статус документа: архитектура MVP `v0.1`. Это описание фиксирует границы
первой рабочей версии, но не запрещает последующую эволюцию системы.

Требования описаны в [`requirements.md`](requirements.md), а технические
решения и выбранные crates — в [`decisions.md`](decisions.md).

## 0. Технологический baseline MVP

Технологический стек зафиксирован в `ADR-0001`–`ADR-0005`:

| Область | Решение |
| --- | --- |
| async runtime | Tokio 1.x |
| HTTP server/router | Axum 0.8.x |
| HTTP client для agent/CLI | Reqwest 0.13.x |
| JSON и DTO | Serde 1.x + serde_json 1.x |
| CLI | Clap 4.x |
| structured diagnostics | tracing 0.1.x + tracing-subscriber 0.3.x |
| IDs | UUID 1.x, v4 + serde |

Зависимости добавляются только вместе с задачами, которые их используют, с
минимальными feature flags. Точные patch-версии фиксируются в `Cargo.lock` при
первом добавлении. Локальный toolchain, проверенный на этапе M0, — Rust
`1.92.0`; explicit project MSRV пока не объявлен.

## 1. Цель и границы MVP

Maestro должен уметь принять описание одной задачи, найти свободный worker,
запустить команду на его хосте и показать результат выполнения. Минимальный
вертикальный сценарий:

1. пользователь отправляет job с командой и аргументами;
2. control plane валидирует job и ставит её в очередь;
3. scheduler выбирает здоровый свободный agent;
4. agent запускает локальный процесс;
5. agent отправляет изменения состояния и код завершения;
6. пользователь получает `succeeded`, `failed`, `stopped` или `lost`.

В первой версии намеренно отсутствуют:

- отказоустойчивый control plane и leader election;
- постоянное хранилище и восстановление состояния после перезапуска;
- контейнеризация, sandbox и полноценная изоляция ресурсов;
- сложное планирование ресурсов, affinity/anti-affinity и service discovery;
- rolling update, volumes, secrets management и multi-region режим;
- аутентификация, TLS и модель доверия для production.

Это не означает, что такие функции не нужны вообще. Они вынесены за границу
MVP, чтобы сначала проверить корректность жизненного цикла job и взаимодействия
control plane с worker.

## 2. Топология процессов

В MVP используется один бинарный файл с двумя режимами запуска:

```text
maestro server     control plane: API + state store + scheduler
maestro agent      worker: registration + polling + executor
maestro submit     CLI-клиент для отправки job в server
maestro status     CLI-клиент для чтения состояния
maestro stop       CLI-клиент для остановки job
```

`server` и `agent` могут работать на одной машине, но остаются отдельными
процессами и общаются через версионируемый HTTP/JSON-протокол. Такое разделение
сохраняет правильные границы Nomad-подобной системы, не требуя в MVP
распределённого кластера.

## 3. Компоненты

### Domain

Чистые типы и правила, не зависящие от сети и операционной системы:

- `JobId`, `AgentId`, `AllocationId`;
- `JobSpec`: пользовательские команда, аргументы, окружение и UTF-8 строка
  рабочей директории; runtime-поля хранятся отдельно;
- `JobStatus` и `AllocationStatus`;
- `Job`, `Agent`, `Allocation`;
- валидация входных данных и допустимые переходы состояния.

Domain не должен знать о Axum, Reqwest, конкретной базе данных, `Command` или
формате CLI. Это позволяет тестировать scheduler и state machine обычными
unit-тестами.

### Control plane

Control plane владеет желаемым и наблюдаемым состоянием jobs и agents.

- **API layer** принимает команды пользователя и сообщения agents, выполняет
  только транспортную десериализацию и первичную проверку.
- **Application/service layer** реализует use cases: submit, list/status,
  stop, register agent, heartbeat, report allocation result.
- **State store** в MVP хранит состояние в памяти процесса. Все изменения
  проходят через сервисный слой и state-transition policy.
- **Scheduler** периодически просматривает pending jobs и healthy idle agents,
  создаёт allocation и резервирует agent. В MVP выбор детерминированный:
  FIFO для jobs и первый подходящий agent.
- **Liveness tracker** обновляет время последнего heartbeat. Если heartbeat
  истёк, agent помечается `unhealthy`, а выполнявшаяся на нём allocation —
  `lost`. Автоматический retry в MVP не выполняется.

### Worker agent

Agent не принимает решений о размещении. Он:

- регистрируется в control plane и получает `agent_id`;
- периодически отправляет heartbeat;
- получает assignment через pull/poll протокол;
- передаёт assignment executor;
- отправляет `started`, `stdout/stderr`-метаданные, exit code и terminal event;
- по команде stop отменяет процесс и сообщает результат.

### Executor

Executor — отдельный модуль worker-а над `std::process::Command` или
асинхронным эквивалентом. Он отвечает за:

- безопасную сборку команды из уже провалидированного `JobSpec`;
- запуск с рабочей директорией и окружением;
- сбор ограниченного stdout/stderr;
- передачу сигнала остановки и ожидание child process;
- нормализацию exit code и причины завершения.

MVP запускает обычные процессы пользователя с правами agent-а. Это сознательное
ограничение: executor не является security boundary и не должен рекламироваться
как sandbox.

## 4. Поток данных

```text
CLI/API client
     |
     v
HTTP API -> Application services -> In-memory state
                              |             ^
                              v             |
                         Scheduler <--------+ reports/heartbeats
                              |
                              v
                     assignment over HTTP
                              |
                              v
                     Agent -> Executor -> OS process
                              |
                              +---- status/exit events ----> server
```

Долгие операции нельзя выполнять внутри lock над state store. Handler или
scheduler сначала атомарно меняет состояние и получает snapshot задания, затем
делает сетевой/процессный I/O, а после этого отправляет отдельное событие с
результатом. Для MVP достаточно `Arc` общего состояния с короткими секциями
`Mutex`/`RwLock`; actor-модель и event broker пока не вводятся.

## 5. Жизненный цикл job

```text
submitted -> pending -> assigned -> running -> succeeded
                                  |          -> failed
                                  |          -> stopped
                                  +--------> lost

pending/assigned/running -> stopped  (явная команда stop)
```

Точные допустимые переходы задаются одной функцией/политикой domain-уровня.
HTTP handlers, scheduler и agent adapter не меняют статус напрямую. Каждое
изменение должно содержать причину и timestamp; для allocation дополнительно
нужен `allocation_id`, чтобы устаревший callback от старого запуска не мог
перезаписать более новое состояние.

## 6. Минимальный протокол

Названия endpoint-ов являются рабочим предложением для MVP и должны быть
зафиксированы контрактными тестами перед реализацией транспорта:

- `POST /v1/jobs` — создать job;
- `GET /v1/jobs` и `GET /v1/jobs/{job_id}` — список и состояние;
- `POST /v1/jobs/{job_id}/stop` — запросить остановку;
- `POST /v1/agents/register` — зарегистрировать agent;
- `POST /v1/agents/{agent_id}/heartbeat` — heartbeat;
- `GET /v1/agents/{agent_id}/assignments/next` — получить следующую assignment;
- `POST /v1/allocations/{allocation_id}/events` — отправить lifecycle event.

Все payload-ы должны иметь явную версию (`v1` в URL), стабильные ID и
машиночитаемые error codes. Polling выбран вместо server push, потому что он
проще для первой реализации и не требует постоянного двустороннего канала.

## 7. Предлагаемая структура Rust-кода

Структура может появляться постепенно вместе с задачами roadmap:

```text
src/
  main.rs                 # сборка CLI и запуск выбранного режима
  config.rs               # конфигурация server/agent
  error.rs                # ошибки границ приложения
  domain/
    mod.rs
    id.rs                 # typed domain identifiers
    job_spec.rs           # user input and size limits
    timestamp.rs          # Unix-millisecond domain time
    job.rs                # runtime job and initial state
    agent.rs              # agent runtime state
    allocation.rs         # assignment links, attempt and state
    dto.rs                # read-only response snapshots
    event.rs              # typed lifecycle event payloads
  application/
    mod.rs
    jobs.rs               # use cases jobs
    agents.rs             # use cases agents
    scheduler.rs          # placement policy и tick
  transport/
    mod.rs
    http.rs               # HTTP routes/DTOs
  state/
    mod.rs                # in-memory store в MVP
  worker/
    mod.rs
    client.rs             # register/heartbeat/poll/report
    executor.rs           # локальный процесс
```

Это логические границы, а не требование создать все файлы заранее. На раннем
этапе допустимо держать несколько модулей компактными; разделение нужно вводить
тогда, когда оно помогает тестировать отдельную ответственность.

## 8. Эволюция после MVP

Следующие изменения должны добавляться через отдельные решения и миграции:

1. SQLite или PostgreSQL для jobs, allocations и event history;
2. durable queue и восстановление незавершённых allocations;
3. retries с backoff и явной retry policy;
4. CPU/memory constraints и более умная placement policy;
5. TLS, authentication, authorization и безопасное хранение secrets;
6. container/runtime isolation и лимиты stdout/stderr;
7. несколько control-plane replicas, leader election и HA;
8. logs/artifacts, service registration и rolling deployments.

Каждый пункт должен сохранять простое domain-ядро и не протаскивать детали
конкретной инфраструктуры в state machine.
