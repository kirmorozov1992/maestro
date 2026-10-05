# Текущий прогресс Maestro

Статус: разработка MVP.

## Последняя завершённая задача

- **Задача:** `M2.3 — Runtime entities`.
- **Результат:** добавлены `Timestamp`, runtime-сущности `Job`, `Agent` и
  `Allocation`, их начальные состояния и read-only snapshots. Добавлены tagged
  `JobEvent` payloads с результатами и причинами; `stopped` deserialization
  отклоняет половинчатую пару allocation/agent ID. Все типы доступны через
  `domain`, архитектура обновлена, ADR-0007 фиксирует формат времени и границы
  модели. Новые зависимости не добавлялись.
- **Изменения:** `src/domain/{mod,timestamp,job,agent,allocation,dto,event}.rs`,
  `codex/docs/architecture.md`, `codex/docs/decisions.md` и этот progress-файл.
  Для воспроизводимой сборки также добавлены в Git существующие Cargo-файлы,
  crate-модули и CLI integration tests (`bd63f92`).
- **Проверки:** успешно выполнены `cargo fmt --all`,
  `cargo fmt --all -- --check`, `cargo check --all-targets`,
  `cargo test --all-targets` (33 unit + 10 CLI, всего 43 теста),
  `cargo clippy --all-targets -- -D warnings` и `git diff --check`.
- **Чистый checkout:** архив HEAD прошёл `cargo fmt --all -- --check`,
  `cargo check --all-targets --offline`, `cargo test --all-targets --offline`
  (43/43) и `cargo clippy --all-targets --offline -- -D warnings`.
- **Дата:** 2026-10-06.

## Следующая задача

`M2.4 — Определить статусы и события`.

Уточнить значения статусов, terminal statuses, обязательные поля и причины
событий `submitted`, `assigned`, `started`, `finished`, `stopped` и `lost`.

## Открытые вопросы и отложенная работа

- M2.3 описывает форму сущностей и payloads; соответствие terminal results
  статусам и семантика событий уточняются в M2.4.
- Переходы состояний, проверка actor/allocation IDs и обработка stale или
  duplicate events остаются в M2.6.
- Пустая команда и проверка working directory остаются в M2.5 и на границе
  agent-а соответственно.

## Предыдущая завершённая задача

- `M2.2 — Описать JobSpec`: отдельная модель пользовательского ввода, проверка
  лимитов UTF-8, уникальность ключей окружения и сохранение аргументов; 31
  проверка проходила до начала M2.3.
