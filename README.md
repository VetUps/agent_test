# Аналитический агент модуля Б

Исполняемый Dash-агент для конкурсного задания по модулю Б "Анализ и визуализация данных". Решение подключается к базе `module_b`, строит интерактивный дашборд по учебным материалам, обновляет данные по таймеру, поддерживает уровни доступа и автоматически дозаполняет недостающие темы синтетическими материалами для демонстрации рабочих сценариев.

## Что реализовано

- интерактивный дашборд на `dash`;
- чтение данных из `module_b.subjects`, `module_b.topics`, `module_b.materials`;
- автообновление данных через `dcc.Interval`;
- уровни доступа `viewer`, `analyst`, `admin`;
- аналитика по всем пунктам модуля Б `2.2`;
- автогенерация недостающих материалов в отдельную таблицу `analytics_generated_materials`, чтобы не ломать базовые таблицы;
- восстановление корректной темы для материалов, если в исходной БД предмет и `topic_id` расходятся.

## Быстрый запуск

1. Создайте `.env` рядом с исполняемым файлом или скопируйте шаблон:
   - [`.env.example`](D:/projects/Chempionat/.env.example)
2. Установите зависимости:
   ```powershell
   rtk python -m pip install -r requirements.txt
   ```
3. Запустите приложение:
   ```powershell
   rtk python run_agent.py
   ```
4. Откройте браузер по адресу `http://127.0.0.1:8050/login`.

## Демо-учётки

- `viewer / viewer123`
- `analyst / analyst123`
- `admin / admin123`

## Сборка в исполняемый файл

```powershell
.\build_exe.ps1
```

После сборки файл появится в `dist\analytics_agent.exe`.

## Структура

- [`run_agent.py`](D:/projects/Chempionat/run_agent.py) — точка входа;
- [`src/analytics_agent/main.py`](D:/projects/Chempionat/src/analytics_agent/main.py) — Flask + Dash, login и callbacks;
- [`src/analytics_agent/db.py`](D:/projects/Chempionat/src/analytics_agent/db.py) — подключение к БД, синтетические записи, нормализация тем;
- [`src/analytics_agent/analytics.py`](D:/projects/Chempionat/src/analytics_agent/analytics.py) — расчёт метрик и графиков;
- [`docs/module_b_analysis.md`](D:/projects/Chempionat/docs/module_b_analysis.md) — разбор задания и критериев.
