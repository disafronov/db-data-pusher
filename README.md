# DB Data Pusher

Получение данных из таблицы PostgreSQL и отправка их в PushGateway.

## Установка

1. Установите uv:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Создайте виртуальное окружение и установите зависимости:
```bash
uv venv
source .venv/bin/activate
uv pip install -e .
```

3. Скопируйте `.env.example` в `.env` и настройте переменные окружения:
```bash
cp .env.example .env
```

## Запуск

```bash
python main.py
```

## Требования

- Python 3.9+
- PostgreSQL
- PushGateway 