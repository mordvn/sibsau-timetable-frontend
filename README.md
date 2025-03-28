# SibSAU Timetable Frontend

Telegram-бот для просмотра расписания Сибирского государственного университета (СибГУ). Бот предоставляет удобный интерфейс для доступа к расписанию занятий, поддерживает подписки на обновления, настройки подгрупп и отслеживание изменений.

## Архитектура проекта

<img src="images/arch.png" width="600" alt="Архитектура проекта">

## Примеры работы

<img src="images/1.jpg" width="500" alt="Пример работы 1">

<img src="images/2.jpg" width="500" alt="Пример работы 2">

## 🚀 Установка и запуск

### Ручная установка

```bash
# Клонирование репозитория
git clone https://github.com/yourusername/sibsau-timetable-frontend.git
cd sibsau-timetable-frontend

# Установка зависимостей (с использованием uv)
uv sync --frozen

# Запуск приложения
uv run python3 app/main.py
```

### Использование Docker (Dockploy, ...)

```bash
# Сборка образа
docker build -t sibsau-timetable-frontend .

# Запуск контейнера
docker run -d --name timetable-frontend \
  --env-file .env \
  sibsau-timetable-frontend
```

## Переменные окружения

Необходимо создать файл `.env` в корне проекта. Пример переменных окружения можно найти в `app/config.py`.

## 🌐 Развертывание на dokploy

1. Выберите Git-репозиторий для развертывания
2. Укажите переменные окружения (смотрите app/config.py)
3. Запустите сборку и развертывание

## 💻 Разработка

### Форматирование кода

```bash
# Форматирование кода с использованием ruff
uv run ruff format .
```

### Как внести свой вклад

1. Форкните репозиторий
2. Создайте ветку для новой функциональности (`git checkout -b feature/amazing-feature`)
3. Внесите свои изменения
4. Закоммитьте изменения (`git commit -m 'add some amazing feature'`)
5. Отправьте изменения в свой форк (`git push origin feature/amazing-feature`)
6. Откройте Pull Request в основной репозиторий

## О проекте

Проект разработан с любовью к Сибирскому государственному университету науки и технологий имени М.Ф. Решетнёва (СибГУ). ❤️
