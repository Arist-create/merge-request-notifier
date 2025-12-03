# GitLab MR Monitor

Dockerized Python application for monitoring GitLab Merge Requests and Jira tasks, sending notifications to Pachca.

## Features

- **GitLab MR Monitor**: Мониторинг Merge Requests в GitLab
  - Отслеживание новых MR
  - Уведомления о достижении целевого количества аппрувов
  - Отслеживание новых комментариев
  - Напоминания о старых MR (>24 рабочих часов)

- **Jira Done Checker**: Проверка статуса задач в Jira
  - Мониторинг замерженных MR с привязанными задачами Jira
  - Напоминания о задачах не в статусе Done/Cancelled (>1 часа после мерджа)

## Configuration

Create `.env` file with your settings:

```bash
GITLAB_TOKEN=your_gitlab_token
CHECK_INTERVAL=30
PACCHA_BOT_TOKEN=your_pachca_token
PACHA_CHAT_ID=your_pachca_chat_id
```

## Quick Start

Запуск обоих сервисов:

```bash
docker-compose up -d
```

Запуск только GitLab мониторинга:

```bash
docker-compose up -d gitlab-merge-monitor
```

Запуск только Jira checker:

```bash
docker-compose up -d jira-done-checker
```

## Logs

Просмотр логов всех сервисов:

```bash
docker-compose logs -f
```

Просмотр логов конкретного сервиса:

```bash
docker-compose logs -f gitlab-merge-monitor
docker-compose logs -f jira-done-checker
```

Или проверка лог-файлов:

```bash
tail -f merge_monitor.log
tail -f jira_done_checker.log
```

## Stop

```bash
docker-compose down
```

## Services

- **gitlab-merge-monitor**: Основной сервис мониторинга MR
- **jira-done-checker**: Сервис проверки статуса задач Jira

## Environment Variables

- `GITLAB_TOKEN`: Токен доступа к GitLab API
- `CHECK_INTERVAL`: Интервал проверки в секундах (по умолчанию 30)
- `PACCHA_BOT_TOKEN`: Токен бота Pachca
- `PACHA_CHAT_ID`: ID чата в Pachca
- `JIRA_TOKEN`: Токен доступа к Jira API
