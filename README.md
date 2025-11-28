# GitLab MR Monitor

Dockerized Python application for monitoring GitLab Merge Requests and sending notifications to Pachca.

## Configuration

Create `.env` file with your settings:

```bash
GITLAB_TOKEN=your_gitlab_token
CHECK_INTERVAL=30
PACCHA_BOT_TOKEN=your_pachca_token
PACHA_CHAT_ID=your_pachca_chat_id
JIRA_TOKEN=your_jira_token
```

## Quick Start

```bash
docker-compose up -d
```

## Logs

View logs:

```bash
docker-compose logs -f
```

Or check the log file:

```bash
tail -f merge_monitor.log
```

## Stop

```bash
docker-compose down
```
