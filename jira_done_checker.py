import base64
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta

import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('jira_done_checker.log'), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
JIRA_TOKEN = os.getenv("JIRA_TOKEN")
PACCHA_BOT_TOKEN = os.getenv("PACCHA_BOT_TOKEN")
PACHA_CHAT_ID = os.getenv("PACHA_CHAT_ID")
JIRA_URL = "https://jira.lamoda.ru"
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "30"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))


def validate_env() -> int:
    required = {
        "GITLAB_TOKEN": GITLAB_TOKEN,
        "JIRA_TOKEN": JIRA_TOKEN,
        "PACCHA_BOT_TOKEN": PACCHA_BOT_TOKEN,
        "PACHA_CHAT_ID": PACHA_CHAT_ID,
    }
    missing = [key for key, value in required.items() if not value]
    if missing:
        raise RuntimeError(f"Не заданы обязательные переменные окружения: {', '.join(missing)}")

    try:
        return int(PACHA_CHAT_ID)
    except ValueError as exc:
        raise RuntimeError("PACHA_CHAT_ID должен быть целым числом") from exc


def send_pacha_message(text, chat_id: int):
    try:
        logger.info("Отправка сообщения в Pachca")
        resp = requests.post(
            "https://api.pachca.com/api/shared/v1/messages",
            json={"message": {"entity_id": chat_id, "content": text}},
            headers={"Authorization": f"Bearer {PACCHA_BOT_TOKEN}", "Content-Type": "application/json"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        logger.info("Сообщение в Pachca успешно отправлено")
        return resp.json()
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения в Pachca: {e}")
        raise


def get_merged_mrs(chat_id: int):
    try:
        logger.info("Получение списка замерженных MR")
        r = requests.get(
            "https://gitlab.lamoda.tech/api/v4/merge_requests?state=merged&author_username=aleksey.kuryshev",
            headers={"PRIVATE-TOKEN": GITLAB_TOKEN},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        project_mrs = r.json()
        logger.info(f"Найдено {len(project_mrs)} замерженных MR")

        for mr in project_mrs:
            mr['_project_id'] = mr.get('project_id', 123)
        return project_mrs
    except requests.exceptions.ConnectionError as e:
        if "NameResolutionError" in str(e) or "Failed to resolve" in str(e):
            logger.error("Не удалось разрешить имя хоста gitlab.lamoda.tech. Завершение программы.")
            logger.error(f"Детали ошибки: {e}")
            try:
                send_pacha_message("❌ Не удалось подключиться к GitLab: ошибка разрешения DNS. Программа завершена.", chat_id)
            except Exception:
                pass
            sys.exit(1)
        logger.error(f"Ошибка подключения к GitLab: {e}")
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении списка замерженных MR: {e}")
        raise


def extract_jira_key_from_text(text):
    try:
        match = re.search(r'\b[A-Z]+-\d+\b', text)
        jira_key = match.group(0) if match else None
        logger.info(f"Найден ключ Jira: {jira_key}")
        return jira_key
    except Exception as e:
        logger.error(f"Ошибка при поиске ключа Jira в тексте: {e}")
        return None


def get_jira_issue_status(jira_key):
    try:
        logger.info(f"Проверка статуса задачи {jira_key} в Jira")
        url = f"{JIRA_URL}/rest/api/2/issue/{jira_key}?fields=status"

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {JIRA_TOKEN}",
        }
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

        if r.status_code == 401 and ":" in JIRA_TOKEN:
            logger.info("Bearer token не сработал, пробую Basic auth")
            auth_headers = {
                "Accept": "application/json",
                "Authorization": f"Basic {base64.b64encode(JIRA_TOKEN.encode()).decode()}",
            }
            r = requests.get(url, headers=auth_headers, timeout=REQUEST_TIMEOUT)

        r.raise_for_status()

        issue_data = r.json()
        status = issue_data["fields"]["status"]["name"]
        logger.info(f"Задача {jira_key} имеет статус: {status}")
        return status
    except Exception as e:
        logger.error(f"Ошибка при получении статуса задачи {jira_key}: {e}")
        return None


def get_mr_merged_time(mr_iid, project_id):
    try:
        logger.info(f"Получение времени мерджа для MR !{mr_iid}")
        r = requests.get(
            f"https://gitlab.lamoda.tech/api/v4/projects/{project_id}/merge_requests/{mr_iid}",
            headers={"PRIVATE-TOKEN": GITLAB_TOKEN},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        time.sleep(1)
        mr_data = r.json()
        return mr_data.get("merged_at")
    except Exception as e:
        logger.error(f"Ошибка при получении времени мерджа для MR !{mr_iid}: {e}")
        return None


def should_send_reminder(merged_at):
    try:
        if not merged_at:
            return False

        try:
            merged_time = datetime.strptime(merged_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            try:
                merged_time = datetime.strptime(merged_at, "%Y-%m-%dT%H:%M:%S.%f%z")
                merged_time = merged_time.replace(tzinfo=None)
            except ValueError:
                logger.error(f"Неизвестный формат времени мерджа: {merged_at}")
                return False

        now = datetime.now()
        time_diff = now - merged_time
        return time_diff > timedelta(hours=1)
    except Exception as e:
        logger.error(f"Ошибка при проверке времени для напоминания: {e}")
        return False


def main():
    chat_id = validate_env()
    logger.info("Запуск проверки задач в статусе Done...")
    send_pacha_message("Запуск проверки задач в статусе Done...", chat_id)
    sent_reminders = {}

    while True:
        try:
            merged_mrs = get_merged_mrs(chat_id)
            reminders_sent = []

            for mr in merged_mrs:
                iid = mr["iid"]
                title = mr["title"]
                project_id = mr['_project_id']
                merged_at = mr.get("merged_at")

                logger.info(f"Проверка MR !{iid}: {title}")
                jira_key = extract_jira_key_from_text(title + " " + mr.get("description", ""))

                if not jira_key:
                    logger.info(f"В MR !{iid} не найден ключ Jira, пропускаем")
                    continue

                if jira_key in sent_reminders:
                    logger.info(f"Для задачи {jira_key} уже отправлялось напоминание, пропускаем")
                    continue

                jira_status = get_jira_issue_status(jira_key)

                if jira_status and jira_status.lower() not in {"done", "cancelled"}:
                    logger.info(f"Задача {jira_key} не в статусе Done или Cancelled (текущий: {jira_status}), проверяем время")

                    if should_send_reminder(merged_at):
                        actual_merged_at = get_mr_merged_time(iid, project_id) or merged_at

                        try:
                            merged_time = datetime.strptime(actual_merged_at, "%Y-%m-%dT%H:%M:%S.%fZ")
                        except ValueError:
                            try:
                                merged_time = datetime.strptime(actual_merged_at, "%Y-%m-%dT%H:%M:%S.%f%z")
                                merged_time = merged_time.replace(tzinfo=None)
                            except ValueError:
                                merged_time = datetime.now() - timedelta(hours=2)

                        time_diff = datetime.now() - merged_time
                        hours_passed = int(time_diff.total_seconds() // 3600)

                        mr_link = mr.get("web_url", "")
                        jira_link = f"{JIRA_URL}/browse/{jira_key}"

                        message = f"⏰ Напоминание: задача {jira_key} не в статусе Done уже {hours_passed} часов после мерджа MR!\n"
                        message += f"MR был замержен: {merged_time.strftime('%Y-%m-%d %H:%M')}\n"
                        message += f"Текущий статус: {jira_status}\n"
                        message += f"Задача: {jira_link}\n"
                        if mr_link:
                            message += f"MR: {mr_link}\n"
                        message += "\nВозможно нужно перевести задачу в статус Done?"

                        logger.info(f"Отправка напоминания для задачи {jira_key}")
                        send_pacha_message(message, chat_id)

                        sent_reminders[jira_key] = datetime.now()
                        reminders_sent.append(jira_key)
                    else:
                        logger.info(f"Для задачи {jira_key} еще рано отправлять напоминание")
                else:
                    logger.info(f"Задача {jira_key} в статусе Done или Cancelled, пропускаем")

            if reminders_sent:
                logger.info(f"Отправлено {len(reminders_sent)} напоминаний: {', '.join(reminders_sent)}")
            else:
                logger.info("Напоминания не требуются")

        except Exception as e:
            logger.error(f"Ошибка в основной программе: {e}", exc_info=True)
            try:
                send_pacha_message(f"❌ Ошибка в проверке задач Jira: {e}", chat_id)
            except Exception as notify_error:
                logger.error(f"Не удалось отправить уведомление об ошибке: {notify_error}")

        logger.info(f"Следующая проверка через {CHECK_INTERVAL} секунд")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
