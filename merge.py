import requests, time, urllib3, re, logging, signal, sys, os, json
from datetime import datetime, timedelta
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', 
                   handlers=[logging.FileHandler('merge_monitor.log'), logging.StreamHandler()])
logger = logging.getLogger(__name__)

GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
TARGET_APPROVALS = 3
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "30"))
PACCHA_BOT_TOKEN = os.getenv("PACCHA_BOT_TOKEN")
PACHA_CHAT_ID = int(os.getenv("PACHA_CHAT_ID"))
JIRA_TOKEN = os.getenv("JIRA_TOKEN")
JIRA_URL = "https://jira.lamoda.ru"

# ----------------------------

def send_pacha_message(text):
    try:
        logger.info(f"Отправка сообщения в Pachca: {text}")
        resp = requests.post("https://api.pachca.com/api/shared/v1/messages", 
                           json={"message": {"entity_id": PACHA_CHAT_ID, "content": text}}, 
                           headers={"Authorization": f"Bearer {PACCHA_BOT_TOKEN}", "Content-Type": "application/json"})
        resp.raise_for_status()
        logger.info("Сообщение в Pachca успешно отправлено")
        return resp.json()
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения в Pachca: {e}")
        raise

def get_open_mrs():
    try:
        logger.info("Получение списка открытых MR")
        r = requests.get(f"https://gitlab.lamoda.tech/api/v4/merge_requests?state=opened&author_username=aleksey.kuryshev", 
                       headers={"PRIVATE-TOKEN": GITLAB_TOKEN}, verify=False)
        r.raise_for_status()
        project_mrs = r.json()
        logger.info(f"Найдено {len(project_mrs)} MR")
        # Сохраняем project_id для каждого MR
        for mr in project_mrs:
            mr['_project_id'] = mr.get('project_id', 123)  # Fallback к 123 если нет project_id
        return project_mrs
    except requests.exceptions.ConnectionError as e:
        if "NameResolutionError" in str(e) or "Failed to resolve" in str(e):
            logger.error(f"Не удалось разрешить имя хоста gitlab.lamoda.tech. Завершение программы.")
            logger.error(f"Детали ошибки: {e}")
            try:
                send_pacha_message("❌ Не удалось подключиться к GitLab: ошибка разрешения DNS. Программа завершена.")
            except:
                pass
            sys.exit(1)
        else:
            logger.error(f"Ошибка подключения к GitLab: {e}")
            raise
    except Exception as e:
        logger.error(f"Ошибка при получении списка MR: {e}")
        raise

def get_approval_count(mr_iid, project_id):
    try:
        logger.info(f"Получение количества аппрувов для MR !{mr_iid}")
        r = requests.get(f"https://gitlab.lamoda.tech/api/v4/projects/{project_id}/merge_requests/{mr_iid}/approvals", 
                       headers={"PRIVATE-TOKEN": GITLAB_TOKEN}, verify=False)
        r.raise_for_status()
        time.sleep(1)
        approvals = len(r.json().get("approved_by", []))
        logger.info(f"MR !{mr_iid} имеет {approvals} аппрувов")
        return approvals
    except Exception as e:
        logger.error(f"Ошибка при получении аппрувов для MR !{mr_iid}: {e}")
        raise

def get_mr_details(mr_iid, project_id):
    try:
        logger.info(f"Получение деталей MR !{mr_iid}")
        r = requests.get(f"https://gitlab.lamoda.tech/api/v4/projects/{project_id}/merge_requests/{mr_iid}", 
                       headers={"PRIVATE-TOKEN": GITLAB_TOKEN}, verify=False)
        r.raise_for_status()
        time.sleep(1)
        return r.json()
    except Exception as e:
        logger.error(f"Ошибка при получении деталей MR !{mr_iid}: {e}")
        raise

def get_mr_comments(mr_iid, project_id):
    try:
        logger.info(f"Получение комментариев для MR !{mr_iid}")
        r = requests.get(f"https://gitlab.lamoda.tech/api/v4/projects/{project_id}/merge_requests/{mr_iid}/notes?sort=desc", 
                       headers={"PRIVATE-TOKEN": GITLAB_TOKEN}, verify=False)
        r.raise_for_status()
        time.sleep(1)
        return r.json()
    except Exception as e:
        logger.error(f"Ошибка при получении комментариев MR !{mr_iid}: {e}")
        raise

def extract_jira_key_from_text(text):
    try:
        match = re.search(r'\b[A-Z]+-\d+\b', text)
        jira_key = match.group(0) if match else None
        logger.info(f"Найден ключ Jira: {jira_key} в тексте: {text[:100]}...")
        return jira_key
    except Exception as e:
        logger.error(f"Ошибка при поиске ключа Jira в тексте: {e}")
        return None

# Глобальный словарь для хранения напоминаний (в памяти)
sent_reminders = {}

def is_weekend(date):
    return date.weekday() >= 5  # 5=суббота, 6=воскресенье

def should_send_reminder(mr_key, created_at):
    try:
        now = datetime.now()
        
        # Парсим дату создания MR с учетом разных форматов
        try:
            mr_created = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            try:
                mr_created = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%f%z")
                # Убираем timezone, оставляем только время
                mr_created = mr_created.replace(tzinfo=None)
            except ValueError:
                logger.error(f"Неизвестный формат даты: {created_at}")
                return False
        
        # Считаем только рабочие часы (исключая выходные)
        work_hours_elapsed = 0
        current_time = mr_created
        
        while current_time < now:
            if not is_weekend(current_time):
                work_hours_elapsed += 1
            current_time += timedelta(hours=1)
        
        logger.info(f"MR {mr_key}: прошло {work_hours_elapsed} рабочих часов")
        
        # Проверяем что прошло больше 24 рабочих часов
        if work_hours_elapsed < 24:
            return False
            
        # Проверяем было ли напоминание за последние 24 часа
        last_reminder = sent_reminders.get(mr_key)
        if last_reminder:
            if now - last_reminder < timedelta(hours=24):
                return False
                
        return True
    except Exception as e:
        logger.error(f"Ошибка при проверке напоминания для MR {mr_key}: {e}")
        return False

def mark_reminder_sent(mr_key):
    sent_reminders[mr_key] = datetime.now()


def main():
    monitored, reported_mrs, shutdown_requested = {}, set(), False
    tracked_comments = {}
    mr_project_ids = {}  # Словарь для хранения project_id по MR iid

    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        if not shutdown_requested:
            shutdown_requested = True
            logger.info("Получен сигнал завершения, начинаю graceful shutdown")
            try:
                send_pacha_message("🛑 Мониторинг MR прекращен")
                logger.info("Отправлено уведомление о прекращении мониторинга")
            except Exception as e:
                logger.error(f"Ошибка при отправке уведомления о завершении: {e}")
            sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Запуск мониторинга MR...")
    send_pacha_message("Запуск мониторинга MR...")

    while True:
        try:
            logger.info("Начало новой итерации проверки")
            open_mrs = get_open_mrs()
            new_mrs = []

            for mr in open_mrs:
                iid = mr["iid"]
                title = mr["title"]
                project_id = mr['_project_id']
                mr_key = f"{iid}"
                
                # Сохраняем project_id для MR
                mr_project_ids[mr_key] = project_id
                
                if mr_key not in monitored:
                    monitored[mr_key] = 0
                    tracked_comments[mr_key] = set()
                    new_mrs.append(f"!{iid}: {title}")
                    logger.info(f"Новый MR !{iid} ({title}) добавлен в мониторинг, project_id: {project_id}")

            if new_mrs:
                new_mrs_text = "\n".join([f"🆕 {mr}" for mr in new_mrs])
                message = f"Новые MR добавлены в мониторинг:\n{new_mrs_text}"
                logger.info(f"Отправка уведомления о {len(new_mrs)} новых MR")
                send_pacha_message(message)

            logger.info(f"Проверка {len(monitored)} MR на аппрувы и комментарии")
            for mr_key in list(monitored.keys()):
                iid = int(mr_key)
                project_id = mr_project_ids.get(mr_key)  # Используем сохраненный project_id
                approvals = get_approval_count(iid, project_id)
                mr_details = get_mr_details(iid, project_id)
                title = mr_details["title"]
                print(f"MR !{iid} ({title}): {approvals} аппрувов")
                
                # Находим текущий MR в списке open_mrs
                current_mr = next((m for m in open_mrs if m["iid"] == iid), None)

                # Проверка новых комментариев
                try:
                    comments = get_mr_comments(iid, project_id)
                    current_comment_ids = {str(comment["id"]) for comment in comments}
                    new_comment_ids = current_comment_ids - tracked_comments.get(mr_key, set())
                    
                    if new_comment_ids:
                        for comment in comments:
                            if str(comment["id"]) in new_comment_ids:
                                # Пропускаем системные сообщения об аппрувах и другие системные уведомления
                                body_lower = comment["body"].lower()
                                is_system_message = (
                                    comment.get("system", False) or
                                    "approved" in body_lower or
                                    "changed" in body_lower or
                                    "requested changes" in body_lower or
                                    "approved this merge request" in body_lower or
                                    "unapproved" in body_lower or
                                    comment["author"]["username"] in ["gitlab-bot", "project_123_bot"]
                                )
                                
                                if is_system_message:
                                    logger.info(f"Пропуск системного сообщения в MR !{iid}: {comment['body'][:100]}")
                                    continue
                                    
                                author = comment["author"]["name"]
                                body = comment["body"][:200] + "..." if len(comment["body"]) > 200 else comment["body"]
                                message = f"💬 Новый комментарий в MR \"{title}\" от {author}:\n{body}"
                                logger.info(f"Отправка уведомления о новом комментарии в MR !{iid}")
                                send_pacha_message(message)
                        
                        tracked_comments[mr_key] = current_comment_ids
                        logger.info(f"Обновлен список отслеживаемых комментариев для MR !{iid}: {len(current_comment_ids)}")
                except Exception as e:
                    logger.error(f"Ошибка при проверке комментариев для MR !{iid}: {e}")

                if not any(m["iid"] == iid for m in open_mrs):
                    logger.info(f"MR !{iid} ({title}) больше не открыт, удален из мониторинга")
                    monitored.pop(mr_key, None)
                    tracked_comments.pop(mr_key, None)
                    mr_project_ids.pop(mr_key, None)  # Удаляем и project_id
                    continue

                if approvals >= TARGET_APPROVALS and mr_key not in reported_mrs:
                    logger.info(f"MR !{iid} ({title}) достиг {approvals} аппрувов, обработка уведомления")
                    
                    mr_details = get_mr_details(iid, project_id)
                    jira_key = extract_jira_key_from_text((mr_details.get("title", "") or "") + " " + (mr_details.get("description", "") or ""))
                    
                    jira_link = f"\nЗадача: {JIRA_URL}/browse/{jira_key}" if jira_key else ""
                    mr_link = f"\nMR: {mr_details.get('web_url', '')}" if mr_details.get('web_url') else ""
                    message = f"🎉 MR \"{title}\" получил {approvals} аппрува!{jira_link}{mr_link}"
                    logger.info(f"Отправка уведомления о достижении целевых аппрувов: {message}")
                    send_pacha_message(message)
                    reported_mrs.add(mr_key)

                # Проверка на напоминание о старом MR
                if current_mr and should_send_reminder(mr_key, current_mr["created_at"]) and approvals < TARGET_APPROVALS:
                    logger.info(f"MR !{iid} ({title}) старше 24 часов и имеет {approvals} аппрувов, отправка напоминания")
                    mr_details = get_mr_details(iid, project_id)
                    jira_key = extract_jira_key_from_text((mr_details.get("title", "") or "") + " " + (mr_details.get("description", "") or ""))
                    
                    jira_link = f"\nЗадача: {JIRA_URL}/browse/{jira_key}" if jira_key else ""
                    mr_link = f"\nMR: {mr_details.get('web_url', '')}" if mr_details.get('web_url') else ""
                    
                    # Парсим дату создания с учетом разных форматов
                    try:
                        created_time = datetime.strptime(current_mr["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    except ValueError:
                        try:
                            created_time = datetime.strptime(current_mr["created_at"], "%Y-%m-%dT%H:%M:%S.%f%z")
                            # Конвертируем UTC время в локальное время
                            if created_time.tzinfo is not None:
                                logger.info(f"Оригинальная дата: {current_mr['created_at']}, спарсено: {created_time}")
                                created_time = created_time.astimezone().replace(tzinfo=None)
                                logger.info(f"После конвертации: {created_time}")
                        except ValueError:
                            logger.error(f"Неизвестный формат даты: {current_mr['created_at']}")
                            continue
                    
                    # Считаем только рабочие часы (исключая выходные)
                    work_hours_elapsed = 0
                    current_time = created_time
                    
                    while current_time < now:
                        if not is_weekend(current_time):
                            work_hours_elapsed += 1
                        current_time += timedelta(hours=1)
                    
                    logger.info(f"Текущее время: {now}, время создания: {created_time}")
                    logger.info(f"Прошло {work_hours_elapsed} рабочих часов")
                    
                    message = f"⏰ MR \"{title}\" ждет уже {work_hours_elapsed} рабочих часов! Можно сделать напоминание.{jira_link}{mr_link}"
                    logger.info(f"Отправка напоминания о старом MR: {message}")
                    send_pacha_message(message)
                    mark_reminder_sent(mr_key)

            logger.info(f"Итерация завершена, следующая проверка через {CHECK_INTERVAL} секунд")

        except Exception as e:
            logger.error(f"Ошибка в основной петле мониторинга: {e}", exc_info=True)
            try:
                send_pacha_message(f"Ошибка: {e}")
            except Exception as notify_error:
                logger.error(f"Не удалось отправить уведомление об ошибке: {notify_error}")

        if shutdown_requested:
            break
            
        for _ in range(CHECK_INTERVAL):
            if shutdown_requested:
                break
            time.sleep(1)

if __name__ == "__main__":
    main()