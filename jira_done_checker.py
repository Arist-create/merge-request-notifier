import requests, time, urllib3, re, logging, sys, os
from datetime import datetime, timedelta
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', 
                   handlers=[logging.FileHandler('jira_done_checker.log'), logging.StreamHandler()])
logger = logging.getLogger(__name__)

GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
JIRA_TOKEN = os.getenv("JIRA_TOKEN")
PACCHA_BOT_TOKEN = os.getenv("PACCHA_BOT_TOKEN")
PACHA_CHAT_ID = int(os.getenv("PACHA_CHAT_ID"))
JIRA_URL = "https://jira.lamoda.ru"
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "30"))

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

def get_merged_mrs():
    try:
        logger.info("Получение списка замерженных MR")
        r = requests.get(f"https://gitlab.lamoda.tech/api/v4/merge_requests?state=merged&author_username=aleksey.kuryshev", 
                       headers={"PRIVATE-TOKEN": GITLAB_TOKEN}, verify=False)
        r.raise_for_status()
        project_mrs = r.json()
        logger.info(f"Найдено {len(project_mrs)} замерженных MR")
        
        # Сохраняем project_id для каждого MR
        for mr in project_mrs:
            mr['_project_id'] = mr.get('project_id', 123)
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
        logger.error(f"Ошибка при получении списка замерженных MR: {e}")
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

def get_jira_issue_status(jira_key):
    try:
        logger.info(f"Проверка статуса задачи {jira_key} в Jira")
        url = f"{JIRA_URL}/rest/api/2/issue/{jira_key}?fields=status"
        
        # Пробуем сначала Bearer token
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {JIRA_TOKEN}"
        }
        
        r = requests.get(url, headers=headers, verify=False)
        
        # Если Bearer не сработал, пробуем Basic auth
        if r.status_code == 401:
            logger.info("Bearer token не сработал, пробую Basic auth")
            import base64
            # Для Basic auth нужно использовать email:api_token
            # Предполагаем что JIRA_TOKEN это email:api_token
            if ":" in JIRA_TOKEN:
                auth_headers = {
                    "Accept": "application/json",
                    "Authorization": f"Basic {base64.b64encode(JIRA_TOKEN.encode()).decode()}"
                }
                r = requests.get(url, headers=auth_headers, verify=False)
        
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
        r = requests.get(f"https://gitlab.lamoda.tech/api/v4/projects/{project_id}/merge_requests/{mr_iid}", 
                       headers={"PRIVATE-TOKEN": GITLAB_TOKEN}, verify=False)
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
            
        # Парсим время мерджа
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
        
        # Проверяем что прошло больше 1 часа
        return time_diff > timedelta(hours=1)
    except Exception as e:
        logger.error(f"Ошибка при проверке времени для напоминания: {e}")
        return False

def main():
    logger.info("Запуск проверки задач в статусе Done...")
    send_pacha_message("Запуск проверки задач в статусе Done...")
    # Глобальный словарь для хранения отправленных уведомлений
    sent_reminders = {}
    while True:
        try:
            merged_mrs = get_merged_mrs()
            reminders_sent = []
            
            for mr in merged_mrs:
                iid = mr["iid"]
                title = mr["title"]
                project_id = mr['_project_id']
                merged_at = mr.get("merged_at")
                
                logger.info(f"Проверка MR !{iid}: {title}")
                
                # Извлекаем ключ Jira из заголовка и описания
                jira_key = extract_jira_key_from_text(title + " " + (mr.get("description", "") or ""))
                
                if not jira_key:
                    logger.info(f"В MR !{iid} не найден ключ Jira, пропускаем")
                    continue
                
                # Проверяем не отправляли ли уже напоминание для этой задачи
                if jira_key in sent_reminders:
                    logger.info(f"Для задачи {jira_key} уже отправлялось напоминание, пропускаем")
                    continue
                
                # Проверяем статус задачи в Jira
                jira_status = get_jira_issue_status(jira_key)
                
                if jira_status and jira_status.lower() != "done" and jira_status.lower() != "cancelled":
                    logger.info(f"Задача {jira_key} не в статусе Done или Cancelled (текущий: {jira_status}), проверяем время")
                    
                    if should_send_reminder(merged_at):
                        # Получаем точное время мерджа
                        actual_merged_at = get_mr_merged_time(iid, project_id) or merged_at
                        
                        try:
                            merged_time = datetime.strptime(actual_merged_at, "%Y-%m-%dT%H:%M:%S.%fZ")
                        except ValueError:
                            try:
                                merged_time = datetime.strptime(actual_merged_at, "%Y-%m-%dT%H:%M:%S.%f%z")
                                merged_time = merged_time.replace(tzinfo=None)
                            except ValueError:
                                merged_time = datetime.now() - timedelta(hours=2)  # Fallback
                        
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
                        message += f"\nВозможно нужно перевести задачу в статус Done?"
                        
                        logger.info(f"Отправка напоминания для задачи {jira_key}")
                        send_pacha_message(message)
                        
                        # Отмечаем что отправили напоминание
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
                send_pacha_message(f"❌ Ошибка в проверке задач Jira: {e}")
            except Exception as notify_error:
                logger.error(f"Не удалось отправить уведомление об ошибке: {notify_error}")

        logger.info(f"Итерация завершена, следующая проверка через {CHECK_INTERVAL} секунд")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
