"""
Конфигурация для FBref scraper
"""

# Настройки задержек (в секундах)
MIN_REQUEST_DELAY = 5.0  # Минимальная задержка между запросами
MAX_REQUEST_DELAY = 10.0  # Максимальная задержка между запросами
LONG_PAUSE_INTERVAL = 5  # Каждые N запросов делать длинную паузу
LONG_PAUSE_MIN = 15.0  # Минимальная длинная пауза
LONG_PAUSE_MAX = 25.0  # Максимальная длинная пауза

# Настройки повторных попыток
MAX_RETRIES = 3  # Максимальное количество повторных попыток при ошибках
RETRY_BASE_DELAY = 10  # Базовая задержка для экспоненциального backoff

# Настройки базы данных
DB_PATH = 'sqlite:///football_data.db'
SEASON = '2023-2024'
COMPETITION = 'Premier League'

# URL конфигурация
# В config.py измените URL на конкретный сезон:
PREMIER_LEAGUE_URL = '/en/comps/9/2023-2024/2023-2024-Premier-League-Stats'


# Режим отладки
DEBUG_MODE = False  # Если True, парсит только первую команду (для быстрого теста)
DEBUG_TEAM_LIMIT = 20  # Количество команд для отладки

# ВАЖНО: После успешного теста установите DEBUG_MODE = False для полного парсинга!

