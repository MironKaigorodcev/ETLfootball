import requests
import time
import random
import pandas as pd
from bs4 import BeautifulSoup
import logging
from config import (
    MIN_REQUEST_DELAY, MAX_REQUEST_DELAY, 
    LONG_PAUSE_INTERVAL, LONG_PAUSE_MIN, LONG_PAUSE_MAX,
    MAX_RETRIES, RETRY_BASE_DELAY
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FBRefScraper:
    def __init__(self, base_url='https://fbref.com'):
        self.base_url = base_url
        self.session = requests.Session()
        
        # Список реалистичных User-Agent'ов (последние версии браузеров)
        self.user_agents = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        
        self._update_headers()
        self.last_request_time = 0
        self.request_delay_min = MIN_REQUEST_DELAY
        self.request_delay_max = MAX_REQUEST_DELAY
        self.request_count = 0

    def _update_headers(self):
        """Обновляет заголовки с случайным User-Agent для имитации разных пользователей"""
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'DNT': '1',
        })

    def _wait_for_rate_limit(self):
        """Имитация человеческого поведения с случайными задержками"""
        elapsed = time.time() - self.last_request_time
        
        # Случайная задержка между запросами
        delay = random.uniform(self.request_delay_min, self.request_delay_max)
        
        # Каждые N запросов делаем более длинную паузу (имитация чтения страницы)
        if self.request_count > 0 and self.request_count % LONG_PAUSE_INTERVAL == 0:
            delay = random.uniform(LONG_PAUSE_MIN, LONG_PAUSE_MAX)
            logger.info(f"🕐 Длинная пауза для имитации чтения страницы...")
        
        if elapsed < delay:
            sleep_time = delay - elapsed
            logger.info(f"⏳ Ожидание {sleep_time:.1f}с перед следующим запросом...")
            time.sleep(sleep_time)

    def get(self, url, retry_count=0):
        """Выполняет GET запрос с имитацией человеческого поведения"""
        self._wait_for_rate_limit()
        
        if not url.startswith('http'):
            url = self.base_url + url
        
        # Периодически меняем User-Agent
        if self.request_count % 3 == 0:
            self._update_headers()
        
        logger.info(f"🌐 Запрос: {url}...")
        
        try:
            # Добавляем Referer для имитации перехода по ссылкам
            headers = self.session.headers.copy()
            if self.request_count > 0:
                headers['Referer'] = self.base_url
            
            response = self.session.get(url, headers=headers, timeout=30)
            
            if response.status_code == 403:
                logger.warning(f"⚠️  Получен 403 Forbidden (попытка {retry_count + 1}/{MAX_RETRIES})")
                
                if retry_count < MAX_RETRIES:
                    # Экспоненциальная задержка при 403
                    wait_time = (2 ** retry_count) * RETRY_BASE_DELAY + random.uniform(5, 15)
                    logger.info(f"⏰ Ожидание {wait_time:.1f}с перед повторной попыткой...")
                    time.sleep(wait_time)
                    
                    # Меняем User-Agent перед повтором
                    self._update_headers()
                    return self.get(url, retry_count + 1)
                else:
                    logger.error(f"❌ Не удалось получить доступ после {MAX_RETRIES} попыток")
                    return None
            
            response.raise_for_status()
            self.last_request_time = time.time()
            self.request_count += 1
            
            logger.info(f"✅ Успешно получено (статус {response.status_code})")
            return response
            
        except requests.exceptions.Timeout:
            logger.error(f"⏱️  Timeout при запросе {url}")
            if retry_count < MAX_RETRIES:
                return self.get(url, retry_count + 1)
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Ошибка при запросе {url}: {e}")
            return None

    def parse_page(self, url):
        """Парсит страницу с имитацией человеческого поведения"""
        response = self.get(url)
        if response:
            # Имитация времени на "чтение" страницы
            read_time = random.uniform(1.0, 3.0)
            logger.info(f"📖 Обработка страницы ({read_time:.1f}с)...")
            time.sleep(read_time)
            return BeautifulSoup(response.content, 'lxml')
        return None

    def get_league_teams(self, league_url):
        """
        Parses the league page to extract team names and links.
        """
        soup = self.parse_page(league_url)
        if not soup:
            return []

        teams = []
        # Look for the standings table. ID usually contains 'overall'
        # We can look for any table that has 'overall' in id, or class 'stats_table'
        # Premier League table is usually the first one or has specific ID.
        
        # More robust: look for 'stats_table' class
        tables = soup.find_all('table', class_='stats_table')
        
        # Usually the first table is the league table
        if not tables:
            logger.error("No stats tables found on league page.")
            return []
            
        # Use the first table found (Standing)
        table = tables[0]
        
        # Iterate rows
        for row in table.find_all('tr'):
            # Team is usually in a 'td' or 'th' with data-stat='team' or 'squad'
            team_cell = row.find('td', {'data-stat': 'squad'}) or row.find('th', {'data-stat': 'squad'}) or \
                        row.find('td', {'data-stat': 'team'}) or row.find('th', {'data-stat': 'team'})
            
            if team_cell:
                link = team_cell.find('a')
                if link:
                    href = link.get('href')
                    name = link.text.strip()
                    # href is like /en/squads/18bb7c10/Arsenal-Stats
                    # fbref_id is 18bb7c10
                    parts = href.split('/')
                    if 'squads' in parts:
                        idx = parts.index('squads')
                        if len(parts) > idx + 1:
                            fbref_id = parts[idx+1]
                            teams.append({
                                'name': name,
                                'url': href,
                                'fbref_id': fbref_id
                            })
        
        return teams

    def get_team_stats(self, team_url):
        """
        Fetches the team page and extracts both squad and player statistics.
        Returns a dict with 'squad_stats' and 'player_stats'.
        """
        if not team_url.startswith('http'):
            team_url = self.base_url + team_url
            
        logger.info(f"Scraping stats from {team_url}...")
        
        # We need the raw HTML for pandas to parse tables, but we might need soup for some metadata
        # pandas read_html can take a URL but we want to use our session with rate limiting.
        response = self.get(team_url)
        if not response:
            return None

        # Parse with pandas
        try:
            # FBref often puts tables in comments to save bandwidth on initial load.
            # We need to remove comments to see all tables.
            content = response.content.decode('utf-8')
            content = content.replace('<!--', '').replace('-->', '')
            
            # Use StringIO to avoid FutureWarning
            from io import StringIO
            dfs = pd.read_html(StringIO(content))
            
            # We also parse with BS4 to map table IDs to dataframes if needed, 
            # but read_html usually gets all visible tables.
            # The challenge is identifying which table is which.
            # Usually tables have captions or we can identify by columns.
            
            # For now, let's try to identify 'Standard Stats' and 'Defense' as requested.
            # We will store them in a dictionary keyed by table type.
            
            stats_data = {
                'squad': {},
                'players': {}
            }
            
            # This is a heuristic mapping. FBref tables are numerous.
            # We will look for specific keywords in table attributes if we could, 
            # but read_html just gives list of DFs.
            # We can re-parse with BS4 to get table IDs, then map to DFs.
            
            soup = BeautifulSoup(content, 'lxml')
            tables = soup.find_all('table')
            
            logger.info(f"   Всего таблиц на странице: {len(tables)}")
            
            # Debug: выводим все ID таблиц
            all_table_ids = [t.get('id', 'NO_ID') for t in tables if t.get('id')]
            logger.debug(f"   ID всех таблиц: {all_table_ids}")
            
            for table in tables:
                table_id = table.get('id', '')
                if not table_id:
                    continue
                    
                # Convert this specific table to df
                try:
                    from io import StringIO
                    df_list = pd.read_html(StringIO(str(table)))
                    if not df_list:
                        continue
                    df = df_list[0]
                except ValueError:
                    continue

                # Check if it's a squad or player table
                # Squad tables usually have 'squads' in ID or are small (2 rows: For, Against).
                # Player tables usually have 'player' in ID or many rows.
                
                # Example IDs: 
                # stats_standard_9 (Standard Stats for players)
                # stats_squads_standard_for (Standard Stats for squad)
                # stats_defense_9 (Defensive actions for players)
                # stats_squads_defense_for (Defensive actions for squad)
                
                # Пропускаем таблицы матчей
                if 'matchlogs' in table_id or 'fixtures' in table_id or 'scores' in table_id:
                    continue
                
                # Таблицы команд содержат 'squads' в ID
                if 'squads' in table_id.lower():
                    stats_data['squad'][table_id] = df
                    logger.debug(f"   Найдена таблица команды: {table_id}")
                # Таблицы игроков содержат 'stats_' но НЕ 'squads'
                elif 'stats_' in table_id and 'squads' not in table_id.lower():
                    stats_data['players'][table_id] = df
                    logger.debug(f"   Найдена таблица игроков: {table_id}")
            
            return stats_data
            
        except Exception as e:
            logger.error(f"Error parsing tables from {team_url}: {e}")
            return None

    def get_match_logs(self, team_url):
        """
        Extracts match logs (Scores & Fixtures) from the team page.
        """
        if not team_url.startswith('http'):
            team_url = self.base_url + team_url
            
        logger.info(f"Scraping match logs from {team_url}...")
        response = self.get(team_url)
        if not response:
            return None
            
        try:
            content = response.content.decode('utf-8')
            content = content.replace('<!--', '').replace('-->', '')
            
            soup = BeautifulSoup(content, 'lxml')
            
            # Look for table with id matching 'matchlogs_for'
            # It might be 'matchlogs_for' or similar.
            table = soup.find('table', id=lambda x: x and 'matchlogs' in x)
            
            if table:
                from io import StringIO
                df_list = pd.read_html(StringIO(str(table)))
                if df_list:
                    return df_list[0]
            
            logger.warning("Match logs table not found.")
            return None
            
        except Exception as e:
            logger.error(f"Error parsing match logs: {e}")
            return None

