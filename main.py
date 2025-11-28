import logging
import pandas as pd
from sqlalchemy.orm import Session
from db import init_db, Team, Player, Match, SquadStat, PlayerStat
from scraper import FBRefScraper
from config import PREMIER_LEAGUE_URL, SEASON, COMPETITION, DEBUG_MODE, DEBUG_TEAM_LIMIT

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def process_team(session: Session, team_data: dict):
    """
    Upserts team data into the database.
    """
    team = session.query(Team).filter_by(fbref_id=team_data['fbref_id']).first()
    if not team:
        team = Team(
            fbref_id=team_data['fbref_id'],
            name=team_data['name'],
            url=team_data['url']
        )
        session.add(team)
        session.commit()
    return team

def process_matches(session: Session, team: Team, df: pd.DataFrame):
    """
    Processes match logs dataframe and stores in DB.
    """
    if df is None or df.empty:
        return

    # Filter for valid matches (Result is not null usually means played)
    # FBref match logs include future fixtures too.
    
    for _, row in df.iterrows():
        # Basic validation
        if pd.isna(row.get('Date')) or row.get('Comp') != 'Premier League':
            continue

        # Parse scores
        gf = row.get('GF')
        ga = row.get('GA')
        
        # Handle future matches where scores are NaN
        if pd.isna(gf): gf = None
        else: gf = int(gf) if str(gf).isdigit() else None
            
        if pd.isna(ga): ga = None
        else: ga = int(ga) if str(ga).isdigit() else None

        attendance = row.get('Attendance')
        if pd.isna(attendance): attendance = None
        else: attendance = int(str(attendance).replace(',', '')) if str(attendance).replace(',', '').isdigit() else None

        # Create or Update Match
        # Identify match by Date and Teams. 
        # Since we iterate by team, we might process the same match twice (once for home, once for away).
        # We should check if it exists.
        
        # This is a simplification. Ideally we resolve opponent to a Team ID.
        # For now, we will just store the match context from this team's perspective 
        # or try to avoid duplicates if we had a robust unique ID (like FBref match ID).
        # FBref match logs have a 'Match Report' link which contains the Match ID.
        
        # Let's try to extract opponent name
        opponent_name = row.get('Opponent')
        
        # Check duplicate by date and team (simple heuristic)
        existing = session.query(Match).filter_by(
            date=pd.to_datetime(row['Date']), 
            home_team_id=team.id if row.get('Venue') == 'Home' else None
        ).first()
        
        if not existing:
            # We only insert if we are the home team to avoid double counting?
            # Or just insert everything and handle duplicates later. 
            # Let's insert if Venue is Home for now to ensure unique matches in DB, 
            # or if we want full logs, we treat them as "Team Match Performance".
            # The DB model 'Match' looks like a single event.
            
            if row.get('Venue') == 'Home':
                match = Match(
                    date=pd.to_datetime(row['Date']),
                    home_team_id=team.id,
                    # away_team_id = ... need to lookup opponent by name
                    home_score=gf,
                    away_score=ga,
                    competition=row.get('Comp'),
                    round=row.get('Round'),
                    venue=row.get('Venue'),
                    attendance=attendance
                )
                session.add(match)

    session.commit()

def process_squad_stats(session: Session, team: Team, stats_data: dict):
    """
    Processes squad and player stats.
    """
    if not stats_data:
        logger.warning(f"⚠️  Нет данных статистики для команды {team.name}")
        return

    # Process Squad Stats (Standard Table)
    squad_tables = stats_data.get('squad', {})
    player_tables = stats_data.get('players', {})
    
    logger.info(f"📊 Найдено таблиц команды: {len(squad_tables)}, игроков: {len(player_tables)}")
    
    # Debug: выводим все найденные таблицы
    if squad_tables:
        logger.info(f"   Таблицы команды: {list(squad_tables.keys())}")
    if player_tables:
        logger.info(f"   Таблицы игроков: {list(player_tables.keys())[:5]}...")
    
    # Find standard table
    standard_df = None
    for table_id, df in squad_tables.items():
        if 'standard' in table_id.lower() and 'squad' in table_id.lower():
            standard_df = df
            logger.info(f"✅ Найдена таблица статистики: {table_id}")
            break
            
    if standard_df is not None and not standard_df.empty:
        logger.info(f"📋 Обработка статистики команды, строк: {len(standard_df)}, колонок: {len(standard_df.columns)}")
        
        # Usually row 0 is the team stats
        row = standard_df.iloc[0]
        
        # Flatten columns if multi-index
        if isinstance(standard_df.columns, pd.MultiIndex):
            new_cols = []
            for col in standard_df.columns:
                if isinstance(col, tuple):
                    # Объединяем непустые части
                    parts = [str(c).strip() for c in col if str(c).strip() and not str(c).startswith('Unnamed')]
                    new_cols.append('_'.join(parts) if parts else str(col[-1]))
                else:
                    new_cols.append(str(col))
            standard_df.columns = new_cols
            row = standard_df.iloc[0]

        # Debug: показываем колонки
        logger.info(f"   Колонки: {list(standard_df.columns)[:10]}...")
        
        # Extract basic stats
        gls = None
        poss = None
        
        for col in standard_df.columns:
            col_str = str(col)
            if 'Gls' in col_str and gls is None: 
                gls = row[col]
                logger.info(f"   Найдено голов: {gls} (колонка: {col})")
            if 'Poss' in col_str and poss is None: 
                poss = row[col]
                logger.info(f"   Найдено владение: {poss} (колонка: {col})")

        # Upsert SquadStat
        existing_stat = session.query(SquadStat).filter_by(
            team_id=team.id, season=SEASON, competition=COMPETITION
        ).first()
        
        if not existing_stat:
            try:
                goals_for_val = int(float(gls)) if gls is not None and str(gls).replace('.','').isdigit() else 0
                poss_val = float(str(poss).replace('%','')) if poss is not None else 0.0
                
                stat = SquadStat(
                    team_id=team.id,
                    season=SEASON,
                    competition=COMPETITION,
                    goals_for=goals_for_val,
                    possession=poss_val
                )
                session.add(stat)
                logger.info(f"✅ Статистика команды сохранена: голы={goals_for_val}, владение={poss_val}%")
            except Exception as e:
                logger.error(f"❌ Ошибка при сохранении статистики: {e}")
        else:
            logger.info(f"ℹ️  Статистика команды уже существует")
    else:
        logger.warning(f"⚠️  Таблица статистики команды не найдена")
    
    # Process player stats
    process_player_stats(session, team, player_tables)

def process_player_stats(session: Session, team: Team, player_tables: dict):
    """Обрабатывает статистику игроков"""
    if not player_tables:
        return
    
    # Ищем таблицу со стандартной статистикой игроков
    standard_table = None
    for table_id, df in player_tables.items():
        if 'standard' in table_id.lower() and 'stats_' in table_id.lower():
            standard_table = df
            logger.info(f"✅ Найдена таблица игроков: {table_id}, строк: {len(df)}")
            break
    
    if standard_table is None or standard_table.empty:
        logger.warning("⚠️  Таблица статистики игроков не найдена")
        return
    
    # Flatten multi-index columns
    if isinstance(standard_table.columns, pd.MultiIndex):
        # Правильно обрабатываем MultiIndex - берем последний уровень или объединяем
        new_cols = []
        for col in standard_table.columns:
            if isinstance(col, tuple):
                # Объединяем непустые части
                parts = [str(c).strip() for c in col if str(c).strip() and not str(c).startswith('Unnamed')]
                new_cols.append('_'.join(parts) if parts else str(col[-1]))
            else:
                new_cols.append(str(col))
        standard_table.columns = new_cols
    
    # Debug: показываем первые колонки
    logger.info(f"   Колонки таблицы игроков: {list(standard_table.columns)[:10]}...")
    
    players_added = 0
    
    for idx, row in standard_table.iterrows():
        try:
            # Пропускаем заголовки и итоговые строки
            # Ищем колонку с именем игрока (может быть 'Player', 'player', или с префиксом)
            player_name = None
            for col in standard_table.columns:
                if 'player' in str(col).lower() and 'Player' in str(col):
                    player_name = row[col]
                    break
            
            if player_name is None:
                # Пробуем стандартные варианты
                player_name = row.get('Player') or row.get('player')
            
            player_name_str = str(player_name).strip()
            
            # Пропускаем заголовки, итоговые строки и пустые значения
            if (pd.isna(player_name) or 
                player_name_str == '' or 
                player_name_str == 'Player' or
                'Squad Total' in player_name_str or
                'Total' in player_name_str):
                continue
            
            # Создаем или находим игрока
            player = session.query(Player).filter_by(
                name=str(player_name).strip(),
                team_id=team.id
            ).first()
            
            if not player:
                player = Player(
                    fbref_id=f"{team.fbref_id}_{idx}",  # Временный ID
                    name=str(player_name).strip(),
                    team_id=team.id
                )
                session.add(player)
                session.flush()  # Получаем ID
            
            # Проверяем существующую статистику
            existing_stat = session.query(PlayerStat).filter_by(
                player_id=player.id,
                season=SEASON,
                competition=COMPETITION
            ).first()
            
            if not existing_stat:
                # Извлекаем статистику - ищем колонки с нужными данными
                goals = 0
                assists = 0
                minutes = 0
                
                for col in standard_table.columns:
                    col_str = str(col)
                    # Ищем голы (Gls, но не xG, npxG и т.д.)
                    if col_str.endswith('Gls') and 'x' not in col_str.lower() and 'np' not in col_str.lower():
                        try:
                            goals = int(float(row[col])) if not pd.isna(row[col]) else 0
                        except:
                            pass
                    # Ищем ассисты
                    elif col_str.endswith('Ast') and 'x' not in col_str.lower():
                        try:
                            assists = int(float(row[col])) if not pd.isna(row[col]) else 0
                        except:
                            pass
                    # Ищем минуты
                    elif 'Min' in col_str and 'per' not in col_str.lower():
                        try:
                            min_val = str(row[col]).replace(',', '')
                            minutes = int(float(min_val)) if min_val.replace('.','').isdigit() else 0
                        except:
                            pass
                
                stat = PlayerStat(
                    player_id=player.id,
                    season=SEASON,
                    competition=COMPETITION,
                    goals=goals,
                    assists=assists,
                    minutes=minutes
                )
                session.add(stat)
                players_added += 1
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке игрока: {e}")
            continue
    
    if players_added > 0:
        logger.info(f"✅ Добавлено статистики игроков: {players_added}")
    else:
        logger.warning("⚠️  Не удалось добавить статистику игроков")

def main():
    logger.info("=" * 60)
    logger.info("🚀 Запуск FBref ETL процесса")
    logger.info("=" * 60)
    
    # 1. Init DB
    SessionLocal = init_db()
    session = SessionLocal()
    logger.info("✅ База данных инициализирована")
    
    # 2. Init Scraper
    scraper = FBRefScraper()
    logger.info("✅ Скрапер инициализирован")
    
    # 3. Get League Teams
    logger.info(f"📋 Получение списка команд из {PREMIER_LEAGUE_URL}...")
    teams = scraper.get_league_teams(PREMIER_LEAGUE_URL)
    
    if not teams:
        logger.error("❌ Не удалось получить список команд. Выход.")
        return

    logger.info(f"✅ Найдено {len(teams)} команд")
    
    # Debug mode - только первая команда
    if DEBUG_MODE:
        teams = teams[:DEBUG_TEAM_LIMIT]
        logger.info(f"🐛 Режим отладки: обрабатываем только {DEBUG_TEAM_LIMIT} команду(ы)")
    
    # 4. Process each team
    for idx, team_info in enumerate(teams, 1):
        logger.info("")
        logger.info("=" * 60)
        logger.info(f"⚽ [{idx}/{len(teams)}] Обработка команды: {team_info['name']}")
        logger.info("=" * 60)
        
        try:
            # Upsert Team
            team = process_team(session, team_info)
            logger.info(f"✅ Команда сохранена в БД")
            
            # Get Match Logs
            logger.info("📊 Получение логов матчей...")
            match_df = scraper.get_match_logs(team_info['url'])
            process_matches(session, team, match_df)
            logger.info(f"✅ Матчи обработаны")
            
            # Get Stats
            logger.info("📈 Получение статистики команды...")
            stats = scraper.get_team_stats(team_info['url'])
            process_squad_stats(session, team, stats)
            logger.info(f"✅ Статистика обработана")
            
            # Checkpointing
            session.commit()
            logger.info(f"💾 Данные команды {team_info['name']} сохранены")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке {team_info['name']}: {e}")
            session.rollback()
            continue
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("🎉 ETL процесс завершен успешно!")
    logger.info("=" * 60)

if __name__ == '__main__':
    main()

