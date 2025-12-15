"""
Примеры запросов к базе данных football_data.db
"""

import sqlite3
import pandas as pd
from db import init_db, Team, Player, Match, SquadStat, PlayerStat
from sqlalchemy import func, desc

def connect_db():
    """Подключение к базе данных"""
    SessionLocal = init_db()
    return SessionLocal()

def query_all_teams():
    """Получить все команды"""
    session = connect_db()
    teams = session.query(Team).all()
    
    print("\n" + "=" * 60)
    print("⚽ ВСЕ КОМАНДЫ В БАЗЕ ДАННЫХ")
    print("=" * 60)
    
    for team in teams:
        print(f"ID: {team.id:2d} | {team.name:20s} | FBRef ID: {team.fbref_id}")
    
    print(f"\nВсего команд: {len(teams)}")
    return teams

def query_team_matches(team_name="Arsenal"):
    """Получить все матчи конкретной команды"""
    session = connect_db()
    
    team = session.query(Team).filter(Team.name.like(f"%{team_name}%")).first()
    
    if not team:
        print(f"❌ Команда '{team_name}' не найдена")
        return
    
    matches = session.query(Match).filter(
        (Match.home_team_id == team.id) | (Match.away_team_id == team.id)
    ).order_by(Match.date.desc()).all()
    
    print("\n" + "=" * 60)
    print(f"📅 МАТЧИ КОМАНДЫ: {team.name}")
    print("=" * 60)
    
    for match in matches:
        date = match.date.strftime("%Y-%m-%d") if match.date else "N/A"
        score = f"{match.home_score}-{match.away_score}" if match.home_score is not None else "vs"
        # Используем поле venue напрямую
        venue = "🏠 Дома" if match.venue == 'Home' else "✈️  В гостях"
        print(f"{date} | {score:5s} | {venue} | {match.competition}")
    
    print(f"\nВсего матчей: {len(matches)}")
    return matches

def query_squad_stats():
    """Получить агрегированную статистику команд из данных игроков"""
    conn = sqlite3.connect('football_data.db')
    
    # SQL запрос для агрегированной статистики команд
    query = """
    SELECT 
        t.name as team,
        COUNT(DISTINCT p.id) as players,
        SUM(ps.goals) as total_goals,
        SUM(ps.assists) as total_assists,
        SUM(ps.minutes) as total_minutes,
        ROUND(AVG(ps.goals), 2) as avg_goals_per_player,
        MAX(ps.goals) as top_scorer_goals
    FROM player_stats ps
    JOIN players p ON ps.player_id = p.id
    JOIN teams t ON p.team_id = t.id
    GROUP BY t.id
    ORDER BY total_goals DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print("\n" + "=" * 70)
    print("📊 АГРЕГИРОВАННАЯ СТАТИСТИКА КОМАНД (из данных игроков)")
    print("=" * 70)
    
    if not df.empty:
        print(f"{'Команда':<20} | {'Игроков':<8} | {'Голы':<6} | {'Ассисты':<8} | {'Минуты':<10} | {'Топ'}")
        print("-" * 70)
        
        for _, row in df.iterrows():
            print(f"{row['team']:<20} | {row['players']:<8} | {row['total_goals']:<6} | {row['total_assists']:<8} | {row['total_minutes']:<10} | {row['top_scorer_goals']}")
        
        print(f"\nВсего команд: {len(df)}")
    else:
        print("⚠️  Нет данных о статистике игроков")
    
    return df

def query_squad_stats_from_matches():
    """Получить статистику команд из результатов матчей"""
    conn = sqlite3.connect('football_data.db')
    
    # SQL запрос для статистики из матчей
    # ВАЖНО: В нашей модели данных home_score = ВСЕГДА голы команды (GF),
    # away_score = ВСЕГДА голы соперника (GA), независимо от venue
    query = """
    SELECT 
        t.name as team,
        COUNT(m.id) as matches,
        SUM(m.home_score) as goals_scored,
        SUM(m.away_score) as goals_conceded,
        SUM(CASE 
            WHEN m.home_score > m.away_score THEN 3
            WHEN m.home_score = m.away_score THEN 1
            ELSE 0 
        END) as points,
        SUM(CASE WHEN m.home_score > m.away_score THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN m.home_score = m.away_score THEN 1 ELSE 0 END) as draws,
        SUM(CASE WHEN m.home_score < m.away_score THEN 1 ELSE 0 END) as losses
    FROM matches m
    JOIN teams t ON m.home_team_id = t.id
    WHERE m.home_score IS NOT NULL
    GROUP BY t.id
    ORDER BY points DESC, (SUM(m.home_score) - SUM(m.away_score)) DESC, SUM(m.home_score) DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print("\n" + "=" * 90)
    print("🏆 ТУРНИРНАЯ ТАБЛИЦА (из результатов матчей)")
    print("=" * 90)
    
    if not df.empty:
        # Добавляем разницу мячей
        df['gd'] = df['goals_scored'] - df['goals_conceded']
        
        print(f"{'#':<3} {'Команда':<20} | {'М':<3} | {'В':<3} | {'Н':<3} | {'П':<3} | {'ГЗ':<4} | {'ГП':<4} | {'РМ':<4} | {'Очки'}")
        print("-" * 90)
        
        for idx, row in df.iterrows():
            print(f"{idx+1:<3} {row['team']:<20} | {row['matches']:<3} | {row['wins']:<3} | {row['draws']:<3} | {row['losses']:<3} | {row['goals_scored']:<4} | {row['goals_conceded']:<4} | {row['gd']:<4} | {row['points']}")
        
        print(f"\nВсего команд: {len(df)}")
        print("\nЛегенда: М=Матчи, В=Победы, Н=Ничьи, П=Поражения, ГЗ=Голы забиты, ГП=Голы пропущены, РМ=Разница мячей")
    else:
        print("⚠️  Нет данных о матчах")
    
    return df

def query_top_scorers(limit=10):
    """Топ бомбардиров"""
    session = connect_db()
    
    top_players = session.query(
        Player.name,
        Team.name.label('team_name'),
        PlayerStat.goals,
        PlayerStat.assists,
        PlayerStat.minutes
    ).join(Team).join(PlayerStat).filter(
        PlayerStat.goals.isnot(None)
    ).order_by(desc(PlayerStat.goals)).limit(limit).all()
    
    print("\n" + "=" * 60)
    print(f"🏆 ТОП-{limit} БОМБАРДИРОВ")
    print("=" * 60)
    print(f"{'Игрок':<25} | {'Команда':<20} | {'Голы':<6} | {'Ассисты':<8} | {'Минуты'}")
    print("-" * 60)
    
    for idx, player in enumerate(top_players, 1):
        name, team, goals, assists, minutes = player
        assists_str = str(assists) if assists else "0"
        minutes_str = str(minutes) if minutes else "N/A"
        print(f"{idx:2d}. {name:<22} | {team:<20} | {goals:<6} | {assists_str:<8} | {minutes_str}")
    
    return top_players

def query_with_pandas():
    """Использование pandas для SQL запросов"""
    conn = sqlite3.connect('football_data.db')
    
    print("\n" + "=" * 60)
    print("📈 АНАЛИЗ С PANDAS")
    print("=" * 60)
    
    # Запрос 1: Статистика команд
    query1 = """
    SELECT 
        t.name as team,
        COUNT(DISTINCT m.id) as matches_played,
        SUM(CASE WHEN m.home_team_id = t.id THEN m.home_score 
                 WHEN m.away_team_id = t.id THEN m.away_score END) as goals_scored,
        SUM(CASE WHEN m.home_team_id = t.id THEN m.away_score 
                 WHEN m.away_team_id = t.id THEN m.home_score END) as goals_conceded
    FROM teams t
    LEFT JOIN matches m ON (m.home_team_id = t.id OR m.away_team_id = t.id)
    WHERE m.home_score IS NOT NULL
    GROUP BY t.id
    ORDER BY goals_scored DESC
    """
    
    df = pd.read_sql_query(query1, conn)
    
    if not df.empty:
        print("\n🎯 Голы забитые и пропущенные:")
        print(df.to_string(index=False))
        
        # Добавляем разницу мячей
        df['goal_difference'] = df['goals_scored'] - df['goals_conceded']
        print("\n📊 С разницей мячей:")
        print(df[['team', 'goals_scored', 'goals_conceded', 'goal_difference']].to_string(index=False))
    else:
        print("⚠️  Нет данных о матчах в базе")
    
    conn.close()
    return df

def query_database_info():
    """Общая информация о базе данных"""
    session = connect_db()
    
    teams_count = session.query(Team).count()
    players_count = session.query(Player).count()
    matches_count = session.query(Match).count()
    squad_stats_count = session.query(SquadStat).count()
    player_stats_count = session.query(PlayerStat).count()
    
    print("\n" + "=" * 60)
    print("💾 ИНФОРМАЦИЯ О БАЗЕ ДАННЫХ")
    print("=" * 60)
    print(f"⚽ Команд:              {teams_count}")
    print(f"👤 Игроков:            {players_count}")
    print(f"📅 Матчей:             {matches_count}")
    print(f"📊 Статистика команд:  {squad_stats_count}")
    print(f"📈 Статистика игроков: {player_stats_count}")
    print("=" * 60)

def main():
    """Запуск всех примеров запросов"""
    print("\n" + "🔍 ПРИМЕРЫ ЗАПРОСОВ К БАЗЕ ДАННЫХ" + "\n")
    
    try:
        # 1. Информация о БД
        query_database_info()
        
        # 2. Все команды
        query_all_teams()
        
        # 3. Матчи конкретной команды (можно изменить название)
        query_team_matches("Arsenal")  # Измените на любую команду
        
        # 4. Турнирная таблица (из матчей)
        query_squad_stats_from_matches()
        
        # 5. Агрегированная статистика команд (из данных игроков)
        query_squad_stats()
        
        # 6. Топ бомбардиров
        query_top_scorers(10)
        
        # 7. Анализ с pandas
        query_with_pandas()
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        print("\n💡 Возможно, база данных пуста. Запустите сначала: python main.py")

if __name__ == "__main__":
    main()

