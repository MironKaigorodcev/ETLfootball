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
        venue = "🏠 Дома" if match.home_team_id == team.id else "✈️  В гостях"
        print(f"{date} | {score:5s} | {venue} | {match.competition}")
    
    print(f"\nВсего матчей: {len(matches)}")
    return matches

def query_squad_stats():
    """Получить статистику всех команд"""
    session = connect_db()
    
    stats = session.query(
        Team.name,
        SquadStat.goals_for,
        SquadStat.goals_against,
        SquadStat.possession,
        SquadStat.season
    ).join(Team).order_by(desc(SquadStat.goals_for)).all()
    
    print("\n" + "=" * 60)
    print("📊 СТАТИСТИКА КОМАНД")
    print("=" * 60)
    print(f"{'Команда':<20} | {'Голы':<10} | {'Пропущено':<12} | {'Владение':<10} | {'Сезон'}")
    print("-" * 60)
    
    for stat in stats:
        name, gf, ga, poss, season = stat
        gf_str = str(gf) if gf else "N/A"
        ga_str = str(ga) if ga else "N/A"
        poss_str = f"{poss:.1f}%" if poss else "N/A"
        print(f"{name:<20} | {gf_str:<10} | {ga_str:<12} | {poss_str:<10} | {season}")
    
    print(f"\nВсего записей: {len(stats)}")
    return stats

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
        
        # 4. Статистика команд
        query_squad_stats()
        
        # 5. Топ бомбардиров
        query_top_scorers(10)
        
        # 6. Анализ с pandas
        query_with_pandas()
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        print("\n💡 Возможно, база данных пуста. Запустите сначала: python main.py")

if __name__ == "__main__":
    main()

