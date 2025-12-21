import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns

# Настройка страницы
st.set_page_config(page_title="Football Analytics Dashboard", layout="wide")
st.title("⚽ Футбольная Аналитика (Pro)")

@st.cache_resource  
def get_connection():
    # check_same_thread=False нужен, чтобы Streamlit не ругался при перезагрузках
    return sqlite3.connect('football_data.db', check_same_thread=False)

conn = get_connection()

# Боковое меню
menu = st.sidebar.radio("Навигация", ["🏆 Турнирная Таблица", "👟 Топ Бомбардиров", "🛡 Анализ Команды"])

# ==========================================
# 1. ТУРНИРНАЯ ТАБЛИЦА (Logic from query_squad_stats_from_matches)
# ==========================================
if menu == "🏆 Турнирная Таблица":
    st.header("Турнирная таблица (Live)")

    # Тот самый мощный SQL запрос из твоего файла
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
        SUM(m.home_score) - SUM(m.away_score) as gd
    FROM matches m
    JOIN teams t ON m.home_team_id = t.id
    WHERE m.home_score IS NOT NULL
    GROUP BY t.id
    ORDER BY points DESC, gd DESC, goals_scored DESC
    """
    
    df = pd.read_sql(query, conn)
    
    # Раскрашиваем таблицу (Градиент по очкам)
    st.dataframe(df.style.background_gradient(subset=['points'], cmap='Greens'))

    # График: Забитые vs Пропущенные
    st.subheader("📊 Эффективность: Атака vs Оборона")
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Scatter plot
    sns.scatterplot(data=df, x='goals_scored', y='goals_conceded', size='points', sizes=(50, 400), hue='points', palette='viridis', ax=ax)
    
    # Подписи названий команд
    for i in range(df.shape[0]):
        ax.text(df.goals_scored[i]+0.2, df.goals_conceded[i], df.team[i], fontsize=9)

    ax.set_xlabel("Забитые голы (Сильная атака ->)")
    ax.set_ylabel("Пропущенные голы (Слабая защита ->)")
    ax.axhline(df['goals_conceded'].mean(), color='gray', linestyle='--', alpha=0.5)
    ax.axvline(df['goals_scored'].mean(), color='gray', linestyle='--', alpha=0.5)
    st.pyplot(fig)

# ==========================================
# 2. ТОП БОМБАРДИРОВ (Logic from query_top_scorers)
# ==========================================
elif menu == "👟 Топ Бомбардиров":
    st.header("Лучшие бомбардиры лиги")
    
    # SQL запрос, объединяющий игроков, статистику и команды
    query = """
    SELECT 
        p.name as player,
        t.name as team,
        ps.goals,
        ps.assists,
        ps.minutes
    FROM player_stats ps
    JOIN players p ON ps.player_id = p.id
    JOIN teams t ON p.team_id = t.id
    WHERE ps.goals IS NOT NULL
    ORDER BY ps.goals DESC
    LIMIT 20
    """
    df = pd.read_sql(query, conn)
    
    # Фильтр по командам
    teams_list = ["Все"] + list(df['team'].unique())
    selected_team = st.selectbox("Фильтр по команде:", teams_list)
    
    if selected_team != "Все":
        df = df[df['team'] == selected_team]

    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Топ-20 Игроков")
        
        # Красивый бар-чарт
        fig, ax = plt.subplots(figsize=(10, 8))
        sns.barplot(data=df, x='goals', y='player', hue='team', dodge=False, ax=ax)
        ax.set_xlabel("Голы")
        ax.set_ylabel("")
        st.pyplot(fig)
        
    with col2:
        st.write("Детальная таблица")
        st.dataframe(df)

# ==========================================
# 3. АНАЛИЗ КОМАНДЫ (Logic from query_team_matches)
# ==========================================
elif menu == "🛡 Анализ Команды":
    st.header("Детальный разбор команды")
    
    # Получаем список команд для выбора
    teams = pd.read_sql("SELECT id, name FROM teams ORDER BY name", conn)
    selected_team_name = st.selectbox("Выберите команду:", teams['name'])
    
    # Находим ID команды
    team_id = teams[teams['name'] == selected_team_name]['id'].values[0]
    
    # Запрос матчей (как в query_team_matches, но адаптированный)
    query = f"""
    SELECT 
        date,
        competition,
        venue,
        home_score,
        away_score,
        CASE 
            WHEN home_team_id = {team_id} THEN 'Home' 
            ELSE 'Away' 
        END as played_at,
        CASE 
            WHEN home_team_id = {team_id} THEN home_score 
            ELSE away_score 
        END as team_goals,
        CASE 
            WHEN home_team_id = {team_id} THEN away_score 
            ELSE home_score 
        END as opponent_goals
    FROM matches
    WHERE home_team_id = {team_id} OR away_team_id = {team_id}
    ORDER BY date
    """
    
    df = pd.read_sql(query, conn)
    
    # Определяем результат (Победа/Ничья/Поражение)
    def get_result(row):
        if row['team_goals'] > row['opponent_goals']: return 'Win'
        elif row['team_goals'] == row['opponent_goals']: return 'Draw'
        else: return 'Loss'
        
    df['result'] = df.apply(get_result, axis=1)
    
    # Метрики (KPI)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Матчей сыграно", len(df))
    col2.metric("Побед", len(df[df['result']=='Win']))
    col3.metric("Забито голов", df['team_goals'].sum())
    col4.metric("Пропущено голов", df['opponent_goals'].sum())
    
    st.markdown("---")
    
    # График динамики голов
    st.subheader("Динамика голов по ходу сезона")
    st.line_chart(df.set_index('date')[['team_goals', 'opponent_goals']])
    
    # Таблица последних матчей
    st.subheader("История матчей")
    
    # Раскраска для таблицы (зеленый выигрыш, красный проигрыш)
    def color_result(val):
        color = '#d4edda' if val == 'Win' else '#f8d7da' if val == 'Loss' else '#fff3cd'
        return f'background-color: {color}'
        
    st.dataframe(df[['date', 'competition', 'played_at', 'team_goals', 'opponent_goals', 'result']].style.applymap(color_result, subset=['result']))