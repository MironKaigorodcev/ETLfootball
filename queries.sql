-- Примеры SQL запросов для football_data.db
-- Можно выполнять через sqlite3 или любой SQL клиент

-- ============================================
-- 1. БАЗОВЫЕ ЗАПРОСЫ
-- ============================================

-- Все команды
SELECT * FROM teams;

-- Все матчи
SELECT * FROM matches ORDER BY date DESC;

-- Все игроки
SELECT * FROM players;

-- ============================================
-- 2. СТАТИСТИКА КОМАНД
-- ============================================

-- Команды с наибольшим количеством голов
SELECT 
    t.name,
    ss.goals_for,
    ss.goals_against,
    ss.possession,
    ss.season
FROM squad_stats ss
JOIN teams t ON ss.team_id = t.id
ORDER BY ss.goals_for DESC;

-- Команды с лучшей защитой (меньше пропущенных)
SELECT 
    t.name,
    ss.goals_against,
    ss.tackles,
    ss.interceptions,
    ss.clearances
FROM squad_stats ss
JOIN teams t ON ss.team_id = t.id
ORDER BY ss.goals_against ASC;

-- ============================================
-- 3. АНАЛИЗ МАТЧЕЙ
-- ============================================

-- Матчи конкретной команды (например, Arsenal)
SELECT 
    m.date,
    CASE 
        WHEN m.home_team_id = t.id THEN 'Дома'
        ELSE 'В гостях'
    END as venue,
    m.home_score || '-' || m.away_score as score,
    m.competition,
    m.attendance
FROM matches m
JOIN teams t ON (m.home_team_id = t.id OR m.away_team_id = t.id)
WHERE t.name LIKE '%Arsenal%'
ORDER BY m.date DESC;

-- Матчи с наибольшей посещаемостью
SELECT 
    m.date,
    ht.name as home_team,
    at.name as away_team,
    m.home_score || '-' || m.away_score as score,
    m.attendance
FROM matches m
JOIN teams ht ON m.home_team_id = ht.id
LEFT JOIN teams at ON m.away_team_id = at.id
WHERE m.attendance IS NOT NULL
ORDER BY m.attendance DESC
LIMIT 10;

-- Результативные матчи (больше всего голов)
SELECT 
    m.date,
    ht.name as home_team,
    at.name as away_team,
    m.home_score || '-' || m.away_score as score,
    (m.home_score + m.away_score) as total_goals
FROM matches m
JOIN teams ht ON m.home_team_id = ht.id
LEFT JOIN teams at ON m.away_team_id = at.id
WHERE m.home_score IS NOT NULL
ORDER BY total_goals DESC
LIMIT 10;

-- ============================================
-- 4. СТАТИСТИКА ИГРОКОВ
-- ============================================

-- Топ бомбардиров
SELECT 
    p.name,
    t.name as team,
    ps.goals,
    ps.assists,
    ps.minutes,
    ps.season
FROM player_stats ps
JOIN players p ON ps.player_id = p.id
JOIN teams t ON p.team_id = t.id
WHERE ps.goals IS NOT NULL
ORDER BY ps.goals DESC
LIMIT 20;

-- Игроки с лучшим соотношением голы/минуты
SELECT 
    p.name,
    t.name as team,
    ps.goals,
    ps.minutes,
    ROUND(CAST(ps.minutes AS FLOAT) / ps.goals, 2) as minutes_per_goal
FROM player_stats ps
JOIN players p ON ps.player_id = p.id
JOIN teams t ON p.team_id = t.id
WHERE ps.goals > 0 AND ps.minutes > 0
ORDER BY minutes_per_goal ASC
LIMIT 20;

-- Лучшие ассистенты
SELECT 
    p.name,
    t.name as team,
    ps.assists,
    ps.goals,
    ps.xag as expected_assists,
    ps.season
FROM player_stats ps
JOIN players p ON ps.player_id = p.id
JOIN teams t ON p.team_id = t.id
WHERE ps.assists IS NOT NULL
ORDER BY ps.assists DESC
LIMIT 20;

-- ============================================
-- 5. АГРЕГИРОВАННАЯ СТАТИСТИКА
-- ============================================

-- Общее количество записей в каждой таблице
SELECT 'Teams' as table_name, COUNT(*) as count FROM teams
UNION ALL
SELECT 'Players', COUNT(*) FROM players
UNION ALL
SELECT 'Matches', COUNT(*) FROM matches
UNION ALL
SELECT 'Squad Stats', COUNT(*) FROM squad_stats
UNION ALL
SELECT 'Player Stats', COUNT(*) FROM player_stats;

-- Средняя посещаемость по командам (домашние матчи)
SELECT 
    t.name,
    COUNT(m.id) as home_matches,
    ROUND(AVG(m.attendance), 0) as avg_attendance,
    MAX(m.attendance) as max_attendance
FROM matches m
JOIN teams t ON m.home_team_id = t.id
WHERE m.attendance IS NOT NULL
GROUP BY t.id
ORDER BY avg_attendance DESC;

-- Статистика голов по командам из матчей
SELECT 
    t.name,
    COUNT(DISTINCT m.id) as matches_played,
    SUM(CASE WHEN m.home_team_id = t.id THEN m.home_score 
             WHEN m.away_team_id = t.id THEN m.away_score END) as goals_scored,
    SUM(CASE WHEN m.home_team_id = t.id THEN m.away_score 
             WHEN m.away_team_id = t.id THEN m.home_score END) as goals_conceded,
    SUM(CASE WHEN m.home_team_id = t.id THEN m.home_score 
             WHEN m.away_team_id = t.id THEN m.away_score END) - 
    SUM(CASE WHEN m.home_team_id = t.id THEN m.away_score 
             WHEN m.away_team_id = t.id THEN m.home_score END) as goal_difference
FROM teams t
LEFT JOIN matches m ON (m.home_team_id = t.id OR m.away_team_id = t.id)
WHERE m.home_score IS NOT NULL
GROUP BY t.id
ORDER BY goal_difference DESC;

-- ============================================
-- 6. ПОИСК И ФИЛЬТРАЦИЯ
-- ============================================

-- Найти игрока по имени
SELECT 
    p.name,
    p.position,
    p.nationality,
    t.name as team
FROM players p
JOIN teams t ON p.team_id = t.id
WHERE p.name LIKE '%Saka%';

-- Матчи в определенный период
SELECT 
    m.date,
    ht.name as home_team,
    at.name as away_team,
    m.home_score || '-' || m.away_score as score
FROM matches m
JOIN teams ht ON m.home_team_id = ht.id
LEFT JOIN teams at ON m.away_team_id = at.id
WHERE m.date BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY m.date DESC;

-- Игроки с желтыми/красными карточками
SELECT 
    p.name,
    t.name as team,
    ps.yellow_cards,
    ps.red_cards,
    (ps.yellow_cards + ps.red_cards * 2) as discipline_score
FROM player_stats ps
JOIN players p ON ps.player_id = p.id
JOIN teams t ON p.team_id = t.id
WHERE ps.yellow_cards > 0 OR ps.red_cards > 0
ORDER BY discipline_score DESC;

