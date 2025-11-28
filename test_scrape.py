from scraper import FBRefScraper
from config import PREMIER_LEAGUE_URL
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_scrape():
    """Тестовый скрипт для проверки работы парсера"""
    print("\n" + "=" * 60)
    print("🧪 ТЕСТОВЫЙ ЗАПУСК ПАРСЕРА")
    print("=" * 60 + "\n")
    
    scraper = FBRefScraper()
    
    print("📋 Получение списка команд...")
    teams = scraper.get_league_teams(PREMIER_LEAGUE_URL)
    
    if not teams:
        print("❌ Не удалось получить список команд")
        print("\n💡 СОВЕТЫ ПО РЕШЕНИЮ ПРОБЛЕМЫ 403:")
        print("  1. Подождите 10-15 минут и попробуйте снова")
        print("  2. Проверьте подключение к интернету")
        print("  3. Попробуйте использовать VPN")
        print("  4. FBref может блокировать ваш IP временно")
        return
    
    print(f"✅ Найдено {len(teams)} команд\n")
    print(f"Первая команда: {teams[0]}\n")
    
    # Test match logs for first team
    print("=" * 60)
    print(f"📊 Тестирование на команде: {teams[0]['name']}")
    print("=" * 60 + "\n")
    
    print("📅 Получение логов матчей...")
    match_logs = scraper.get_match_logs(teams[0]['url'])
    if match_logs is not None:
        print(f"✅ Получено {len(match_logs)} записей о матчах")
        print(f"Колонки: {match_logs.columns.tolist()}")
        print("\nПервые 3 матча:")
        print(match_logs.head(3))
    else:
        print("❌ Не удалось получить логи матчей")
    
    print("\n" + "=" * 60)
    print("📈 Получение статистики...")
    stats = scraper.get_team_stats(teams[0]['url'])
    if stats:
        print(f"✅ Статистика команды: {len(stats['squad'])} таблиц")
        print(f"   Таблицы команды: {list(stats['squad'].keys())}")
        print(f"✅ Статистика игроков: {len(stats['players'])} таблиц")
        print(f"   Таблицы игроков: {list(stats['players'].keys())[:5]}...")
    else:
        print("❌ Не удалось получить статистику")
    
    print("\n" + "=" * 60)
    print("✅ ТЕСТ ЗАВЕРШЕН")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    test_scrape()

