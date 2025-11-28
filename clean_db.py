#!/usr/bin/env python3
"""
Скрипт для очистки базы данных и повторного запуска парсинга
"""

import os
import sys

def main():
    db_file = 'football_data.db'
    log_file = 'scraper.log'
    
    print("🗑️  Очистка данных...")
    
    # Удаляем базу данных
    if os.path.exists(db_file):
        os.remove(db_file)
        print(f"✅ Удалена база данных: {db_file}")
    else:
        print(f"ℹ️  База данных не найдена: {db_file}")
    
    # Удаляем лог файл
    if os.path.exists(log_file):
        os.remove(log_file)
        print(f"✅ Удален лог файл: {log_file}")
    else:
        print(f"ℹ️  Лог файл не найден: {log_file}")
    
    print("\n✨ Очистка завершена!")
    print("\n💡 Теперь запустите: python main.py")

if __name__ == "__main__":
    main()

