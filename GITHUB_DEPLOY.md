# 🚀 Инструкция по загрузке проекта на GitHub

## Шаг 1: Инициализация Git репозитория

```bash
cd /Users/mironkajgorodcev/ETLfootball

# Инициализируем git (если еще не инициализирован)
git init

# Добавляем все файлы
git add .

# Делаем первый коммит
git commit -m "Initial commit: FBref Premier League ETL Scraper"
```

## Шаг 2: Создание репозитория на GitHub

1. Перейдите на [github.com](https://github.com)
2. Нажмите **"+"** → **"New repository"**
3. Заполните:
   - **Repository name**: `ETLfootball` или `fbref-premier-league-scraper`
   - **Description**: `⚽ ETL scraper for Premier League statistics from FBref.com`
   - **Public** или **Private** (на ваш выбор)
   - ❌ НЕ добавляйте README, .gitignore, license (они уже есть)
4. Нажмите **"Create repository"**

## Шаг 3: Подключение к GitHub

GitHub покажет команды. Используйте вариант для **existing repository**:

```bash
# Добавляем remote (замените YOUR_USERNAME на ваш username)
git remote add origin https://github.com/YOUR_USERNAME/ETLfootball.git

# Или используйте SSH (если настроен)
git remote add origin git@github.com:YOUR_USERNAME/ETLfootball.git

# Переименовываем ветку в main (если нужно)
git branch -M main

# Загружаем на GitHub
git push -u origin main
```

## Шаг 4: Проверка

1. Обновите страницу репозитория на GitHub
2. Убедитесь, что все файлы загружены
3. README.md должен отображаться на главной странице

## 🎉 Готово!

Ваш проект теперь на GitHub! Другие пользователи могут:

```bash
# Клонировать проект
git clone https://github.com/YOUR_USERNAME/ETLfootball.git
cd ETLfootball

# Установить зависимости
python3 -m venv venv
source venv/bin/activate  # или: source .venv/bin/activate
pip install -r requirements.txt

# Запустить
python main.py
```

## 📝 Дополнительные настройки

### Добавить Topics на GitHub:
1. На странице репозитория нажмите ⚙️ рядом с "About"
2. Добавьте topics: `python`, `web-scraping`, `football`, `premier-league`, `fbref`, `etl`, `data-analysis`

### Добавить GitHub Actions (опционально):
Создайте `.github/workflows/test.yml` для автоматического тестирования

### Защитить main branch:
Settings → Branches → Add rule → Require pull request reviews

## 🔄 Обновление репозитория

После внесения изменений:

```bash
git add .
git commit -m "Описание изменений"
git push origin main
```

## ⚠️ Важно

Перед загрузкой убедитесь, что:
- ✅ `.gitignore` настроен правильно
- ✅ Нет секретных данных (API ключей, паролей)
- ✅ `football_data.db` в `.gitignore` (не загружаем БД)
- ✅ `venv/` и `.venv/` в `.gitignore`

## 🆘 Проблемы?

### "fatal: remote origin already exists"
```bash
git remote remove origin
git remote add origin https://github.com/YOUR_USERNAME/ETLfootball.git
```

### "Permission denied (publickey)"
Используйте HTTPS вместо SSH или настройте SSH ключи:
```bash
git remote set-url origin https://github.com/YOUR_USERNAME/ETLfootball.git
```

### Большой размер репозитория
Если случайно добавили `.db` файлы:
```bash
git rm --cached football_data.db
git commit -m "Remove database file"
git push origin main
```

