# 📊 РНП Wildberries — Аналитический Дашборд

Полнофункциональный аналитический дашборд для управления продажами и аналитики на маркетплейсе Wildberries. Интегрируется с официальным WB API для получения реальных данных о продажах, остатках, рекламе и логистике.

## ✨ Возможности

- **📈 Ключевые метрики** — выручка, заказы, конверсия, средняя цена
- **🔄 Воронка продаж** — анализ пути покупателя от просмотра до покупки
- **💾 Управление остатками** — отслеживание запасов на складе
- **💰 Финансовая аналитика** — маржа, комиссии, логистика, хранение
- **📊 Динамика показателей** — тренды продаж, выручки, конверсии
- **🎯 Гибкие фильтры** — по датам, брендам, категориям товаров
- **🔐 Проверка доступа API** — валидация токена и категорий доступа
- **📋 Мониторинг данных** — статус таблиц, актуальность информации
- **🐳 Docker** — быстрое развертывание в контейнере

## 🚀 Быстрый старт

### Предварительные требования

- Docker и Docker Compose
- Токен API Wildberries с необходимыми категориями доступа

### Установка и запуск

#### 1. Клонируйте репозиторий

```bash
git clone https://github.com/dcierra/RNPWildberries.git
cd RNPWildberries
```

#### 2. Создайте файл `.env`

```bash
cp .env.example .env
```

Отредактируйте `.env` с вашими параметрами:

#### 3. Запустите контейнеры

```bash
docker-compose up -d
```

#### 4. Откройте дашборд

Откройте браузер и перейдите на:
```
http://localhost:8501
```

## 📋 Структура проекта

```
skolkovoproj/
├── .env.example             # Пример переменных окружения
├── .gitignore               # Исключения для Git
├── docker-compose.yml       # Оркестрация контейнеров
├── Dockerfile               # Конфигурация Docker образа
├── entrypoint.sh            # Скрипт инициализации контейнера
├── requirements.txt         # Python зависимости
├── README.md                # Этот файл
├── LICENSE                  # MIT License
│
├── config/                  # Конфигурация приложения
│   ├── base_config.py       # Базовая конфигурация
│   ├── db_config.py         # Параметры БД
│   ├── logging_config.py    # Настройки логирования
│   ├── settings.py          # Глобальные настройки
│   └── wb_config.py         # Конфигурация API WB
│
├── dashboard/               # Streamlit приложение
│   └── app.py               # Главный дашборд
│
├── logs/                    # Логирование
│   ├── logger.py            # Конфигурация логгера
│   └── __init__.py
│
└── wb/                      # WB API интеграция
    ├── api.py               # Клиент API Wildberries
    ├── task_runner.py       # Фоновые задачи
    ├── __init__.py
    ├── db/                  # Работа с БД
    │   ├── connector.py     # Подключение к PostgreSQL
    │   ├── utils.py         # Утилиты БД
    │   ├── __init__.py
    │   └── models/          # SQLAlchemy модели
    │       ├── acceptance_report.py
    │       ├── advert.py
    │       ├── advert_cost.py
    │       ├── advert_fullstat.py
    │       ├── advert_nm_report.py
    │       ├── advert_nm_report_extended.py
    │       ├── fbs_stock.py
    │       ├── fbs_warehouse.py
    │       ├── nm_id_card.py
    │       ├── paid_storage.py
    │       ├── supplier_order.py
    │       ├── supplier_sale.py
    │       ├── supplier_stock.py
    │       ├── tariff_box.py
    │       ├── tariff_commission.py
    │       └── __init__.py
    │
    ├── methods/            # Методы API по категориям
    │   ├── acceptance_reports.py
    │   ├── advert_costs.py
    │   ├── advert_fullstats.py
    │   ├── advert_list.py
    │   ├── advert_nm_report.py
    │   ├── advert_nm_report_extended.py
    │   ├── fbs_warehouses.py
    │   ├── nmids_list.py
    │   ├── paid_storage.py
    │   ├── stocks_fbs.py
    │   ├── supplier_orders.py
    │   ├── supplier_sales.py
    │   ├── supplier_stocks.py
    │   ├── tariff_box.py
    │   ├── tariff_commissions.py
    │   └── __init__.py
    │
    └── pydantic_models/    # Pydantic модели валидации
        └── __init__.py
```

## 🔑 API Доступ Wildberries

Дашборд требует токена WB API с следующими категориями доступа:

- **🏪 Marketplace** — доступ к каталогу товаров
- **📊 Statistics** — статистика продаж
- **📝 Content** — управление контентом товаров
- **📈 Analytics** — аналитика и отчеты
- **📢 Promotion** — информация о рекламе
- **ℹ️ Common** — общая информация

## 🗄️ База данных

Дашборд использует PostgreSQL для хранения данных с материализованными представлениями для оптимизации производительности.

### Схема БД

Включает следующие основные таблицы:

- `advert_nm_report` — отчеты по товарам (просмотры, клики, заказы)
- `supplier_sales` — данные о продажах и выкупах
- `supplier_stocks` — остатки товаров
- `paid_storage` — услуги хранения
- `advert_costs` — затраты на рекламу
- `supplier_orders` — информация о заказах
- `tariff_commission` — комиссии маркетплейса
- `fbs_stock` — остатки FBS сервиса

Плюс вспомогательные таблицы для справочников и справки.

### Материализованные представления

- `mv_wb_pivot_by_day_dl` — кэшированные расчеты по дням (обновляется автоматически)

## ⚙️ Конфигурация

### Streamlit параметры

Отредактируйте `.streamlit/config.toml`:

```toml
[theme]
base = "light"                          # "light" или "dark"
primaryColor = "#4A3AFF"                # Основной цвет
backgroundColor = "#ffffff"             # Фон
secondaryBackgroundColor = "#f5f5f5"    # Вторичный фон
textColor = "#000000"                   # Цвет текста
```

## 🐳 Docker

### Запуск всех сервисов

```bash
docker compose up -d
```

### Просмотр логов

```bash
docker compose logs -f app          # Логи приложения
docker compose logs -f postgres     # Логи БД
```
А также в папке `logs/`

### Остановка сервисов

```bash
docker compose down
```

### Удаление данных БД

```bash
docker compose down -v
```

## 🔄 Автоматическое обновление данных

Дашборд автоматически:

- Синхронизирует данные с WB API
- Обновляет материализованные представления
- Кэширует результаты для оптимизации производительности

## 📝 Логирование

Логи сохраняются в папке `logs/`:

- `app.log` — основные логи приложения

## 📄 Лицензия

Этот проект распространяется под лицензией MIT. Смотрите файл [LICENSE](LICENSE) для деталей.
