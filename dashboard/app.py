import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text
from wb.api import WBApi, WBCategory
from wb.db.connector import get_session
from datetime import datetime, timedelta, date
import numpy as np
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title='WB Дашборд',
    layout='wide',
    initial_sidebar_state='expanded'
)

REQUIRED_CATEGORIES = [
    WBCategory.MARKETPLACE,
    WBCategory.STATISTICS,
    WBCategory.CONTENT,
    WBCategory.ANALYTICS,
    WBCategory.PROMOTION,
    WBCategory.COMMON,
]

CATEGORY_EMOJI = {
    WBCategory.MARKETPLACE: '🏪',
    WBCategory.STATISTICS: '📊',
    WBCategory.CONTENT: '📝',
    WBCategory.ANALYTICS: '📈',
    WBCategory.PROMOTION: '📢',
    WBCategory.COMMON: 'ℹ️',
    WBCategory.PRICES: '💰',
    WBCategory.FEEDBACK: '💬',
    WBCategory.CHAT: '💭',
    WBCategory.SUPPLIES: '📦',
    WBCategory.RETURNS: '↩️',
    WBCategory.DOCUMENTS: '📄',
    WBCategory.FINANCE: '💳',
}

TABLES_TO_MONITOR = {
    'acceptance_reports': 'shk_create_date_on',
    'advert_costs': 'upd_time_at',
    'advert_full_stats': 'date_at',
    'advert_list': 'change_time_at',
    'advert_nm_report': 'dt_on',
    'advert_nm_report_extended': 'dt_on',
    'nmids_list': None,
    'paid_storage': 'date_on',
    'stat_fbs_warehouses': None,
    'stat_stocks_fbs': None,
    'supplier_orders': 'date_on',
    'supplier_sales': 'date_on',
    'supplier_stocks': 'date_receiving',
    'tariffs_box': 'upload_at',
    'tariffs_commission': 'upload_at',
}


# Токены
@st.cache_data(ttl=6000)
def validate_wb_token():
    try:
        wb = WBApi()
        check_result = wb.validate_token(
            required_categories=[
                WBCategory.MARKETPLACE,
                WBCategory.STATISTICS,
                WBCategory.CONTENT,
                WBCategory.ANALYTICS,
                WBCategory.PROMOTION,
                WBCategory.COMMON,
            ]
        )
        return check_result
    except Exception as e:
        return {
            'valid': False,
            'accessible_categories': [],
            'inaccessible_categories': REQUIRED_CATEGORIES,
            'errors': str(e)
        }


st.sidebar.markdown('---')
st.sidebar.markdown('### 🔑 Статус токена WB API')

token_info = validate_wb_token()

if token_info['valid'] is False:
    st.sidebar.error('❌ Проблемы с доступом к API')
else:
    st.sidebar.success('✅ Токен валиден и активен')

with st.sidebar.expander('📋 Детали', expanded=not token_info['valid']):
    st.markdown('**Требуемые категории API:**')

    for category in REQUIRED_CATEGORIES:
        emoji = CATEGORY_EMOJI.get(category, '📌')

        if category in token_info['accessible_categories']:
            st.markdown(f'{emoji} **{category.value}** :green[✓]')
        elif category in token_info['inaccessible_categories']:
            st.markdown(f'{emoji} **{category.value}** :red[✗]')
        else:
            st.markdown(f'{emoji} **{category.value}** :orange[?]')

    st.markdown('---')
    col1, col2 = st.columns(2)
    col1.metric('Доступно', len(token_info['accessible_categories']))
    col2.metric('Недоступно', len(token_info['inaccessible_categories']))

st.sidebar.markdown('---')

if not token_info['valid']:
    st.title('📊 Wildberries Analytics Dashboard')

    st.error('### ⚠️ Невозможно загрузить данные')

    st.markdown(
        """
            Дашборд не может быть запущен из-за проблем с токеном API Wildberries.
        """
    )

    st.markdown('#### 🚫 Недоступные категории:')

    cols = st.columns(2)
    for idx, category in enumerate(token_info['inaccessible_categories']):
        emoji = CATEGORY_EMOJI.get(category, '📌')
        with cols[idx % 2]:
            st.warning(f'{emoji} **{category.value}**')

    st.markdown('---')
    st.info(
        """
            ### 🔧 Как исправить проблему:
        
            1. **Перейдите в личный кабинет Wildberries**
               - Откройте раздел: Настройки → Интеграции по API
        
            2. **Проверьте токен**
               - Убедитесь, что токен активен (срок действия: 180 дней)
               - Проверьте, что не выбрана опция 'Тестовый контур'
        
            3. **Проверьте доступ к категориям**
               - Токен должен иметь доступ ко всем категориям выше
               - При создании токена отметьте все необходимые категории
        
            4. **Создайте новый токен при необходимости**
               - Если текущий токен устарел или имеет ограничения
               - Скопируйте новый токен и обновите в настройках приложения
        
            5. **Перезапустите контейнер**
               - После исправления перезапустите контейнер
        """
    )

    with st.expander('📚 Дополнительная информация'):
        st.markdown(
            f"""
                **Требуемые категории для работы дашборда:**
            """
        )
        for category in REQUIRED_CATEGORIES:
            emoji = CATEGORY_EMOJI.get(category, '📌')
            st.markdown(f'- {emoji} {category.value}')

        st.markdown(
            """
                ---
                **Полезные ссылки:**
                - [Документация WB API](https://dev.wildberries.ru/openapi/api-information)
                - [Создание токена](https://dev.wildberries.ru/openapi/api-information#tag/Avtorizaciya)
            """
        )

    st.stop()


# Таблицы
@st.cache_data(ttl=10)
def check_table_status():
    session = get_session()
    try:
        status = {}

        for table_name, date_column in TABLES_TO_MONITOR.items():
            info = {
                'loaded': False,
                'count': 0,
                'last_update': None,
                'is_fresh': False,
                'error': None,
                'date_column': date_column
            }

            try:
                count = session.execute(
                    text(f"SELECT COUNT(*) FROM {table_name}")
                ).fetchone()[0]
                info['count'] = count
                info['loaded'] = count > 0

                if not date_column:
                    status[table_name] = info
                    continue

                result = session.execute(
                    text(f"SELECT MAX({date_column}) FROM {table_name}")
                ).fetchone()

                max_date = result[0] if (result and result[0]) else None
                info['last_update'] = max_date

                if max_date:
                    if isinstance(max_date, date) and not isinstance(max_date, datetime):
                        max_date = datetime.combine(max_date, datetime.min.time())

                    info['is_fresh'] = (datetime.now() - max_date).days <= 1

            except Exception as e:
                info['error'] = str(e)

            status[table_name] = info

        return status
    finally:
        session.close()


st.markdown('---')
st.sidebar.markdown('### 📋 Статус таблиц данных')

table_monitor_block = st.sidebar.container()
with table_monitor_block:
    table_status = check_table_status()

    all_ok = True
    for t, info in table_status.items():
        if info['error']:
            all_ok = False
            break
        if not info['loaded']:
            all_ok = False
            break

    if not all_ok:
        st_autorefresh(interval=10 * 1000, key='refresh_tables_status')

    with st.sidebar.expander('📋 Детали', expanded=False):
        for table, info in table_status.items():
            if info['error']:
                st.markdown(f'❌ **{table}** — ошибка')
                st.caption(info['error'])
                st.markdown('---')
                continue

            emoji = '🟢' if info['loaded'] else '🔴'
            st.markdown(f'**{emoji} {table}**')

            col1, col2 = st.columns(2)
            col1.metric('Строк', info['count'])

            if not info['date_column']:
                col2.metric('Дата', '—')
                st.caption('Дата не задана')
                st.markdown('---')
                continue

            if info['last_update']:
                freshness = '🟩 свежая' if info['is_fresh'] else '🟧 устарела'
                col2.metric('Актуальность', freshness)
                st.caption(f'Колонка даты: `{info["date_column"]}`')
                st.caption(f'Обновлено: {info["last_update"]}')
            else:
                col2.metric('Актуальность', 'нет данных')
                st.caption(f'Колонка даты: `{info["date_column"]}`')
                st.caption('Обновлено: —')

# Дашборд
st.title('📊 Wildberries Дашборд')


# Получение инфо о продавце
@st.cache_data(ttl=3600)
def get_seller_info():
    try:
        wb = WBApi()
        info = wb.seller_info()
        return info
    except:
        return None


seller_info = get_seller_info()
if seller_info:
    st.markdown('---')
    with st.expander('ℹ️ Информация о продавце'):
        info_col1, info_col2, info_col3 = st.columns(3)

        with info_col1:
            st.metric('Наименование продавца', seller_info.get('name'))

        with info_col2:
            st.metric('Уникальный ID продавца на Wildberries', seller_info.get('sid'))

        with info_col3:
            st.metric('Торговое наименование продавца', seller_info.get('tradeMark'))


def get_dashboard_data():
    session = get_session()
    try:
        try:
            is_populated = session.execute(
                text(
                    """
                        SELECT EXISTS(
                            SELECT 1 FROM mv_wb_pivot_by_day_dl LIMIT 1
                        )
                    """
                )
            ).scalar()
        except Exception as e:
            if 'not been populated' in str(e).lower():
                is_populated = False
            else:
                raise

        if not is_populated:
            st.warning(
                '⏳ **Материализованное представление еще загружается...**\n\n'
                'Дашборд будет доступен через несколько минут. '
                'Пожалуйста, обновите страницу позже.'
            )
            session.close()
            return pd.DataFrame()

        query = text("""
            SELECT 
                sa_name,
                "nm_rep.nm_id",
                "nm_rep.date_on",
                "nm_rep.open_card_count",
                "nm_rep.add_to_cart_count",
                "nm_rep.orders_count",
                "nm_rep.orders_sum_rub",
                "adv_fs.sum",
                "stk.date_on",
                "stk.summ",
                "stk.in_way_to_client",
                "stk.in_way_from_client",
                "stk.quantity_full",
                "stkf.date_on" as "stkf.date_on_data",
                "stkf.quantity",
                "sl.date_on" as "sl.date_on_data",
                "sl.all_logistics",
                "sl.redemption_percentage",
                "psl.date_on" as "psl.date_on_data",
                "psl.summ" as "psl.summ_data",
                "ar.date_on" as "ar.date_on_data",
                "ar.count",
                "ar.total",
                "tc.paid_storage_kgvp",
                "tc.subject_name",
                "fs.count_cancel_orders",
                "fs.count_orders_oper",
                "fs.sum_cancel_orders",
                "fs.count_return_sales",
                "fs.sum_return_sales",
                "fs.count_item_in_way",
                "fs.sum_item_in_way",
                "fs.count_sales",
                "fs.sum_sales",
                "fs.sum_orders_after_spp",
                "fs.sum_sales_after_spp",
                "brand_name",
                "stk.quantity_full_at_end_week",
                "stk.quantity_full_at_end_month",
                "stkf.quantity_at_end_week",
                "stkf.quantity_at_end_month"
            FROM mv_wb_pivot_by_day_dl
            ORDER BY "nm_rep.date_on" DESC
        """)

        result = session.execute(query)
        rows = result.fetchall()

        if rows:
            df = pd.DataFrame(rows, columns=result.keys())
            df["nm_rep.date_on"] = pd.to_datetime(df["nm_rep.date_on"])
            return df
        else:
            st.warning('Данные еще не были получены. Подождите загрузки.')
            return pd.DataFrame()
    except Exception as e:
        st.error(f'Ошибка подключения к БД: {e}')
        return pd.DataFrame()
    finally:
        session.close()


def calculate_metrics(df):
    if df.empty:
        return {}

    latest_date = df["nm_rep.date_on"].max()
    latest_df = df[df["nm_rep.date_on"] == latest_date]

    prev_date = df[df["nm_rep.date_on"] < latest_date]["nm_rep.date_on"].max()
    if pd.notna(prev_date):
        prev_df = df[df["nm_rep.date_on"] == prev_date]
    else:
        prev_df = pd.DataFrame()

    metrics = {
        'avg_price': df["nm_rep.orders_sum_rub"].sum() / max(df["nm_rep.orders_count"].sum(), 1) if df["nm_rep.orders_count"].sum() > 0 else 0,
        'avg_price_prev': prev_df["nm_rep.orders_sum_rub"].sum() / max(prev_df["nm_rep.orders_count"].sum(), 1) if not prev_df.empty and prev_df["nm_rep.orders_count"].sum() > 0 else 0,

        'open_cards': df["nm_rep.open_card_count"].sum(),
        'open_cards_prev': prev_df["nm_rep.open_card_count"].sum() if not prev_df.empty else 0,

        'add_to_cart': df["nm_rep.add_to_cart_count"].sum(),
        'add_to_cart_prev': prev_df["nm_rep.add_to_cart_count"].sum() if not prev_df.empty else 0,

        'orders': df["nm_rep.orders_count"].sum(),
        'orders_prev': prev_df["nm_rep.orders_count"].sum() if not prev_df.empty else 0,

        'revenue': df["nm_rep.orders_sum_rub"].sum(),
        'revenue_prev': prev_df["nm_rep.orders_sum_rub"].sum() if not prev_df.empty else 0,

        'adv_spend': df["adv_fs.sum"].sum(),
        'adv_spend_prev': prev_df["adv_fs.sum"].sum() if not prev_df.empty else 0,

        'stock_value': df["stk.summ"].sum(),
        'stock_qty': df["stk.quantity_full"].sum(),

        'returns': df["fs.count_return_sales"].sum(),
        'returns_sum': df["fs.sum_return_sales"].sum(),

        'cancels': df["fs.count_cancel_orders"].sum(),
        'cancels_sum': df["fs.sum_cancel_orders"].sum(),
    }

    return metrics

def format_number(value, decimals=0):
    if pd.isna(value):
        return "0" if decimals == 0 else f"{0:.{decimals}f}"
    try:
        if decimals == 0:
            return f"{int(round(value)):,}".replace(",", " ")
        else:
            s = f"{value:,.{decimals}f}"
            return s.replace(",", " ")
    except Exception:
        return str(value)


df = get_dashboard_data()

if df.empty:
    st.stop()

# Sidebar filters
st.markdown('---')
st.sidebar.header('Фильтры')

min_date = df["nm_rep.date_on"].min().date()
max_date = df["nm_rep.date_on"].max().date()

default_start = (max_date - timedelta(days=30))
if default_start < min_date:
    default_start = min_date

default_range = (default_start, max_date)

date_range = st.sidebar.date_input(
    'Диапазон дат',
    value=default_range,
    min_value=min_date,
    max_value=max_date
)

brands = st.sidebar.multiselect(
    'Бренды',
    df['brand_name'].dropna().unique(),
    default=df['brand_name'].dropna().unique()[:5] if len(df['brand_name'].dropna().unique()) > 0 else []
)

subjects = st.sidebar.multiselect(
    'Категории товаров',
    df['tc.subject_name'].dropna().unique(),
    default=[]
)

# Filter data
filtered_df = df.copy()

if date_range:
    start_date = pd.to_datetime(date_range[0])
    end_date = pd.to_datetime(date_range[-1]) + timedelta(days=1)
    filtered_df = filtered_df[(filtered_df["nm_rep.date_on"] >= start_date) & (filtered_df["nm_rep.date_on"] < end_date)]

if brands:
    filtered_df = filtered_df[filtered_df['brand_name'].isin(brands)]

if subjects:
    filtered_df = filtered_df[filtered_df['tc.subject_name'].isin(subjects)]

# Calculate metrics
metrics = calculate_metrics(filtered_df)

# Display key metrics
st.subheader('📈 Ключевые показатели')

col1, col2, col3, col4 = st.columns(4)

with col1:
    delta_rev = metrics.get('revenue', 0) - metrics.get('revenue_prev', 0)
    delta_rev_pct = (delta_rev / metrics.get('revenue_prev', 1) * 100) if metrics.get('revenue_prev', 0) != 0 else 0
    st.metric(
        '💰 Выручка',
        f'{format_number(metrics.get("revenue", 0), 0)} ₽',
        # delta=f'{format_number(delta_rev, 0)} ₽ ({delta_rev_pct:.1f}%)',
        # delta_color='inverse'
    )

with col2:
    delta_ord = metrics.get('orders', 0) - metrics.get('orders_prev', 0)
    delta_ord_pct = (delta_ord / metrics.get('orders_prev', 1) * 100) if metrics.get('orders_prev', 0) != 0 else 0
    st.metric(
        '📦 Заказы',
        f'{format_number(metrics.get("orders", 0))}',
        # delta=f'{format_number(delta_ord)} ({delta_ord_pct:.1f}%)',
        # delta_color='inverse'
    )

with col3:
    st.metric(
        '💵 Средняя цена',
        f'{format_number(metrics.get("avg_price", 0), 0)} ₽'
    )

with col4:
    st.metric(
        '📢 Реклама расходы',
        f'{format_number(metrics.get("adv_spend", 0), 0)} ₽'
    )

# Вторая строка показателей
col5, col6, col7, col8 = st.columns(4)

with col5:
    st.metric(
        '👁️ Перешли в карточку',
        f'{format_number(metrics.get("open_cards", 0))}'
    )

with col6:
    st.metric(
        '🛒 Добавили в корзину',
        f'{format_number(metrics.get("add_to_cart", 0))}'
    )

with col7:
    conversion_cart = (metrics.get('add_to_cart', 0) / max(metrics.get('open_cards', 1), 1) * 100)
    st.metric(
        '🛍️ Конверсия в корзину',
        f'{conversion_cart:.1f}%'
    )

with col8:
    conversion_order = (metrics.get('orders', 0) / max(metrics.get('open_cards', 1), 1) * 100)
    st.metric(
        '✅ Конверсия в заказ',
        f'{conversion_order:.1f}%'
    )

st.markdown('---')

# Tabs for different views
tab1, tab2, tab3 = st.tabs(['📊 Динамика', '🔄 Воронка продаж', '💾 Остатки'])

with tab1:
    st.subheader('Динамика показателей')

    # Time series by day
    daily_stats = filtered_df.groupby('nm_rep.date_on').agg({
        'nm_rep.orders_count': 'sum',
        'nm_rep.orders_sum_rub': 'sum',
        'nm_rep.open_card_count': 'sum',
        'nm_rep.add_to_cart_count': 'sum',
        'adv_fs.sum': 'sum',
    }).reset_index()

    daily_stats.columns = ['date', 'orders', 'revenue', 'opens', 'carts', 'adv_spend']

    # Выручка и заказы
    fig_combined = go.Figure()

    # Выручка
    fig_combined.add_trace(go.Scatter(
        x=daily_stats['date'],
        y=daily_stats['revenue'],
        mode='lines+markers',
        name='Выручка',
        fill='tozeroy',
        line=dict(color='#1f77b4', width=2),
        fillcolor='rgba(31, 119, 180, 0.2)',
        yaxis='y'
    ))

    # Заказы
    fig_combined.add_trace(go.Scatter(
        x=daily_stats['date'],
        y=daily_stats['orders'],
        mode='lines+markers',
        name='Заказы',
        fill='tozeroy',
        line=dict(color='#ff7f0e', width=2),
        fillcolor='rgba(255, 127, 14, 0.2)',
        yaxis='y2'
    ))

    fig_combined.update_layout(
        title='Выручка и заказы по дням',
        xaxis_title='Дата',
        yaxis=dict(
            title=dict(text='Выручка (₽)', font=dict(color='#1f77b4')),
            tickfont=dict(color='#1f77b4')
        ),
        yaxis2=dict(
            title=dict(text='Заказы', font=dict(color='#ff7f0e')),
            tickfont=dict(color='#ff7f0e'),
            overlaying='y',
            side='right'
        ),
        hovermode='x unified',
        template='plotly_white',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        )
    )

    st.plotly_chart(fig_combined, width='stretch')

    # Воронка конверсии
    fig_funnel_no_stack = go.Figure()

    # Открытия карточки
    fig_funnel_no_stack.add_trace(go.Scatter(
        x=daily_stats['date'],
        y=daily_stats['opens'],
        name='Открытия',
        mode='lines+markers',
        line=dict(color='rgba(31, 119, 180, 0.8)', width=2),
        hovertemplate='<b>Открытия</b><br>%{x|%d %b}<br>%{y}<extra></extra>'
    ))

    # В корзину
    fig_funnel_no_stack.add_trace(go.Scatter(
        x=daily_stats['date'],
        y=daily_stats['carts'],
        name='В корзину',
        mode='lines+markers',
        line=dict(color='rgba(44, 160, 44, 0.8)', width=2),
        hovertemplate='<b>В корзину</b><br>%{x|%d %b}<br>%{y}<extra></extra>'
    ))

    # Заказы
    fig_funnel_no_stack.add_trace(go.Scatter(
        x=daily_stats['date'],
        y=daily_stats['orders'],
        name='Заказы',
        mode='lines+markers',
        line=dict(color='rgba(255, 127, 14, 0.8)', width=2),
        hovertemplate='<b>Заказы</b><br>%{x|%d %b}<br>%{y}<extra></extra>'
    ))

    fig_funnel_no_stack.update_layout(
        title='Воронка конверсии по дням',
        xaxis_title='Дата',
        yaxis_title='Количество',
        hovermode='x unified',
        template='plotly_white',
        height=400,
        legend=dict(x=0.01, y=0.99)
    )

    st.plotly_chart(fig_funnel_no_stack, width='stretch')

with tab2:
    st.subheader('Фактические данные')

    decimal_columns = [
        'tc.paid_storage_kgvp',
        'sl.redemption_percentage',
        'sl.all_logistics',
        'psl.summ_data',
        'ar.total',
        'ar.count',
        'adv_fs.sum',
        'fs.sum_return_sales',
        'fs.count_return_sales',
        'fs.sum_cancel_orders',
        'fs.count_cancel_orders',
        'fs.sum_orders_after_spp',
        'fs.count_orders_oper',
        'stk.quantity_full',
        'stk.in_way_from_client',
        'stk.summ',
        'stkf.quantity',
        'nm_rep.open_card_count',
        'nm_rep.add_to_cart_count',
        'nm_rep.orders_count',
        'nm_rep.orders_sum_rub',
    ]

    for col in decimal_columns:
        if col in filtered_df.columns:
            filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce').fillna(0)

    daily_pivot = filtered_df.groupby('nm_rep.date_on').agg(
        {
            'nm_rep.open_card_count': 'sum',
            'nm_rep.add_to_cart_count': 'sum',
            'nm_rep.orders_count': 'sum',
            'nm_rep.orders_sum_rub': 'sum',
            'fs.count_return_sales': 'sum',
            'fs.sum_return_sales': 'sum',
            'fs.count_cancel_orders': 'sum',
            'fs.sum_cancel_orders': 'sum',
            'adv_fs.sum': 'sum',
            'sl.all_logistics': 'sum',
            'psl.summ_data': 'sum',
            'ar.total': 'sum',
            'stk.quantity_full': 'sum',
            'stkf.quantity': 'sum',
            'fs.sum_orders_after_spp': 'sum',
            'fs.count_orders_oper': 'sum',
        }
    ).reset_index()

    filtered_df['redemption_weighted_orders'] = filtered_df['sl.redemption_percentage'] * filtered_df['nm_rep.orders_count']

    filtered_df['redemption_weighted_revenue'] = filtered_df['sl.redemption_percentage'] * filtered_df['nm_rep.orders_sum_rub']

    filtered_df['commission_amount'] = (filtered_df['tc.paid_storage_kgvp'] / 100) * filtered_df['redemption_weighted_revenue']

    filtered_df['fbo_turnover'] = filtered_df['stk.quantity_full'] + filtered_df['stk.in_way_from_client']

    daily_redemption = filtered_df.groupby('nm_rep.date_on').agg({
        'redemption_weighted_orders': 'sum',
        'redemption_weighted_revenue': 'sum',
        'commission_amount': 'sum',
        'fbo_turnover': 'sum',
    }).reset_index()

    daily_pivot = daily_pivot.merge(daily_redemption, on='nm_rep.date_on', how='left')

    daily_pivot = daily_pivot.sort_values('nm_rep.date_on', ascending=False)

    # Конверсия
    daily_pivot['Конверсия в корзину, %'] = (
        daily_pivot['nm_rep.add_to_cart_count'] / daily_pivot['nm_rep.open_card_count'].replace(0, 1) * 100
    ).round(1)

    daily_pivot['Конверсия в заказ, %'] = (
        daily_pivot['nm_rep.orders_count'] / daily_pivot['nm_rep.add_to_cart_count'].replace(0, 1) * 100
    ).round(1)

    daily_pivot['Конверсия в выкуп, %'] = (
        daily_pivot['redemption_weighted_orders'] / daily_pivot['nm_rep.orders_count'].replace(0, 1) * 100
    ).round(1)

    # Прогноз продаж
    daily_pivot['Прогноз продаж, шт'] = daily_pivot['redemption_weighted_orders'].round(0)
    daily_pivot['Прогноз продаж, руб'] = daily_pivot['redemption_weighted_revenue'].round(0)

    # Цены
    daily_pivot['Средняя цена, руб'] = (
        daily_pivot['nm_rep.orders_sum_rub'] / daily_pivot['nm_rep.orders_count'].replace(0, 1)
    ).round(0)

    daily_pivot['Средняя цена, руб. (после СПП)'] = (
        daily_pivot['fs.sum_orders_after_spp'] / daily_pivot['fs.count_orders_oper'].replace(0, 1)
    ).round(0)

    daily_pivot['Скидка СПП, %'] = (
        ((daily_pivot['nm_rep.orders_sum_rub'] / daily_pivot['nm_rep.orders_count'].replace(0, 1)) -
         (daily_pivot['fs.sum_orders_after_spp'] / daily_pivot['fs.count_orders_oper'].replace(0, 1))) /
        (daily_pivot['nm_rep.orders_sum_rub'] / daily_pivot['nm_rep.orders_count'].replace(0, 1)) * 100
    ).round(1)

    # Логистика
    daily_pivot['Логистика всего, руб'] = daily_pivot['sl.all_logistics'].round(0)
    daily_pivot['Логистика, %'] = (
        daily_pivot['sl.all_logistics'] / daily_pivot['redemption_weighted_revenue'].replace(0, 1) * 100
    ).round(1)

    # Хранение
    daily_pivot['Хранение всего, руб'] = daily_pivot['psl.summ_data'].round(0)
    daily_pivot['Хранение, %'] = (
        daily_pivot['psl.summ_data'] / daily_pivot['redemption_weighted_revenue'].replace(0, 1) * 100
    ).round(1)

    # Приемка
    daily_pivot['Платная приемка всего, руб'] = daily_pivot['ar.total'].round(0)
    daily_pivot['Платная приемка, %'] = (
        daily_pivot['ar.total'] / daily_pivot['redemption_weighted_revenue'].replace(0, 1) * 100
    ).round(1)

    # Комиссия
    daily_pivot['Комиссия МП всего, руб'] = daily_pivot['commission_amount'].round(0)
    daily_pivot['Комиссия МП, %'] = (
        daily_pivot['commission_amount'] / daily_pivot['redemption_weighted_revenue'].replace(0, 1) * 100
    ).round(1)

    # Марж 1
    daily_pivot['m_1'] = (
        daily_pivot['redemption_weighted_revenue'] -
        (daily_pivot['sl.all_logistics'] + daily_pivot['psl.summ_data'] +
         daily_pivot['ar.total'] + daily_pivot['commission_amount'])
    ).round(0)

    daily_pivot['Марж-ая прибыль 1 всего, руб'] = daily_pivot['m_1']

    daily_pivot['Марж-ая прибыль 1, %'] = (
        daily_pivot['m_1'] / daily_pivot['redemption_weighted_revenue'].replace(0, 1) * 100
    ).round(1)

    daily_pivot['Внутренняя реклама, руб'] = daily_pivot['adv_fs.sum'].round(0)

    daily_pivot['ДРРп, %'] = (
        daily_pivot['adv_fs.sum'] / daily_pivot['redemption_weighted_revenue'].replace(0, 1) * 100
    ).round(1)

    daily_pivot['ДРРз, %'] = (
        daily_pivot['adv_fs.sum'] / daily_pivot['nm_rep.orders_sum_rub'].replace(0, 1) * 100
    ).round(1)

    # Марж 2
    daily_pivot['m_2'] = (
        daily_pivot['m_1'] - daily_pivot['adv_fs.sum']
    ).round(0)

    daily_pivot['Марж-ая прибыль 2 всего, руб'] = daily_pivot['m_2']

    daily_pivot['Марж-ая прибыль 2 на ед, руб'] = (
        daily_pivot['m_2'] / daily_pivot['redemption_weighted_orders'].replace(0, 1)
    ).round(0)

    daily_pivot['Марж-ая прибыль 2, %'] = (
        daily_pivot['m_2'] / daily_pivot['redemption_weighted_revenue'].replace(0, 1) * 100
    ).round(1)

    # Остатки
    daily_pivot['Остатки FBO, шт'] = daily_pivot['stk.quantity_full'].round(0)

    daily_pivot['Оборачиваемость FBO, дн'] = (
        daily_pivot['fbo_turnover'] / daily_pivot['nm_rep.orders_count'].replace(0, 1)
    ).round(1)

    daily_pivot['Остатки FBS, шт'] = daily_pivot['stkf.quantity'].round(0)

    daily_pivot['Оборачиваемость FBS, дн'] = (
        daily_pivot['stkf.quantity'] / daily_pivot['nm_rep.orders_count'].replace(0, 1)
    ).round(1)

    # Остальное
    daily_pivot['Перешли в карточку, шт'] = daily_pivot['nm_rep.open_card_count'].round(0)
    daily_pivot['Положили в корзину, шт'] = daily_pivot['nm_rep.add_to_cart_count'].round(0)
    daily_pivot['Заказали, шт'] = daily_pivot['nm_rep.orders_count'].round(0)
    daily_pivot['Заказали, руб'] = daily_pivot['nm_rep.orders_sum_rub'].round(0)

    metrics_order = [
        'Воронка',
        'Перешли в карточку, шт',
        'Конверсия в корзину, %',
        'Положили в корзину, шт',
        'Конверсия в заказ, %',
        'Заказали, шт',
        'Заказали, руб',
        'Конверсия в выкуп, %',
        'Прогноз продаж, шт',
        'Прогноз продаж, руб',
        'Средняя цена, руб',
        'Средняя цена, руб. (после СПП)',
        'Скидка СПП, %',
        'Удержание MP',
        'Логистика всего, руб',
        'Логистика, %',
        'Хранение всего, руб',
        'Хранение, %',
        'Платная приемка всего, руб',
        'Платная приемка, %',
        'Комиссия МП всего, руб',
        'Комиссия МП, %',
        'Марж-ая прибыль 1 всего, руб',
        'Марж-ая прибыль 1, %',
        'Внутренняя реклама, руб',
        'ДРРп, %',
        'ДРРз, %',
        'Марж-ая прибыль 2 всего, руб',
        'Марж-ая прибыль 2 на ед, руб',
        'Марж-ая прибыль 2, %',
        'Остатки FBO, шт',
        'Оборачиваемость FBO, дн',
        'Остатки FBS, шт',
        'Оборачиваемость FBS, дн',
    ]

    column_mapping = {
        'nm_rep.date_on': 'Дата',
    }

    daily_pivot = daily_pivot.rename(columns=column_mapping)

    metrics_df = daily_pivot.set_index('Дата').T

    metrics_df.columns = [
        col.strftime('%d.%m.%Y') if isinstance(col, (pd.Timestamp, datetime)) else col
        for col in metrics_df.columns
    ]

    metrics_df['Итог за выбранный период'] = metrics_df.sum(axis=1)

    percentage_rows = [
        'Конверсия в корзину, %',
        'Конверсия в заказ, %',
        'Конверсия в выкуп, %',
        'Скидка СПП, %',
        'Логистика, %',
        'Хранение, %',
        'Платная приемка, %',
        'Комиссия МП, %',
        'Марж-ая прибыль 1, %',
        'ДРРп, %',
        'ДРРз, %',
        'Марж-ая прибыль 2, %',
    ]

    sum_rows = [
        'Перешли в карточку, шт',
        'Положили в корзину, шт',
        'Заказали, шт',
        'Заказали, руб',
        'Прогноз продаж, шт',
        'Прогноз продаж, руб',
        'Логистика всего, руб',
        'Хранение всего, руб',
        'Платная приемка всего, руб',
        'Комиссия МП всего, руб',
        'Марж-ая прибыль 1 всего, руб',
        'Внутренняя реклама, руб',
        'Марж-ая прибыль 2 всего, руб',
        'Остатки FBO, шт',
        'Остатки FBS, шт',
    ]

    turnover_rows = ['Оборачиваемость FBO, дн', 'Оборачиваемость FBS, дн']

    metrics_df['Итог за выбранный период'] = 0.0

    for row in metrics_df.index:
        row_data = pd.to_numeric(metrics_df.loc[row, metrics_df.columns[:-1]], errors='coerce')

        if row in percentage_rows:
            metrics_df.loc[row, 'Итог за выбранный период'] = row_data.mean()
        elif row in turnover_rows:
            metrics_df.loc[row, 'Итог за выбранный период'] = row_data.mean()
        elif row in sum_rows:
            metrics_df.loc[row, 'Итог за выбранный период'] = row_data.sum()
        else:
            metrics_df.loc[row, 'Итог за выбранный период'] = row_data.sum()

    final_metrics = []
    for metric in metrics_order:
        if metric in column_mapping.values():
            final_metrics.append(metric)
        elif metric in metrics_df.index:
            final_metrics.append(metric)

    existing_metrics = [m for m in final_metrics if m in metrics_df.index]
    metrics_df = metrics_df.reindex(existing_metrics)

    def format_metric(val, metric_name):
        if pd.isna(val):
            return ''
        try:
            val = float(val)
        except:
            return str(val)

        if '%' in metric_name:
            return f'{val:.1f}%'
        elif 'руб' in metric_name:
            return f'{int(val):,}'.replace(',', ' ')
        elif 'шт' in metric_name:
            return f'{int(val):,}'.replace(',', ' ')
        elif 'дн' in metric_name:
            return f'{val:.0f}'
        else:
            return f'{int(val):,}'.replace(',', ' ')


    def format_dataframe_for_display(df):
        formatted_df = pd.DataFrame(index=df.index, columns=df.columns, dtype='object')

        for idx in formatted_df.index:
            for col in formatted_df.columns:
                val = df.loc[idx, col]
                formatted_df.loc[idx, col] = format_metric(val, idx)

        return formatted_df


    display_df = format_dataframe_for_display(metrics_df)

    st.dataframe(display_df, height=800, width='stretch')

with tab3:
    st.subheader('Остатки товаров')

    if filtered_df.empty:
        st.info("Нет данных для выбранного периода")
    else:
        last_day = filtered_df["nm_rep.date_on"].max()
        last_df = filtered_df[filtered_df["nm_rep.date_on"] == last_day]

        stock_summary = pd.DataFrame({
            'Метрика': [
                'Всего на складе',
                'На пути к клиенту',
                'На пути от клиента (возвраты)',
                'В пути (общее)',
                'Стоимость остатков'
            ],
            'Значение': [
                f'{format_number(last_df["stk.quantity_full"].sum(), 0)}',
                f'{format_number(last_df["stk.in_way_to_client"].sum(), 0)}',
                f'{format_number(last_df["stk.in_way_from_client"].sum(), 0)}',
                f'{format_number(last_df["stk.in_way_to_client"].sum() + last_df["stk.in_way_from_client"].sum(), 0)}',
                f'{format_number(last_df["stk.summ"].sum(), 0)} ₽'
            ]
        })

        st.dataframe(stock_summary, width='stretch', hide_index=True)

        if 'sa_name' in last_df.columns:
            stock_by_article = last_df.groupby('sa_name').agg({
                'stk.quantity_full': 'sum',
                'stk.summ': 'sum'
            }).reset_index().sort_values('stk.quantity_full', ascending=True).head(1000)

            fig_stock = px.bar(
                stock_by_article,
                y='sa_name',
                x='stk.quantity_full',
                orientation='h',
                labels={'stk.quantity_full': 'Количество', 'sa_name': 'Артикул'}
            )
            st.plotly_chart(fig_stock, width='stretch')
        else:
            st.info('Колонка sa_name отсутствует в данных.')

st.markdown('---')
st.caption('📊 Dashboard обновляется каждые 60 минут. Последнее обновление: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


