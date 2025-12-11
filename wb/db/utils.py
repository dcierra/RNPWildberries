import sqlalchemy.orm
from sqlalchemy import text

from logs import app_logger


def try_to_find_model(
    model: sqlalchemy.orm.DeclarativeBase,
    session: sqlalchemy.orm.Session,
    filters: dict,
    updates: dict
):
    model_in_db = session.query(model).filter_by(**filters).first()

    if model_in_db:
        for key, value in updates.items():
            if key != 'id' and not key.startswith('_'):
                setattr(
                    model_in_db,
                    key,
                    value
                )
        session.commit()
        return True
    return False


def create_views(engine):
    views_sql = {
        # 1
        "view_wb_acceptance_report_rnp": """
CREATE OR REPLACE VIEW view_wb_acceptance_report_rnp AS
SELECT war.shk_create_date_on,
       war.nm_id,
       sum(war.count) AS count,
       (sum(war.total))::numeric AS total
FROM acceptance_reports war
GROUP BY war.shk_create_date_on, war.nm_id;
""",
        # 2
        "view_wb_advert_cost_from_doc": """
CREATE OR REPLACE VIEW view_wb_advert_cost_from_doc AS
WITH advert_groups AS (
    SELECT date(advert_full_stats.date_at) AS date_at,
           advert_full_stats.advert_id,
           advert_full_stats.nm_id,
           sum(advert_full_stats.sum) AS adv_cost
    FROM advert_full_stats
    GROUP BY date(advert_full_stats.date_at),
             advert_full_stats.advert_id,
             advert_full_stats.nm_id
),
statistics_groups AS (
    SELECT date(advert_costs.upd_time_at) AS date_at,
           advert_costs.advert_id,
           sum(advert_costs.upd_sum) AS stat_cost
    FROM advert_costs
    GROUP BY date(advert_costs.upd_time_at),
             advert_costs.advert_id
),
real_adv_cost AS (
    SELECT a.date_at,
           a.nm_id,
           a.advert_id,
           COALESCE(
             (round((((s.stat_cost)::double precision
             * (a.adv_cost / NULLIF(sum(a.adv_cost)
                  OVER (PARTITION BY a.date_at, a.advert_id),
                  (0)::double precision))))::numeric, 2))::double precision,
             a.adv_cost
           ) AS stat_cost_real
    FROM advert_groups a
    LEFT JOIN statistics_groups s
      ON ( (s.date_at = a.date_at) AND (s.advert_id = a.advert_id) )
)
SELECT real_adv_cost.date_at,
       real_adv_cost.nm_id,
       sum(real_adv_cost.stat_cost_real) AS stat_cost_real
FROM real_adv_cost
GROUP BY real_adv_cost.date_at, real_adv_cost.nm_id;
""",
        # 3
        "view_wb_nmids_list_updated": """
CREATE OR REPLACE VIEW view_wb_nmids_list_updated AS
WITH all_sku AS (
    SELECT nmids_list.nm_id,
           nmids_list.subject_id,
           nmids_list.subject_name,
           nmids_list.vendor_code,
           nmids_list.brand,
           nmids_list.title,
           nmids_list.barcode,
           nmids_list.tech_size,
           nmids_list.length,
           nmids_list.width,
           nmids_list.height,
           row_number() OVER (
             PARTITION BY nmids_list.barcode, nmids_list.nm_id
             ORDER BY nmids_list.updated_at DESC
           ) AS rn
    FROM nmids_list
)
SELECT all_sku.nm_id,
       all_sku.subject_id,
       all_sku.subject_name,
       all_sku.vendor_code,
       all_sku.brand,
       all_sku.title,
       all_sku.barcode,
       all_sku.tech_size,
       all_sku.length,
       all_sku.width,
       all_sku.height,
       all_sku.rn
FROM all_sku
WHERE (all_sku.rn = 1);
""",
        # 4
        "view_wb_nmids_list_updated_no_barcode": """
CREATE OR REPLACE VIEW view_wb_nmids_list_updated_no_barcode AS
WITH all_sku AS (
    SELECT nmids_list.nm_id,
           nmids_list.subject_id,
           nmids_list.subject_name,
           nmids_list.vendor_code,
           nmids_list.brand,
           nmids_list.title,
           nmids_list.length,
           nmids_list.width,
           nmids_list.height,
           row_number() OVER (
             PARTITION BY nmids_list.nm_id
             ORDER BY nmids_list.updated_at DESC
           ) AS rn
    FROM nmids_list
)
SELECT all_sku.nm_id,
       all_sku.subject_id,
       all_sku.subject_name,
       all_sku.vendor_code,
       all_sku.brand,
       all_sku.title,
       all_sku.length,
       all_sku.width,
       all_sku.height,
       all_sku.rn
FROM all_sku
WHERE (all_sku.rn = 1);
""",
        # 5
        "view_wb_orders_sales_cancels_returns_by_order_date": """
CREATE OR REPLACE VIEW view_wb_orders_sales_cancels_returns_by_order_date AS
WITH last_orders AS (
    SELECT supplier_orders.date_on,
           supplier_orders.nm_id,
           supplier_orders.barcode,
           supplier_orders.total_price,
           supplier_orders.discount_percent,
           supplier_orders.spp,
           supplier_orders.finished_price,
           supplier_orders.price_with_disc,
           supplier_orders.is_cancel,
           supplier_orders.srid,
           row_number() OVER (
             PARTITION BY supplier_orders.srid
             ORDER BY supplier_orders.last_change_date_on DESC
           ) AS rn
    FROM supplier_orders
),
last_sales AS (
    SELECT supplier_sales.date_on,
           supplier_sales.nm_id,
           supplier_sales.barcode,
           supplier_sales.total_price,
           supplier_sales.discount_percent,
           supplier_sales.spp,
           supplier_sales.payment_sale_amount,
           supplier_sales.for_pay,
           supplier_sales.finished_price,
           supplier_sales.price_with_disc,
           supplier_sales.sale_id,
           supplier_sales.srid,
           row_number() OVER (
             PARTITION BY supplier_sales.sale_id, supplier_sales.srid
             ORDER BY supplier_sales.last_change_date_at DESC
           ) AS rn
    FROM supplier_sales
)
SELECT date(o.date_on) AS date_on,
       o.nm_id,
       count(o.srid) AS count_orders,
       sum(o.price_with_disc) AS sum_orders,
       count(
         CASE WHEN (o.is_cancel = true) THEN o.srid ELSE NULL::character varying END
       ) AS count_cancel_orders,
       sum(
         CASE WHEN (o.is_cancel = true) THEN o.price_with_disc ELSE (0)::double precision END
       ) AS sum_cancel_orders,
       count(s.srid) AS count_sales,
       COALESCE(sum(s.price_with_disc), (0)::double precision) AS sum_sales,
       count(r.srid) AS count_return_sales,
       COALESCE(sum(r.price_with_disc), (0)::double precision) AS sum_return_sales,
       ((count(o.srid) - count(s.srid))
         - count(
             CASE WHEN (o.is_cancel = true) THEN o.srid ELSE NULL::character varying END
           )) AS count_item_in_way,
       ((sum(o.price_with_disc) - COALESCE(sum(s.price_with_disc), (0)::double precision))
         - sum(
             CASE WHEN (o.is_cancel = true) THEN o.price_with_disc ELSE (0)::double precision END
           )) AS sum_item_in_way,
       sum(o.finished_price) AS sum_orders_after_spp,
       sum((COALESCE(s.finished_price, (0)::double precision)
         - COALESCE(r.finished_price, (0)::double precision))) AS sum_sales_after_spp
FROM ((last_orders o
  LEFT JOIN last_sales s
    ON (
      ((s.srid)::text = (o.srid)::text)
      AND ((s.sale_id)::text ~~ 'S%'::text)
      AND (s.rn = 1)
    ))
  LEFT JOIN last_sales r
    ON (
      ((r.srid)::text = (o.srid)::text)
      AND ((r.sale_id)::text ~~ 'R%'::text)
      AND (r.rn = 1)
    ))
WHERE (o.rn = 1)
GROUP BY date(o.date_on), o.nm_id
ORDER BY date(o.date_on), o.nm_id;
""",
        # 6
        "view_wb_paid_storage_logistika": """
CREATE OR REPLACE VIEW view_wb_paid_storage_logistika AS
SELECT paid_storage.date_on,
       lower(TRIM(BOTH FROM paid_storage.vendor_code)) AS vendor_code,
       paid_storage.nm_id,
       (sum(paid_storage.warehouse_price))::numeric AS summ
FROM paid_storage
GROUP BY paid_storage.date_on,
         paid_storage.vendor_code,
         paid_storage.nm_id;
""",
        # 7
        "view_wb_percent_of_buy_with_warehouse_type": """
CREATE OR REPLACE VIEW view_wb_percent_of_buy_with_warehouse_type AS
SELECT date(o.date_on) AS date,
       TRIM(BOTH FROM lower((o.supplier_article)::text)) AS supplierarticle,
       o.nm_id,
       o.warehouse_name,
       o.warehouse_type,
       count(DISTINCT o.srid) AS count_orders,
       count(DISTINCT
         CASE WHEN o.is_cancel THEN o.srid ELSE NULL::character varying END
       ) AS count_cancel_orders,
       count(DISTINCT s.srid) AS count_sales,
       count(DISTINCT
         CASE WHEN ((s.sale_id)::text ~~ 'R%'::text) THEN s.srid ELSE NULL::character varying END
       ) AS count_return_sales
FROM (supplier_orders o
  LEFT JOIN supplier_sales s
    ON ((s.srid)::text = (o.srid)::text))
WHERE (o.date_on >= '2024-08-01 00:00:00'::timestamp without time zone)
GROUP BY date(o.date_on),
         TRIM(BOTH FROM lower((o.supplier_article)::text)),
         o.nm_id,
         o.warehouse_name,
         o.warehouse_type;
""",
        # 8
        "view_wb_redemption_percentage_by_30_days": """
CREATE OR REPLACE VIEW view_wb_redemption_percentage_by_30_days AS
SELECT view_wb_percent_of_buy_with_warehouse_type.nm_id,
       ((sum(view_wb_percent_of_buy_with_warehouse_type.count_sales)
         - sum(view_wb_percent_of_buy_with_warehouse_type.count_return_sales))
        / sum(view_wb_percent_of_buy_with_warehouse_type.count_orders)) AS redemption_percentage
FROM view_wb_percent_of_buy_with_warehouse_type
WHERE ((view_wb_percent_of_buy_with_warehouse_type.date >= (CURRENT_DATE - '37 days'::interval))
  AND (view_wb_percent_of_buy_with_warehouse_type.date <= (CURRENT_DATE - '7 days'::interval)))
GROUP BY view_wb_percent_of_buy_with_warehouse_type.nm_id;
""",
        # 9
        "view_wb_redemption_percentage_dynamic_7day": """
CREATE OR REPLACE VIEW view_wb_redemption_percentage_dynamic_7day AS
SELECT d.date_on,
       d.nm_id,
       CASE
         WHEN (sum(s.count_sales) = (0)::numeric) THEN (0)::numeric
         ELSE round((sum(s.count_sales)
           / (sum(s.count_sales) + sum(s.count_cancel_orders))),
           3)
       END AS redemption_percentage
FROM (view_wb_orders_sales_cancels_returns_by_order_date d
  CROSS JOIN view_wb_orders_sales_cancels_returns_by_order_date s)
WHERE ((s.date_on >= (d.date_on - '14 days'::interval))
  AND (s.date_on <= d.date_on)
  AND (s.nm_id = d.nm_id)
  AND (s.count_orders > 0))
GROUP BY d.date_on, d.nm_id
ORDER BY d.date_on DESC, d.nm_id;
""",
        # 10
        "view_wb_stocks": """
CREATE OR REPLACE VIEW view_wb_stocks AS
SELECT supplier_stocks.date_receiving,
       supplier_stocks.nm_id,
       sum(supplier_stocks.quantity) AS summ,
       sum(supplier_stocks.in_way_to_client) AS in_way_to_client,
       sum(supplier_stocks.in_way_from_client) AS in_way_from_client,
       sum(supplier_stocks.quantity_full) AS quantity_full
FROM supplier_stocks
GROUP BY supplier_stocks.date_receiving, supplier_stocks.nm_id;
""",
        # 11
        "view_wb_stocks_fbs": """
CREATE OR REPLACE VIEW view_wb_stocks_fbs AS
SELECT s.date_on,
       nl.nm_id,
       sum(s.amount) AS quantity
FROM stat_stocks_fbs s
LEFT JOIN view_wb_nmids_list_updated nl
  ON ((nl.barcode)::text = (s.sku)::text)
WHERE (s.amount > 0)
GROUP BY s.date_on, nl.nm_id;
""",
        # 12
        "view_wb_tariffs_commission": """
CREATE OR REPLACE VIEW view_wb_tariffs_commission AS
SELECT wtc.id,
       wtc.upload_at,
       wtc.kgvp_marketplace,
       wtc.kgvp_supplier,
       wtc.kgvp_supplier_express,
       wtc.paid_storage_kgvp,
       wtc.parent_id,
       wtc.parent_name,
       wtc.subject_id,
       wtc.subject_name,
       nl.nm_id
FROM tariffs_commission wtc
LEFT JOIN (
    SELECT DISTINCT nmids_list.nm_id,
           nmids_list.subject_name,
           row_number() OVER (PARTITION BY nmids_list.nm_id
                              ORDER BY nmids_list.updated_at DESC) AS rn
    FROM nmids_list
) nl
  ON ((nl.subject_name)::text = (wtc.subject_name)::text)
WHERE (nl.rn = 1);
""",
        # 13
        "sum_for_logistics": """
CREATE OR REPLACE VIEW sum_for_logistics AS
WITH volume_by_liter AS (
    SELECT t.nm_id,
           (((avg(t.length) * avg(t.width)) * avg(t.height)) / (1000)::numeric) AS total_volume_by_item,
           ((((avg(t.length) * avg(t.width)) * avg(t.height)) / (1000)::numeric) - (1)::numeric) AS volume_add_liter
    FROM (
        SELECT nmids_list.id,
               nmids_list.nm_id,
               nmids_list.imt_id,
               nmids_list.nm_uuid,
               nmids_list.subject_id,
               nmids_list.subject_name,
               nmids_list.vendor_code,
               nmids_list.brand,
               nmids_list.title,
               nmids_list.barcode,
               nmids_list.tech_size,
               nmids_list.wb_size,
               nmids_list.length,
               nmids_list.width,
               nmids_list.height,
               nmids_list.created_at,
               nmids_list.updated_at,
               row_number() OVER (
                 PARTITION BY nmids_list.barcode
                 ORDER BY nmids_list.updated_at DESC
               ) AS rn
        FROM nmids_list
    ) t
    WHERE (t.rn = 1)
    GROUP BY t.nm_id
),
avg_wb_tariffs_box AS (
    SELECT tariffs_box.warehouse_name,
           avg(tariffs_box.box_delivery_base) AS avg_box_delivery_base,
           avg(tariffs_box.box_delivery_liter) AS avg_box_delivery_liter
    FROM tariffs_box
    GROUP BY tariffs_box.warehouse_name
),
logistics_by_warehouse_and_item AS (
    SELECT pob.date AS date_on,
           pob.supplierarticle AS sa_article,
           pob.nm_id,
           pob.warehouse_name,
           pob.warehouse_type,
           pob.count_orders,
           pob.count_cancel_orders,
           pob.count_sales,
           pob.count_return_sales,
           vbl.total_volume_by_item,
           vbl.volume_add_liter,
           COALESCE(wb_seller.box_delivery_base,
                    wb_wb.box_delivery_base,
                    avg_wb.avg_box_delivery_base,
                    avg_seller.avg_box_delivery_base) AS box_delivery_base,
           COALESCE(wb_seller.box_delivery_liter,
                    wb_wb.box_delivery_liter,
                    avg_wb.avg_box_delivery_liter,
                    avg_seller.avg_box_delivery_liter) AS box_delivery_liter,
           ((50)::numeric * ((1)::numeric - rp7.redemption_percentage)) AS reverse_logistics,
           (((COALESCE(wb_seller.box_delivery_base,
                        wb_wb.box_delivery_base,
                        avg_wb.avg_box_delivery_base,
                        avg_seller.avg_box_delivery_base)
             + (COALESCE(wb_seller.box_delivery_liter,
                        wb_wb.box_delivery_liter,
                        avg_wb.avg_box_delivery_liter,
                        avg_seller.avg_box_delivery_liter)
               * (vbl.volume_add_liter)::double precision))
            + (((50)::numeric * ((1)::numeric - rp7.redemption_percentage)))::double precision)
           * (pob.count_orders)::double precision) AS all_logistics
    FROM ((((((view_wb_percent_of_buy_with_warehouse_type pob
            LEFT JOIN volume_by_liter vbl
              ON ((pob.nm_id = vbl.nm_id)))
           LEFT JOIN tariffs_box wb_seller
             ON ((((wb_seller.warehouse_name)::text ~~ 'Маркетплейс%'::text)
                  AND ((pob.warehouse_type)::text = 'Склад продавца'::text)
                  AND (wb_seller.upload_at = pob.date))))
          LEFT JOIN tariffs_box wb_wb
             ON ((((wb_wb.warehouse_name)::text = (pob.warehouse_name)::text)
                  AND ((pob.warehouse_type)::text = 'Склад WB'::text)
                  AND (wb_wb.upload_at = pob.date))))
         LEFT JOIN avg_wb_tariffs_box avg_wb
           ON ((((avg_wb.warehouse_name)::text = (pob.warehouse_name)::text)
                AND ((pob.warehouse_type)::text = 'Склад WB'::text))))
         CROSS JOIN avg_wb_tariffs_box avg_seller)
         LEFT JOIN view_wb_redemption_percentage_dynamic_7day rp7
           ON (((pob.nm_id = rp7.nm_id)
                AND (pob.date = rp7.date_on))))
    WHERE ((avg_seller.warehouse_name)::text ~~ 'Маркетплейс%'::text)
)
SELECT l.date_on,
       l.sa_article,
       l.nm_id,
       sum(l.all_logistics) AS all_logistics,
       sum(l.count_sales) AS count_sales,
       sum(l.count_return_sales) AS count_return_sales,
       sum(l.count_orders) AS count_orders,
       rp.redemption_percentage
FROM (logistics_by_warehouse_and_item l
  LEFT JOIN view_wb_redemption_percentage_by_30_days rp
    ON ((rp.nm_id = l.nm_id)))
GROUP BY l.date_on, l.sa_article, l.nm_id, rp.redemption_percentage;
""",
        # 14
        "view_wb_advert_nm_report_union_djem": """
CREATE OR REPLACE VIEW view_wb_advert_nm_report_union_djem AS
WITH normal_nm_rep AS (
    SELECT nmr.dt_on AS date,
           nmr.nm_id AS nmid,
           nmr.open_card_count AS opencardcount,
           nmr.add_to_cart_count AS addtocartcount,
           nmr.orders_count AS orderscount,
           nmr.orders_sum_rub AS orderssumrub,
           nmr.buyouts_count AS buyoutscount,
           nmr.buyouts_sum_bub AS buyoutssumrub
    FROM advert_nm_report nmr
),
min_date_all AS (
    SELECT min(normal_nm_rep.date) AS min_date
    FROM normal_nm_rep
)
SELECT nm.dt_on,
       nm.nm_id,
       nm.open_card_count,
       nm.add_to_cart_count,
       nm.orders_count,
       nm.orders_sum_rub,
       nm.buyouts_count,
       nm.buyouts_sum_rub
FROM (advert_nm_report_extended nm
  CROSS JOIN min_date_all d)
WHERE (d.min_date > nm.dt_on)
UNION ALL
SELECT normal_nm_rep.date AS dt_on,
       normal_nm_rep.nmid AS nm_id,
       normal_nm_rep.opencardcount AS open_card_count,
       normal_nm_rep.addtocartcount AS add_to_cart_count,
       normal_nm_rep.orderscount AS orders_count,
       normal_nm_rep.orderssumrub AS orders_sum_rub,
       normal_nm_rep.buyoutscount AS buyouts_count,
       normal_nm_rep.buyoutssumrub AS buyouts_sum_rub
FROM normal_nm_rep;
""",
        # 15
        "dasboard_summary": """
CREATE OR REPLACE VIEW dasboard_summary AS
WITH aggregated_nm_report AS (
    SELECT view_wb_advert_nm_report_union_djem.nm_id,
           view_wb_advert_nm_report_union_djem.dt_on,
           sum(view_wb_advert_nm_report_union_djem.open_card_count) AS open_card_count,
           sum(view_wb_advert_nm_report_union_djem.add_to_cart_count) AS add_to_cart_count,
           sum(view_wb_advert_nm_report_union_djem.orders_count) AS orders_count,
           sum(view_wb_advert_nm_report_union_djem.orders_sum_rub) AS orders_sum_rub,
           sum(view_wb_advert_nm_report_union_djem.buyouts_count) AS buyouts_count,
           sum(view_wb_advert_nm_report_union_djem.buyouts_sum_rub) AS buyouts_sum_bub,
           NULL::double precision AS buyout_percent,
           NULL::double precision AS add_to_cart_conversion,
           NULL::double precision AS cart_to_order_conversion
    FROM view_wb_advert_nm_report_union_djem
    GROUP BY view_wb_advert_nm_report_union_djem.nm_id,
             view_wb_advert_nm_report_union_djem.dt_on
),
aggregated_advert_fullstats AS (
    SELECT advert_full_stats.nm_id,
           date(advert_full_stats.date_at) AS date_fullstats,
           advert_full_stats.name AS item_name,
           sum(advert_full_stats.views) AS views,
           sum(advert_full_stats.clicks) AS clicks,
           avg(advert_full_stats.ctr) AS ctr,
           avg(advert_full_stats.cpc) AS cpc,
           sum(advert_full_stats.sum) AS sum,
           sum(advert_full_stats.atbs) AS atbs,
           sum(advert_full_stats.orders) AS orders,
           avg(advert_full_stats.cr) AS cr,
           sum(advert_full_stats.shks) AS shks,
           sum(advert_full_stats.sum_price) AS sum_price,
           avg(advert_full_stats.avg_position) AS avg_position
    FROM advert_full_stats
    GROUP BY advert_full_stats.nm_id,
             (date(advert_full_stats.date_at)),
             advert_full_stats.name
)
SELECT nm_rep.nm_id,
       nm_rep.dt_on,
       nm_rep.open_card_count,
       nm_rep.add_to_cart_count,
       nm_rep.orders_count,
       nm_rep.orders_sum_rub,
       nm_rep.buyouts_count,
       nm_rep.buyouts_sum_bub,
       nm_rep.buyout_percent,
       nm_rep.add_to_cart_conversion,
       nm_rep.cart_to_order_conversion,
       adv_fs.nm_id AS adv_nm_id,
       adv_fs.date_fullstats,
       adv_fs.item_name,
       adv_fs.views,
       adv_fs.clicks,
       adv_fs.ctr,
       adv_fs.cpc,
       adv_fs.sum,
       adv_fs.atbs,
       adv_fs.orders,
       adv_fs.cr,
       adv_fs.shks,
       adv_fs.sum_price,
       adv_fs.avg_position,
       sku.title AS name,
       sku.barcode,
       NULL::double precision AS prime_price,
       sku.nm_id AS wb_nm_id
FROM ((aggregated_nm_report nm_rep
  LEFT JOIN aggregated_advert_fullstats adv_fs
    ON ((nm_rep.dt_on = adv_fs.date_fullstats)
       AND (nm_rep.nm_id = adv_fs.nm_id)))
  LEFT JOIN nmids_list sku
    ON ((nm_rep.nm_id = sku.nm_id) AND (sku.nm_id IS NOT NULL)));
""",
    }

    with engine.begin() as conn:
        for view_name, sql in views_sql.items():
            conn.execute(text(sql))


def create_materialized_views(engine):
    with engine.begin() as conn:
        check_mv = text(
            """
                SELECT EXISTS(
                    SELECT 1
                    FROM pg_matviews
                    WHERE matviewname = 'mv_wb_pivot_by_day_dl'
                    AND schemaname = 'public'
                )
            """
        )

        mv_exists = conn.execute(check_mv).scalar()

        if not mv_exists:
            app_logger.info(
                msg='Creating materialized view mv_wb_pivot_by_day_dl'
            )

            conn.execute(
                text(
                    """
                        CREATE MATERIALIZED VIEW mv_wb_pivot_by_day_dl AS
                        SELECT nml.title AS sa_name,
                               nm_rep.nm_id AS "nm_rep.nm_id",
                               nm_rep.dt_on AS "nm_rep.date_on",
                               nm_rep.open_card_count AS "nm_rep.open_card_count",
                               nm_rep.add_to_cart_count AS "nm_rep.add_to_cart_count",
                               nm_rep.orders_count AS "nm_rep.orders_count",
                               nm_rep.orders_sum_rub AS "nm_rep.orders_sum_rub",
                               ac.stat_cost_real AS "adv_fs.sum",
                               stk.date_receiving AS "stk.date_on",
                               stk.summ AS "stk.summ",
                               stk.in_way_to_client AS "stk.in_way_to_client",
                               stk.in_way_from_client AS "stk.in_way_from_client",
                               stk.quantity_full AS "stk.quantity_full",
                               stkf.date_on AS "stkf.date_on",
                               stkf.quantity AS "stkf.quantity",
                               sl.date_on AS "sl.date_on",
                               sl.all_logistics AS "sl.all_logistics",
                               rp.redemption_percentage AS "sl.redemption_percentage",
                               psl.date_on AS "psl.date_on",
                               psl.summ AS "psl.summ",
                               ar.shk_create_date_on AS "ar.date_on",
                               ar.count AS "ar.count",
                               ar.total AS "ar.total",
                               tc.paid_storage_kgvp AS "tc.paid_storage_kgvp",
                               tc.subject_name AS "tc.subject_name",
                               (0)::numeric AS ozon_commission,
                               fs.count_cancel_orders AS "fs.count_cancel_orders",
                               fs.sum_cancel_orders AS "fs.sum_cancel_orders",
                               fs.count_return_sales AS "fs.count_return_sales",
                               fs.sum_return_sales AS "fs.sum_return_sales",
                               (fs.count_item_in_way)::numeric AS "fs.count_item_in_way",
                               fs.sum_item_in_way AS "fs.sum_item_in_way",
                               fs.count_sales AS "fs.count_sales",
                               fs.sum_sales AS "fs.sum_sales",
                               fs.sum_orders_after_spp AS "fs.sum_orders_after_spp",
                               fs.sum_sales_after_spp AS "fs.sum_sales_after_spp",
                               fs.count_orders AS "fs.count_orders_oper",
                               fs.sum_orders AS "fs.sum_orders_oper",
                               (0)::numeric AS "sl.base_logistics_tariff_no_avg_coef",
                               upper((nml.brand)::text) AS brand_name,
                               CASE
                                   WHEN (nm_rep.dt_on = (date_trunc('week'::text, (nm_rep.dt_on)::timestamp with time zone) + '6 days'::interval))
                                     THEN stk.quantity_full
                                   WHEN (nm_rep.dt_on = (CURRENT_DATE - '1 day'::interval))
                                     THEN stk.quantity_full
                                   ELSE (0)::bigint
                               END AS "stk.quantity_full_at_end_week",
                               CASE
                                   WHEN (nm_rep.dt_on = ((date_trunc('month'::text, (nm_rep.dt_on)::timestamp with time zone) + '1 mon'::interval) - '1 day'::interval))
                                     THEN stk.quantity_full
                                   WHEN (nm_rep.dt_on = (CURRENT_DATE - '1 day'::interval))
                                     THEN stk.quantity_full
                                   ELSE (0)::bigint
                               END AS "stk.quantity_full_at_end_month",
                               CASE
                                   WHEN (nm_rep.dt_on = (date_trunc('week'::text, (nm_rep.dt_on)::timestamp with time zone) + '6 days'::interval))
                                     THEN stkf.quantity
                                   WHEN (nm_rep.dt_on = (CURRENT_DATE - '1 day'::interval))
                                     THEN stkf.quantity
                                   ELSE (0)::bigint
                               END AS "stkf.quantity_at_end_week",
                               CASE
                                   WHEN (nm_rep.dt_on = ((date_trunc('month'::text, (nm_rep.dt_on)::timestamp with time zone) + '1 mon'::interval) - '1 day'::interval))
                                     THEN stkf.quantity
                                   WHEN (nm_rep.dt_on = (CURRENT_DATE - '1 day'::interval))
                                     THEN stkf.quantity
                                   ELSE (0)::bigint
                               END AS "stkf.quantity_at_end_month"
                        FROM dasboard_summary nm_rep
                        LEFT JOIN view_wb_stocks stk
                            ON nm_rep.dt_on = stk.date_receiving AND nm_rep.nm_id = stk.nm_id
                        LEFT JOIN view_wb_stocks_fbs stkf
                            ON nm_rep.dt_on = stkf.date_on AND nm_rep.nm_id = stkf.nm_id
                        LEFT JOIN sum_for_logistics sl
                            ON nm_rep.dt_on = sl.date_on AND nm_rep.nm_id = sl.nm_id
                        LEFT JOIN view_wb_paid_storage_logistika psl
                            ON nm_rep.dt_on = psl.date_on AND nm_rep.nm_id = psl.nm_id
                        LEFT JOIN view_wb_acceptance_report_rnp ar
                            ON nm_rep.dt_on = ar.shk_create_date_on AND nm_rep.nm_id = ar.nm_id
                        LEFT JOIN view_wb_tariffs_commission tc
                            ON nm_rep.dt_on = tc.upload_at AND nm_rep.nm_id = tc.nm_id
                        LEFT JOIN view_wb_orders_sales_cancels_returns_by_order_date fs
                            ON nm_rep.dt_on = fs.date_on AND nm_rep.nm_id = fs.nm_id
                        LEFT JOIN view_wb_redemption_percentage_dynamic_7day rp
                            ON nm_rep.dt_on = rp.date_on AND nm_rep.nm_id = rp.nm_id
                        LEFT JOIN view_wb_advert_cost_from_doc ac
                            ON nm_rep.dt_on = ac.date_at AND nm_rep.nm_id = ac.nm_id
                        LEFT JOIN view_wb_nmids_list_updated_no_barcode nml
                            ON nm_rep.nm_id = nml.nm_id
                        WITH NO DATA;
                """
                )
            )

            try:
                conn.execute(
                    text(
                        """
                            CREATE UNIQUE INDEX idx_mv_wb_pivot_unique 
                            ON mv_wb_pivot_by_day_dl ("nm_rep.nm_id", "nm_rep.date_on");
                        """
                    )
                )
                app_logger.info(
                    msg='Index idx_mv_wb_pivot_unique created successfully'
                )
            except Exception as e:
                app_logger.error(
                    msg=f'Index creation warning: {str(e)}'
                )

            try:
                conn.execute(
                    text(
                    """
                        CREATE INDEX idx_mv_wb_pivot_date 
                        ON mv_wb_pivot_by_day_dl ("nm_rep.date_on");
                    """
                    )
                )
                app_logger.info(
                    msg='Index idx_mv_wb_pivot_date created successfully'
                )
            except Exception as e:
                app_logger.error(
                    msg=f'Index creation warning: {str(e)}'
                )
        else:
            app_logger.info(
                msg='Materialized view mv_wb_pivot_by_day_dl already exists, skipping creation'
            )
