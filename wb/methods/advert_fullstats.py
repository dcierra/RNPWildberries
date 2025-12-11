from datetime import datetime, timedelta

import sqlalchemy.orm
from sqlalchemy import func

from logs import app_logger
from wb.api import WBApi
from dateutil import parser

from wb.db import try_to_find_model
from wb.db.models import AdvertFullStat, Advert
from wb.pydantic_models import AdvertFullStatResponse


def get_unique_advert_ids(
    session: sqlalchemy.orm.Session,
):
    three_month_ago = datetime.now() - timedelta(days=90)

    rn = func.row_number().over(
        partition_by=[Advert.advert_id],
        order_by=Advert.change_time_at.desc()
    ).label('rn')

    sub_query = (
        session.query(
            Advert.advert_id,
            Advert.advert_status,
            Advert.change_time_at,
            rn
        ).filter(
            Advert.change_time_at >= three_month_ago
        ).subquery()
    )

    result = (
        session.query(
            func.array_agg(
                func.distinct(sub_query.c.advert_id)
            ).label('advert_ids')
        )
        .filter(
            sub_query.c.rn == 1,
            sub_query.c.advert_status.in_([7, 9, 11])
        ).first()
    )

    advert_ids = result.advert_ids if result and result.advert_ids else []
    advert_ids = list(map(int, advert_ids))

    return advert_ids


def chunk_list(
    lst: list[int],
    chunk_size: int
):
    for index in range(0, len(lst), chunk_size):
        yield lst[index:index + chunk_size]


def main(
    session: sqlalchemy.orm.Session
):
    app_logger.info(
        msg='Start work'
    )

    wb = WBApi()

    advert_ids = get_unique_advert_ids(
        session=session
    )

    if advert_ids is None:
        app_logger.error(
            msg=f'Does not have an advert in the database for last 3 month'
        )
        return

    filtered_advert_full_stats_list = []

    interval_end = (datetime.now()).strftime('%Y-%m-%d')
    interval_begin = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    for advert_ids_chunk in chunk_list(
        lst=advert_ids,
        chunk_size=50
    ):
        try:
            advert_full_stats_data = wb.advert_full_stats(
                advert_ids=advert_ids_chunk,
                interval_begin=interval_begin,
                interval_end=interval_end
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with get data from WB, error: {str(e)}'
            )
            advert_full_stats_data = []

        for advert_full_stat_data in advert_full_stats_data:
            try:
                advert_full_stat_response = AdvertFullStatResponse(**advert_full_stat_data)
            except Exception as e:
                app_logger.exception(
                    msg=f'Problem with unpack data: {str(e)}'
                )
                continue

            for day in advert_full_stat_response.days or []:
                try:
                    if day.day_date:
                        advert_date = parser.parse(
                            timestr=day.day_date
                        )

                        advert_id = advert_full_stat_response.advert_id

                        booster_stats = advert_full_stat_response.booster_stats or []
                        day_apps = day.day_apps or []

                        if day_apps:
                            for app in day_apps:
                                app_type = app.app_type

                                app_nms = app.app_nms or []
                                if app_nms:
                                    for nm in app_nms:
                                        advert_full_stat_dict = {
                                            'date_at': advert_date,
                                            'advert_id': advert_id,
                                            'app_type': app_type,
                                            'name': nm.nm_name,
                                            'nm_id': nm.nm_id,
                                            'views': nm.nm_views,
                                            'clicks': nm.nm_clicks,
                                            'ctr': nm.nm_ctr,
                                            'cpc': nm.nm_cpc,
                                            'sum': nm.nm_sum,
                                            'atbs': nm.nm_atbs,
                                            'orders': nm.nm_orders,
                                            'cr': nm.nm_cr,
                                            'shks': nm.nm_shks,
                                            'sum_price': nm.nm_sum_price,
                                            'canceled': nm.nm_canceled
                                        }

                                        if booster_stats:
                                            for booster_stat in booster_stats:
                                                booster_date = parser.parse(
                                                    timestr=booster_stat.booster_date
                                                )

                                                if (
                                                    booster_date == advert_date and
                                                    booster_stat.booster_nm == advert_full_stat_dict.get('nm_id')
                                                ):
                                                    advert_full_stat_dict.update(
                                                        {
                                                            'avg_position': booster_stat.booster_avg_position
                                                        }
                                                    )

                                        if not try_to_find_model(
                                            session=session,
                                            model=AdvertFullStat,
                                            filters={
                                                'app_type': advert_full_stat_dict.get('app_type'),
                                                'advert_id': advert_full_stat_dict.get('advert_id'),
                                                'nm_id': advert_full_stat_dict.get('nm_id'),
                                            },
                                            updates=advert_full_stat_dict
                                        ):
                                            filtered_advert_full_stats_list.append(
                                                AdvertFullStat(**advert_full_stat_dict)
                                            )
                except Exception as e:
                    app_logger.exception(
                        msg=f'Problem with advert {advert_full_stat_data}, error: {str(e)}'
                    )
                    continue

    if filtered_advert_full_stats_list:
        try:
            session.bulk_save_objects(filtered_advert_full_stats_list)
            session.commit()

            app_logger.info(
                msg=f'Successfully was saved - {len(filtered_advert_full_stats_list)} elements to db'
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with bulk save objects - {len(filtered_advert_full_stats_list)} elements, error: {str(e)}'
            )
            session.rollback()
    else:
        app_logger.info(
            msg=f'No new records'
        )

    app_logger.info(
        msg='End work'
    )


if __name__ == '__main__':
    main()
