from datetime import datetime, timedelta

import sqlalchemy.orm
from sqlalchemy import text

from logs import app_logger
from wb.api import WBApi
from dateutil import parser

from wb.db import try_to_find_model
from wb.db.models import AdvertNMReport


def get_all_nmids_union(
    session: sqlalchemy.orm.Session
) -> list[int]:
    statement = text(
        """
            SELECT DISTINCT nm_id FROM (
                SELECT nm_id FROM supplier_stocks
                UNION
                SELECT nm_id FROM nmids_list
            ) AS unioned_nmids
        """
    )

    result = session.execute(statement).scalars().all()
    return result


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

    nmids_list = get_all_nmids_union(
        session=session
    )

    if nmids_list is None:
        app_logger.error(
            msg=f'Does not have an nm id'
        )
        return

    date_to = (datetime.now()).strftime('%Y-%m-%d')
    date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    saved_count = 0

    for nmids_chunk in chunk_list(
        lst=nmids_list,
        chunk_size=20
    ):
        filtered_advert_nm_report_list = []

        try:
            advert_nm_report_data = wb.advert_nm_report(
                nmids=nmids_chunk,
                date_from=date_from,
                date_to=date_to
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with get data from WB, error: {str(e)}'
            )
            advert_nm_report_data = []

        for advert_nm_report_element in advert_nm_report_data:
            history_element = advert_nm_report_element.get('history')

            if history_element:
                for date_history_element in history_element:
                    advert_nm_report_info = {
                        'nm_id': advert_nm_report_element.get('nmID'),
                        'imt_name': advert_nm_report_element.get('imtName'),
                        'vendor_code': advert_nm_report_element.get('vendorCode'),

                        'dt_on': date_history_element.get('dt'),
                        'open_card_count': date_history_element.get('openCardCount'),
                        'add_to_cart_count': date_history_element.get('addToCartCount'),
                        'add_to_cart_conversion': date_history_element.get('addToCartConversion'),
                        'orders_count': date_history_element.get('ordersCount'),
                        'orders_sum_rub': date_history_element.get('ordersSumRub'),
                        'cart_to_order_conversion': date_history_element.get('cartToOrderConversion'),
                        'buyouts_count': date_history_element.get('buyoutsCount'),
                        'buyouts_sum_bub': date_history_element.get('buyoutsSumRub'),
                        'buyout_percent': date_history_element.get('buyoutPercent')
                    }

                    dt = advert_nm_report_info.get('dt_on')
                    if dt:
                        advert_nm_report_info['dt_on'] = parser.parse(
                            timestr=dt
                        )

                    if not try_to_find_model(
                        session=session,
                        model=AdvertNMReport,
                        filters={
                            'nm_id': advert_nm_report_info.get('nm_id'),
                            'dt_on': advert_nm_report_info.get('dt_on'),
                        },
                        updates=advert_nm_report_info
                    ):
                        filtered_advert_nm_report_list.append(AdvertNMReport(**advert_nm_report_info))

        if filtered_advert_nm_report_list:
            try:
                session.bulk_save_objects(filtered_advert_nm_report_list)
                session.commit()

                saved_count += len(filtered_advert_nm_report_list)
            except Exception:
                session.rollback()

    if saved_count > 0:
        app_logger.info(
            msg=f'Successfully was saved - {saved_count} elements to db'
        )
    else:
        app_logger.info(
            msg=f'No new records'
        )

    app_logger.info(
        msg='End work'
    )


if __name__ == '__main__':
    main()
