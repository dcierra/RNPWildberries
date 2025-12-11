import csv
import datetime
import os
import tempfile
import time
import uuid
import zipfile

import sqlalchemy.orm
from dateutil.relativedelta import relativedelta
from sqlalchemy import func

from logs import app_logger
from wb.api import WBApi
from dateutil import parser

from wb.db import try_to_find_model
from wb.db.models import AdvertNMReportExtended


def get_advert_nm_extended_report(
    wb: WBApi,
    date_start: datetime,
    date_end: datetime,
    nm_ids: list[int] = None
) -> list[dict]:
    report_uuid = str(uuid.uuid4())

    report_created = wb.create_extended_advert_nm_report(
        report_uuid=report_uuid,
        start_date=date_start,
        end_date=date_end,
        nm_ids=nm_ids if nm_ids else []
    )

    if not report_created:
        app_logger.error(
            msg=f'Failed to create report with UUID: {report_uuid}'
        )
        return []

    with tempfile.TemporaryDirectory() as temp_dir:
        zip_path = os.path.join(
            temp_dir, f'{report_uuid}.zip'
        )

        for _ in range(5):
            time.sleep(30)

            downloaded_path = wb.download_extended_advert_nm_report(
                report_uuid=report_uuid,
                save_path=zip_path
            )

            if not downloaded_path:
                continue
            break

        if not downloaded_path:
            app_logger.error(
                msg=f'Failed to download report with UUID: {report_uuid}'
            )
            return []

        report_data = []
        with zipfile.ZipFile(
            file=zip_path,
            mode='r'
        ) as zip_ref:
            csv_files = [
                f for f in zip_ref.namelist() if f.endswith('.csv')
            ]

            if not csv_files:
                app_logger.error(
                    msg=f'No CSV files found in the archive for report UUID: {report_uuid}'
                )
                return []

            csv_path = os.path.join(
                temp_dir, csv_files[0]
            )

            zip_ref.extract(
                member=csv_files[0],
                path=temp_dir
            )

            with open(
                file=csv_path,
                mode='r',
                encoding='utf-8'
            ) as csv_file:
                csv_reader = csv.DictReader(
                    f=csv_file
                )

                for row in csv_reader:
                    numeric_fields = [
                        'openCardCount',
                        'addToCartCount',
                        'ordersCount',
                        'ordersSumRub',
                        'buyoutsCount',
                        'buyoutsSumRub',
                        'cancelCount',
                        'cancelSumRub',
                        'addToCartConversion',
                        'cartToOrderConversion',
                        'buyoutPercent',
                    ]

                    for field in numeric_fields:
                        if field in row and row[field]:
                            try:
                                if '.' in row[field]:
                                    row[field] = float(row[field])
                                else:
                                    row[field] = int(row[field])
                            except ValueError:
                                pass

                    report_data.append(row)

        return report_data


def compute_date_range(
    session: sqlalchemy.orm.Session
):
    max_dt_on = session.query(
        func.max(AdvertNMReportExtended.dt_on)
    ).scalar()

    if not max_dt_on:
        date_from = datetime.date(datetime.datetime.now().year, 1, 1)
    else:
        date_from = (max_dt_on.date() if isinstance(max_dt_on, datetime.datetime) else max_dt_on) - relativedelta(
            months=1
        )

    date_to = datetime.date.today()

    return date_from.strftime('%Y-%m-%d'), date_to.strftime('%Y-%m-%d')


def main(
    session: sqlalchemy.orm.Session
):
    app_logger.info(
        msg='Start work'
    )

    wb = WBApi()

    date_from, date_to = compute_date_range(
        session=session,
    )

    filtered_extended_advert_nm_report_list = []

    try:
        report_data = get_advert_nm_extended_report(
            wb=wb,
            date_start=date_from,
            date_end=date_to,
        )
    except Exception as e:
        app_logger.error(
            f'Error while processing report data - {str(e)}'
        )
        return

    if not report_data:
        app_logger.warning(
            f'No data received'
        )
        return
    for report_item in report_data:
        try:
            db_record = {
                'nm_id': int(report_item.get('nmID', 0)),
                'dt_on': report_item.get('dt'),
                'open_card_count': int(report_item.get('openCardCount', 0)),
                'add_to_cart_count': int(report_item.get('addToCartCount', 0)),
                'orders_count': int(report_item.get('ordersCount', 0)),
                'orders_sum_rub': int(report_item.get('ordersSumRub', 0)),
                'buyouts_count': int(report_item.get('buyoutsCount', 0)),
                'buyouts_sum_rub': int(report_item.get('buyoutsSumRub', 0)),
                'cancel_count': int(report_item.get('cancelCount', 0)),
                'cancel_sum_rub': int(report_item.get('cancelSumRub', 0)),
                'add_to_cart_conversion': float(report_item.get('addToCartConversion', 0.0)),
                'cart_to_order_conversion': float(report_item.get('cartToOrderConversion', 0.0)),
                'buyout_percent': int(report_item.get('buyoutPercent', 0)),
            }

            dt_on = db_record.get('dt_on')
            if dt_on:
                db_record['dt_on'] = parser.parse(
                    timestr=dt_on
                )

            if not try_to_find_model(
                session=session,
                model=AdvertNMReportExtended,
                filters={
                    'nm_id': db_record.get('nm_id'),
                    'dt_on': db_record.get('dt_on'),
                },
                updates=db_record
            ):
                filtered_extended_advert_nm_report_list.append(AdvertNMReportExtended(**db_record))
        except Exception as e:
            app_logger.error(
                f'Error with processing record: {report_item} - {str(e)}'
            )
            continue

    if filtered_extended_advert_nm_report_list:
        try:
            session.bulk_save_objects(filtered_extended_advert_nm_report_list)
            session.commit()

            app_logger.info(
                msg=f'Successfully was saved - {len(filtered_extended_advert_nm_report_list)} elements to db'
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with bulk save objects - {len(filtered_extended_advert_nm_report_list)} elements,'
                    f' error: {str(e)}'
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
