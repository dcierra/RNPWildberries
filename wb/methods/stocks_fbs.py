from datetime import datetime

import sqlalchemy.orm

from logs import app_logger
from wb.api import WBApi
from dateutil import parser

from wb.db import try_to_find_model
from wb.db.models import AdvertNMReport, NmIDCard, FbsWarehouse, FbsStock


def get_unique_barcodes(
    session: sqlalchemy.orm.Session
):
    rows = session.query(
        NmIDCard.barcode
    ).filter(
        NmIDCard.barcode.isnot(None)
    ).distinct().all()

    return [row[0] for row in rows]


def get_unique_warehouses(
    session: sqlalchemy.orm.Session
):
    rows = session.query(
        FbsWarehouse.warehouse_id
    ).distinct().all()

    return [row[0] for row in rows]


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

    barcode_list = get_unique_barcodes(
        session=session
    )

    if barcode_list is None:
        app_logger.error(
            msg=f'Does not have an barcodes'
        )
        return

    warehouse_list = get_unique_warehouses(
        session=session
    )

    if warehouse_list is None:
        app_logger.error(
            msg=f'Does not have an warehouses'
        )
        return

    filtered_stocks_fbs_list = []

    date_on = datetime.now().date()

    for warehouse_id in warehouse_list:
        for batch in chunk_list(
            lst=barcode_list,
            chunk_size=1000
        ):
            try:
                stocks_fbs_data = wb.get_stocks_fbs(
                    warehouse_id=warehouse_id,
                    barcodes=batch
                )
            except Exception as e:
                app_logger.error(
                    msg=f'Problem with get data from WB, error: {str(e)}'
                )
                stocks_fbs_data = []

            for stock_fbs_element in stocks_fbs_data:
                fbs_stock_info = {
                    'amount': stock_fbs_element.get('amount'),
                    'sku': stock_fbs_element.get('sku'),
                    'warehouse_id': warehouse_id,
                    'date_on': date_on
                }

                if not try_to_find_model(
                    session=session,
                    model=FbsStock,
                    filters={
                        'sku': fbs_stock_info.get('sku'),
                        'date_on': fbs_stock_info.get('date_on'),
                    },
                    updates=fbs_stock_info
                ):
                    filtered_stocks_fbs_list.append(FbsStock(**fbs_stock_info))

    if filtered_stocks_fbs_list:
        try:
            session.bulk_save_objects(filtered_stocks_fbs_list)
            session.commit()

            app_logger.info(
                msg=f'Successfully was saved - {len(filtered_stocks_fbs_list)} elements to db'
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with bulk save objects - {len(filtered_stocks_fbs_list)} elements, error: {str(e)}'
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
