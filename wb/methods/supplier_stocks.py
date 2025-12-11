from datetime import datetime

import sqlalchemy.orm

from logs import app_logger
from wb.api import WBApi
from dateutil import parser

from wb.db import SupplierStock, try_to_find_model


def main(
    session: sqlalchemy.orm.Session
):
    app_logger.info(
        msg='Start work'
    )

    wb = WBApi()

    current_date = datetime.today().date()

    filtered_stocks_list = []

    try:
        stocks_data = wb.supplier_stocks()
    except Exception as e:
        app_logger.error(
            msg=f'Problem with get data from WB, error: {str(e)}'
        )
        stocks_data = []

    for stock_data in stocks_data:
        stock_data_dict = {
            'last_change_date_at': stock_data.get('lastChangeDate'),
            'warehouse_name': stock_data.get('warehouseName'),
            'supplier_article': stock_data.get('supplierArticle'),
            'nm_id': stock_data.get('nmId'),
            'barcode': stock_data.get('barcode'),
            'quantity': stock_data.get('quantity'),
            'in_way_to_client': stock_data.get('inWayToClient'),
            'in_way_from_client': stock_data.get('inWayFromClient'),
            'quantity_full': stock_data.get('quantityFull'),
            'category': stock_data.get('category'),
            'subject': stock_data.get('subject'),
            'brand': stock_data.get('brand'),
            'tech_size': stock_data.get('techSize'),
            'price': stock_data.get('Price'),
            'discount': stock_data.get('Discount'),
            'is_supply': stock_data.get('isSupply'),
            'is_realization': stock_data.get('isRealization'),
            'sc_code': stock_data.get('SCCode'),
            'date_receiving': current_date
        }

        lastChangeDate = stock_data_dict.get('last_change_date_at')
        if lastChangeDate:
            stock_data_dict['last_change_date_at'] = parser.parse(
                timestr=lastChangeDate
            )

        if not try_to_find_model(
            session=session,
            model=SupplierStock,
            filters={
                'nm_id': stock_data_dict.get('nm_id'),
                'barcode': stock_data_dict.get('barcode'),
                'warehouse_name': stock_data_dict.get('warehouse_name'),
                'date_receiving': stock_data_dict.get('date_receiving'),
            },
            updates=stock_data_dict
        ):
            filtered_stocks_list.append(SupplierStock(**stock_data_dict))

    if filtered_stocks_list:
        try:
            session.bulk_save_objects(filtered_stocks_list)
            session.commit()

            app_logger.info(
                msg=f'Successfully was saved - {len(filtered_stocks_list)} elements to db'
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with bulk save objects - {len(filtered_stocks_list)} elements, error: {str(e)}'
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
