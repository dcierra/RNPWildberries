from datetime import datetime, timedelta

import sqlalchemy.orm

from logs import app_logger
from wb.api import WBApi
from dateutil import parser

from wb.db import try_to_find_model
from wb.db.models import SupplierOrder


def main(
    session: sqlalchemy.orm.Session
):
    app_logger.info(
        msg='Start work'
    )

    wb = WBApi()

    date_from = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')

    filtered_orders_list = []

    try:
        orders_data = wb.supplier_orders(
            date_from=date_from
        )
    except Exception as e:
        app_logger.error(
            msg=f'Problem with get data from WB, error: {str(e)}'
        )
        orders_data = []

    for order_data in orders_data:
        order_data_dict = {
            'date_on': order_data.get('date'),
            'last_change_date_on': order_data.get('lastChangeDate'),
            'warehouse_name': order_data.get('warehouseName'),
            'warehouse_type': order_data.get('warehouseType'),
            'country_name': order_data.get('countryName'),
            'oblast_okrug_name': order_data.get('oblastOkrugName'),
            'region_name': order_data.get('regionName'),
            'supplier_article': order_data.get('supplierArticle'),
            'nm_id': order_data.get('nmId'),
            'barcode': order_data.get('barcode'),
            'category': order_data.get('category'),
            'subject': order_data.get('subject'),
            'brand': order_data.get('brand'),
            'tech_size': order_data.get('techSize'),
            'income_id': order_data.get('incomeID'),
            'is_supply': order_data.get('isSupply'),
            'is_realization': order_data.get('isRealization'),
            'total_price': order_data.get('totalPrice'),
            'discount_percent': order_data.get('discountPercent'),
            'spp': order_data.get('spp'),
            'finished_price': order_data.get('finishedPrice'),
            'price_with_disc': order_data.get('priceWithDisc'),
            'is_cancel': order_data.get('isCancel'),
            'cancel_date_at': order_data.get('cancelDate'),
            'order_type': order_data.get('order_type'),
            'sticker': order_data.get('sticker'),
            'g_number': order_data.get('gNumber'),
            'srid': order_data.get('srid'),
        }

        lastChangeDate = order_data_dict.get('last_change_date_on')
        if lastChangeDate:
            order_data_dict['last_change_date_on'] = parser.parse(
                timestr=lastChangeDate
            )

        date = order_data_dict.get('date_on')
        if date:
            order_data_dict['date_on'] = parser.parse(
                timestr=date
            )

        cancelDate = order_data_dict.get('cancel_date_at')
        if cancelDate:
            order_data_dict['cancel_date_at'] = parser.parse(
                timestr=cancelDate
            )

        if not try_to_find_model(
            session=session,
            model=SupplierOrder,
            filters={
                'srid': order_data_dict.get('srid'),
                'nm_id': order_data_dict.get('nm_id'),
            },
            updates=order_data_dict
        ):
            filtered_orders_list.append(SupplierOrder(**order_data_dict))

    if filtered_orders_list:
        try:
            session.bulk_save_objects(filtered_orders_list)
            session.commit()

            app_logger.info(
                msg=f'Successfully was saved - {len(filtered_orders_list)} elements to db'
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with bulk save objects - {len(filtered_orders_list)} elements, error: {str(e)}'
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
