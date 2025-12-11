from datetime import datetime, timedelta

import sqlalchemy.orm

from logs import app_logger
from wb.api import WBApi
from dateutil import parser

from wb.db import try_to_find_model
from wb.db.models import SupplierSale


def main(
    session: sqlalchemy.orm.Session
):
    app_logger.info(
        msg='Start work'
    )

    wb = WBApi()

    date_from = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')

    filtered_sales_list = []

    try:
        sales_data = wb.supplier_sales(
            date_from=date_from
        )
    except Exception as e:
        app_logger.error(
            msg=f'Problem with get data from WB, error: {str(e)}'
        )
        sales_data = []

    for sale_data in sales_data:
        sale_date_dict = {
            'date_on': sale_data.get('date'),
            'last_change_date_at': sale_data.get('lastChangeDate'),
            'warehouse_name': sale_data.get('warehouseName'),
            'warehouse_type': sale_data.get('warehouseType'),
            'country_name': sale_data.get('countryName'),
            'oblast_okrug_name': sale_data.get('oblastOkrugName'),
            'region_name': sale_data.get('regionName'),
            'supplier_article': sale_data.get('supplierArticle'),
            'nm_id': sale_data.get('nmId'),
            'barcode': sale_data.get('barcode'),
            'category': sale_data.get('category'),
            'subject': sale_data.get('subject'),
            'brand': sale_data.get('brand'),
            'tech_size': sale_data.get('techSize'),
            'income_id': sale_data.get('incomeID'),
            'is_supply': sale_data.get('isSupply'),
            'is_realization': sale_data.get('isRealization'),
            'total_price': sale_data.get('totalPrice'),
            'discount_percent': sale_data.get('discountPercent'),
            'spp': sale_data.get('spp'),
            'payment_sale_amount': sale_data.get('paymentSaleAmount'),
            'for_pay': sale_data.get('forPay'),
            'finished_price': sale_data.get('finishedPrice'),
            'price_with_disc': sale_data.get('priceWithDisc'),
            'sale_id': sale_data.get('saleID'),
            'order_type': sale_data.get('orderType'),
            'sticker': sale_data.get('stocker'),
            'g_number': sale_data.get('gNumber'),
            'srid': sale_data.get('srid'),
        }

        lastChangeDate = sale_date_dict.get('last_change_date_at')
        if lastChangeDate:
            sale_date_dict['last_change_date_at'] = parser.parse(
                timestr=lastChangeDate
            )

        date = sale_date_dict.get('date_on')
        if date:
            sale_date_dict['date_on'] = parser.parse(
                timestr=date
            )

        if not try_to_find_model(
            session=session,
            model=SupplierSale,
            filters={
                'srid': sale_date_dict.get('srid'),
                'nm_id': sale_date_dict.get('nm_id'),
            },
            updates=sale_date_dict
        ):
            filtered_sales_list.append(SupplierSale(**sale_date_dict))

    if filtered_sales_list:
        try:
            session.bulk_save_objects(filtered_sales_list)
            session.commit()

            app_logger.info(
                msg=f'Successfully was saved - {len(filtered_sales_list)} elements to db'
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with bulk save objects - {len(filtered_sales_list)} elements, error: {str(e)}'
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
