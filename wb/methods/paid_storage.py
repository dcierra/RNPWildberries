from datetime import datetime, timedelta

import sqlalchemy.orm

from logs import app_logger
from wb.api import WBApi
from dateutil import parser

from wb.db import try_to_find_model
from wb.db.models import PaidStorage


def main(
    session: sqlalchemy.orm.Session
):
    app_logger.info(
        msg='Start work'
    )

    wb = WBApi()

    filtered_paid_storage_list = []

    try:
        uuid = wb.uuid_for_paid_storage(
            date_from=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
            date_to=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
        )
    except Exception as e:
        app_logger.error(
            msg=f'Problem with getting uuid from wb, error: {str(e)}'
        )
        return

    try:
        wb_status = wb.check_status_paid_storage(
            task_id=uuid
        )
    except Exception as e:
        app_logger.error(
            msg=f'Problem with check status, error: {str(e)}'
        )
        return

    if wb_status != 'ok':
        app_logger.error(
            msg=f'Return incorrect value'
        )
        return

    try:
        paid_storage_list = wb.paid_storage(
            task_id=uuid
        )
    except Exception as e:
        app_logger.error(
            msg=f'Problem with get data from WB, error: {str(e)}'
        )
        paid_storage_list = []

    for paid_storage_element in paid_storage_list:
        paid_storage_dict = {
            'date_on': paid_storage_element.get('date'),
            'log_warehouse_coef': paid_storage_element.get('logWarehouseCoef'),
            'office_id': paid_storage_element.get('officeId'),
            'warehouse': paid_storage_element.get('warehouse'),
            'warehouse_coef': paid_storage_element.get('warehouseCoef'),
            'gi_id': paid_storage_element.get('giId'),
            'chrt_id': paid_storage_element.get('chrtId'),
            'size': paid_storage_element.get('size'),
            'barcode': paid_storage_element.get('barcode'),
            'subject': paid_storage_element.get('subject'),
            'brand': paid_storage_element.get('brand'),
            'vendor_code': paid_storage_element.get('vendorCode'),
            'nm_id': paid_storage_element.get('nmId'),
            'volume': paid_storage_element.get('volume'),
            'calc_type': paid_storage_element.get('calcType'),
            'warehouse_price': paid_storage_element.get('warehousePrice'),
            'barcodes_count': paid_storage_element.get('barcodesCount'),
            'pallet_place_code': paid_storage_element.get('palletPlaceCode'),
            'pallet_count': paid_storage_element.get('palletCount'),
            'original_date_on': paid_storage_element.get('originalDate'),
        }

        paid_storage_date = paid_storage_dict.get('date_on')
        if paid_storage_date:
            paid_storage_dict['date_on'] = parser.parse(
                timestr=paid_storage_date
            )

        original_date = paid_storage_dict.get('original_date_on')
        if original_date:
            paid_storage_dict['original_date_on'] = parser.parse(
                timestr=original_date
            )

        if not try_to_find_model(
            session=session,
            model=PaidStorage,
            filters={
                'office_id': paid_storage_dict.get('office_id'),
                'gi_id': paid_storage_dict.get('gi_id'),
                'chrt_id': paid_storage_dict.get('chrt_id'),
                'calc_type': paid_storage_dict.get('calc_type'),
                'nm_id': paid_storage_dict.get('nm_id'),
            },
            updates=paid_storage_dict
        ):
            filtered_paid_storage_list.append(PaidStorage(**paid_storage_dict))

    if filtered_paid_storage_list:
        try:
            session.bulk_save_objects(filtered_paid_storage_list)
            session.commit()

            app_logger.info(
                msg=f'Successfully was saved - {len(filtered_paid_storage_list)} elements to db'
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with bulk save objects - {len(filtered_paid_storage_list)} elements, error: {str(e)}'
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
