import sqlalchemy.orm

from logs import app_logger
from wb.api import WBApi

from wb.db import try_to_find_model
from wb.db.models import FbsWarehouse


def main(
    session: sqlalchemy.orm.Session
):
    app_logger.info(
        msg='Start work'
    )

    wb = WBApi()

    filtered_fbs_warehouse_list = []

    try:
        fbs_warehouse_data = wb.get_warehouses()
    except Exception as e:
        app_logger.error(
            msg=f'Problem with get data from WB, error: {str(e)}'
        )
        fbs_warehouse_data = []

    for fbs_warehouse_element in fbs_warehouse_data:
        fbs_warehouse_info = {
            'name': fbs_warehouse_element.get('name'),
            'office_id': fbs_warehouse_element.get('officeId'),
            'warehouse_id': fbs_warehouse_element.get('id'),
            'cargo_type': fbs_warehouse_element.get('cargoType'),
            'delivery_type': fbs_warehouse_element.get('deliveryType'),
        }

        if not try_to_find_model(
            session=session,
            model=FbsWarehouse,
            filters={
                'warehouse_id': fbs_warehouse_info.get('warehouse_id'),
                'delivery_type': fbs_warehouse_info.get('delivery_type'),
                'cargo_type': fbs_warehouse_info.get('cargo_type'),
                'office_id': fbs_warehouse_info.get('office_id'),
            },
            updates=fbs_warehouse_info
        ):
            filtered_fbs_warehouse_list.append(FbsWarehouse(**fbs_warehouse_info))

    if filtered_fbs_warehouse_list:
        try:
            session.bulk_save_objects(filtered_fbs_warehouse_list)
            session.commit()

            app_logger.info(
                msg=f'Successfully was saved - {len(filtered_fbs_warehouse_list)} elements to db'
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with bulk save objects - {len(filtered_fbs_warehouse_list)} elements, error: {str(e)}'
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
