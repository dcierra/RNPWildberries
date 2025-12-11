from datetime import date

import sqlalchemy.orm

from logs import app_logger
from wb.api import WBApi

from wb.db import try_to_find_model
from wb.db.models import TariffBox


def convert_fields_to_float(
    data: dict,
    fields: list[str],
) -> dict:
    try:
        for field in fields:
            value = data.get(field)

            if value is not None and value != '':
                try:
                    if isinstance(value, str):
                        data[field] = float(value.replace(',', '.'))
                    else:
                        data[field] = float(value)
                except (ValueError, TypeError) as e:
                    data[field] = 0.0
            else:
                data[field] = 0.0

        return data
    except Exception:
        return data


def main(
    session: sqlalchemy.orm.Session
):
    app_logger.info(
        msg='Start work'
    )

    wb = WBApi()

    upload_at = date.today()

    filtered_tariffs_box = []

    try:
        tariffs_box = wb.tariffs_box_and_pallet(
            date=upload_at.strftime('%Y-%m-%d')
        )
    except Exception as e:
        app_logger.error(
            msg=f'Problem with get data from WB, error: {str(e)}'
        )
        tariffs_box = []

    for tariff_box in tariffs_box:
        tariff_box_dict = {
            'warehouse_name': tariff_box.get('warehouseName'),
            'box_delivery_and_storage_expr': tariff_box.get('boxDeliveryAndStorageExpr'),
            'box_delivery_base': tariff_box.get('boxDeliveryBase'),
            'box_delivery_liter': tariff_box.get('boxDeliveryLiter'),
            'box_storage_base': tariff_box.get('boxStorageBase'),
            'box_storage_liter': tariff_box.get('boxStorageLiter'),
            'upload_at': upload_at,
        }

        try:
            tariff_box_data = convert_fields_to_float(
                data=tariff_box_dict,
                fields=[
                    'box_delivery_and_storage_expr',
                    'box_delivery_base',
                    'box_delivery_liter',
                    'box_storage_base',
                    'box_storage_liter',
                ]
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with convert fields to float - {tariff_box_dict}, error: {str(e)}'
            )

            continue

        if not try_to_find_model(
            session=session,
            model=TariffBox,
            filters={
                'warehouse_name': tariff_box_data.get('warehouse_name'),
                'upload_at': tariff_box_data.get('upload_at'),
            },
            updates=tariff_box_data
        ):
            filtered_tariffs_box.append(TariffBox(**tariff_box_data))

    if filtered_tariffs_box:
        try:
            session.bulk_save_objects(filtered_tariffs_box)
            session.commit()

            app_logger.info(
                msg=f'Successfully was saved - {len(filtered_tariffs_box)} elements to db'
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with bulk save objects - {len(filtered_tariffs_box)} elements, error: {str(e)}'
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
