from datetime import date

import sqlalchemy.orm

from logs import app_logger
from wb.api import WBApi

from wb.db import try_to_find_model
from wb.db.models import TariffBox, TariffCommission


def main(
    session: sqlalchemy.orm.Session
):
    app_logger.info(
        msg='Start work'
    )

    wb = WBApi()

    upload_at = date.today()

    filtered_tariffs_commission = []

    try:
        tariffs_commission = wb.tariffs_commission()
    except Exception as e:
        app_logger.error(
            msg=f'Problem with get data from WB, error: {str(e)}'
        )
        tariffs_commission = []

    for tariff_commission in tariffs_commission:
        tariff_commission_dict = {
            'kgvp_marketplace': tariff_commission.get('kgvpMarketplace'),
            'kgvp_supplier': tariff_commission.get('kgvpSupplier'),
            'kgvp_supplier_express': tariff_commission.get('kgvpSupplierExpress'),
            'paid_storage_kgvp': tariff_commission.get('paidStorageKgvp'),
            'parent_id': tariff_commission.get('parentID'),
            'parent_name': tariff_commission.get('parentName'),
            'subject_id': tariff_commission.get('subjectID'),
            'subject_name': tariff_commission.get('subjectName'),
            'upload_at': upload_at
        }

        if not try_to_find_model(
            session=session,
            model=TariffCommission,
            filters={
                'parent_id': tariff_commission_dict.get('parent_id'),
                'subject_id': tariff_commission_dict.get('subject_id'),
                'upload_at': tariff_commission_dict.get('upload_at'),
            },
            updates=tariff_commission_dict
        ):
            filtered_tariffs_commission.append(TariffCommission(**tariff_commission_dict))

    if filtered_tariffs_commission:
        try:
            session.bulk_save_objects(filtered_tariffs_commission)
            session.commit()

            app_logger.info(
                msg=f'Successfully was saved - {len(filtered_tariffs_commission)} elements to db'
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with bulk save objects - {len(filtered_tariffs_commission)} elements, error: {str(e)}'
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
