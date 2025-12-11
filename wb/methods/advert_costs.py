from datetime import datetime, timedelta

import sqlalchemy.orm

from logs import app_logger
from wb.api import WBApi
from dateutil import parser

from wb.db import try_to_find_model
from wb.db.models import AdvertCost


def main(
    session: sqlalchemy.orm.Session
):
    app_logger.info(
        msg='Start work'
    )

    wb = WBApi()

    filtered_advert_cost_list = []

    try:
        advert_costs = wb.get_advert_cost(
            date_from=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
            date_to=(datetime.now()).strftime('%Y-%m-%d'),
        )
    except Exception as e:
        app_logger.error(
            msg=f'Problem with getting data from wb, error: {str(e)}'
        )
        return

    for advert_cost in advert_costs:
        advert_cost_dict = {
            'upd_time_at': advert_cost.get('updTime'),
            'camp_name': advert_cost.get('campName'),
            'payment_type': advert_cost.get('paymentType'),
            'upd_num': advert_cost.get('updNum'),
            'upd_sum': advert_cost.get('updSum'),
            'advert_id': advert_cost.get('advertId'),
            'advert_type': advert_cost.get('advertType'),
            'advert_status': advert_cost.get('advertStatus'),
        }

        upd_time = advert_cost_dict.get('upd_time_at')
        if upd_time:
            advert_cost_dict['upd_time_at'] = parser.parse(
                timestr=upd_time
            )

        if not try_to_find_model(
            session=session,
            model=AdvertCost,
            filters={
                'advert_id': advert_cost_dict.get('advert_id'),
            },
            updates=advert_cost_dict
        ):
            filtered_advert_cost_list.append(AdvertCost(**advert_cost_dict))

    if filtered_advert_cost_list:
        try:
            session.bulk_save_objects(filtered_advert_cost_list)
            session.commit()

            app_logger.info(
                msg=f'Successfully was saved - {len(filtered_advert_cost_list)} elements to db'
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with bulk save objects - {len(filtered_advert_cost_list)} elements, error: {str(e)}'
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
