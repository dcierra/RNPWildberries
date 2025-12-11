import sqlalchemy.orm

from logs import app_logger
from wb.api import WBApi
from dateutil import parser

from wb.db import try_to_find_model
from wb.db.models import Advert


def main(
    session: sqlalchemy.orm.Session
):
    app_logger.info(
        msg='Start work'
    )

    wb = WBApi()

    filtered_adverts_list = []

    try:
        advert_list = wb.advert_list()
    except Exception as e:
        app_logger.error(
            msg=f'Problem with get data from WB, error: {str(e)}'
        )
        advert_list = []

    for advert in advert_list:
        advert_main_data = {
            'advert_type': advert.get('type'),
            'advert_count': advert.get('count'),
            'advert_status': advert.get('status')
        }

        for advert_el in advert.get('advert_list', []):
            advert_info = {
                'advert_id': advert_el.get('advertId'),
                'change_time_at': advert_el.get('changeTime')
            }

            advert_info.update(advert_main_data)

            change_time = advert_info.get('change_time_at')
            if change_time:
                advert_info['change_time_at'] = parser.parse(
                    timestr=change_time
                )

            if not try_to_find_model(
                session=session,
                model=Advert,
                filters={
                    'advert_id': advert_info.get('advert_id'),
                },
                updates=advert_info
            ):
                filtered_adverts_list.append(Advert(**advert_info))

    if filtered_adverts_list:
        try:
            session.bulk_save_objects(filtered_adverts_list)
            session.commit()

            app_logger.info(
                msg=f'Successfully was saved - {len(filtered_adverts_list)} elements to db'
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with bulk save objects - {len(filtered_adverts_list)} elements, error: {str(e)}'
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
