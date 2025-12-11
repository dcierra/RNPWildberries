import sqlalchemy.orm

from logs import app_logger
from wb.api import WBApi
from dateutil import parser

from wb.db import try_to_find_model
from wb.db.models import NmIDCard


def main(
    session: sqlalchemy.orm.Session
):
    app_logger.info(
        msg='Start work'
    )

    wb = WBApi()

    filtered_nmid_cards_list = []

    try:
        nmid_cards_data = wb.cards_list()
    except Exception as e:
        app_logger.error(
            msg=f'Problem with get data from WB, error: {str(e)}'
        )
        nmid_cards_data = []

    for nmid_card_data in nmid_cards_data:
        sizes = nmid_card_data.get('sizes')

        if sizes:
            for nmid_card_size in sizes:
                nmid_card = {
                    'nm_id': nmid_card_data.get('nmID'),
                    'imt_id': nmid_card_data.get('imtID'),
                    'nm_uuid': nmid_card_data.get('nmUUID'),
                    'subject_id': nmid_card_data.get('subjectID'),
                    'subject_name': nmid_card_data.get('subjectName'),
                    'vendor_code': nmid_card_data.get('vendorCode'),
                    'brand': nmid_card_data.get('brand'),
                    'title': nmid_card_data.get('title'),
                    'chrt_id': nmid_card_size.get('chrtID'),
                    'tech_size': nmid_card_size.get('techSize'),
                    'wb_size': nmid_card_size.get('wbSize'),
                    'length': nmid_card_data.get('dimensions').get('length'),
                    'width': nmid_card_data.get('dimensions').get('width'),
                    'height': nmid_card_data.get('dimensions').get('height'),
                    'created_at': nmid_card_data.get('createdAt'),
                    'updated_at': nmid_card_data.get('updatedAt'),
                }

                if len(nmid_card_size['skus']) > 0:
                    nmid_card['barcode'] = nmid_card_size.get('skus')[0]
                else:
                    nmid_card['barcode'] = None

                createdAt = nmid_card.get('created_at')
                if createdAt:
                    nmid_card['created_at'] = parser.parse(
                        timestr=createdAt
                    )

                updatedAt = nmid_card.get('updated_at')
                if updatedAt:
                    nmid_card['updated_at'] = parser.parse(
                        timestr=updatedAt
                    )

                if not try_to_find_model(
                    session=session,
                    model=NmIDCard,
                    filters={
                        'nm_id': nmid_card.get('nm_id'),
                        'chrt_id': nmid_card.get('chrt_id'),
                    },
                    updates=nmid_card
                ):
                    filtered_nmid_cards_list.append(NmIDCard(**nmid_card))

    if filtered_nmid_cards_list:
        try:
            session.bulk_save_objects(filtered_nmid_cards_list)
            session.commit()

            app_logger.info(
                msg=f'Successfully was saved - {len(filtered_nmid_cards_list)} elements to db'
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with bulk save objects - {len(filtered_nmid_cards_list)} elements, error: {str(e)}'
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
