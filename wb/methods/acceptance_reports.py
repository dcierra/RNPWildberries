import time
from datetime import datetime, timedelta

import sqlalchemy.orm

from logs import app_logger
from wb.api import WBApi

from wb.db import try_to_find_model
from wb.db.models import AcceptanceReport


def main(
    session: sqlalchemy.orm.Session
):
    app_logger.info(
        msg='Start work'
    )

    wb = WBApi()

    date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    date_to = (datetime.now()).strftime('%Y-%m-%d')

    filtered_acceptance_reports_list = []

    task_id = wb.get_id_acceptance_reports(
        date_from=date_from,
        date_to=date_to
    ).get('taskId')

    for _ in range(5):
        time.sleep(60)

        status = wb.get_status_acceptance_reports(
            task_id=task_id
        )
        status = status.get('status')

        if status == 'done':
            try:
                acceptance_reports = wb.get_acceptance_reports(
                    task_id=task_id
                )
            except Exception as e:
                app_logger.error(
                    f'Problem with get_acceptance_reports WB, error: {str(e)}'
                )
                continue

            if acceptance_reports:
                for acceptance_report in acceptance_reports:
                    acceptance_report_info = {
                        'income_id': acceptance_report.get('incomeId'),
                        'nm_id': acceptance_report.get('nmID'),
                        'shk_create_date_on': acceptance_report.get('shkCreateDate'),
                        'count': acceptance_report.get('count'),
                        'gi_create_date_on': acceptance_report.get('giCreateDate'),
                        'subject_name': acceptance_report.get('subjectName'),
                        'total': acceptance_report.get('total'),
                    }

                    if not try_to_find_model(
                        session=session,
                        model=AcceptanceReport,
                        filters={
                            'income_id': acceptance_report_info.get('income_id'),
                            'nm_id': acceptance_report_info.get('nm_id'),
                        },
                        updates=acceptance_report_info
                    ):
                        filtered_acceptance_reports_list.append(AcceptanceReport(**acceptance_report_info))
            else:
                app_logger.info(
                    msg=f'Has no acceptance reports for date period: {date_from} - {date_to}'
                )
                break

    if filtered_acceptance_reports_list:
        try:
            session.bulk_save_objects(filtered_acceptance_reports_list)
            session.commit()

            app_logger.info(
                msg=f'Successfully was saved - {len(filtered_acceptance_reports_list)} elements to db'
            )
        except Exception as e:
            app_logger.error(
                msg=f'Problem with bulk save objects - {len(filtered_acceptance_reports_list)} elements, '
                    f'error: {str(e)}'
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
