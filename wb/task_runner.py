import importlib
import pkgutil
import threading
from pathlib import Path
from sqlalchemy import text

from logs import app_logger
from wb.api import WBApi, WBCategory
from wb.db.connector import get_session, init_db


def check_token():
    try:
        wb = WBApi()

        wb.validate_token(
            required_categories=[
                WBCategory.MARKETPLACE,
                WBCategory.STATISTICS,
                WBCategory.CONTENT,
                WBCategory.ANALYTICS,
                WBCategory.PROMOTION,
                WBCategory.COMMON,
            ],
            return_inaccessible_categories_str=True
        )

        return None
    except Exception as e:
        return str(e)


def refresh_materialized_view(session):
    try:
        session.execute(
            text(
                'REFRESH MATERIALIZED VIEW CONCURRENTLY mv_wb_pivot_by_day_dl;'
            )
        )

        session.commit()

        app_logger.info(
            msg='CONCURRENTLY refresh successful'
        )
    except Exception as e:
        session.rollback()

        if 'not populated' in str(e).lower():
            app_logger.info(
                msg='MV is empty, using regular refresh...'
            )

            try:
                session.execute(
                    text(
                        'REFRESH MATERIALIZED VIEW mv_wb_pivot_by_day_dl;'
                    )
                )

                session.commit()

                app_logger.info(
                    msg='Regular refresh successful'
                )
            except Exception as refresh_error:
                session.rollback()

                app_logger.error(
                    msg=f'Regular refresh failed: {str(refresh_error)}'
                )
                raise
        else:
            app_logger.error(
                msg=f'CONCURRENTLY refresh failed: {str(e)}'
            )
            raise


def run_module(
    module_name: str
):
    session = get_session()

    try:
        module = importlib.import_module(module_name)

        if hasattr(module, 'main'):
            module.main(session=session)
            session.commit()
        else:
            app_logger.warning(
                msg=f'{module_name} not has func main(), skip'
            )
    except Exception as e:
        app_logger.error(
            msg=f'Error in {module_name}: {str(e)}'
        )
        session.rollback()
    finally:
        session.close()


def run_all_methods():
    try:
        methods_path = Path(__file__).parent / 'methods'
        init_db()

        check_error = check_token()
        if check_error:
            app_logger.error(
                msg=check_error
            )
            return

        threads = []
        for module_info in pkgutil.iter_modules([str(methods_path)]):
            module_name = f'wb.methods.{module_info.name}'

            if module_info.name in ['advert_fullstats', 'advert_nm_report', 'stocks_fbs']:
                continue

            thread = threading.Thread(
                target=run_module,
                args=(module_name,)
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for module_name in ['wb.methods.advert_fullstats', 'wb.methods.advert_nm_report', 'wb.methods.stocks_fbs']:
            run_module(
                module_name=module_name
            )

        app_logger.info(
            msg='Start refresh mv_wb_pivot_by_day_dl'
        )

        session = get_session()
        refresh_materialized_view(
            session=session
        )
        session.commit()
        session.close()

        app_logger.info(
            msg='mv_wb_pivot_by_day_dl refreshed successfully'
        )
    except Exception as e:
        app_logger.error(
            msg=f'Problem with run task runner: {str(e)}'
        )


if __name__ == '__main__':
    run_all_methods()
