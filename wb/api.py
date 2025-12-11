from enum import Enum
from time import sleep
from typing import Literal

from requests import RequestException

from config import settings
import requests


class WBCategory(str, Enum):
    CONTENT = 'Контент'
    ANALYTICS = 'Аналитика'
    PRICES = 'Цены и скидки'
    MARKETPLACE = 'Маркетплейс'
    STATISTICS = 'Статистика'
    PROMOTION = 'Продвижение'
    FEEDBACK = 'Вопросы и отзывы'
    CHAT = 'Чат с покупателями'
    SUPPLIES = 'Поставки'
    RETURNS = 'Возвраты покупателями'
    DOCUMENTS = 'Документы'
    FINANCE = 'Финансы'
    COMMON = 'Тарифы, Новости, Информация о продавце'


class TokenValidationError(Exception):
    pass


class WBApi:
    BASE_URL: str = settings.wb.BASE_URL

    CATEGORY_PING_URLS = {
        WBCategory.CONTENT: 'https://content-api.wildberries.ru/ping',
        WBCategory.ANALYTICS: 'https://seller-analytics-api.wildberries.ru/ping',
        WBCategory.PRICES: 'https://discounts-prices-api.wildberries.ru/ping',
        WBCategory.MARKETPLACE: 'https://marketplace-api.wildberries.ru/ping',
        WBCategory.STATISTICS: 'https://statistics-api.wildberries.ru/ping',
        WBCategory.PROMOTION: 'https://advert-api.wildberries.ru/ping',
        WBCategory.FEEDBACK: 'https://feedbacks-api.wildberries.ru/ping',
        WBCategory.CHAT: 'https://buyer-chat-api.wildberries.ru/ping',
        WBCategory.SUPPLIES: 'https://supplies-api.wildberries.ru/ping',
        WBCategory.RETURNS: 'https://returns-api.wildberries.ru/ping',
        WBCategory.DOCUMENTS: 'https://documents-api.wildberries.ru/ping',
        WBCategory.FINANCE: 'https://finance-api.wildberries.ru/ping',
        WBCategory.COMMON: 'https://common-api.wildberries.ru/ping',
    }

    def __init__(
        self,
        token: str | None = None
    ):
        if token is None:
            self._token = settings.wb.TOKEN.get_secret_value()
        else:
            self._token = token

    def validate_token(
        self,
        required_categories: list[WBCategory],
        return_inaccessible_categories_str: bool = False,
    ) -> dict[str, any]:
        inaccessible_categories = []
        accessible_categories = []
        errors = {}

        for category in required_categories:
            if category not in self.CATEGORY_PING_URLS:
                errors[category] = f'Неизвестная категория: {category.value}'
                inaccessible_categories.append(category)
                continue

            ping_url = self.CATEGORY_PING_URLS[category]

            try:
                response = requests.get(
                    url=ping_url,
                    headers={'Authorization': self._token},
                    timeout=10
                )

                match response.status_code:
                    case 200:
                        accessible_categories.append(category)
                    case 401:
                        error_msg = f'Токен не авторизован для категории "{category.value}"'
                        errors[category] = error_msg
                        inaccessible_categories.append(category)
                    case 403:
                        error_msg = f'Доступ к категории "{category.value}" запрещён'
                        errors[category] = error_msg
                        inaccessible_categories.append(category)
                    case _:
                        error_msg = f'Ошибка проверки категории "{category.value}": статус {response.status_code}'
                        errors[category] = error_msg
                        inaccessible_categories.append(category)
            except requests.exceptions.RequestException as e:
                error_msg = f'Ошибка соединения с API для категории "{category.value}": {str(e)}'
                errors[category] = error_msg
                inaccessible_categories.append(category)

        result = {
            'valid': len(inaccessible_categories) == 0,
            'accessible_categories': accessible_categories,
            'inaccessible_categories': inaccessible_categories,
            'errors': errors
        }

        if inaccessible_categories:
            inaccessible_categories_str = '; '.join(f'{category.value}' for category in inaccessible_categories)

            if return_inaccessible_categories_str:
                raise TokenValidationError(
                    f'Token does not have access to the following categories: {inaccessible_categories_str}'
                )

        return result

    def __request(
        self,
        method: str,
        endpoint: str,
        base_url: str | None = None,

        data: dict = None,
        json_data: dict | list = None,
        params: dict = None,
        headers: dict = None,
        retries: int = 30,
        backoff_time: int = 60,
    ):
        if data is None:
            data = {}

        if json_data is None:
            json_data = {}

        if params is None:
            params = {}

        if headers is None:
            headers = {
                'Authorization': self._token
            }

        url = f'{self.BASE_URL}/{endpoint}' if base_url is None else f'{base_url}/{endpoint}'

        response = requests.request(
            method=method,
            url=url,
            headers=headers if headers else None,
            json=json_data if json_data else None,
            data=data if data else None,
            params=params if params else None
        )

        match response.status_code:
            case 200:
                return response.json()
            case 204:
                raise RequestException('No content')
            case 401:
                raise RequestException(f'{response.text}')
            case 429:
                sleep(backoff_time)
                if retries > 0:
                    return self.__request(
                        method=method,
                        base_url=base_url,
                        endpoint=endpoint,
                        headers=headers,
                        json_data=json_data,
                        data=data,
                        params=params,
                        retries=retries - 1,
                        backoff_time=backoff_time
                    )
                else:
                    raise RequestException('Maximum retries exceeded.')
            case _:
                raise RequestException(f'Unexpected status code - {response.status_code}, error: {response.text}')

    def supplier_stocks(
        self,
        date_from: str = '2018-01-01',
        endpoint: str = 'api/v1/supplier/stocks',
    ):
        params = {
            'dateFrom': date_from,
        }

        data = self.__request(
            method='GET',
            endpoint=endpoint,
            params=params,
        )

        if data:
            return data

        raise RequestException(
            f'Data is none'
        )

    def advert_list(
        self,
        endpoint: str = 'adv/v1/promotion/count',
        url: str = 'https://advert-api.wildberries.ru'
    ):
        data = self.__request(
            method='GET',
            base_url=url,
            endpoint=endpoint,
        )

        if data:
            adverts = data.get('adverts')
            if adverts:
                return adverts

        raise RequestException(
            f'Data is none'
        )

    def advert_full_stats(
        self,
        advert_ids: list[int],
        interval_begin: str,
        interval_end: str,

        endpoint: str = 'adv/v3/fullstats',
        url: str = 'https://advert-api.wildberries.ru',
    ):
        params = {
            'ids': ','.join(str(ad_id) for ad_id in advert_ids),
            'beginDate': interval_begin,
            'endDate': interval_end
        }

        data = self.__request(
            method='GET',
            base_url=url,
            params=params,
            endpoint=endpoint
        )

        if data:
            return data

        raise RequestException(
            f'Data is none'
        )

    def cards_list(
        self,
        endpoint: str = 'content/v2/get/cards/list',
        base_url: str = 'https://content-api.wildberries.ru',
    ):
        body = {
            'settings': {
                'cursor': {
                    'limit': 100
                },
                'filter': {
                    'withPhoto': 1
                }
            }
        }

        result = []

        response = self.__request(
            method='POST',
            base_url=base_url,
            endpoint=endpoint,
            json_data=body
        )
        result.extend(response.get('cards', []))

        cursor = response.get('cursor', {})
        while cursor.get('total') == 100:
            body['settings']['cursor'].update(
                {
                    'updatedAt': cursor.get('updatedAt'),
                    'nmID': cursor.get('nmID')
                }
            )

            response = self.__request(
                method='POST',
                base_url=base_url,
                endpoint=endpoint,
                json_data=body
            )
            result.extend(response.get('cards', []))
            cursor = response.get('cursor', {})

        if cursor.get('total') and cursor.get('total') != 0:
            body['settings']['cursor'].update(
                {
                    'limit': cursor.get('total'),
                    'updatedAt': cursor.get('updatedAt'),
                    'nmID': cursor.get('nmID')
                }
            )
            response = self.__request(
                method='POST',
                base_url=base_url,
                endpoint=endpoint,
                json_data=body
            )
            result.extend(response.get('cards', []))

        return result

    def advert_nm_report(
        self,
        nmids: list[int],
        date_from: str,
        date_to: str,

        url: str = 'https://seller-analytics-api.wildberries.ru',
        endpoint: str = 'api/v2/nm-report/detail/history',
    ):
        json_data = {
            'nmIDs': nmids,
            'period': {
                'begin': date_from,
                'end': date_to
            }
        }

        data = self.__request(
            method='POST',
            base_url=url,
            endpoint=endpoint,
            json_data=json_data,
        )

        if data:
            return data.get('data')

        raise RequestException(
            f'Data is none'
        )

    def tariffs_box_and_pallet(
        self,
        date: str,

        endpoint: Literal[
            'box',
            'pallet',
        ] = 'box',
        url: str = 'https://common-api.wildberries.ru/api/v1/tariffs',
    ):
        params = {
            'date': date,
        }

        data = self.__request(
            method='GET',
            base_url=url,
            endpoint=endpoint,
            params=params,
        )

        if data:
            response_data = data.get('response')
            if response_data:
                unpacked_data = response_data.get('data')
                if unpacked_data:
                    return unpacked_data.get('warehouseList')

        raise RequestException(
            f'Data is none'
        )

    def tariffs_commission(
        self,
        locale: Literal[
            'ru',
            'en',
            'zh'
        ] = 'ru',
        url: str = 'https://common-api.wildberries.ru',
        endpoint: str = 'api/v1/tariffs/commission',
    ):
        params = {
            'locale': locale,
        }

        data = self.__request(
            method='GET',
            base_url=url,
            endpoint=endpoint,
            params=params,
        )

        if data:
            return data.get('report')

        raise RequestException(
            f'Data is none'
        )

    def uuid_for_paid_storage(
        self,
        date_from: str,
        date_to: str,

        url: str = 'https://seller-analytics-api.wildberries.ru',
        endpoint: str = 'api/v1/paid_storage',
    ):
        params = {
            'dateFrom': date_from,
            'dateTo': date_to
        }

        data = self.__request(
            method='GET',
            base_url=url,
            endpoint=endpoint,
            params=params,
        ).get('data')

        if data:
            return data.get('taskId')

        raise RequestException(
            f'Data is none'
        )

    def check_status_paid_storage(
        self,
        task_id: str,

        url: str = 'https://seller-analytics-api.wildberries.ru',
        endpoint: str = 'api/v1/paid_storage/tasks/{task_id}/status'
    ):
        while True:
            data = self.__request(
                method='GET',
                base_url=url,
                endpoint=endpoint.format(task_id=task_id)
            ).get('data')

            if data:
                status = data.get('status')

                if status.lower() == 'done':
                    return 'ok'
                elif status is None:
                    raise RequestException(
                        'Status is not available'
                    )
            else:
                raise RequestException(
                    'Data is None'
                )

            sleep(5)

    def paid_storage(
        self,
        task_id: str,

        url: str = 'https://seller-analytics-api.wildberries.ru',
        endpoint: str = 'api/v1/paid_storage/tasks/{task_id}/download'
    ):
        data = self.__request(
            method='GET',
            base_url=url,
            endpoint=endpoint.format(task_id=task_id)
        )

        if data:
            return data

        raise RequestException(
            'Data is None'
        )

    def supplier_sales(
        self,
        date_from: str,

        url: str = 'https://statistics-api.wildberries.ru',
        endpoint: str = 'api/v1/supplier/sales'
    ):
        params = {
            'dateFrom': date_from,
        }

        data = self.__request(
            method='GET',
            base_url=url,
            endpoint=endpoint,
            params=params,
        )

        if data:
            return data

        raise RequestException(
            f'Data is none'
        )

    def supplier_orders(
        self,
        date_from: str,
        url: str = 'https://statistics-api.wildberries.ru',
        endpoint: str = 'api/v1/supplier/orders',
    ):
        all_orders = []
        current_date_from = date_from

        while True:
            params = {
                'dateFrom': current_date_from,
            }

            data = self.__request(
                method='GET',
                base_url=url,
                endpoint=endpoint,
                params=params,
            )

            if not data:
                break

            all_orders.extend(data)

            last_change_date = data[-1].get('lastChangeDate')
            if not last_change_date:
                break

            current_date_from = last_change_date

        return all_orders

    def create_extended_advert_nm_report(
        self,
        report_uuid: str,
        start_date: str,
        end_date: str,
        report_type: str = 'DETAIL_HISTORY_REPORT',
        nm_ids: list[int] = None,
        aggregation_level: str = 'day',
        skip_deleted_nm: bool = False,

        url: str = 'https://seller-analytics-api.wildberries.ru',
        endpoint: str = 'api/v2/nm-report/downloads'
    ):
        if nm_ids is None: nm_ids = []

        json_data = {
            'id': report_uuid,
            'reportType': report_type,
            'params': {
                'nmIDs': nm_ids,
                'startDate': start_date,
                'endDate': end_date,
                'aggregationLevel': aggregation_level,
                'skipDeletedNm': skip_deleted_nm
            }
        }

        data = self.__request(
            method='POST',
            base_url=url,
            endpoint=endpoint,
            json_data=json_data,
        )

        if data:
            return True

        return False

    def download_extended_advert_nm_report(
        self,
        report_uuid: str,
        save_path: str = 'report.zip'
    ):
        headers = {
            'Authorization': self._token
        }

        response = requests.get(
            url=f'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/downloads/file/{report_uuid}',
            headers=headers,
            stream=True
        )

        if response.status_code == 200:
            with open(save_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            return save_path

        return None

    def get_stocks_fbs(
        self,
        warehouse_id: int,
        barcodes: list[int],

        url: str = 'https://marketplace-api.wildberries.ru',
        endpoint: str = 'api/v3/stocks/{warehouse_id}'
    ):
        json_data = {
            'skus': barcodes
        }

        data = self.__request(
            method='POST',
            base_url=url,
            endpoint=endpoint.format(warehouse_id=warehouse_id),
            json_data=json_data
        )

        if data:
            stocks = data.get('stocks')
            if stocks:
                return stocks

        raise RequestException(
            f'Data is none'
        )

    def get_id_acceptance_reports(
        self,
        date_from: str,
        date_to: str,

        url: str = 'https://seller-analytics-api.wildberries.ru',
        endpoint: str = 'api/v1/acceptance_report'
    ):
        params = {
            'dateFrom': date_from,
            'dateTo': date_to
        }

        data = self.__request(
            method='GET',
            base_url=url,
            endpoint=endpoint,
            params=params,
        )

        if data:
            return data.get('data')

        raise RequestException(
            f'Data is none'
        )

    def get_status_acceptance_reports(
        self,
        task_id: str,

        url: str = 'https://seller-analytics-api.wildberries.ru',
        endpoint: str = 'api/v1/acceptance_report/tasks/{task_id}/status'
    ):
        data = self.__request(
            method='GET',
            base_url=url,
            endpoint=endpoint.format(task_id=task_id)
        )

        if data:
            return data.get('data')

        raise RequestException(
            f'Data is none'
        )

    def get_acceptance_reports(
        self,
        task_id: str,

        url: str = 'https://seller-analytics-api.wildberries.ru',
        endpoint: str = 'api/v1/acceptance_report/tasks/{task_id}/download'
    ):
        data = self.__request(
            method='GET',
            base_url=url,
            endpoint=endpoint.format(task_id=task_id)
        )

        if data:
            return data

        return None

    def get_advert_cost(
        self,
        date_from: str,
        date_to: str,

        url: str = 'https://advert-api.wildberries.ru',
        endpoint: str = 'adv/v1/upd',
    ):
        params = {
            'from': date_from,
            'to': date_to,
        }

        data = self.__request(
            method='GET',
            base_url=url,
            endpoint=endpoint,
            params=params,
        )

        if data:
            return data

        raise RequestException(f'Data is none')

    def get_warehouses(
        self,
        url: str = 'https://marketplace-api.wildberries.ru',
        endpoint: str = 'api/v3/warehouses',
    ):
        data = self.__request(
            method='GET',
            base_url=url,
            endpoint=endpoint,
        )

        if data:
            return data

        raise RequestException(
            f'Data is none'
        )

    def seller_info(
        self,
        url: str = 'https://common-api.wildberries.ru',
        endpoint: str = 'api/v1/seller-info',
    ):
        data = self.__request(
            method='GET',
            base_url=url,
            endpoint=endpoint,
        )

        if data:
            return data

        raise RequestException('Data is none')
