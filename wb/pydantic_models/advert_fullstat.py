from pydantic import BaseModel, Field


class Nm(BaseModel):
    nm_views: int | None = Field(default=None, alias='views')
    nm_clicks: int | None = Field(default=None, alias='clicks')
    nm_ctr: float | None = Field(default=None, alias='ctr')
    nm_cpc: float | None = Field(default=None, alias='cpc')
    nm_sum: float | None = Field(default=None, alias='sum')
    nm_atbs: int | None = Field(default=None, alias='atbs')
    nm_orders: int | None = Field(default=None, alias='orders')
    nm_cr: float | None = Field(default=None, alias='cr')
    nm_shks: int | None = Field(default=None, alias='shks')
    nm_sum_price: float | None = Field(default=None, alias='sum_price')
    nm_name: str | None = Field(default=None, alias='name')
    nm_id: int | None = Field(default=None, alias='nmId')
    nm_canceled: int | None = Field(default=None, alias='canceled')


class App(BaseModel):
    app_type: int | None = Field(default=None, alias='appType')
    app_atbs: int | None = Field(default=None, alias='atbs')
    app_canceled: int | None = Field(default=None, alias='canceled')
    app_clicks: int | None = Field(default=None, alias='clicks')
    app_cpc: float | None = Field(default=None, alias='cpc')
    app_cr: float | None = Field(default=None, alias='cr')
    app_ctr: float | None = Field(default=None, alias='ctr')
    app_orders: int | None = Field(default=None, alias='orders')
    app_shks: int | None = Field(default=None, alias='shks')
    app_sum: float | None = Field(default=None, alias='sum')
    app_sum_price: float | None = Field(default=None, alias='sum_price')
    app_views: int | None = Field(default=None, alias='views')

    app_nms: list[Nm] | None = Field(default=None, alias='nms')


class BoosterStat(BaseModel):
    booster_date: str | None = Field(default=None, alias='date')
    booster_nm: int | None = Field(default=None, alias='nm')
    booster_avg_position: float | None = Field(default=None, alias='avg_position')


class Day(BaseModel):
    day_atbs: int | None = Field(default=None, alias='atbs')
    day_canceled: int | None = Field(default=None, alias='canceled')
    day_clicks: int | None = Field(default=None, alias='clicks')
    day_cpc: float | None = Field(default=None, alias='cpc')
    day_cr: float | None = Field(default=None, alias='cr')
    day_ctr: float | None = Field(default=None, alias='ctr')
    day_date: str | None = Field(default=None, alias='date')
    day_orders: int | None = Field(default=None, alias='orders')
    day_shks: int | None = Field(default=None, alias='shks')
    day_sum: float | None = Field(default=None, alias='sum')
    day_sum_price: float | None = Field(default=None, alias='sum_price')
    day_views: int | None = Field(default=None, alias='views')

    day_apps: list[App] | None = Field(default=None, alias='apps')


class AdvertFullStatResponse(BaseModel):
    advert_id: int | None = Field(default=None, alias='advertId')

    # main info
    main_atbs: int | None = Field(default=None, alias='atbs')
    main_canceled: int | None = Field(default=None, alias='canceled')
    main_clicks: int | None = Field(default=None, alias='clicks')
    main_cpc: float | None = Field(default=None, alias='cpc')
    main_cr: float | None = Field(default=None, alias='cr')
    main_ctr: float | None = Field(default=None, alias='ctr')
    main_orders: int | None = Field(default=None, alias='orders')
    main_shks: int | None = Field(default=None, alias='shks')
    main_sum: float | None = Field(default=None, alias='sum')
    main_sum_price: float | None = Field(default=None, alias='sum_price')
    main_views: int | None = Field(default=None, alias='views')

    days: list[Day] | None = Field(default=None)
    booster_stats: list[BoosterStat] | None = Field(default=None, alias='boosterStats')
