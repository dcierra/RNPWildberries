from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from config.settings import Settings
from wb.db.utils import create_views, create_materialized_views

settings = Settings()
db_settings = settings.db

Base = declarative_base()

engine = create_engine(
    db_settings.database_url,
    **db_settings.engine_settings
)

SessionLocal = sessionmaker(
    bind=engine
)


def get_session():
    return SessionLocal()


def init_db():
    Base.metadata.create_all(
        bind=engine
    )

    create_views(
        engine=engine
    )

    create_materialized_views(
        engine=engine
    )
