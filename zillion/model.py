import sqlalchemy as sa

from zillion.configs import zillion_config

zillion_engine = sa.create_engine(zillion_config["ZILLION_DB_URL"], pool_pre_ping=True)
zillion_metadata = sa.MetaData()
zillion_metadata.bind = zillion_engine

Warehouses = sa.Table(
    "warehouses",
    zillion_metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String(128), nullable=False, unique=True),
    sa.Column("params", sa.Text, nullable=False),
    sa.Column("meta", sa.Text),
    sa.Column("created_at", sa.DateTime, server_default=sa.func.NOW()),
)

ReportSpecs = sa.Table(
    "report_specs",
    zillion_metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("warehouse_id", sa.Integer, nullable=False),
    sa.Column("params", sa.Text, nullable=False),
    sa.Column("meta", sa.Text),
    sa.Column("created_at", sa.DateTime, server_default=sa.func.NOW()),
)

DimensionValues = sa.Table(
    "dimension_values",
    zillion_metadata,
    sa.Column("name", sa.String(100), primary_key=True),
    sa.Column("warehouse_id", sa.Integer, primary_key=True),
    sa.Column("values", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime, server_default=sa.func.NOW()),
)

zillion_metadata.create_all(zillion_engine)
