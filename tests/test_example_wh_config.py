from zillion.configs import load_warehouse_config
from zillion.core import RollupTypes, info
from zillion.warehouse import Warehouse


config = load_warehouse_config("example_wh_config.json")


def test_example_wh_init():
    wh = Warehouse(config=config)
    wh.print_info()


def test_example_wh_report1():
    wh = Warehouse(config=config)
    result = wh.execute(
        metrics=["sales", "leads", "revenue"], dimensions=["partner_name"]
    )
    assert result
    info(result.df)


def test_example_wh_report2():
    wh = Warehouse(config=config)
    result = wh.execute(
        metrics=["sales", "leads", "revenue"],
        dimensions=["campaign_name"],
        criteria=[("partner_name", "=", "Partner A")],
    )
    assert result
    info(result.df)


def test_example_wh_report3():
    wh = Warehouse(config=config)
    result = wh.execute(
        metrics=["sales", "leads", "revenue"],
        dimensions=["partner_name", "campaign_name"],
        rollup=RollupTypes.ALL,
    )
    assert result
    info(result.df)
