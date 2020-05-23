from zillion import Warehouse

config = "https://raw.githubusercontent.com/totalhack/zillion/master/examples/example_wh_config.json"
wh = Warehouse(config=config)

result = wh.execute(metrics=["revenue"], dimensions=["partner_name"])
print(result.df)
