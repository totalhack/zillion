from datetime import datetime, timedelta

import pytest

from .test_utils import *
from zillion.nlp import *


def n_days_ago(n):
    return (datetime.now() - timedelta(days=n)).date()


GENERIC_NLP_QUERIES = [
    [
        "show me revenue and sales for yesterday",
        dict(
            metrics=["revenue", "sales"],
            criteria=[["date", "=", str(n_days_ago(1))]],
        ),
    ],
    [
        "revenue and sales by date for the last 30 days. Rows with more than 5 sales.",
        dict(
            metrics=["revenue", "sales"],
            dimensions=["date"],
            criteria=[["date", ">=", str(n_days_ago(30))]],
            row_filters=[["sales", ">", 5]],
        ),
    ],
    [
        "top 10 campaigns by revenue yesterday for ad engine Google. Include totals.",
        dict(
            metrics=["revenue"],
            dimensions=["campaign"],
            criteria=[
                ["date", "=", str(n_days_ago(1))],
                ["ad_engine", "=", "Google"],
            ],
            limit=10,
            order_by=[["revenue", "desc"]],
            rollup="totals",
        ),
    ],
]


@pytest.mark.nlp
def test_text_to_report_no_fields():
    for query, expected in GENERIC_NLP_QUERIES:
        print(f"Testing query: {query}")
        report = text_to_report_params(query, prompt_version="no_fields")
        print(f"Report: {report}")
        assert report == expected


ALL_FIELDS_NLP_QUERIES = [
    [
        "show me revenue and sales for yesterday",
        dict(
            metrics=["revenue", "sales"],
            criteria=[["date", "=", str(n_days_ago(1))]],
        ),
    ],
]


@pytest.mark.nlp
def test_text_to_report_all_fields(config):
    wh = Warehouse(config=config)
    for query, expected in ALL_FIELDS_NLP_QUERIES:
        print(f"Testing query: {query}")
        report = text_to_report_params(query, warehouse=wh, prompt_version="all_fields")
        print(f"Report: {report}")
        assert report == expected


DIMENSION_FIELDS_NLP_QUERIES = [
    [
        "show me revenue and sales for yesterday",
        dict(
            metrics=["revenue", "sales"],
            criteria=[["date", "=", str(n_days_ago(1))]],
        ),
    ],
]


@pytest.mark.nlp
def test_text_to_report_dimension_fields(config):
    wh = Warehouse(config=config)
    for query, expected in DIMENSION_FIELDS_NLP_QUERIES:
        print(f"Testing query: {query}")
        report = text_to_report_params(
            query, warehouse=wh, prompt_version="dimension_fields"
        )
        print(f"Report: {report}")
        assert report == expected


@pytest.mark.nlp
def test_init_warehouse_embeddings(config):
    wh = Warehouse(config=config)
    wh.init_embeddings()


@pytest.mark.nlp
def test_openai_embeddings_cached():
    key = zillion_config["OPENAI_API_KEY"]
    emb = OpenAIEmbeddingsCached(openai_api_key=key)
    res = emb.embed_query("revenue")
    assert emb._cache["revenue"] == res

    res = emb.embed_documents(["sales", "revenue"])
    assert emb._cache["sales"] == res[0]
    assert emb._cache["revenue"] == res[1]


@pytest.mark.nlp
def test_map_warehouse_report_params(config):
    wh = Warehouse(config=config)
    wh.init_embeddings()
    query = "Revenue and Sales by day for the last 7 days"
    params = text_to_report_params(query)
    report = map_warehouse_report_params(wh, params)
    expected = dict(
        metrics=["revenue", "sales"],
        dimensions=["date"],
        criteria=[("date", ">=", str(n_days_ago(7)))],
    )
    assert report == expected


@pytest.mark.nlp
def test_warehouse_execute_text(config):
    wh = Warehouse(config=config)
    query = "revenue and sales by campaign name"
    res = wh.execute_text(query)
    assert res and res.rowcount == 5 and res.df.loc["Campaign 1A"]["revenue"] == 83.0


@pytest.mark.nlp
def test_nlp_datasource_from_db_file(ds_config):
    ds_name = "testdb1"
    data_url = "https://github.com/totalhack/zillion/blob/master/tests/testdb1?raw=true"
    ds = DataSource.from_db_file(
        data_url, name=ds_name, config=ds_config, if_exists="replace", nlp=True
    )
    assert ds
    print()  # Format test output
    ds.print_info()
