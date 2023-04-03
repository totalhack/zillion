import pytest
from unittest.mock import patch

TEST_DB_URL = "https://github.com/totalhack/zillion/blob/master/tests/testdb1?raw=true"


def test_bootstrap_datasource_config():
    from zillion.scripts.bootstrap_datasource_config import main

    with patch(
        "argparse._sys.argv",
        ["bootstrap_datasource_config.py", TEST_DB_URL, "/tmp/config.json", "--verify"],
    ):
        main()


@pytest.mark.nlp
def test_nlp_bootstrap_datasource_config():
    from zillion.scripts.bootstrap_datasource_config import main

    with patch(
        "argparse._sys.argv",
        [
            "bootstrap_datasource_config.py",
            TEST_DB_URL,
            "/tmp/config.json",
            "--verify",
            "--nlp",
        ],
    ):
        main()
