Your help and feedback are greatly appreciated. Whether it's supporting/testing
a new datasource type, finding bugs, or suggesting features, every little bit
helps make `Zillion` reach its potential. 

Please also consider manicuring or configuring datasets that others may find
useful. With as little as a CSV and a short JSON configuration file you can
give back to the community. You can host these shared datasources easily with
GitHub.

## **How to Contribute**

1.  Check for open issues or open a new issue to start a discussion around a
    feature idea or a bug.
2.  Fork [the repository](https://github.com/totalhack/zillion) on GitHub to
    start making your changes to the **master** branch (or branch off of it).
3.  Write a test which shows that the bug was fixed or that the feature works
    as expected.
4.  Send a [pull request](https://help.github.com/en/articles/creating-a-pull-request-from-a-fork). Add yourself to
    [AUTHORS](https://github.com/totalhack/zillion/blob/master/AUTHORS.md).

## **Development Setup**

```shell
# Clone this repo
git clone https://github.com/totalhack/zillion.git
cd zillion

# Install dependencies
# Note: activate your venv first if desired!
pip install ".[dev]"

# Bring up test databases -- test data will init the first time
# You can optionally run these DBs directly on your machine instead
docker-compose up

# Run tests
export ZILLION_CONFIG=$(pwd)/tests/test_config.yaml
cd tests
pytest
```

## **Good Bug Reports**

Please be aware of the following things when filing bug reports:

1. Avoid raising duplicate issues. *Please* use the GitHub issue search feature
   to check whether your bug report or feature request has been mentioned in
   the past. Duplicate bug reports and feature requests are a huge maintenance
   burden on the limited resources of the project. If it is clear from your
   report that you would have struggled to find the original, that's ok, but
   if searching for a selection of words in your issue title would have found
   the duplicate then the issue will likely be closed.
2. When filing bug reports about exceptions or tracebacks, please include the
   *complete* traceback. Partial tracebacks, or just the exception text, are
   not helpful. Issues that do not contain complete tracebacks may be closed
   without warning.
3. Make sure you provide a suitable amount of information to work with.

## **Questions**

The GitHub issue tracker is for *bug reports* and *feature requests*. Please do
not use it to ask questions about how to use Zillion.
