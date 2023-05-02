#!/usr/bin/env python

from contextlib import contextmanager
import os

from setuptools import find_packages, setup


def find_deploy_scripts(path, include_patterns, exclude_patterns=[]):
    cmd = "FILES=`find %s -path %s" % (path, (" -o -path ").join(include_patterns))
    if exclude_patterns:
        cmd += " | grep -v -E '(%s)'" % ("|").join(exclude_patterns)
    cmd += "`;"
    cmd += " for FILE in $FILES; do if [ `echo $FILE | xargs grep -l '/usr/bin/env python'` ] || [ `echo $FILE | grep -v .py` ]; then echo $FILE; fi; done"
    h = os.popen(cmd)
    out = h.read()
    h.close()
    return out.split()


@contextmanager
def load_file(fname):
    f = open(os.path.join(os.path.dirname(__file__), fname))
    try:
        yield f
    finally:
        f.close()


with load_file("README.md") as f:
    README = f.read()

with load_file("requirements.txt") as f:
    requires = f.read().split("\n")

# Split git requirements to fill in dependency_links
git_requires = [x for x in requires if "git" in x]
non_git_requires = [x for x in requires if "git" not in x]
for repo in git_requires:
    # Append git egg references
    non_git_requires.append(repo.split("egg=")[-1])


extras_require = {
    "mysql": ["pymysql~=1.0.2"],
    "postgres": ["psycopg2-binary~=2.9.5"],
    "duckdb": [
        "duckdb~=0.7.1",
        "duckdb-engine~=0.7.0",
    ],
    "nlp": [
        "langchain==0.0.115",
        "openai==0.27.2",
        "tiktoken==0.3.3",
        "qdrant-client==1.1.3",
    ],
    "dev": [
        "black",
        "pre-commit",
        "pylint==2.4.4",
        "pytest==7.1.2",
        "pytest-xdist==3.1.0",
        "twine==3.1.1",
        "wheel",
        "mkdocs==1.1.2",
        "mkdocs-material==5.2.1",
        "mkdocs-material-extensions==1.0",
        "mkdocs-minify-plugin==0.3.0",
        "mkautodoc==0.2.0",
        "psycopg2-binary==2.9.5",
        "jinja2<3.1.0",
    ],
}
extras_require["complete"] = sorted(set(sum(extras_require.values(), [])))

exec(open("zillion/version.py").read())

setup(
    name="zillion",
    description="Make sense of it all. Data modeling and analytics with a sprinkle of AI.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/totalhack/zillion",
    maintainer="totalhack",
    version=__version__,
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    python_requires=">=3.6",
    scripts=find_deploy_scripts(
        "zillion", ["\\*.py", "\\*.sh", "\\*.sql"], ["__init__"]
    ),
    packages=find_packages(exclude=["tests.*", "tests"]),
    include_package_data=True,
    install_requires=non_git_requires,
    dependency_links=git_requires,
    extras_require=extras_require,
)
