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
    "postgres": ["psycopg2==2.8.5"],
    "dev": [
        "black",
        "pre-commit",
        "pylint==2.4.4",
        "pytest==5.4.1",
        "twine==3.1.1",
        "wheel",
        "mkdocs==1.1.2",
        "mkdocs-material==5.2.1",
        "mkdocs-material-extensions==1.0",
        "mkdocs-minify-plugin==0.3.0",
        "mkautodoc==0.1.0",
    ],
}
extras_require["complete"] = sorted(set(sum(extras_require.values(), [])))

exec(open("zillion/version.py").read())

setup(
    name="zillion",
    description="Make sense of it all.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/totalhack/zillion",
    author="totalhack",
    author_email="none@none.com",
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
