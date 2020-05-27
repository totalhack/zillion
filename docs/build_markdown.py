import enum
import importlib
import inspect
import os
import pkgutil
import shutil

import markdown
from tlbx import st

import zillion


CWD = os.path.dirname(os.path.abspath(__file__))

OPTS = dict(
    extensions=["pymdownx.snippets", "admonition"],
    extension_configs={"pymdownx.snippets": {"base_path": CWD}},
)


def get_classes(module):
    return set(
        [
            x
            for x in inspect.getmembers(module, inspect.isclass)
            if (not x[0].startswith("_"))
            and x[1].__module__ == module.__name__
            and not type(x[1]) is enum.EnumMeta
        ]
    )


def get_funcs(module):
    return set(
        [
            x
            for x in inspect.getmembers(module, inspect.isfunction)
            if (not x[0].startswith("_")) and x[1].__module__ == module.__name__
        ]
    )


def get_object_attributes(obj):
    return set(
        [y[0] for y in inspect.getmembers(obj, lambda x: not inspect.isroutine(x))]
    )


def get_zillion_members(obj):
    member_set = set()
    for cls in obj.mro():
        if not cls.__module__.startswith("zillion"):
            break
        member_set |= cls.__dict__.keys()
    member_set = {x for x in member_set if (not x.startswith("_"))}
    member_set -= get_object_attributes(obj)
    return sorted(member_set)


def process_markdown(infile, outfile, **opts):
    with open(infile, "r") as f:
        text = f.read()
    md = markdown.Markdown(**opts)
    md.convert(text)
    md = u"\n".join(md.lines)
    with open(outfile, "w") as f:
        f.write(md)


def linkcode_resolve(obj):
    try:
        fn = inspect.getsourcefile(inspect.unwrap(obj))
    except TypeError:
        fn = None
    if not fn:
        return None

    try:
        source, lineno = inspect.getsourcelines(obj)
    except OSError:
        lineno = None

    if lineno:
        linespec = "#L{:d}-L{:d}".format(lineno, lineno + len(source) - 1)
    else:
        linespec = ""

    fn = os.path.relpath(fn, start=os.path.dirname(zillion.__file__))

    return "https://github.com/totalhack/zillion/blob/master/zillion/" "{}{}".format(
        fn, linespec
    )


def create_module_file(fullname):
    module = importlib.import_module(fullname)
    classes = get_classes(module)
    funcs = get_funcs(module)

    out = "[//]: # (This is an auto-generated file. Do not edit)\n"
    out += "# Module %s\n\n" % fullname

    for name, obj in sorted(classes):
        if issubclass(obj, Exception):
            # These cause errors in inspect.signature call in mkautodoc
            continue

        codelink = linkcode_resolve(obj)
        if codelink:
            out += "\n## [%s](%s)\n\n" % (name, codelink)
        else:
            out += "\n## %s\n\n" % name

        if obj.__bases__ and obj.__bases__ != (object,):
            base_names = ", ".join(
                [x.__module__ + "." + x.__name__ for x in obj.__bases__]
            )
            out += "*Bases*: %s\n\n" % base_names

        members = get_zillion_members(obj)
        if members:
            members = ":members: " + " ".join(members)
        else:
            members = ""

        out += CLASS_TEMPLATE % dict(name=fullname + "." + name, members=members)
        out += "\n"

    for name, obj in sorted(funcs):
        codelink = linkcode_resolve(obj)
        if codelink:
            out += "\n## [%s](%s)\n\n" % (name, codelink)
        else:
            out += "\n## %s\n\n" % name
        out += FUNC_TEMPLATE % dict(name=fullname + "." + name)
        out += "\n"

    filename = "%s/mkdocs/%s" % (CWD, fullname + ".md")
    print("Writing %s" % filename)
    with open(filename, "w") as f:
        f.write(out)


# -------- Build main README


INPUT_FILE = "%s/readme.md" % CWD
OUTPUT_FILE = "%s/../README.md" % CWD
print("Building %s from %s" % (OUTPUT_FILE, INPUT_FILE))
process_markdown(INPUT_FILE, OUTPUT_FILE, **OPTS)


# -------- CONTRIBUTING.md


INPUT_FILE = "%s/markdown/contributing.md" % CWD
OUTPUT_FILE = "%s/../CONTRIBUTING.md" % CWD
print("Building %s from %s" % (OUTPUT_FILE, INPUT_FILE))
shutil.copyfile(INPUT_FILE, OUTPUT_FILE)


INPUT_FILE = "%s/markdown/contributing.md" % CWD
OUTPUT_FILE = "%s/mkdocs/contributing.md" % CWD
print("Building %s from %s" % (OUTPUT_FILE, INPUT_FILE))
shutil.copyfile(INPUT_FILE, OUTPUT_FILE)


# -------- Build mkdocs index


INPUT_FILE = "%s/mkdocs_index.md" % CWD
OUTPUT_FILE = "%s/mkdocs/index.md" % CWD
print("Building %s from %s" % (OUTPUT_FILE, INPUT_FILE))
process_markdown(INPUT_FILE, OUTPUT_FILE, **OPTS)


# -------- Build API docs


API_FILE = "%s/mkdocs/api.md" % CWD
out = "# API Reference\n"

CLASS_TEMPLATE = """::: %(name)s
    :docstring:
    %(members)s
"""

FUNC_TEMPLATE = """::: %(name)s
    :docstring:
"""

walk = pkgutil.walk_packages(["../zillion"])

for module in walk:
    fullname = "zillion." + module.name
    path = fullname + ".md"
    md = "* [%s](%s)" % (fullname, path)
    out += "\n%s" % md
    create_module_file(fullname)

with open(API_FILE, "w") as f:
    f.write(out)
