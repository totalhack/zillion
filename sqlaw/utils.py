from collections import OrderedDict, MutableMapping, Callable
from functools import wraps
from importlib import import_module
import inspect
try:
    import simplejson as json
    from simplejson import JSONEncoder
except ImportError:
    print('WARNING: Failed to import simplejson, falling back to built-in json')
    import json
    from json import JSONEncoder
from pprint import pformat
import string
import sys

import climax
from dateutil import parser as dateparser
from orderedset import OrderedSet

#-------- Command line utils

def st():
    import pdb
    pdb.Pdb().set_trace(inspect.currentframe().f_back)

@climax.parent()
@climax.argument('--debug', action='store_true')
def testcli():
    pass

def prompt_user(msg, answers):
    answer = None
    answers = [str(x).lower() for x in answers]
    display_answers = '[%s] ' % '/'.join(answers)
    while (answer is None) or (answer.lower() not in answers):
        answer = input('%s %s' % (msg, display_answers))
    return answer

#-------- Object utils

def get_class_vars(cls):
    return [i for i in dir(cls) if (not isinstance(i, Callable)) and (not i.startswith('_'))]

def get_class_var_values(cls):
    return [getattr(cls, i) for i in dir(cls) if (not isinstance(i, Callable)) and (not i.startswith('_'))]

def import_object(name):
    if '.' not in name:
        frame = sys._getframe(1)
        module_name = frame.f_globals['__name__']
        object_name = name
    else:
        module_name = '.'.join(name.split('.')[:-1])
        object_name = name.split('.')[-1]
    return getattr(import_module(module_name), object_name)

# https://stackoverflow.com/questions/1389180/automatically-initialize-instance-variables
def initializer(func):
    names, varargs, keywords, defaults = inspect.getargspec(func)
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        for name, arg in list(zip(names[1:], args)) + list(kwargs.items()):
            setattr(self, name, arg)
        if defaults:
            for i in range(len(defaults)):
                index = -(i + 1)
                if not hasattr(self, names[index]):
                    setattr(self, names[index], defaults[index])
        func(self, *args, **kwargs)
    return wrapper

class MappingMixin(MutableMapping):
    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]

    def __delitem__(self, key):
        del self.__dict__[key]

    def __iter__(self):
        return iter(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

#-------- Logging utils

class FontSpecialChars:
    ENDC = '\033[0m'

class FontColors:
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    NONE = '' # Will use default terminal coloring

RESERVED_COLORS = [
    'RED',    # errors
    'YELLOW', # warnings
    'BLACK',  # to avoid conflicts with terminal defaults
    'WHITE'   # to avoid conflicts with terminal defaults
]
COLOR_OPTIONS = [x for x in get_class_vars(FontColors) if x not in RESERVED_COLORS]

class FontEffects:
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    INVERTED = '\033[7m'

def log(msg, label='parent', indent=0, color=None, autocolor=False, format_func=pformat):
    if not isinstance(msg, str):
        msg = pformat(msg)

    if indent is not None and int(indent):
        msg = msg + (' ' * int(indent))

    if label:
        if label == 'parent':
            label = sys._getframe().f_back.f_code.co_name
        msg = label.strip() + ':' + msg

    if (not color) and autocolor:
        assert label, 'No label provided, can not use autocolor'
        color_index = ord(label[0]) % len(COLOR_OPTIONS)
        color = COLOR_OPTIONS[color_index]

    if color:
        msg = getattr(FontColors, color.upper()) + msg + FontSpecialChars.ENDC

    print(msg)

def dbg(msg, label='parent', config=None, **kwargs):
    if config and not config.get('DEBUG', False):
        return

    if label == 'parent':
        label = sys._getframe().f_back.f_code.co_name
    log(msg, label=label, autocolor=True, **kwargs)

def warn(msg, label='WARNING'):
    log(msg, label=label, color='yellow')

def error(msg, label='ERROR'):
    log(msg, label=label, color='red')

class PrintMixin:
    repr_attrs = []

    def __repr__(self):
        if self.repr_attrs:
            return "<%s %s>" % (type(self).__name__, ' '.join(['%s=%s' % (field, getattr(self, field))
                                                               for field in self.repr_attrs]))
        return "<%s %s>" % (type(self).__name__, id(self))

    def __str__(self):
        return str(vars(self))

#-------- String utils

def get_string_format_args(s):
    return [tup[1] for tup in string.Formatter().parse(s) if tup[1] is not None]

def string_has_format_args(s):
    if get_string_format_args(s):
        return True
    return False

#-------- Dict/JSON/Set utils

# https://stackoverflow.com/questions/7204805/dictionaries-of-dictionaries-merge
def dictmerge(x, y, path=None, overwrite=False):
    if path is None:
        path = []
    for key in y:
        if key in x:
            if isinstance(x[key], (dict, MutableMapping)) and isinstance(y[key], (dict, MutableMapping)):
                dictmerge(x[key], y[key], path + [str(key)], overwrite=overwrite)
            elif x[key] == y[key]:
                pass # same leaf value
            else:
                if not overwrite:
                    raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
                x[key] = y[key]
        else:
            x[key] = y[key]
    return x

# https://stackoverflow.com/questions/16664874/how-can-i-add-an-element-at-the-top-of-an-ordereddict-in-python
class OrderedDictPlus(OrderedDict):
    def prepend(self, key, value):
        self.update({key:value})
        self.move_to_end(key, last=False)

def _default(self, obj):
    return getattr(obj.__class__, 'to_json', _default.default)(obj)

_default.default = JSONEncoder().default
JSONEncoder.default = _default

class JSONMixin:
    # Probably needs a better home
    def to_dict(self):
        if isinstance(self, dict):
            result = self
        else:
            result = self.__dict__.copy()
        for k, v in result.items():
            if hasattr(v, 'to_dict'):
                result[k] = v.to_dict()
        return result

    # This is used for _defaults in JSON encoding
    def to_json(self):
        return self.__dict__

    def to_jsons(self):
        return json.dumps(self.__dict__)

def orderedsetify(obj):
    '''Take ordered iterable and turn it into OrderedSet'''
    if isinstance(obj, OrderedSet):
        return obj
    if isinstance(obj, (list, tuple)):
        return OrderedSet(obj)
    assert False, 'Not sure how to setify %s' % obj

#-------- Date utils

def parse_date(s):
    return dateparser.parse(s)
