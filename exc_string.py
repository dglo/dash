#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
############################################################################

"""
Utility functions for formatting exceptions and stack traces so that they are
guaranteed to fit in a single line and contain only chars in specified
encoding.  Very useful for logging and handling dead end exceptions.
"""

# Written by Dmitry Dvoinikov <dmitry@targeted.org> (c) 2005
# Distributed under MIT license.
#
# Sample (test.py), line numbers added for clarity:
#
# 1. from exc_string import *
# 2: set_exc_string_encoding("ascii")
# 3: class foo(object):
# 4:     def __init__(self):
#             # note non-ascii char in the middle and newline
# 5:         raise Exception("z\xffz\n")
# 6: try:
# 7:     foo()
# 8: except:
# 9:     assert exc_string() == \
#          "Exception(\"z?z \") in __init__() (test.py:5) <- ?() (test.py:7)"
#
# The (2 times longer) source code with self-tests is available from:
# http://www.targeted.org/python/recipes/exc_string.py
#
# (c) 2005 Dmitry Dvoinikov <dmitry@targeted.org>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
#  sell copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
############################################################################

from __future__ import print_function

from sys import exc_info, version_info
from traceback import extract_stack, extract_tb
from os import path

#############################################################################


__all__ = ["exc_string", "trace_string", "force_string",
           "get_exc_string_encoding", "set_exc_string_encoding"]

###############################################################################

exc_string_encoding = "windows-1251"


def get_exc_string_encoding():
    return exc_string_encoding


def set_exc_string_encoding(encoding):
    global exc_string_encoding
    exc_string_encoding = encoding

###########################################################################


force_string_translate_map = " ????????\t ?? ??????????????????" + \
    "".join([chr(i) for i in range(32, 256)])


def force_string(v):
    if isinstance(v, str):
        if version_info >= (3, 0):
            return v
        v = v.decode(exc_string_encoding,
                     "replace").encode(exc_string_encoding, "replace")
        return v.translate(force_string_translate_map)

    if version_info < (3, 0):
        if isinstance(v, unicode):
            v = v.encode(exc_string_encoding, "replace")
            return v.translate(force_string_translate_map)

    try:
        v = str(v)
    except:
        return "unable to convert %s to string, str() failed" % \
          v.__class__.__name__
    else:
        return force_string(v)

###############################################################################


def _reversed(r):
    result = list(r)
    result.reverse()
    return result


def trace_string(tb=None):
    return " <- ".join(
        [force_string("%s() (%s:%s)" % (m, path.split(f)[1], n))
         for f, n, m, _ in _reversed(tb or extract_stack()[: -1])])

###############################################################################


def exc_string():

    try:

        t, v, tb = exc_info()
        if t is None:
            return "no exception"
        if v is not None:
            v = force_string(v)
        else:
            v = force_string(t)
        if hasattr(t, "__name__"):
            t = t.__name__
        else:
            t = type(t).__name__

        return "%s(\"%s\") in %s" % (t, v, trace_string(extract_tb(tb)))

    except:
        return "exc_string() failed to extract exception string"

#############################################################################


if __name__ == '__main__':  # run self-tests

    print("self-testing module exc_string.py:")

    # force_string tests

    set_exc_string_encoding("windows-1251")
    assert get_exc_string_encoding() == "windows-1251"

    russian = ("wMHCw8TFqMbHyMnKy8zNzs/Q0dLT1NXW19jZ3Nva3d"
               "7f4OHi4+TluObn6Onq6+zt7u/w8fLz9PX29/j5/Pv6"
               "/f7/").decode("base64")
    russian_unicode = russian.decode("windows-1251")
    assert isinstance(russian_unicode, unicode)
    ss = force_string(russian_unicode)
    assert isinstance(ss, str) and ss == russian

    hebrew = u"".join([unichr(i) for i in range(0x590, 0x5ff)])
    assert isinstance(hebrew, unicode)
    ss = force_string(hebrew)
    assert ss == "?" * 0x6f

    assert force_string(None) == "None"
    assert force_string(10) == "10"
    assert force_string(Exception("foo")) == "foo"
    assert force_string(Exception(10)) == "10"
    assert force_string(Exception(russian)) == russian
    assert force_string(Exception(russian_unicode)) == \
        "unable to convert Exception to string, str() failed"

    class Foo(object):
        def __str__(self):
            raise "foo"
    assert force_string(Foo()) == \
        "unable to convert Foo to string, str() failed"

    class Bar(object):
        def __str__(self):
            return self  # nasty, eh ?
    assert force_string(Bar()) == \
        "unable to convert Bar to string, str() failed"

    # trace_string() tests:
    assert trace_string() == "<module>() (exc_string.py:180)"

    def foo():
        assert trace_string() == ("foo() (exc_string.py:183) <- test() "
                                  "(exc_string.py:189) <- <module>() "
                                  "(exc_string.py:191)")

    class bar(object):
        def test(self):
            foo()

    bar().test()

    # exc_string() tests:

    from sys import exc_clear
    exc_clear()
    assert exc_string() == "no exception"

    try:
        raise Exception(russian)
    except:
        assert exc_string() == \
            "Exception(\"%s\") in <module>() (exc_string.py:200)" % russian

    try:
        raise u"throwing unicode is deprecated"
    except:
        assert exc_string() == ("TypeError(\"exceptions must be old-style "
                                "classes or derived from BaseException, not "
                                "unicode\") in <module>() (exc_string.py:206)")

    try:
        1 / 0
    except:
        assert exc_string() == \
            ("ZeroDivisionError(\"integer division or modulo by zero\") in "
             "<module>() (exc_string.py:213)")

    class MyException(Exception):
        pass

    try:
        raise MyException(hebrew)
    except:
        assert exc_string() == \
            ("MyException(\"unable to convert MyException to string, str() "
             "failed\") in <module>() (exc_string.py:223)")

    def foo():
        raise MyException(russian)

    class bar2(object):
        def __init__(self):
            foo()

    try:
        bar2()
    except:
        assert exc_string() == \
            ("MyException(\"%s\") in foo() (exc_string.py:230) "
             "<- __init__() (exc_string.py:234) <- <module>() "
             "(exc_string.py:237)") % russian

    set_exc_string_encoding("ascii")
    assert get_exc_string_encoding() == "ascii"

    try:
        bar()
    except:
        assert exc_string() == \
            ("MyException(\"%s\") in foo() (exc_string.py:230) "
             "<- __init__() (exc_string.py:234) <- <module>() "
             "(exc_string.py:248)") % ("?" * len(russian))

    def recur():
        recur()

    try:
        recur()
    except:
        assert exc_string().startswith(("RuntimeError(\"maximum recursion "
                                        "depth exceeded\") in recur() "
                                        "(exc_string.py:256) <- ") * 100)

    print("ok")

###############################################################################
# EOF
