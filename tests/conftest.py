"""Shared pytest bootstrap for a deterministic local test run."""

import sys


# Some Windows environments can block while a TestClient worker imports AnyIO
# and writes bytecode to the interpreter installation.  Tests do not need .pyc
# files, so disable future bytecode writes before those worker imports happen.
sys.dont_write_bytecode = True
