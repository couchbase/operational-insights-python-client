# The generated gRPC stubs import each other as top-level packages (e.g.
# `from columnar import caps_pb2`), so this directory has to be importable
# in its own right.
import os
import sys

sys.path.append(os.path.dirname(__file__))
