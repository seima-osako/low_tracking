import time
import sys

_time = None

def start():
  global _time
  _time = time.time()

def end():
  val = time.time() - _time
  print(f"elapsed: {val}")