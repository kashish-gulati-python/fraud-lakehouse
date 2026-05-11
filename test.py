import signal
import sys

def handler(signum, frame):
  print('Signal received, exiting gracefully...')
  sys.exit(0)

signal.signal(signal.SIGINT, handler)
print('Press Ctrl+C to trigger signal')