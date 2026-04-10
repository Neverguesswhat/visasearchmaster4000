import sys
import os

# Use the virtualenv's Python
VENV = os.path.join(os.path.dirname(__file__), 'venv')
INTERP = os.path.join(VENV, 'bin', 'python3')
if sys.executable != INTERP:
    os.execl(INTERP, INTERP, *sys.argv)

# Add app to path
sys.path.insert(0, os.path.dirname(__file__))

# No subfolder prefix needed — app lives at domain root
os.environ['BASE_PATH'] = ''

from server import app as application
