"""
WSGI entry point for Gunicorn.
Run with: gunicorn --workers 4 --bind 127.0.0.1:5000 wsgi:app
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

from web.app import create_app

app = create_app()

if __name__ == '__main__':
    app.run()
