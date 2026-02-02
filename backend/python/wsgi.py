"""
WSGI entry point for Gunicorn.
Run with: gunicorn --workers 4 --bind 127.0.0.1:5000 wsgi:app
"""

import os
import sys
from pathlib import Path

# Add scheduler to path
sys.path.insert(0, str(Path(__file__).parent))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Import and create app
from web.app import create_app
from scheduler.config import SchedulerConfig

# Load scheduler config
config = SchedulerConfig.from_yaml()

# Create the application
app = create_app(config)

if __name__ == '__main__':
    app.run()
