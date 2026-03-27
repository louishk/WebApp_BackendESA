"""
Gunicorn configuration for ESA Backend.

Usage: gunicorn -c gunicorn.conf.py wsgi:app
"""

# Server socket
bind = "127.0.0.1:5000"

# Worker processes
workers = 4
worker_class = "sync"

# Timeouts
timeout = 120          # Kill worker if request takes >120s
graceful_timeout = 30  # Time for worker to finish after SIGTERM

# Worker recycling — prevents memory leaks from long-running workers
max_requests = 1000
max_requests_jitter = 100  # Random 0-100 added to stagger restarts

# Logging
accesslog = "-"   # stdout
errorlog = "-"    # stderr
loglevel = "info"

# Process naming
proc_name = "esa-backend"

# Preload app for faster worker spawning (shares app code across forks)
preload_app = True
