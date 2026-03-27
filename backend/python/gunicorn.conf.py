"""
Gunicorn configuration for ESA Backend.

For local development: gunicorn -c gunicorn.conf.py wsgi:app
Production uses systemd/esa-backend.service (CLI flags take precedence).
"""

# Server socket
bind = "127.0.0.1:5000"

# Worker processes
workers = 4
threads = 2
worker_class = "gthread"

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
