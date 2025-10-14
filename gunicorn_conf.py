import os

# Cloud Run injects PORT
bind = f"0.0.0.0:{os.getenv('PORT', '8000')}"

# Sensible defaults for Cloud Run single CPU
workers = int(os.getenv("WEB_CONCURRENCY", "1"))
threads = int(os.getenv("THREADS_PER_WORKER", "8"))

# Timeouts / keepalive
timeout = int(os.getenv("TIMEOUT", "65"))
graceful_timeout = int(os.getenv("GRACEFUL_TIMEOUT", "30"))
keepalive = int(os.getenv("KEEPALIVE", "75"))

# Log to stdout/stderr
accesslog = "-"
errorlog = "-"
