import os

# Bind to the port Cloud Run provides
bind = f"0.0.0.0:{os.getenv('PORT', '8080')}"

# Single async worker is typical for 1 vCPU in Cloud Run
workers = int(os.getenv("WEB_CONCURRENCY", "1"))

# Trust the Cloud Run/Load Balancer proxy for client IPs
forwarded_allow_ips = "*"

# Timeouts / keepalive
timeout = int(os.getenv("TIMEOUT", "300")) # 300 to match ES Cloud
graceful_timeout = int(os.getenv("GRACEFUL_TIMEOUT", "30"))
keepalive = int(os.getenv("KEEPALIVE", "75"))

# Logging to stdout/stderr for Cloud Run
accesslog = "-"
errorlog = "-"
loglevel = os.getenv("LOGLEVEL", "info")