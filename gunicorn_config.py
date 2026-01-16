import multiprocessing
import os

bind = "0.0.0.0:8000"
workers = multiprocessing.cpu_count() * 2 + 1
threads = 2
timeout = 600
accesslog = "-"
errorlog = "-"
loglevel = "info"
keepalive = 5
