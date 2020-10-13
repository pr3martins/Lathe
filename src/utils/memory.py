from psutil import Process

def memory_size():
    return Process().memory_info().rss

def memory_percent():
    return Process().memory_percent()
