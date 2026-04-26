from logmap import logmap

log = logmap('lltk')
logger = log


def log_on():
    logmap.loud()


def log_off():
    logmap.quiet()
