from typing import Callable, Deque
from collections import deque
from functools import wraps
from time import sleep, time
import tap_ordway.configs as TAP_CONFIG

# Modified from `singer.utils.ratelimit`
def ratelimit(func) -> Callable:
    """Decorator for rate limiting requests based on the `rate_limit_rps` property in config"""
    times: Deque[float] = deque()

    @wraps(func)
    def wrapper(*args, **kwargs):
        one_second = 1
        limit = TAP_CONFIG.rate_limit_rps

        # In effect, user disabled rate limiting
        if limit is not None:
            if len(times) >= limit:
                tim0 = times.pop()
                tim = time()

                sleep_time = one_second - (tim - tim0)

                if sleep_time > 0:
                    sleep(sleep_time)

            times.appendleft(time())

        return func(*args, **kwargs)

    return wrapper
