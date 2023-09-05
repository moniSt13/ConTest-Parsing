import functools
import time


def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()  # 1
        value = func(*args, **kwargs)
        end_time = time.perf_counter()  # 2
        run_time = end_time - start_time  # 3

        if args[0].settings.print_statistics:
            print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        return value

    return wrapper_timer
