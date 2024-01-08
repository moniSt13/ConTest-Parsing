def timer(func):
    import functools
    import time

    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time

        if args[0].settings.print_statistics:
            print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        return value

    return wrapper_timer
