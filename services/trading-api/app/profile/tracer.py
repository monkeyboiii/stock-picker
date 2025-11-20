import time

from loguru import logger


def trace_elapsed(unit='ms'):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            match unit:
                case 'us':
                    elapsed_time = int((end_time - start_time) * 1000_000)
                    formatted_time = f"{elapsed_time:12_}"
                case 's':
                    elapsed_time = end_time - start_time
                    formatted_time = f"{elapsed_time:12_.3f}"
                case 'ms' | _:
                    elapsed_time = (end_time - start_time) * 1000
                    formatted_time = f"{elapsed_time:12_.3f}"

            logger.trace(f"Function '{func.__name__[:20]:20}' executed in {formatted_time} {unit}")
            return result
        return wrapper
    return decorator


@trace_elapsed()
def _example():
    time.sleep(1.05)


if __name__ == '__main__':
    import sys
    logger.remove()
    logger.add(sys.stdout, level='TRACE')
    logger.add('tracing.log', level='TRACE')
    _example()
