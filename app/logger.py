from loguru import logger
import sys
from config import settings
import asyncio
from functools import wraps


def configure_logging():
    level = "TRACE" if settings.DEBUG else "INFO"
    logger.remove()
    logger.add(
        sink=sys.stdout,
        level=level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{module}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )


def _format_args_kwargs(args, kwargs):
    args_str = ", ".join(
        [repr(arg) if len(repr(arg)) < 50 else f"{repr(arg)[:47]}..." for arg in args]
    )
    kwargs_str = ", ".join(
        [
            f"{k}={repr(v) if len(repr(v)) < 50 else f'{repr(v)[:47]}...'}"
            for k, v in kwargs.items()
        ]
    )

    if args_str and kwargs_str:
        return f"{args_str}, {kwargs_str}"
    elif args_str:
        return args_str
    else:
        return kwargs_str


def _format_return_value(value):
    repr_value = repr(value)
    if len(repr_value) > 100:
        return f"{repr_value[:97]}..."
    return repr_value


def trace(func):
    if asyncio.iscoroutinefunction(func):

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            formatted_args = _format_args_kwargs(args, kwargs)

            logger.trace(f"Function {func.__name__}({formatted_args}) called")

            result = await func(*args, **kwargs)

            formatted_result = _format_return_value(result)

            logger.trace(
                f"Function {func.__name__} result returned: {formatted_result}"
            )

            return result

        return async_wrapper
    else:

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            formatted_args = _format_args_kwargs(args, kwargs)

            logger.trace(f"Function {func.__name__}({formatted_args}) called")

            result = func(*args, **kwargs)

            formatted_result = _format_return_value(result)

            logger.trace(
                f"Function {func.__name__} result returned: {formatted_result}"
            )

            return result

        return sync_wrapper
