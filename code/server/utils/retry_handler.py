"""
重试处理器 - 处理API限流和其他临时错误
"""

import time
import logging
from functools import wraps
from typing import Any, Callable

logger = logging.getLogger(__name__)


def retry_on_rate_limit(max_retries: int = 3, wait_time: int = 15):
    """
    重试装饰器，处理API限流错误
    
    Args:
        max_retries: 最大重试次数
        wait_time: 等待时间（秒）
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    error_msg = str(e).lower()
                    
                    # 检查是否是限流错误
                    if any(phrase in error_msg for phrase in [
                        "too many requests",
                        "rate limit",
                        "throttling",
                        "quota exceeded"
                    ]):
                        last_exception = e
                        
                        if attempt < max_retries:
                            logger.warning(f"API限流错误，等待{wait_time}秒后重试 (尝试 {attempt + 1}/{max_retries + 1}): {e}")
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"API限流错误，已达最大重试次数: {e}")
                            raise e
                    else:
                        # 非限流错误，直接抛出
                        raise e
            
            # 如果所有重试都失败，抛出最后一个异常
            raise last_exception
        
        return wrapper
    return decorator


def create_retry_agent(agent_class, *args, **kwargs):
    """
    创建带重试功能的Agent
    """
    original_call = agent_class.__call__
    
    @retry_on_rate_limit(max_retries=3, wait_time=15)
    def retry_call(self, *call_args, **call_kwargs):
        return original_call(self, *call_args, **call_kwargs)
    
    agent_class.__call__ = retry_call
    return agent_class(*args, **kwargs)
