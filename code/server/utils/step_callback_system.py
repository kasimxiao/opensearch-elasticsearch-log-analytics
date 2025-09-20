"""
通用回调系统
负责管理和发送实时输出更新
"""

import logging
from typing import Dict, List, Any, Callable, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class StepCallbackSystem:
    """通用回调系统，支持灵活的输出方式"""
    
    def __init__(self):
        self.callback_function = None
        self.current_session_id = None
        self.output_history = []
        
    def set_callback(self, callback_function: Callable):
        """设置回调函数"""
        self.callback_function = callback_function
        
    def set_session_id(self, session_id: str):
        """设置当前会话ID"""
        self.current_session_id = session_id
        
    def emit_output(self, data_type: str, content: Any, title: str = None, status: str = "processing"):
        """
        发送通用输出事件
        
        Args:
            data_type: 数据类型 ("text", "json", "chart")
            content: 输出内容
            title: 输出标题
            status: 状态 (processing, success, error)
        """
        try:
            timestamp = datetime.now()
            
            # 构建通用输出消息
            output_data = {
                "type": "output",
                "session_id": self.current_session_id,
                "data_type": data_type,
                "content": content,
                "title": title or f"{data_type.upper()} 输出",
                "status": status,
                "timestamp": timestamp.isoformat()
            }
            
            # 记录输出历史
            self.output_history.append(output_data)
            
            # 发送更新
            if self.callback_function:
                self.callback_function(output_data)
                
            logger.debug(f"发送{data_type}输出: {title}")
                
        except Exception as e:
            logger.error(f"发送输出失败: {str(e)}")
    
    def emit_text(self, content: Any, title: str = None, status: str = "processing"):
        """发送文本输出"""
        self.emit_output("text", content, title, status)
    
    def emit_json(self, content: Any, title: str = None, status: str = "processing"):
        """发送JSON输出"""
        self.emit_output("json", content, title, status)
    
    def emit_chart(self, content: Any, title: str = None, status: str = "processing"):
        """发送图表输出"""
        self.emit_output("chart", content, title, status)
        
    def get_output_history(self) -> List[Dict[str, Any]]:
        """获取输出历史记录"""
        return self.output_history.copy()
        
    def clear_history(self):
        """清除输出历史记录"""
        self.output_history.clear()
