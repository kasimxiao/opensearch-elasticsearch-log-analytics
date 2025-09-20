"""
对话历史管理器
负责管理多轮对话的上下文信息
"""

import logging
from typing import Dict, List, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class ConversationHistoryManager:
    """对话历史管理器类"""
    
    def __init__(self, max_history_length: int = 10):
        """
        初始化对话历史管理器
        
        Args:
            max_history_length: 最大历史记录长度
        """
        self.conversation_history = []
        self.max_history_length = max_history_length
        
    def add_to_conversation_history(self, user_query: str, semantic_result: Dict[str, Any] = None, response: str = None):
        """
        添加对话记录到历史中，增强上下文信息记录
        
        Args:
            user_query: 用户查询
            semantic_result: 语义分析结果
            response: 系统响应
        """
        conversation_entry = {
            "timestamp": datetime.now().isoformat(),
            "user_query": user_query,
            "semantic_result": semantic_result,
            "response": response
        }
        
        # 如果有语义分析结果，提取关键信息用于快速上下文检索
        if semantic_result and semantic_result.get("success"):
            conversation_entry["quick_context"] = {
                "intent_type": semantic_result.get("intent_type"),
                "rewritten_query": semantic_result.get("rewritten_query"),
                "log_type": semantic_result.get("entities", {}).get("log_type"),
                "aws_service": semantic_result.get("entities", {}).get("aws_service"),
                "keywords": semantic_result.get("entities", {}).get("keywords", []),
                "time_range": semantic_result.get("time_range"),
                "has_context_rewrite": bool(semantic_result.get("context_used") and "上下文" in semantic_result.get("context_used", ""))
            }
        
        self.conversation_history.append(conversation_entry)
        
        # 保持历史记录在限制范围内
        if len(self.conversation_history) > self.max_history_length:
            self.conversation_history = self.conversation_history[-self.max_history_length:]
        
    
    def get_conversation_context(self) -> str:
        """
        获取对话上下文，用于语义改写，增强多轮对话支持
        
        Returns:
            str: 格式化的对话上下文
        """
        if not self.conversation_history:
            return "无对话历史"
        
        context_parts = []
        recent_conversations = self.conversation_history[-5:]  # 只使用最近5轮对话
        
        for i, entry in enumerate(recent_conversations, 1):
            user_query = entry.get("user_query", "")
            semantic_result = entry.get("semantic_result", {})
            timestamp = entry.get("timestamp", "")
            
            if semantic_result and semantic_result.get("success"):
                intent_type = semantic_result.get("intent_type", "unknown")
                rewritten_query = semantic_result.get("rewritten_query", user_query)
                time_range = semantic_result.get("time_range", {})
                entities = semantic_result.get("entities", {})
                
                # 构建详细的上下文信息
                context_info = f"第{i}轮对话:"
                context_info += f"\n  - 用户查询: {user_query}"
                context_info += f"\n  - 意图类型: {intent_type}"
                context_info += f"\n  - 改写查询: {rewritten_query}"
                
                # 添加时间信息
                if time_range.get("start_time") and time_range.get("end_time"):
                    context_info += f"\n  - 时间范围: {time_range['start_time']} 到 {time_range['end_time']}"
                
                # 添加实体信息
                if entities.get("log_type"):
                    context_info += f"\n  - 日志类型: {entities['log_type']}"
                if entities.get("aws_service"):
                    context_info += f"\n  - AWS服务: {entities['aws_service']}"
                if entities.get("keywords"):
                    context_info += f"\n  - 关键词: {', '.join(entities['keywords'])}"
                
                context_parts.append(context_info)
            else:
                context_parts.append(f"第{i}轮对话:\n  - 用户查询: {user_query}\n  - 状态: 分析失败或未完成")
        
        return "\n\n".join(context_parts)
    
    def clear_conversation_history(self):
        """清除对话历史"""
        self.conversation_history.clear()
    
    def get_relevant_context_for_query(self, current_query: str) -> Dict[str, Any]:
        """
        获取与当前查询最相关的上下文信息
        
        Args:
            current_query: 当前用户查询
            
        Returns:
            Dict[str, Any]: 相关的上下文信息
        """
        if not self.conversation_history:
            return {
                "has_context": False,
                "relevant_entries": [],
                "last_query": None,
                "last_intent": None,
                "last_time_range": None,
                "last_entities": {}
            }
        
        # 获取最近的对话记录
        last_entry = self.conversation_history[-1]
        last_semantic = last_entry.get("semantic_result", {})
        
        # 分析当前查询中的指代词和关联词
        current_query_lower = current_query.lower()
        has_reference_words = any(word in current_query_lower for word in [
            "再", "还", "也", "同样", "这个", "那个", "它", "继续", "接着", 
            "然后", "另外", "此外", "相同", "类似", "一样"
        ])
        
        # 分析时间指代
        has_time_reference = any(phrase in current_query_lower for phrase in [
            "同样的时间", "相同时间", "那个时间", "这个时间段", "同一时间"
        ])
        
        relevant_entries = []
        
        # 如果有指代词，获取最相关的历史记录
        if has_reference_words or has_time_reference or len(current_query.strip()) < 10:
            # 获取最近3轮对话作为相关上下文
            for entry in self.conversation_history[-3:]:
                if entry.get("semantic_result", {}).get("success"):
                    relevant_entries.append(entry)
        
        return {
            "has_context": len(relevant_entries) > 0,
            "relevant_entries": relevant_entries,
            "last_query": last_entry.get("user_query"),
            "last_intent": last_semantic.get("intent_type"),
            "last_time_range": last_semantic.get("time_range"),
            "last_entities": last_semantic.get("entities", {}),
            "has_reference_words": has_reference_words,
            "has_time_reference": has_time_reference,
            "query_length": len(current_query.strip())
        }
