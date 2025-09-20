"""
语义分析工具模块
负责分析用户查询的语义，识别意图类型和时间范围
"""

import json
import logging
import re
import sys
import os
from typing import Dict, Any
from datetime import datetime, timedelta
from strands import Agent
from strands.models import BedrockModel

# 添加父目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)


class SemanticAnalysisTool:
    """语义分析工具类"""
    
    def __init__(self, model_config_manager, conversation_history_manager=None, step_callback_system=None):
        """
        初始化语义分析工具
        
        Args:
            model_config_manager: 模型配置管理器
            conversation_history_manager: 对话历史管理器（可选）
            step_callback_system: 步骤回调系统（可选）
        """
        self.model_config_manager = model_config_manager
        self.conversation_history_manager = conversation_history_manager
        self.step_callback_system = step_callback_system
        
    def analyze(self, query: str, emit_callbacks: bool = True) -> Dict[str, Any]:
        """
        分析用户查询的语义，识别意图类型和时间范围
        
        Args:
            query: 用户查询字符串
            emit_callbacks: 是否发送回调（默认True）
            
        Returns:
            Dict[str, Any]: 语义分析结果
        """
        try:
            # 参数验证
            if not isinstance(query, str):
                return {
                    "success": False,
                    "error": "query参数必须是字符串类型",
                    "query": query
                }
            
            query = query.strip()
            if not query:
                return {
                    "success": False,
                    "error": "query参数不能为空",
                    "query": query
                }

    
            # 调用核心语义分析方法
            self._emit_text("正在执行语义分析", "语义分析", "processing")
            
            result = self._perform_semantic_analysis(query)
            
            # 只在允许时发送分析结果回调
            
            if result.get("success", False):
                self._emit_json(result, "语义分析", "success")
            else:
                self._emit_text(result.get("error", "语义分析失败"), "语义分析", "error")

            return result
                
        except Exception as e:
            logger.error(f"语义分析工具执行失败: {str(e)}")
            return {
                "success": False,
                "error": f"语义分析失败: {str(e)}",
                "query": query
            }
    
    def _perform_semantic_analysis(self, query: str) -> Dict[str, Any]:
        """
        执行语义分析的核心方法
        
        Args:
            query: 用户查询字符串
            
        Returns:
            Dict[str, Any]: 语义分析结果
        """
        # 获取当前时间信息，用于时间转换
        current_time = datetime.now()
        current_time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
        current_date_str = current_time.strftime("%Y-%m-%d")
        current_hour = current_time.hour
        current_weekday = current_time.strftime("%A")  # 星期几
        
        # 创建专门的语义分析 Agent
        semantic_analysis_prompt = """你是一个专业的语义分析助手。你的任务是分析用户的自然语言查询，准确识别其意图并提取相关信息。

你需要：
1. 准确识别查询意图类型（log_query/aws_docs_query/general_chat）
2. 基于当前时间提取时间范围信息并转换为标准格式
3. 将query请求改写的更加明确、清晰，支持多轮对话的上下文理解
4. 提取其他关键实体信息
5. 评估分析的置信度

语义改写规则：
- 将模糊的表达改写为明确的描述
- 补充缺失的上下文信息
- 标准化技术术语和时间表达
- 保持原意的同时提高查询的精确性
- 支持基于对话历史的上下文理解

请始终以结构化的JSON格式返回分析结果，确保准确性和一致性。"""

        # 获取对话上下文
        conversation_context = self._get_conversation_context()
        
        # 使用 strands agent 进行智能语义分析，支持多轮对话上下文
        analysis_query = f"""
        请分析以下用户查询的语义，并基于对话上下文进行智能改写。

        当前时间信息：
        - 当前时间: {current_time_str}
        - 当前日期: {current_date_str}
        - 当前小时: {current_hour}
        - 今天是: {current_weekday}

        对话历史上下文：
        {conversation_context}

        用户查询：{query}

        多轮对话分析要求：
        1. 意图类型识别：
           - log_query: 日志查询相关（查看日志、分析错误、监控、统计等）
           - aws_docs_query: AWS文档查询相关（AWS服务、最佳实践、配置指南等）
           - general_chat: 通用对话（问候、闲聊等）

        2. 上下文理解和语义改写（重点增强）：
           - 深度分析对话历史，理解用户的连续意图和关联性
           - 识别指代关系：处理"它"、"这个"、"那个"、"同样的"、"再看看"等指代词
           - 继承上下文信息：时间范围、日志类型、查询对象等
           - 补充省略信息：基于历史对话补充当前查询中省略的关键信息
           - 保持逻辑连贯：确保改写后的查询与对话流程逻辑一致
           
           多轮对话改写示例：
           * 上下文：用户刚查询了"错误日志" → 当前："再看看昨天的" → 改写："查询昨天全天的错误日志记录"
           * 上下文：用户查询了"最近1小时的性能分析" → 当前："同样的时间段，看看访问量" → 改写："分析最近1小时内的访问量统计"
           * 上下文：用户查询了"数据库连接错误" → 当前："这个问题的影响范围" → 改写："分析数据库连接错误的影响范围和受影响的系统组件"
           * 上下文：用户查询了"CloudFront日志" → 当前："有异常吗" → 改写："检查CloudFront日志中的异常和错误记录"
           * 上下文：用户查询了"最近24小时的日志" → 当前："筛选出错误的" → 改写："筛选最近24小时内的错误日志记录"

        3. 时间范围智能推断（基于当前时间 {current_time_str}）：
           - 优先使用当前查询中的时间描述
           - 如果当前查询没有时间信息，继承上下文中的时间范围
           - 如果都没有明确的时间信息，返回null表示缺少时间范围
           - 时间转换示例：
             {self._get_time_conversion_examples(current_time)}
           - 处理相对时间表达式：如"2小时前"、"30分钟内"、"本周"、"上个月"等
           - 处理指代时间：如"同样的时间段"、"相同时间"等
           - 重要：只有用户明确提供时间信息时才设置时间范围，否则返回null

        4. 关键实体智能提取（增强日志源识别）：
           - 当前查询中的实体信息
           - 继承对话历史中相关的实体信息
           - 日志类型智能识别规则：
             * AWS服务日志：自动识别各种AWS服务名称及其变体（如CloudFront/cloudfront、WAF/waf、ALB/alb/负载均衡等）
             * 应用程序日志：识别应用类型、组件名称、业务模块等
             * 日志性质分类：错误日志(error/4xx/5xx)、访问日志(access)、审计日志(audit)、性能日志(performance)等
             * 数据源识别：根据环境、系统、服务等上下文信息进行分类
             * 技术栈识别：数据库、缓存、消息队列、API网关等技术组件
           - 服务范围：支持所有主流云服务、开源组件和自定义应用程序日志
           - 关键词：技术术语、错误类型、性能指标等
           - 错误状态码识别：4xx、5xx、404、500等

        5. 改写质量要求：
           - 改写后的查询必须完整、明确、可执行
           - 保持用户原始意图不变
           - 补充必要的上下文信息
           - 标准化技术术语和表达方式
           - 确保时间范围的准确性
           - 明确指定日志源和查询类型

        请严格按照以下JSON格式返回分析结果：
        {{
            "intent_type": "意图类型",
            "confidence": 置信度数值,
            "rewritten_query": "基于上下文改写后的明确查询",
            "rewrite_reason": "改写原因和使用的上下文信息",
            "context_used": "具体使用的对话上下文",
            "time_range": {{
                "start_time": "YYYY-MM-DD HH:MM:SS或null（仅当用户明确提供时间信息时才设置）",
                "end_time":  "YYYY-MM-DD HH:MM:SS或null（仅当用户明确提供时间信息时才设置）",
                "has_explicit_time": "true/false（用户是否明确提供了时间信息）",
                "original_description": "用户原始时间描述"
            }},
            "entities": {{
                "log_type": "日志类型或null（仅当用户明确指定日志类型时才设置）",
                "aws_service": "AWS服务或null",
                "keywords": ["关键词列表"],
                "error_codes": ["错误状态码列表"],
                "has_explicit_log_source": "true/false（用户是否明确指定了日志源）"
            }},
            "current_time": "{current_time_str}"
        }}
        
        重要提醒：
        - 只有当用户明确提供时间信息（如"最近1小时"、"今天"、"过去24小时"、"半年内"等）时，才设置start_time和end_time
        - 只有当用户明确指定日志类型（如"CloudFront日志"、"错误日志"、"访问日志"等）时，才设置log_type
        - 如果用户没有明确提供这些信息，请将相应字段设置为null，并将has_explicit_time和has_explicit_log_source设置为false
        - 特别注意：如果查询中提到"CloudFront"，应该设置log_type为"cloudfront"，has_explicit_log_source为true
        - 如果查询中提到"半年内"、"过去半年"等，应该计算对应的时间范围
        """

        try:
            # 获取语义分析任务的模型配置（使用高性能模型）
            semantic_config = self.model_config_manager.get_model_config('claude_3_7_sonnet')
            
            # 创建专门的语义分析模型
            semantic_model = BedrockModel(
                model_id=semantic_config.model_id,
                temperature=semantic_config.temperature,
                region_name=semantic_config.region
            )
            
            # 创建Agent进行语义分析（不传递tools参数）
            semantic_agent = Agent(
                system_prompt=semantic_analysis_prompt,
                model=semantic_model
            )
            
            # 执行语义分析
            response = semantic_agent(analysis_query)
            response_text = str(response)
            
        except Exception as e:
            logger.error(f"语义分析执行失败: {str(e)}")
            # 如果失败，返回错误信息
            return {
                "success": False,
                "error": f"语义分析执行失败: {str(e)}",
                "query": query
            }
        
        # 解析AI响应
        try:
            # response_text已经在上面获取了，直接使用
            # 尝试从文本中提取JSON
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                # 清理可能的注释和格式问题
                json_str = re.sub(r'//.*?\n', '\n', json_str)
                json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
                
                result = json.loads(json_str)
                
                # 验证返回结果的必需字段
                required_fields = ["intent_type", "confidence", "time_range", "entities", "rewritten_query"]
                missing_fields = [field for field in required_fields if field not in result]
                
                if missing_fields:
                    logger.warning(f"语义分析结果缺少必需字段: {missing_fields}")
                    # 为缺失的字段提供默认值
                    if "rewritten_query" in missing_fields:
                        result["rewritten_query"] = query  # 如果没有改写，使用原查询
                        result["rewrite_reason"] = "未进行改写"
     
                    # 重新检查缺失字段
                    missing_fields = [field for field in required_fields if field not in result]
                    if missing_fields:
                        return {
                            "success": False,
                            "error": f"语义分析结果不完整，缺少字段: {', '.join(missing_fields)}",
                            "query": query,
                            "partial_result": result
                        }
                
                # 验证time_range结构
                time_range = result.get("time_range", {})
                if not isinstance(time_range, dict):
                    result["time_range"] = {
                        "start_time": None,
                        "end_time": None,
                        "original_description": None
                    }
                
                # 验证entities结构
                entities = result.get("entities", {})
                if not isinstance(entities, dict):
                    result["entities"] = {
                        "log_type": None,
                        "aws_service": None,
                        "keywords": []
                    }
                
                # 确保keywords是列表
                if not isinstance(entities.get("keywords"), list):
                    result["entities"]["keywords"] = []
                
                # 添加额外信息
                result["query"] = query
                result["success"] = True
                
                # 确保包含原始查询和改写查询
                if "rewritten_query" not in result:
                    result["rewritten_query"] = query
                    result["rewrite_reason"] = "查询已足够明确，无需改写"
                
                # 确保包含上下文相关字段
                if "rewrite_reason" not in result:
                    result["rewrite_reason"] = "基于查询内容进行标准化改写"
                
                if "context_used" not in result:
                    result["context_used"] = "无对话上下文" if not self._has_conversation_history() else "使用了对话历史上下文"
                
                return result
                
            else:
                logger.warning("AI响应中未找到JSON格式的分析结果")
                return {
                    "success": False,
                    "error": "语义分析失败：AI响应中未找到有效的JSON格式结果",
                    "query": query,
                    "raw_response": response_text[:500] + "..." if len(response_text) > 500 else response_text
                }
                
        except json.JSONDecodeError as e:
            logger.warning(f"解析AI语义分析结果失败: {str(e)}")
            return {
                "success": False,
                "error": f"语义分析失败：JSON解析错误 - {str(e)}",
                "query": query,
                "json_string": json_str[:200] + "..." if len(json_str) > 200 else json_str
            }

    def _get_conversation_context(self) -> str:
        """
        获取对话上下文，用于语义改写
        
        Returns:
            str: 格式化的对话上下文
        """
        if not self.conversation_history_manager:
            return "无对话历史"
        
        return self.conversation_history_manager.get_conversation_context()
    
    def _has_conversation_history(self) -> bool:
        """
        检查是否有对话历史
        
        Returns:
            bool: 是否有对话历史
        """
        if not self.conversation_history_manager:
            return False
        
        return len(self.conversation_history_manager.conversation_history) > 0
    
    def _get_time_conversion_examples(self, current_time: datetime) -> str:
        """
        生成时间转换示例，帮助AI更好地理解时间转换
        
        Args:
            current_time: 当前时间
            
        Returns:
            str: 时间转换示例字符串
        """
        examples = []
        
        # 最近一小时
        one_hour_ago = current_time - timedelta(hours=1)
        examples.append(f'"最近一小时" → 从 {one_hour_ago.strftime("%Y-%m-%d %H:%M:%S")} 到 {current_time.strftime("%Y-%m-%d %H:%M:%S")}')
        
        # 今天
        today_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = current_time.replace(hour=23, minute=59, second=59, microsecond=999999)
        examples.append(f'"今天" → 从 {today_start.strftime("%Y-%m-%d %H:%M:%S")} 到 {today_end.strftime("%Y-%m-%d %H:%M:%S")}')
        
        # 过去24小时
        twenty_four_hours_ago = current_time - timedelta(hours=24)
        examples.append(f'"过去24小时" → 从 {twenty_four_hours_ago.strftime("%Y-%m-%d %H:%M:%S")} 到 {current_time.strftime("%Y-%m-%d %H:%M:%S")}')
        
        # 昨天
        yesterday = current_time - timedelta(days=1)
        yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        examples.append(f'"昨天" → 从 {yesterday_start.strftime("%Y-%m-%d %H:%M:%S")} 到 {yesterday_end.strftime("%Y-%m-%d %H:%M:%S")}')
        
        # 过去一周
        one_week_ago = current_time - timedelta(days=7)
        examples.append(f'"过去一周" → 从 {one_week_ago.strftime("%Y-%m-%d %H:%M:%S")} 到 {current_time.strftime("%Y-%m-%d %H:%M:%S")}')
        
        # 过去30分钟
        thirty_minutes_ago = current_time - timedelta(minutes=30)
        examples.append(f'"过去30分钟" → 从 {thirty_minutes_ago.strftime("%Y-%m-%d %H:%M:%S")} 到 {current_time.strftime("%Y-%m-%d %H:%M:%S")}')
        
        # 过去一个月
        one_month_ago = current_time - timedelta(days=30)
        examples.append(f'"过去一个月" → 从 {one_month_ago.strftime("%Y-%m-%d %H:%M:%S")} 到 {current_time.strftime("%Y-%m-%d %H:%M:%S")}')
        
        # 过去半年（6个月）
        six_months_ago = current_time - timedelta(days=180)
        examples.append(f'"过去半年" 或 "半年内" → 从 {six_months_ago.strftime("%Y-%m-%d %H:%M:%S")} 到 {current_time.strftime("%Y-%m-%d %H:%M:%S")}')
        
        # 过去一年
        one_year_ago = current_time - timedelta(days=365)
        examples.append(f'"过去一年" → 从 {one_year_ago.strftime("%Y-%m-%d %H:%M:%S")} 到 {current_time.strftime("%Y-%m-%d %H:%M:%S")}')
        
        return '\n                 '.join(examples)
    
    def _emit_text(self, content: Any, title: str = None, status: str = "processing"):
        """发送文本输出"""
        if self.step_callback_system:
            self.step_callback_system.emit_text(content, title, status)
    
    def _emit_json(self, content: Any, title: str = None, status: str = "processing"):
        """发送JSON输出"""
        if self.step_callback_system:
            self.step_callback_system.emit_json(content, title, status)
    
    def _emit_chart(self, content: Any, title: str = None, status: str = "processing"):
        """发送图表输出"""
        if self.step_callback_system:
            self.step_callback_system.emit_chart(content, title, status)
