"""
基于strands-agents框架的日志查询代理 - 重构版本
提供语义识别、日志查询和AWS文档查询功能
"""

import json
import logging
import re
import warnings
import os
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import boto3
from decimal import Decimal

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from strands import Agent, tool
from strands.models import BedrockModel
from strands.tools.mcp import MCPClient
from mcp import StdioServerParameters, stdio_client

from opensearch_client import OpenSearchClient

# 安全导入重试处理器
try:
    from utils.retry_handler import retry_on_rate_limit
except ImportError:
    # 如果导入失败，创建一个空的装饰器
    def retry_on_rate_limit(max_retries=3, wait_time=15):
        def decorator(func):
            return func
        return decorator

from config import config, get_model_config_manager, get_model_config
from dynamodb_client import DynamoDBClient, SearchEngineConfigClient, DSLQueryClient


# 导入重构后的模块
import sys
import os

# 确保当前目录在Python路径中
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

try:
    from tools.semantic_analysis_tool import SemanticAnalysisTool
    from tools.log_query_tool import LogQueryTool
    from tools.aws_docs_tool import AWSDocsTool
    from utils.conversation_manager import ConversationHistoryManager
    from utils.step_callback_system import StepCallbackSystem
except ImportError as e:
    # 如果相对导入失败，尝试绝对导入
    import importlib.util
    
    # 手动导入模块
    def import_module_from_path(module_name, file_path):
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    
    # 导入各个模块
    semantic_module = import_module_from_path("semantic_analysis_tool", 
                                            os.path.join(current_dir, "tools", "semantic_analysis_tool.py"))
    SemanticAnalysisTool = semantic_module.SemanticAnalysisTool
    
    log_query_module = import_module_from_path("log_query_tool", 
                                             os.path.join(current_dir, "tools", "log_query_tool.py"))
    LogQueryTool = log_query_module.LogQueryTool
    
    aws_docs_module = import_module_from_path("aws_docs_tool", 
                                            os.path.join(current_dir, "tools", "aws_docs_tool.py"))
    AWSDocsTool = aws_docs_module.AWSDocsTool
    
    conversation_module = import_module_from_path("conversation_manager", 
                                                os.path.join(current_dir, "utils", "conversation_manager.py"))
    ConversationHistoryManager = conversation_module.ConversationHistoryManager
    
    step_callback_module = import_module_from_path("step_callback_system", 
                                                 os.path.join(current_dir, "utils", "step_callback_system.py"))
    StepCallbackSystem = step_callback_module.StepCallbackSystem

# 抑制ThreadPoolExecutor相关的ScriptRunContext警告
warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
warnings.filterwarnings("ignore", message=".*ScriptRunContext.*")
warnings.filterwarnings("ignore", category=UserWarning)

# 设置环境变量来抑制Streamlit相关警告
os.environ.setdefault("STREAMLIT_SERVER_HEADLESS", "true")
os.environ.setdefault("STREAMLIT_BROWSER_GATHER_USAGE_STATS", "false")

# 初始化日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 抑制特定的日志警告
logging.getLogger("streamlit").setLevel(logging.ERROR)
logging.getLogger("streamlit.runtime.scriptrunner.script_run_context").setLevel(logging.ERROR)

# 设置标志，表示不使用额外的上下文处理工具
CONTEXT_UTILS_AVAILABLE = False

# 初始化AWS文档MCP客户端
aws_docs_client = None
AWS_DOCS_MCP_AVAILABLE = False

# 初始化AWS文档MCP客户端
aws_docs_client = None
AWS_DOCS_MCP_AVAILABLE = False

def initialize_aws_docs_client():
    """初始化AWS文档MCP客户端"""
    global aws_docs_client, AWS_DOCS_MCP_AVAILABLE
    
    if aws_docs_client is not None:
        return aws_docs_client
    
    try:
        aws_docs_client = MCPClient(
            lambda: stdio_client(
                StdioServerParameters(
                    command="uvx", args=["awslabs.aws-documentation-mcp-server@latest"]
                )
            )
        )
        
        # 启动MCP客户端会话
        # 检查客户端是否有启动方法
        if hasattr(aws_docs_client, 'start'):
            aws_docs_client.start()
        else:
            # 手动初始化客户端连接
            if not hasattr(aws_docs_client, '_client') or aws_docs_client._client is None:
                aws_docs_client._client = aws_docs_client._client_factory()
        
        # 验证客户端是否可用
        try:
            tools = aws_docs_client.list_tools_sync()
            if tools:
                AWS_DOCS_MCP_AVAILABLE = True
                logger.info(f"AWS文档MCP客户端已启用，工具数量: {len(tools)}")
            else:
                AWS_DOCS_MCP_AVAILABLE = False
                logger.error("AWS文档MCP客户端初始化失败：无法获取工具列表")
        except Exception as e:
            AWS_DOCS_MCP_AVAILABLE = False
            logger.error(f"AWS文档MCP客户端验证失败: {str(e)}")
            
    except Exception as e:
        AWS_DOCS_MCP_AVAILABLE = False
        aws_docs_client = None
        logger.error(f"AWS文档MCP客户端初始化失败: {str(e)}")
    
    return aws_docs_client


def convert_decimal_to_serializable(obj):
    """递归转换对象中的Decimal类型为可序列化的类型"""
    if isinstance(obj, Decimal):
        # 如果是整数，转换为int，否则转换为float
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    elif isinstance(obj, dict):
        return {key: convert_decimal_to_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimal_to_serializable(item) for item in obj]
    else:
        return obj


# 系统提示词
SYSTEM_PROMPT = """你是一个智能助手，具有以下能力：

1. 语义识别：分析用户的自然语言查询，理解查询意图，将用户的请求改写更加明确更加清晰，特别擅长时间表达式的转换
2. 日志查询：基于用户意图查询和分析日志数据
3. AWS文档查询：查询AWS相关文档和最佳实践
4. 通用对话：回答日志分析相关的问题

重要规则：在执行任何其他操作之前，必须首先调用 init_start() 工具进行初始化。

你有以下工具可以使用：

0. init_start() -> Dict[str, Any]
   - 功能：初始化启动工具，什么也不做，但是执行前都要先经过这个工具
   - 参数：无参数
   - 返回：包含成功状态和消息的结果
   - 使用场景：每次开始处理用户请求时必须首先调用

1. semantic_analysis(query: str) -> Dict[str, Any]
   - 功能：分析用户查询的语义，识别意图类型和时间范围
   - 参数：query - 用户的自然语言查询字符串（必需，非空）
   - 返回：包含意图类型、时间范围、实体信息的结构化结果
   - 使用场景：需要理解用户查询意图和时间范围时

2. query_logs_advanced(
   query: str,
   rewritten_query: str,
   intent_type: str,
   log_type: str,
   start_time: str,
   end_time: str,
   keywords: List[str] = None,
   aws_service: str = "",
   error_codes: List[str] = None
) -> Dict[str, Any]
   - 功能：基于语义分析结果进行高级日志查询
   - 参数：
     * query - 用户的原始查询字符串（必需，非空）
     * rewritten_query - 改写后的查询字符串（必需，非空）
     * intent_type - 查询意图类型，如"log_query"（必需）
     * log_type - 日志类型，如"cloudfront", "alb", "waf"（必需）
     * start_time - 开始时间，格式"YYYY-MM-DD HH:MM:SS"（必需）
     * end_time - 结束时间，格式"YYYY-MM-DD HH:MM:SS"（必需）
     * keywords - 关键词列表（可选）
     * aws_service - AWS服务名称（可选）
     * error_codes - 错误代码列表（可选）
   - 返回：包含查询结果、图表数据、分析报告的完整响应
   - 使用场景：日志查询相关问题
   - 注意：必须先调用semantic_analysis获取语义分析结果，然后提取具体字段传递

3. query_aws_docs(query: str) -> Dict[str, Any]
   - 功能：查询AWS文档和最佳实践
   - 参数：query - AWS相关的查询字符串（必需，非空）
   - 返回：包含AWS文档信息和相关链接的结果
   - 使用场景：AWS服务相关问题

重要的工具使用规则：
1. 参数验证：所有工具的字符串参数都必须是非空字符串
2. 所有的query首先调用semantic_analysis，再基于semantic_analysis确认调用哪个mcp tool
3. 错误处理：如果工具返回success=False，需要检查error字段并相应处理
4. 参数传递：确保按照工具定义的参数类型和格式传递参数

工具使用指南：
重要：无论处理什么类型的请求，都必须首先调用 init_start() 进行初始化。

1. 对于日志查询相关的问题（如"查询错误日志"、"分析最近一小时的日志"、"CloudFront日志分析"等）：
   步骤0：首先调用 init_start() 进行初始化
   步骤1：然后调用 semantic_analysis(query) 分析用户意图和时间范围
   步骤2：检查语义分析结果，确保包含必要信息：
          - 如果缺少时间范围，提示用户补充时间信息
          - 如果缺少日志源，提示用户指定日志类型
   步骤3：如果语义分析成功且信息完整，调用 query_logs_advanced 进行查询
   
   新的调用格式（使用具体参数）：
   从semantic_analysis结果中提取具体字段，然后调用：
   
   示例：
   semantic_result = semantic_analysis("查询CloudFront半年内4xx/5xx错误")
   query_logs_advanced(
       query="查询CloudFront半年内4xx/5xx错误",
       rewritten_query=semantic_result["rewritten_query"],
       intent_type=semantic_result["intent_type"],
       log_type=semantic_result["entities"]["log_type"],
       start_time=semantic_result["time_range"]["start_time"],
       end_time=semantic_result["time_range"]["end_time"],
       keywords=semantic_result["entities"].get("keywords", []),
       aws_service=semantic_result["entities"].get("aws_service", ""),
       error_codes=semantic_result["entities"].get("error_codes", [])
   )
   
   重要：必须从semantic_analysis结果中提取每个具体字段，不能直接传递整个字典
   
2. 对于AWS相关的问题（如"如何配置S3"、"Lambda最佳实践"等）：
   步骤0：首先调用 init_start() 进行初始化
   步骤1：然后调用 query_aws_docs(query)
   
3. 对于一般性问题或概念解释：
   步骤0：首先调用 init_start() 进行初始化
   步骤1：然后直接回答，不需要调用其他工具

特别注意事项：
- 当用户查询包含"CloudFront"时，语义分析应该识别log_type为"cloudfront"
- 当用户查询包含"半年内"、"过去半年"时，应该计算对应的时间范围
- 如果语义分析返回success=False或缺少必要信息，不要调用query_logs_advanced
- 参数传递时确保semantic_result是完整的字典对象，不是字符串

错误处理策略：
- 如果semantic_analysis失败，检查错误信息并向用户说明
- 如果query_logs_advanced参数缺失，确保从semantic_result中正确提取所有必需字段
- 如果参数类型错误，检查字段提取是否正确（使用.get()方法处理可选字段）
- 禁止重复调用semantic_analysis，一次对话只调用一次
- 如果缺少必要参数，引导用户提供完整信息

请始终使用专业、清晰的中文回复，并确保提供有价值的见解和建议。在调用工具时，请严格按照参数要求传递正确的参数类型和格式。
"""

class LogQueryAgent:
    """日志查询代理类 - 重构版本"""
    
    def __init__(self, region: str = None):
        """
        初始化日志查询代理
        
        Args:
            region: AWS区域，如果未指定则从配置文件读取
        """
        try:
            # 声明全局变量
            global AWS_DOCS_MCP_AVAILABLE
            
            # 从配置文件获取region，如果参数未提供的话
            if region is None:
                # 使用模型配置中的默认region
                model_config = get_model_config()
                region = model_config.region
            
            # 保存区域信息
            self.region = region
            
            # 初始化模型配置管理器
            self.model_config_manager = get_model_config_manager()
            
            # 初始化步骤回调系统
            self.step_callback_system = StepCallbackSystem()
            
            # 初始化对话历史管理器
            self.conversation_history_manager = ConversationHistoryManager()
            
            # 初始化 Bedrock 模型，支持多种模型作为备选
            # 从模型配置管理器获取默认模型的region
            model_config = self.model_config_manager.get_model_config()
            bedrock_region = model_config.region
            self.bedrock_model = self._initialize_bedrock_model(bedrock_region)
            
            # 初始化 DynamoDB 客户端
            self.dynamodb_client = DynamoDBClient(
                region=config.DYNAMODB_REGION,
                table_name=config.DYNAMODB_METADATA_TABLE
            )
            
            # 初始化搜索引擎配置客户端
            self.config_client = SearchEngineConfigClient(
                region=config.DYNAMODB_REGION,
                table_name=config.DYNAMODB_DATASOURCE_TABLE
            )
            
            # 初始化DSL查询客户端
            self.dsl_client = DSLQueryClient(
                region=config.DYNAMODB_REGION,
                table_name=config.DYNAMODB_DSL_TABLE
            )
            
            # 初始化工具模块
            self.semantic_tool = SemanticAnalysisTool(
                self.model_config_manager, 
                self.conversation_history_manager,
                self.step_callback_system
            )
            
            self.log_query_tool = LogQueryTool(
                self.model_config_manager,
                self.dynamodb_client,
                self.config_client,
                self.dsl_client,
                self.step_callback_system
            )
            
            # 尝试初始化AWS文档MCP客户端
            client = initialize_aws_docs_client()
            
            self.aws_docs_tool = AWSDocsTool(
                self.bedrock_model,
                client,
                AWS_DOCS_MCP_AVAILABLE
            )
            
            # 定义工具函数
            self._setup_tools()
            
            # 初始化 Agent
            self.agent = Agent(
                system_prompt=SYSTEM_PROMPT,
                model=self.bedrock_model,
                tools=self.tools
            )
            
        except Exception as e:
            logger.error(f"初始化LogQueryAgent失败: {str(e)}")
            raise
    
    def _setup_tools(self):
        """设置工具函数"""
        
        # 声明全局变量
        global AWS_DOCS_MCP_AVAILABLE
        
        @tool
        def init_start() -> Dict[str, Any]:
            """
            初始化启动工具，什么也不做，但是执行前都要先经过这个工具。
            
            返回格式：
            {
                "success": bool,
                "message": str
            }
            """
            return {
                "success": True,
                "message": "初始化启动完成"
            }
        
        @tool
        def semantic_analysis(query: str) -> Dict[str, Any]:
            """
            分析用户查询的语义，识别意图类型和时间范围，并进行语义改写。
            
            参数：
            - query (str): 必需参数，用户的自然语言查询字符串。
            
            功能：
            - 识别查询意图类型（日志查询/AWS文档查询/通用对话）
            - 当意图为日志查询时，将自然语言时间描述转换为标准时间格式
            - 提取关键实体信息（日志类型、AWS服务、关键词等）
            - 语义改写：将模糊查询改写为明确、清晰的表达
            - 支持多轮对话的上下文理解和查询优化
            
            返回格式：
            {
                "success": bool,
                "intent_type": str,
                "confidence": float,
                "rewritten_query": str,
                "time_range": {
                    "start_time": str,
                    "end_time": str,
                },
                "entities": {
                    "log_type": str,
                    "aws_service": str,
                    "keywords": list
                }
            }
            """
            if not isinstance(query, str) or not query.strip():
                return {
                    "success": False,
                    "error": "query参数必须是非空字符串",
                    "query": query
                }
            
            # 发送语义分析开始状态
            self.step_callback_system.emit_text(
                {"message": "开始分析用户查询的语义和意图"},
                "语义分析",
                "processing"
            )
            
            # 直接调用语义分析工具，不使用回调系统
            # 这样避免了ThreadPoolExecutor中的Streamlit上下文问题
            result = self.semantic_tool.analyze(query.strip(), emit_callbacks=False)
            
            # 更新语义分析状态
            if result.get("success", False):
                self.conversation_history_manager.add_to_conversation_history(query, result)
                
                # 发送语义分析结果（JSON格式）
                result_data = {
                    "intent_type": result.get("intent_type", "unknown"),
                    "confidence": result.get("confidence", 0),
                    "original_query": query,
                    "rewritten_query": result.get("rewritten_query", query),
                    "rewrite_reason": result.get("rewrite_reason", ""),
                    "context_used": result.get("context_used", ""),
                    "time_range": result.get("time_range", {}),
                    "entities": result.get("entities", {}),
                    "analysis_status": "成功完成多轮对话语义分析"
                }
                
                self.step_callback_system.emit_json(
                    result_data,
                    "语义分析",
                    "success"
                )
            else:
                error_msg = result.get("error", "语义分析失败")
                self.step_callback_system.emit_text(
                    {"error": error_msg},
                    "语义分析",
                    "error"
                )
            
            return result
        
        @tool
        def query_logs_advanced(
            query: str,
            rewritten_query: str,
            intent_type: str,
            log_type: str,
            start_time: str,
            end_time: str,
            keywords: List[str] = None,
            aws_service: str = "",
            error_codes: List[str] = None
        ) -> Dict[str, Any]:
            """
            高级日志查询工具，基于语义分析结果进行智能日志查询。
            
            参数：
            - query (str): 用户的原始查询字符串
            - rewritten_query (str): 改写后的查询字符串
            - intent_type (str): 查询意图类型，如"log_query"
            - log_type (str): 日志类型，如"cloudfront", "alb", "waf"等
            - start_time (str): 开始时间，格式"YYYY-MM-DD HH:MM:SS"
            - end_time (str): 结束时间，格式"YYYY-MM-DD HH:MM:SS"
            - keywords (List[str]): 关键词列表，可选
            - aws_service (str): AWS服务名称，可选
            - error_codes (List[str]): 错误代码列表，可选
            
            返回：
            Dict[str, Any]: 包含success字段和查询结果的字典
            """
            # 记录调用信息
            logger.info(f"🔍 query_logs_advanced被调用 - query: {query[:50]}..., log_type: {log_type}")
            
            # 参数验证
            if not isinstance(query, str) or not query.strip():
                error_msg = f"❌ 参数错误：query必须是非空字符串"
                logger.error(error_msg)
                return {"success": False, "error": error_msg}
            
            if not isinstance(rewritten_query, str) or not rewritten_query.strip():
                error_msg = f"❌ 参数错误：rewritten_query必须是非空字符串"
                logger.error(error_msg)
                return {"success": False, "error": error_msg}
            
            if not log_type:
                error_msg = f"❌ 参数错误：log_type不能为空"
                logger.error(error_msg)
                return {"success": False, "error": error_msg}
            
            # 重构semantic_result字典
            semantic_result = {
                "success": True,
                "query": query,
                "rewritten_query": rewritten_query,
                "intent_type": intent_type,
                "time_range": {
                    "start_time": start_time,
                    "end_time": end_time,
                    "has_explicit_time": True
                },
                "entities": {
                    "log_type": log_type,
                    "aws_service": aws_service,
                    "keywords": keywords or [],
                    "error_codes": error_codes or []
                }
            }
            
            try:
                logger.info(f"✅ 开始执行query_logs_advanced - log_type: {log_type}")
                result = self.log_query_tool.query_logs(rewritten_query, semantic_result)
                
                if result.get("success"):
                    logger.info(f"✅ query_logs_advanced调用成功")
                    return result
                else:
                    error_msg = result.get("error", "未知错误")
                    logger.error(f"❌ query_logs_advanced内部错误 - {error_msg}")
                    result["error"] = f"查询执行失败：{error_msg}"
                    return result
                    
            except Exception as e:
                error_msg = f"❌ query_logs_advanced执行异常: {str(e)}"
                logger.error(error_msg, exc_info=True)
                return {
                    "success": False,
                    "error": error_msg,
                    "query": query
                }
        
        @tool
        def query_aws_docs(query: str) -> Dict[str, Any]:
            """
            查询AWS文档和最佳实践。
            
            参数：
            - query (str): 必需参数，AWS相关的查询字符串
            
            返回格式：
            {
                "success": bool,
                "query": str,
                "aws_service": str,
                "response": str,
                "documents": [
                    {
                        "title": str,
                        "url": str,
                        "summary": str
                    }
                ]
            }
            """
            if not isinstance(query, str) or not query.strip():
                return {
                    "success": False,
                    "error": "query参数必须是非空字符串",
                    "query": query
                }
            return self.aws_docs_tool.query_aws_docs(query.strip())
        
        # 创建工具列表（包含init_start作为第一个工具）
        self.tools = [
            init_start,
            semantic_analysis,
            query_logs_advanced,
            query_aws_docs
        ]
        
        # 如果AWS文档MCP客户端可用，添加MCP工具
        if AWS_DOCS_MCP_AVAILABLE and aws_docs_client:
            try:
                aws_docs_tools = aws_docs_client.list_tools_sync()
                self.tools.extend(aws_docs_tools)
            except Exception as e:
                logger.error(f"获取AWS文档MCP工具失败: {str(e)}")
        else:
            logger.warning("AWS文档MCP客户端不可用，AWS文档查询功能将不可用")
    
    def set_step_callback(self, callback_function):
        """设置步骤回调函数"""
        self.step_callback_system.set_callback(callback_function)
        
    def set_session_id(self, session_id: str):
        """设置当前会话ID"""
        self.step_callback_system.set_session_id(session_id)
        
    def emit_text(self, content: Any, title: str = None, status: str = "processing"):
        """发送文本输出"""
        self.step_callback_system.emit_text(content, title, status)
    
    def emit_json(self, content: Any, title: str = None, status: str = "processing"):
        """发送JSON输出"""
        self.step_callback_system.emit_json(content, title, status)
    
    def emit_chart(self, content: Any, title: str = None, status: str = "processing"):
        """发送图表输出"""
        self.step_callback_system.emit_chart(content, title, status)
    
    def clear_conversation_history(self):
        """清除对话历史"""
        self.conversation_history_manager.clear_conversation_history()
    
    def process_query_with_context(self, query: str) -> str:
        """
        处理带上下文的查询，这是主要的对外接口
        
        Args:
            query: 用户查询
            
        Returns:
            str: 处理结果
        """
        try:
            # 先在主线程中执行语义分析并发送回调
            self.step_callback_system.emit_text("正在执行语义分析", "语义分析", "processing")
            
            # 调用语义分析（不发送回调）
            semantic_result = self.semantic_tool.analyze(query.strip(), emit_callbacks=False)
            
            # 在主线程中发送语义分析结果回调
            if semantic_result.get("success", False):
                result_data = {
                    "intent_type": semantic_result.get("intent_type", "unknown"),
                    "confidence": semantic_result.get("confidence", 0),
                    "original_query": query,
                    "rewritten_query": semantic_result.get("rewritten_query", query),
                    "rewrite_reason": semantic_result.get("rewrite_reason", ""),
                    "context_used": semantic_result.get("context_used", ""),
                    "time_range": semantic_result.get("time_range", {}),
                    "entities": semantic_result.get("entities", {}),
                    "analysis_status": "成功完成多轮对话语义分析"
                }
                
                self.step_callback_system.emit_json(
                    result_data,
                    "语义分析",
                    "success"
                )
                
                # 更新对话历史
                self.conversation_history_manager.add_to_conversation_history(query, semantic_result)
            else:
                error_msg = semantic_result.get("error", "语义分析失败")
                self.step_callback_system.emit_text(
                    {"error": error_msg},
                    "语义分析",
                    "error"
                )
            
            # 使用Agent处理查询
            @retry_on_rate_limit(max_retries=3, wait_time=15)
            def call_agent():
                return self.agent(query)
            
            response = call_agent()
            
            # 将响应添加到对话历史（如果还没有添加的话）
            if self.conversation_history_manager.conversation_history:
                last_entry = self.conversation_history_manager.conversation_history[-1]
                if last_entry.get("user_query") == query and not last_entry.get("response"):
                    last_entry["response"] = str(response)
            
            return str(response)
            
        except Exception as e:
            logger.error(f"处理查询失败: {str(e)}")
            error_response = f"处理查询时发生错误: {str(e)}"
            
            # 记录错误到对话历史
            self.conversation_history_manager.add_to_conversation_history(query, None, error_response)
            
            return error_response
    
    def _initialize_bedrock_model(self, region: str):
        """
        初始化Bedrock模型，支持多种模型作为备选
        """
        # 首先从配置文件获取模型列表
        model_candidates = []
        
        # 从配置管理器获取所有可用模型
        try:
            available_models = self.model_config_manager.list_available_models()
            for model_info in available_models:
                model_candidates.append({
                    "model_id": model_info["model_id"],
                    "name": model_info["display_name"],
                    "provider": model_info["provider"]
                })
        except Exception as e:
            logger.warning(f"无法从配置文件加载模型: {str(e)}")
        
        # 如果配置文件中没有模型，使用硬编码的备选方案
        if not model_candidates:
            model_candidates = [
                {
                    "model_id": "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                    "name": "Claude 3.7 Sonnet",
                    "provider": "Anthropic"
                }
            ]
        
        last_error = None
        
        for model_config in model_candidates:
            try:
                bedrock_model = BedrockModel(
                    model_id=model_config["model_id"],
                    temperature=0.1,
                    region_name=region
                )
                
                # 测试模型是否可用
                test_agent = Agent(
                    system_prompt="你是一个测试助手。",
                    model=bedrock_model
                )
                
                @retry_on_rate_limit(max_retries=2, wait_time=15)
                def test_model():
                    return test_agent("测试")
                
                test_response = test_model()
                logger.info(f"已启用模型: {model_config['name']}")
                
                return bedrock_model
                
            except Exception as e:
                error_msg = str(e)
                last_error = error_msg
                logger.warning(f"模型 {model_config['name']} 初始化失败: {error_msg}")
                continue
        
        # 如果所有模型都失败了，抛出异常
        error_msg = f"所有Bedrock模型初始化都失败了。最后一个错误: {last_error}"
        logger.error(error_msg)
        raise Exception(error_msg)
    
    def process_query(self, query: str, session_id: str = None, conversation_context: Dict = None) -> Dict[str, Any]:
        """
        处理用户查询的主入口方法
        
        Args:
            query: 用户查询
            session_id: 会话ID（可选）
            conversation_context: 对话上下文（可选）
            
        Returns:
            Dict[str, Any]: 处理结果
        """
        try:
            # 参数验证
            if not isinstance(query, str):
                return {
                    "success": False,
                    "error": "query参数必须是字符串类型",
                    "query": query,
                    "session_id": session_id
                }
            
            query = query.strip()
            if not query:
                return {
                    "success": False,
                    "error": "query参数不能为空",
                    "query": query,
                    "session_id": session_id
                }
            
            
            # 直接让 agent 处理用户查询
            @retry_on_rate_limit(max_retries=3, wait_time=15)
            def call_agent():
                return self.agent(query)
            
            result = call_agent()
            response_text = str(result)
            
            # 构建返回结果
            result_data = {
                "success": True,
                "response": response_text,
                "query": query,
                "type": "agent_response"
            }
            
            # 添加会话信息（如果提供）
            if session_id:
                result_data["session_id"] = session_id
            if conversation_context:
                result_data["conversation_context"] = conversation_context
            
            return result_data
            
        except Exception as e:
            logger.error(f"处理用户查询失败: {str(e)}")
            return {
                "success": False,
                "error": f"处理用户查询失败: {str(e)}",
                "query": query,
                "session_id": session_id
            }


# 创建全局代理实例（使用配置文件中的默认region）
log_query_agent = LogQueryAgent()
