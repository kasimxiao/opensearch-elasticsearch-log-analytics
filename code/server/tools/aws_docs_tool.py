"""
AWS文档查询工具模块
负责查询AWS文档和最佳实践
只使用AWS文档MCP服务器，不提供基础模式
"""

import logging
import re
from typing import Dict, Any
from strands import Agent

logger = logging.getLogger(__name__)


class AWSDocsTool:
    """AWS文档查询工具类"""
    
    def __init__(self, bedrock_model, aws_docs_client=None, aws_docs_available=False):
        """
        初始化AWS文档查询工具
        
        Args:
            bedrock_model: Bedrock模型实例
            aws_docs_client: AWS文档MCP客户端（必需）
            aws_docs_available: AWS文档MCP是否可用
        """
        self.bedrock_model = bedrock_model
        self.aws_docs_client = aws_docs_client
        self.aws_docs_available = aws_docs_available
        
    def query_aws_docs(self, query: str) -> Dict[str, Any]:
        """
        查询AWS文档和最佳实践
        
        Args:
            query: AWS相关的查询字符串
            
        Returns:
            Dict[str, Any]: 查询结果
        """
        try:
            # 参数验证
            if not isinstance(query, str):
                return {
                    "success": False,
                    "error": "query参数必须是字符串类型",
                    "query": query,
                    "error_type": "parameter_error"
                }
            
            query = query.strip()
            if not query:
                return {
                    "success": False,
                    "error": "query参数不能为空",
                    "query": query,
                    "error_type": "parameter_error"
                }
            
            # 检查AWS文档MCP客户端是否可用
            if not self.aws_docs_available or not self.aws_docs_client:
                error_details = self._get_mcp_unavailable_error()
                return {
                    "success": False,
                    "error": error_details["error"],
                    "error_type": "mcp_unavailable",
                    "query": query,
                    "troubleshooting": error_details["troubleshooting"],
                    "requirements": error_details["requirements"]
                }
            
            # 验证MCP客户端会话状态
            session_error = self._check_mcp_session()
            if session_error:
                return {
                    "success": False,
                    "error": session_error["error"],
                    "error_type": "mcp_session_error",
                    "query": query,
                    "troubleshooting": session_error["troubleshooting"]
                }
            
            # 构建AWS文档查询
            aws_docs_prompt = f"""
            用户查询: {query}
            
            请使用AWS文档MCP工具查询与此问题相关的AWS官方文档，并提供准确、详细的信息。
            
            请以中文回复，并确保回复清晰、专业且有帮助。
            """
            
            # 创建专门的AWS文档查询Agent
            try:
                aws_docs_agent = Agent(
                    system_prompt="你是AWS文档专家，负责使用AWS文档MCP工具查询和解释AWS官方文档。请始终使用中文回复。",
                    model=self.bedrock_model,
                    tools=self.aws_docs_client.list_tools_sync()
                )
                
                # 执行AWS文档查询
                response = aws_docs_agent(aws_docs_prompt)
                
            except Exception as e:
                logger.error(f"AWS文档查询执行失败: {str(e)}")
                return {
                    "success": False,
                    "error": f"AWS文档查询执行失败: {str(e)}",
                    "error_type": "query_execution_error",
                    "query": query,
                    "troubleshooting": [
                        "检查网络连接是否正常",
                        "确认AWS文档MCP服务器是否正在运行",
                        "检查uvx和相关依赖是否正确安装",
                        "尝试重启应用程序"
                    ]
                }
            
            # 将AgentResult转换为字符串
            response_text = str(response)
            
            # 从响应中提取文档引用
            docs_references = []
            
            # 尝试提取文档URL
            urls = re.findall(r'https?://docs\.aws\.amazon\.com/[^\s\)]+', response_text)
            for i, url in enumerate(urls):
                docs_references.append({
                    "title": f"AWS文档 {i+1}",
                    "url": url,
                    "summary": f"相关AWS文档链接 {i+1}"
                })
            
            # 构建响应
            result = {
                "success": True,
                "query": query,
                "aws_service": self._extract_aws_service(query),
                "response": response_text,
                "documents": docs_references,
                "source": "aws_docs_mcp"
            }
            
            return result
            
        except Exception as e:
            logger.error(f"查询AWS文档失败: {str(e)}")
            return {
                "success": False,
                "error": f"查询AWS文档时发生未预期的错误: {str(e)}",
                "error_type": "unexpected_error",
                "query": query,
                "troubleshooting": [
                    "请检查系统日志获取详细错误信息",
                    "确认所有依赖项都已正确安装",
                    "尝试重启应用程序",
                    "如果问题持续存在，请联系技术支持"
                ]
            }
    
    def _get_mcp_unavailable_error(self) -> Dict[str, Any]:
        """
        获取MCP不可用时的详细错误信息
        
        Returns:
            Dict[str, Any]: 错误详情
        """
        return {
            "error": "AWS文档MCP服务器不可用，无法查询AWS官方文档",
            "troubleshooting": [
                "确认已安装uvx: 运行 'pip install uvx' 或 'pipx install uvx'",
                "确认可以访问AWS文档MCP服务器: 运行 'uvx awslabs.aws-documentation-mcp-server@latest --help'",
                "检查网络连接，确保可以访问外部服务",
                "检查防火墙设置，确保允许MCP服务器通信",
                "尝试手动启动MCP服务器进行测试"
            ],
            "requirements": [
                "Python 3.8+",
                "uvx (Universal eXecutable runner)",
                "网络连接到AWS文档MCP服务器",
                "正确的系统权限"
            ]
        }
    
    def _check_mcp_session(self) -> Dict[str, Any]:
        """
        检查MCP客户端会话状态
        
        Returns:
            Dict[str, Any]: 如果有错误返回错误信息，否则返回None
        """
        try:
            # 尝试获取工具列表来验证会话状态
            tools = self.aws_docs_client.list_tools_sync()
            if not tools:
                return {
                    "error": "AWS文档MCP客户端会话无效：无法获取工具列表",
                    "troubleshooting": [
                        "MCP服务器可能未正确启动",
                        "尝试重启应用程序",
                        "检查MCP服务器进程是否正在运行",
                        "确认网络连接正常"
                    ]
                }
            return None
            
        except Exception as e:
            error_msg = str(e)
            if "client session is not running" in error_msg.lower():
                return {
                    "error": "AWS文档MCP客户端会话未运行",
                    "troubleshooting": [
                        "MCP客户端会话已断开或未正确初始化",
                        "请重启应用程序以重新建立MCP连接",
                        "确认uvx和AWS文档MCP服务器可以正常启动",
                        "检查系统资源是否充足"
                    ]
                }
            else:
                return {
                    "error": f"AWS文档MCP客户端会话检查失败: {error_msg}",
                    "troubleshooting": [
                        "MCP客户端可能处于异常状态",
                        "尝试重启应用程序",
                        "检查系统日志获取详细错误信息",
                        "确认所有依赖项都已正确安装"
                    ]
                }
    
    def _extract_aws_service(self, query: str) -> str:
        """
        从查询中提取AWS服务名称
        
        Args:
            query: 用户查询
            
        Returns:
            str: AWS服务名称
        """
        query_lower = query.lower()
        
        # 常见AWS服务映射
        aws_services = {
            's3': 'S3',
            'lambda': 'Lambda',
            'ec2': 'EC2',
            'rds': 'RDS',
            'dynamodb': 'DynamoDB',
            'cloudfront': 'CloudFront',
            'cloudwatch': 'CloudWatch',
            'iam': 'IAM',
            'vpc': 'VPC',
            'elb': 'ELB',
            'alb': 'ALB',
            'api gateway': 'API Gateway',
            'sns': 'SNS',
            'sqs': 'SQS',
            'kinesis': 'Kinesis',
            'redshift': 'Redshift',
            'elasticsearch': 'Elasticsearch',
            'opensearch': 'OpenSearch'
        }
        
        for service_key, service_name in aws_services.items():
            if service_key in query_lower:
                return service_name
        
        return ""
