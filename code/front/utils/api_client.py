"""
API客户端模块
用于直接调用后端代码
"""

import os
import json
import sys
from typing import Dict, List, Any, Optional
import logging
from pathlib import Path
from dotenv import load_dotenv
import time
import uuid
import functools
from datetime import datetime, timedelta

# 添加server目录到Python路径以导入模型配置
server_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'server')
if server_path not in sys.path:
    sys.path.append(server_path)

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 缓存装饰器
def cache_result(ttl_seconds=300):
    """
    缓存函数结果的装饰器
    
    Args:
        ttl_seconds: 缓存有效期（秒）
    """
    def decorator(func):
        cache = {}
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存键
            key = str(args) + str(sorted(kwargs.items()))
            
            # 检查缓存是否存在且有效
            if key in cache:
                result, timestamp = cache[key]
                if datetime.now() - timestamp < timedelta(seconds=ttl_seconds):
                    logger.debug(f"从缓存获取结果: {func.__name__}")
                    return result
            
            # 调用原始函数
            result = func(*args, **kwargs)
            
            # 缓存结果
            cache[key] = (result, datetime.now())
            
            return result
        
        # 添加清除缓存的方法
        wrapper.clear_cache = lambda: cache.clear()
        
        return wrapper
    
    return decorator

# 加载环境变量
env_path = Path(__file__).parent.parent.parent / "server" / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
    logger.info(f"已加载环境变量: {env_path}")

# 添加后端代码路径
server_path = str(Path(__file__).parent.parent.parent / "server")
if server_path not in sys.path:
    sys.path.append(server_path)
    logger.info(f"已添加后端代码路径: {server_path}")

# 导入后端模块
try:
    from dynamodb_client import SearchEngineConfigClient, DynamoDBClient, DSLQueryClient
    from config import config
    logger.info("成功导入后端模块")
except ImportError as e:
    logger.error(f"导入后端模块失败: {str(e)}")
    raise

class APIClient:
    """API客户端类，用于直接调用后端代码"""
    
    def __init__(self):
        """
        初始化API客户端
        """
        # 初始化后端客户端
        try:
            logger.info(f"开始初始化后端客户端，区域: {config.DYNAMODB_REGION}")
            logger.info(f"数据源表: {config.DYNAMODB_DATASOURCE_TABLE}")
            logger.info(f"元数据表: {config.DYNAMODB_METADATA_TABLE}")
            logger.info(f"DSL查询表: {config.DYNAMODB_DSL_TABLE}")
            
            # 初始化配置客户端
            self.config_client = SearchEngineConfigClient(
                region=config.DYNAMODB_REGION,
                table_name=config.DYNAMODB_DATASOURCE_TABLE
            )
            logger.info("成功初始化配置客户端")
            
            # 初始化字段客户端
            self.field_client = DynamoDBClient(
                region=config.DYNAMODB_REGION,
                table_name=config.DYNAMODB_METADATA_TABLE
            )
            logger.info("成功初始化字段客户端")
            
            # 初始化查询客户端
            self.query_client = DSLQueryClient(
                region=config.DYNAMODB_REGION,
                table_name=config.DYNAMODB_DSL_TABLE
            )
            logger.info("成功初始化查询客户端")
            
            # 确保表存在
            logger.info("开始创建表（如果不存在）")
            self.config_client.create_table_if_not_exists()
            self.field_client.create_table_if_not_exists()
            self.query_client.create_table_if_not_exists()
            
            logger.info("成功初始化后端客户端")
        except Exception as e:
            logger.error(f"初始化后端客户端失败: {str(e)}")
            # 在开发环境中，我们可以使用空的客户端
            self.config_client = None
            self.field_client = None
            self.query_client = None
            raise
            
        # 我们不再需要启动本地服务器，因为我们直接调用后端代码
    
    def _handle_error(self, e: Exception) -> Dict:
        """
        处理异常并返回错误信息
        
        Args:
            e: 异常对象
            
        Returns:
            Dict: 包含错误信息的字典
        """
        logger.error(f"操作失败: {str(e)}")
        return {"error": str(e)}
    
    # 数据源配置相关方法
    @cache_result(ttl_seconds=60)  # 缓存60秒
    def list_search_engine_configs(self) -> List[Dict[str, Any]]:
        """
        获取所有搜索引擎配置
        
        Returns:
            List[Dict]: 配置列表，如果发生错误，返回包含error键的字典
        """
        try:
            # 检查客户端是否初始化成功
            if self.config_client is None:
                logger.error("配置客户端未初始化")
                return []
                
            # 直接调用后端代码
            logger.info("开始获取搜索引擎配置列表")
            configs = self.config_client.list_search_engine_configs()
            logger.info(f"获取到 {len(configs)} 个配置")
            return configs
        except Exception as e:
            logger.error(f"获取搜索引擎配置列表失败: {str(e)}")
            return []
    
    @cache_result(ttl_seconds=60)  # 缓存60秒
    def get_search_engine_config(self, config_id: str) -> Optional[Dict[str, Any]]:
        """
        获取特定搜索引擎配置
        
        Args:
            config_id: 配置ID
            
        Returns:
            Optional[Dict]: 配置信息
        """
        try:
            # 直接调用后端代码
            config = self.config_client.get_search_engine_config(config_id)
            return config
        except Exception as e:
            logger.error(f"获取搜索引擎配置失败: {str(e)}")
            return None
    
    def save_search_engine_config(self, config_data: Dict[str, Any], config_id: str = None) -> Optional[str]:
        """
        保存搜索引擎配置
        
        Args:
            config_data: 配置数据
            config_id: 配置ID（可选）
            
        Returns:
            Optional[str]: 配置ID
        """
        try:
            # 直接调用后端代码
            result = self.config_client.save_search_engine_config(config_data, config_id)
            
            # 清除相关缓存
            self.list_search_engine_configs.clear_cache()
            if config_id:
                self.get_search_engine_config.clear_cache()
            
            return result
        except Exception as e:
            logger.error(f"保存搜索引擎配置失败: {str(e)}")
            return None
    
    def delete_search_engine_config(self, config_id: str) -> bool:
        """
        删除搜索引擎配置
        
        Args:
            config_id: 配置ID
            
        Returns:
            bool: 是否删除成功
        """
        try:
            # 直接调用后端代码
            result = self.config_client.delete_search_engine_config(config_id)
            
            # 清除相关缓存
            self.list_search_engine_configs.clear_cache()
            self.get_search_engine_config.clear_cache()
            
            return result
        except Exception as e:
            logger.error(f"删除搜索引擎配置失败: {str(e)}")
            return False
    
    def test_search_engine_connection(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        测试搜索引擎连接
        
        Args:
            config_data: 配置数据
            
        Returns:
            Dict: 测试结果，包含 success 和 message 字段
        """
        try:
            # 导入 OpenSearch 客户端
            from opensearch_client import OpenSearchClient
            
            # 创建临时客户端进行测试
            client = OpenSearchClient(config_data)
            
            # 测试连接
            result = client.test_connection()
            if isinstance(result, bool):
                if result:
                    return {"success": True, "message": "连接成功"}
                else:
                    return {"success": False, "message": "连接失败"}
            elif isinstance(result, dict):
                return result
            else:
                return {"success": False, "message": f"连接测试返回了意外的结果类型: {type(result)}"}
        except Exception as e:
            logger.error(f"测试搜索引擎连接失败: {str(e)}")
            return {"success": False, "message": f"连接失败: {str(e)}"}
    
    # 索引字段管理相关方法
    @cache_result(ttl_seconds=300)  # 缓存5分钟
    def get_indices(self, config_id: str) -> List[str]:
        """
        获取索引列表
        
        Args:
            config_id: 配置ID
            
        Returns:
            List[str]: 索引列表
        """
        try:
            # 获取配置信息
            config = self.config_client.get_search_engine_config(config_id)
            if not config:
                return []
            
            # 导入 OpenSearch 客户端
            from opensearch_client import OpenSearchClient
            
            # 创建客户端
            client = OpenSearchClient(config)
            
            # 获取索引列表
            indices_info = client.get_indices_list()
            
            # 提取索引名称
            indices = [index_data['index_name'] for index_data in indices_info]
            
            return indices
        except Exception as e:
            logger.error(f"获取索引列表失败: {str(e)}")
            return []
    
    @cache_result(ttl_seconds=300)  # 缓存5分钟
    def get_index_fields(self, config_id: str, selected_index: str, query_index_name: str = None) -> List[Dict[str, Any]]:
        """
        获取索引字段信息
        
        Args:
            config_id: 配置ID
            selected_index: 选择的索引名称（用于从OpenSearch获取）
            query_index_name: 查询索引名称（用于从DynamoDB获取）
            
        Returns:
            List[Dict]: 字段信息列表
        """
        try:
            # 如果提供了查询索引名称，从DynamoDB获取字段信息
            if query_index_name:
                logger.info(f"尝试使用查询索引名称 {query_index_name} 从DynamoDB获取字段信息")
                fields = self.field_client.get_index_fields(query_index_name)
                if fields:
                    logger.info(f"成功从DynamoDB获取到查询索引 {query_index_name} 的字段信息")
                    return fields
                else:
                    logger.info(f"DynamoDB中没有找到查询索引 {query_index_name} 的字段信息")
                    return []
            
            # 如果提供了索引名称和配置ID，从OpenSearch获取字段信息
            elif selected_index and config_id:
                logger.info(f"尝试从OpenSearch获取索引 {selected_index} 的字段信息")
                
                # 获取配置信息
                config = self.config_client.get_search_engine_config(config_id)
                if not config:
                    logger.error(f"未找到配置ID {config_id} 的配置信息")
                    return []
                
                # 导入 OpenSearch 客户端
                from opensearch_client import OpenSearchClient
                
                # 创建客户端
                client = OpenSearchClient(config)
                
                # 获取字段信息
                fields_info = client.get_index_mapping(selected_index)
                
                # 转换为标准格式
                fields = []
                if 'fields' in fields_info and isinstance(fields_info['fields'], list):
                    for field_info in fields_info['fields']:
                        fields.append({
                            "index_name": selected_index,  # 使用选择的索引名称
                            "field_name": field_info.get('field_name', '') or field_info.get('field_path', '').split('.')[-1],
                            "field_type": field_info.get("field_type", "unknown"),
                            "description": "",
                        })
                
                logger.info(f"成功从OpenSearch获取索引 {selected_index} 的字段信息")
                return fields
            
            # 如果没有提供查询索引名称和索引名称，返回空列表
            else:
                logger.warning("未提供查询索引名称或索引名称，无法获取字段信息")
                return []
        except Exception as e:
            logger.error(f"获取索引字段信息失败: {str(e)}")
            return []
    
    def update_field_description(self, index_name: str, field_name: str, description_data: Dict[str, str]) -> bool:
        """
        更新字段描述信息
        
        Args:
            index_name: 索引名称
            field_name: 字段名称
            description_data: 描述信息，包含description
            
        Returns:
            bool: 是否更新成功
        """
        try:
            result = self.field_client.update_field_description(
                index_name,
                field_name,
                description_data.get("description", "")
            )
            
            # 清除相关缓存
            self.get_index_fields.clear_cache()
            
            return result
        except Exception as e:
            logger.error(f"更新字段描述信息失败: {str(e)}")
            return False
    
    def batch_update_descriptions(self, updates: List[Dict[str, Any]]) -> int:
        """
        批量更新字段描述
        
        Args:
            updates: 更新信息列表
            
        Returns:
            int: 成功更新的字段数量
        """
        try:
            return self.field_client.batch_update_descriptions(updates)
        except Exception as e:
            logger.error(f"批量更新字段描述失败: {str(e)}")
            return 0
    
    # DSL查询管理相关方法
    def save_dsl_query(self, query_data: Dict[str, Any], query_id: str = None) -> Optional[str]:
        """
        保存DSL查询示例
        
        Args:
            query_data: 查询数据，包含data_source_id, description, dsl_query, tags等字段
                        以及log_field_metadata_index_name字段
            query_id: 查询ID（可选）
            
        Returns:
            Optional[str]: 查询ID
        """
        try:
            # 确保query_data中包含log_field_metadata_index_name字段
            if 'log_field_metadata_index_name' not in query_data and 'data_source_id' in query_data:
                query_data['log_field_metadata_index_name'] = query_data['data_source_id']
            
            # 提取参数并调用后端方法
            result = self.query_client.save_dsl_query(
                data_source_id=query_data.get('data_source_id', ''),
                description=query_data.get('description', ''),
                dsl_query=query_data.get('dsl_query', ''),
                query_id=query_id,
                tags=query_data.get('tags', []),
                category=query_data.get('name', ''),  # 使用name作为category
                log_field_metadata_index_name=query_data.get('log_field_metadata_index_name', '')
            )
            
            # 清除相关缓存
            self.list_dsl_queries.clear_cache()
            
            return result
        except Exception as e:
            logger.error(f"保存DSL查询示例失败: {str(e)}")
            return None
    
    def get_dsl_query(self, query_id: str) -> Optional[Dict[str, Any]]:
        """
        获取DSL查询示例
        
        Args:
            query_id: 查询ID
            
        Returns:
            Optional[Dict]: 查询信息
        """
        try:
            return self.query_client.get_dsl_query(query_id)
        except Exception as e:
            logger.error(f"获取DSL查询示例失败: {str(e)}")
            return None
    
    @cache_result(ttl_seconds=60)  # 缓存60秒
    def list_dsl_queries(self, config_id: str = None, index_name: str = None) -> List[Dict[str, Any]]:
        """
        获取DSL查询示例列表
        
        Args:
            config_id: 配置ID（可选）
            index_name: 索引名称（可选，用作log_field_metadata_index_name）
            
        Returns:
            List[Dict]: 查询示例列表
        """
        try:
            # 将index_name作为log_field_metadata_index_name传递
            return self.query_client.list_dsl_queries(data_source_id=config_id, log_field_metadata_index_name=index_name)
        except Exception as e:
            logger.error(f"获取DSL查询示例列表失败: {str(e)}")
            return []
    
    def delete_dsl_query(self, query_id: str) -> bool:
        """
        删除DSL查询示例
        
        Args:
            query_id: 查询ID
            
        Returns:
            bool: 是否删除成功
        """
        try:
            result = self.query_client.delete_dsl_query(query_id)
            
            # 清除相关缓存
            self.list_dsl_queries.clear_cache()
            
            return result
        except Exception as e:
            logger.error(f"删除DSL查询示例失败: {str(e)}")
            return False
    
    def execute_dsl_query(self, config_id: str, index_name: str, query_dsl: str) -> Dict[str, Any]:
        """
        执行DSL查询
        
        Args:
            config_id: 配置ID
            index_name: 索引名称
            query_dsl: DSL查询语句
            
        Returns:
            Dict: 查询结果
        """
        try:
            # 获取配置信息
            config = self.config_client.get_search_engine_config(config_id)
            if not config:
                return {"error": "配置不存在"}
            
            # 导入 OpenSearch 客户端
            from opensearch_client import OpenSearchClient
            
            # 创建客户端
            client = OpenSearchClient(config)
            
            # 执行查询
            return client.execute_query(index_name, query_dsl)
        except Exception as e:
            logger.error(f"执行DSL查询失败: {str(e)}")
            return {"error": str(e)}
    # 索引信息相关方法
    def save_index_info(self, index_name: str, description: str) -> bool:
        """
        保存索引信息
        
        Args:
            index_name: 索引名称
            description: 索引描述
            
        Returns:
            bool: 是否保存成功
        """
        try:
            current_time = datetime.utcnow().isoformat()
            
            # 获取当前索引的字段信息
            current_fields = self.field_client.get_index_fields(index_name)
            
            # 获取当前索引元数据
            response = self.field_client.table.get_item(
                Key={
                    'index_name': index_name
                }
            )
            
            if 'Item' in response:
                # 索引存在，更新索引描述
                item = response['Item']
                version = item.get('version', 0) + 1
                
                # 执行更新
                self.field_client.table.update_item(
                    Key={
                        'index_name': index_name
                    },
                    UpdateExpression="SET index_description = :index_description, updated_at = :updated_at, version = :version",
                    ExpressionAttributeValues={
                        ':index_description': description,
                        ':updated_at': current_time,
                        ':version': version
                    }
                )
                
                logger.info(f"更新索引 {index_name} 的描述: {description}")
                return True
            else:
                # 索引不存在，创建新记录
                item = {
                    'index_name': index_name,
                    'description': {},  # 空字段描述
                    'index_description': description,  # 索引描述
                    'created_at': current_time,
                    'updated_at': current_time,
                    'version': 1
                }
                
                # 保存到DynamoDB
                self.field_client.table.put_item(Item=item)
                
                logger.info(f"创建索引 {index_name} 的新记录，包含索引描述: {description}")
                return True
            
        except Exception as e:
            logger.error(f"保存索引信息失败: {str(e)}")
            return False
    
    @cache_result(ttl_seconds=300)  # 缓存5分钟
    def get_index_info(self, index_name: str) -> Optional[Dict[str, Any]]:
        """
        获取索引信息
        
        Args:
            index_name: 索引名称
            
        Returns:
            Optional[Dict]: 索引信息
        """
        try:
            # 从log_field_metadata表中获取索引信息
            metadata = self.get_index_metadata(index_name)
            if metadata:
                index_info = {
                    "index_name": index_name,
                    "description": metadata.get("index_description", ""),
                    "created_at": metadata.get("created_at", ""),
                    "updated_at": metadata.get("updated_at", ""),
                    "version": metadata.get("version", 1)
                }
                logger.info(f"获取到索引 {index_name} 的信息: {index_info}")
                return index_info
            return None
        except Exception as e:
            logger.error(f"获取索引信息失败: {str(e)}")
            return None
    
    @cache_result(ttl_seconds=300)  # 缓存5分钟
    def get_index_metadata(self, index_name: str) -> Optional[Dict[str, Any]]:
        """
        获取索引元数据
        
        Args:
            index_name: 索引名称
            
        Returns:
            Optional[Dict]: 索引元数据
        """
        try:
            # 从DynamoDB获取索引元数据
            response = self.field_client.table.get_item(
                Key={
                    'index_name': index_name
                }
            )
            
            if 'Item' in response:
                item = response['Item']
                # 提取元数据字段
                metadata = {
                    "index_name": index_name,
                    "index_description": item.get("index_description", ""),
                    "created_at": item.get("created_at", ""),
                    "updated_at": item.get("updated_at", ""),
                    "version": item.get("version", 1)
                }
                logger.info(f"获取到索引 {index_name} 的元数据: {metadata}")
                return metadata
            logger.warning(f"未找到索引 {index_name} 的元数据")
            return None
        except Exception as e:
            logger.error(f"获取索引元数据失败: {str(e)}")
            return None
    
    @cache_result(ttl_seconds=300)  # 缓存5分钟
    def list_index_info(self) -> List[Dict[str, Any]]:
        """
        获取所有索引信息
        
        Returns:
            List[Dict]: 索引信息列表
        """
        try:
            # 获取所有索引
            indices = self.get_all_indices()
            
            # 获取每个索引的详细信息
            result = []
            for index_name in indices:
                index_info = self.get_index_info(index_name)
                if index_info:
                    result.append(index_info)
            
            return result
        except Exception as e:
            logger.error(f"获取索引信息列表失败: {str(e)}")
            return []
            
    @cache_result(ttl_seconds=300)  # 缓存5分钟
    def get_all_indices(self) -> List[str]:
        """
        获取所有索引名称
        
        Returns:
            List[str]: 索引名称列表
        """
        try:
            # 从log_field_metadata表中获取所有索引名称
            return self.field_client.get_all_indices()
        except Exception as e:
            logger.error(f"获取索引名称列表失败: {str(e)}")
            return []
    
    @cache_result(ttl_seconds=300)  # 缓存5分钟
    def get_all_query_index_names(self) -> List[str]:
        """
        获取所有查询索引名称
        
        Returns:
            List[str]: 查询索引名称列表
        """
        try:
            # 从log_field_metadata表中获取所有索引名称
            return self.get_all_indices()
        except Exception as e:
            logger.error(f"获取查询索引名称列表失败: {str(e)}")
            return []
            
    def save_index_with_fields(self, index_name: str, index_description: str, fields: List[Dict[str, Any]]) -> bool:
        """
        保存索引信息和字段到log_field_metadata表
        
        Args:
            index_name: 查询索引名称
            index_description: 索引描述
            fields: 字段信息列表
            
        Returns:
            bool: 是否保存成功
        """
        try:
            current_time = datetime.utcnow().isoformat()
            
            # 确保使用查询索引名称
            query_index_name = index_name
            
            # 构建字段描述字典
            field_descriptions = {}
            for field in fields:
                field_name = field.get('field_name', '')
                field_type = field.get('field_type', 'unknown')
                description = field.get('description', '')
                
                if field_name:
                    field_descriptions[field_name] = {
                        'type': field_type,
                        'description': description
                    }
            
            # 检查索引是否已存在
            response = self.field_client.table.get_item(
                Key={
                    'index_name': query_index_name
                }
            )
            
            if 'Item' in response:
                # 索引存在，更新字段描述和索引描述
                # 执行更新
                self.field_client.table.update_item(
                    Key={
                        'index_name': query_index_name
                    },
                    UpdateExpression="SET description = :description, index_description = :index_description, updated_at = :updated_at, version = version + :inc",
                    ExpressionAttributeValues={
                        ':description': field_descriptions,
                        ':index_description': index_description,
                        ':updated_at': current_time,
                        ':inc': 1
                    }
                )
                logger.info(f"更新查询索引 {query_index_name} 的信息，描述: {index_description}")
            else:
                # 索引不存在，创建新记录
                item = {
                    'index_name': query_index_name,
                    'description': field_descriptions,  # 字段描述
                    'index_description': index_description,  # 索引描述
                    'created_at': current_time,
                    'updated_at': current_time,
                    'version': 1
                }
                
                # 保存到DynamoDB
                self.field_client.table.put_item(Item=item)
                logger.info(f"创建新查询索引 {query_index_name} 的记录，描述: {index_description}")
            
            # 清除相关缓存
            self.get_index_fields.clear_cache()
            self.get_index_info.clear_cache()
            self.get_index_metadata.clear_cache()
            self.list_index_info.clear_cache()
            self.get_all_indices.clear_cache()
            self.get_all_query_index_names.clear_cache()
            
            logger.info(f"成功保存查询索引 {query_index_name} 的信息和字段")
            return True
            
        except Exception as e:
            logger.error(f"保存索引信息和字段失败: {str(e)}")
            return False
            
    def batch_update_field_descriptions(self, index_name: str, field_descriptions: Dict[str, str], index_description: str = None, config_id: str = None) -> bool:
        """
        批量更新字段描述
        
        Args:
            index_name: 索引名称
            field_descriptions: 字段描述字典，键为字段名称，值为描述
            index_description: 索引描述（可选）
            config_id: 配置ID（可选）
            
        Returns:
            bool: 是否更新成功
        """
        try:
            # 获取当前索引的字段信息
            current_fields = self.field_client.get_index_fields(index_name)
            
            # 构建字段类型映射
            field_types = {}
            for field in current_fields:
                field_name = field.get('field_name', '')
                field_type = field.get('field_type', 'unknown')
                if field_name:
                    field_types[field_name] = field_type
            
            # 获取当前索引元数据
            response = self.field_client.table.get_item(
                Key={
                    'index_name': index_name
                }
            )
            
            current_time = datetime.utcnow().isoformat()
            
            if 'Item' in response:
                item = response['Item']
                # 完全替换字段描述，而不是增量更新
                new_descriptions = {}
                index_description = item.get('index_description', '') if index_description is None else index_description
                created_at = item.get('created_at', current_time)
                version = item.get('version', 0) + 1
                
                # 构建新的字段描述字典
                for field_name, description in field_descriptions.items():
                    field_type = field_types.get(field_name, 'unknown')
                    new_descriptions[field_name] = {
                        'type': field_type,
                        'description': description
                    }
                
                # 准备更新表达式和表达式属性值
                update_expression = "SET description = :description, updated_at = :updated_at, version = :version"
                expression_values = {
                    ':description': new_descriptions,
                    ':updated_at': current_time,
                    ':version': version
                }
                
                # 如果提供了索引描述，则添加到更新表达式中
                if index_description is not None:
                    update_expression += ", index_description = :index_description"
                    expression_values[':index_description'] = index_description
                
                # 如果提供了配置ID，则添加到更新表达式中
                if config_id:
                    update_expression += ", config_id = :config_id"
                    expression_values[':config_id'] = config_id
                
                # 执行更新
                self.field_client.table.update_item(
                    Key={
                        'index_name': index_name
                    },
                    UpdateExpression=update_expression,
                    ExpressionAttributeValues=expression_values
                )
                
                logger.info(f"更新索引 {index_name} 的字段描述，保留索引描述: {index_description}")
            else:
                # 创建新记录
                field_descriptions_dict = {}
                for field_name, description in field_descriptions.items():
                    field_type = field_types.get(field_name, 'unknown')
                    field_descriptions_dict[field_name] = {
                        'type': field_type,
                        'description': description
                    }
                
                # 保存到DynamoDB
                item = {
                    'index_name': index_name,
                    'description': field_descriptions_dict,
                    'index_description': '' if index_description is None else index_description,  # 使用提供的索引描述或默认空字符串
                    'created_at': current_time,
                    'updated_at': current_time,
                    'version': 1
                }
                
                # 如果提供了配置ID，则添加到记录中
                if config_id:
                    item['config_id'] = config_id
                self.field_client.table.put_item(Item=item)
                
                logger.info(f"创建索引 {index_name} 的新记录，包含字段描述")
            
            # 清除相关缓存
            self.get_index_fields.clear_cache()
            self.get_index_info.clear_cache()
            self.get_index_metadata.clear_cache()
            self.list_index_info.clear_cache()
            self.get_all_indices.clear_cache()
            self.get_all_query_index_names.clear_cache()
            
            return True
        except Exception as e:
            logger.error(f"批量更新字段描述失败: {str(e)}")
            return False
    
    def delete_query_index(self, index_name: str) -> bool:
        """
        删除查询索引及其所有字段信息
        
        Args:
            index_name: 查询索引名称
            
        Returns:
            bool: 是否删除成功
        """
        try:
            # 调用DynamoDB客户端的删除方法
            result = self.field_client.delete_index_fields(index_name)
            
            if result:
                # 清除相关缓存
                self.get_index_fields.clear_cache()
                self.get_index_info.clear_cache()
                self.get_index_metadata.clear_cache()
                self.list_index_info.clear_cache()
                self.get_all_indices.clear_cache()
                self.get_all_query_index_names.clear_cache()
                
                logger.info(f"成功删除查询索引 {index_name}")
                return True
            else:
                logger.error(f"删除查询索引 {index_name} 失败")
                return False
        except Exception as e:
            logger.error(f"删除查询索引失败: {str(e)}")
            return False
    
    # 自然语言搜索相关方法
    def natural_language_search(self, config_id: str, index_name: str, query: str, size: int = 100) -> Dict[str, Any]:
        """
        执行自然语言搜索
        
        Args:
            config_id: 配置ID
            index_name: 索引名称
            query: 自然语言查询
            size: 返回结果数量
            
        Returns:
            Dict: 搜索结果
        """
        try:
            # 获取配置信息
            config = self.config_client.get_search_engine_config(config_id)
            if not config:
                return {"error": "配置不存在"}
            
            # 导入自然语言搜索模块
            from natural_language_search import NaturalLanguageSearchEngine
            
            # 创建OpenSearch客户端
            from opensearch_client import OpenSearchClient
            opensearch_client = OpenSearchClient(config)
            
            # 创建自然语言搜索引擎
            search_engine = NaturalLanguageSearchEngine(
                opensearch_client=opensearch_client
            )
            
            # 执行自然语言搜索
            result = search_engine.natural_language_search(
                index_name=index_name,
                natural_query=query,
                execute_query=True,
                size=size
            )
            
            return result
        except Exception as e:
            logger.error(f"执行自然语言搜索失败: {str(e)}")
            return {"error": str(e)}
    
    def get_chart_recommendation(self, query_results: Dict[str, Any], user_query: str) -> Dict[str, Any]:
        """
        获取图表推荐
        
        Args:
            query_results: 查询结果
            user_query: 用户查询
            
        Returns:
            Dict: 图表推荐结果
        """
        try:
            # 导入Bedrock客户端和任务类型
            from bedrock_client import BedrockClient
            from model_config import TaskType
            bedrock_client = BedrockClient()
            
            # 构建提示词
            prompt = f"""
            用户查询: {user_query}
            
            查询结果: {json.dumps(query_results, indent=2, ensure_ascii=False)}
            
            请分析这些查询结果，并推荐最合适的图表类型来可视化数据。返回JSON格式，包含:
            - chart_type: 图表类型 (bar, line, pie, scatter等)
            - title: 图表标题
            - x_axis: X轴数据字段 (如适用)
            - y_axis: Y轴数据字段 (如适用)
            - values: 值字段 (如适用于饼图)
            - names: 名称字段 (如适用于饼图)
            - reasoning: 选择该图表类型的理由
            """
            
            # 调用Claude
            response = bedrock_client.invoke_claude(
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                system_prompt="你是一个数据可视化专家，擅长从查询结果中推荐最合适的图表类型。请分析数据特征，考虑数据类型、分布和用户意图，提供最佳可视化建议。",
                task_type=TaskType.CHART_GENERATION
            )
            
            if response["success"]:
                try:
                    # 解析JSON
                    chart_recommendation = json.loads(response["content"])
                    
                    return {
                        "success": True,
                        "recommendation": chart_recommendation,
                        "usage": response["usage"]
                    }
                except json.JSONDecodeError as e:
                    logger.error(f"解析图表推荐JSON失败: {str(e)}")
                    return {
                        "success": False,
                        "error": f"JSON解析失败: {str(e)}",
                        "raw_response": response["content"]
                    }
            else:
                return {
                    "success": False,
                    "error": response["error"]
                }
        except Exception as e:
            logger.error(f"获取图表推荐失败: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def analyze_log_data(self, query_results: Dict[str, Any], user_query: str) -> Dict[str, Any]:
        """
        分析日志数据
        
        Args:
            query_results: 查询结果
            user_query: 用户查询
            
        Returns:
            Dict: 分析结果
        """
        try:
            # 导入Bedrock客户端和任务类型
            from bedrock_client import BedrockClient
            from model_config import TaskType
            bedrock_client = BedrockClient()
            
            # 构建提示词
            prompt = f"""
            用户查询: {user_query}
            
            查询结果: {json.dumps(query_results, indent=2, ensure_ascii=False)}
            
            请分析这些日志数据，并提供以下内容:
            1. 总体分析和见解
            2. 可能的问题原因或解决方案
            3. 建议的下一步行动
            
            返回JSON格式，包含:
            - analysis: 分析结果文本
            - insights: 关键见解列表
            - next_steps: 建议的下一步行动列表
            """
            
            # 调用Claude
            response = bedrock_client.invoke_claude(
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                system_prompt="你是一个日志分析专家，擅长从日志数据中提取见解和发现问题。请提供详细的分析和建议，以帮助用户理解日志数据并采取适当的行动。",
                task_type=TaskType.ANALYSIS_REPORT
            )
            
            if response["success"]:
                try:
                    # 解析JSON
                    analysis_result = json.loads(response["content"])
                    
                    return {
                        "success": True,
                        "analysis": analysis_result,
                        "usage": response["usage"]
                    }
                except json.JSONDecodeError as e:
                    logger.error(f"解析分析结果JSON失败: {str(e)}")
                    return {
                        "success": False,
                        "error": f"JSON解析失败: {str(e)}",
                        "raw_response": response["content"]
                    }
            else:
                return {
                    "success": False,
                    "error": response["error"]
                }
        except Exception as e:
            logger.error(f"分析日志数据失败: {str(e)}")
            return {"success": False, "error": str(e)}
    
    # Strands Agent相关方法
    def analyze_query_intent(self, query: str, agent_id: str = None, agent_alias_id: str = None) -> Dict[str, Any]:
        """
        分析查询意图
        
        Args:
            query: 用户查询
            agent_id: Agent ID（可选）
            agent_alias_id: Agent别名ID（可选）
            
        Returns:
            Dict: 分析结果
        """
        try:
            # 导入后端模块
            from strands_agent import StrandsAgent
            
            # 创建Strands Agent客户端
            agent = StrandsAgent()
            
            # 如果提供了Agent配置，设置它
            if agent_id and agent_alias_id:
                agent.set_agent_config(agent_id, agent_alias_id)
            
            # 分析查询意图
            result = agent.analyze_query_intent(query)
            
            return result
        except Exception as e:
            logger.error(f"分析查询意图失败: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "is_log_query": False,
                "query_type": "unknown",
                "confidence": 0
            }
    
    def execute_multi_step_task(self, query: str, context: Dict[str, Any] = None, 
                               agent_id: str = None, agent_alias_id: str = None) -> Dict[str, Any]:
        """
        执行多步骤任务
        
        Args:
            query: 用户查询
            context: 上下文信息（可选）
            agent_id: Agent ID（可选）
            agent_alias_id: Agent别名ID（可选）
            
        Returns:
            Dict: 执行结果
        """
        try:
            # 导入后端模块
            from strands_agent import StrandsAgent
            
            # 创建Strands Agent客户端
            agent = StrandsAgent()
            
            # 如果提供了Agent配置，设置它
            if agent_id and agent_alias_id:
                agent.set_agent_config(agent_id, agent_alias_id)
            
            # 执行多步骤任务
            result = agent.execute_multi_step_task(query, context)
            
            return result
        except Exception as e:
            logger.error(f"执行多步骤任务失败: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "result": None
            }
