"""
Elasticsearch客户端操作模块
提供索引管理和查询功能 - 支持Elasticsearch 6.8版本
"""

import json
import logging
from typing import Dict, List, Any, Optional, Tuple

# 可选导入elasticsearch包
try:
    from elasticsearch import Elasticsearch
    from elasticsearch.exceptions import ElasticsearchException
    ELASTICSEARCH_AVAILABLE = True
except ImportError:
    ELASTICSEARCH_AVAILABLE = False
    # 创建占位符类
    class Elasticsearch:
        pass
    class ElasticsearchException(Exception):
        pass

logger = logging.getLogger(__name__)


class ElasticsearchClient:
    """Elasticsearch客户端类"""
    
    def __init__(self, config_data: Dict[str, Any] = None, 
                 host: str = None,
                 credentials: Tuple[str, str] = None,
                 port: int = 443,
                 use_ssl: bool = True,
                 verify_certs: bool = False,
                 ssl_show_warn: bool = False,
                 http_compress: bool = True,
                 timeout: int = 30):
        """
        初始化Elasticsearch客户端
        
        Args:
            config_data: 配置数据字典，如果提供则使用其中的配置
            host: Elasticsearch域名
            credentials: 用户名密码元组 (username, password)
            port: 端口号，默认443
            use_ssl: 是否使用SSL，默认True
            verify_certs: 是否验证证书，默认False
            http_compress: 是否启用gzip压缩，默认True
            timeout: 超时时间（秒），默认30
        """
        # 检查elasticsearch包是否可用
        if not ELASTICSEARCH_AVAILABLE:
            raise ImportError("elasticsearch包未安装，请运行: pip install -r requirements.txt")
            
        # 如果提供了配置数据，则使用其中的配置
        if config_data:
            self.host = config_data.get('host')
            
            # 确保数值类型是整数，处理可能的Decimal类型
            port = config_data.get('port', 443)
            self.port = int(port) if port is not None else 443
            
            # 根据认证类型设置认证信息
            auth_type = config_data.get('auth_type')
            if auth_type == 'basic':
                self.credentials = (config_data.get('username'), config_data.get('password'))
            elif auth_type == 'api_key':
                self.api_key = config_data.get('api_key')
                self.credentials = None
            elif auth_type == 'aws_sigv4':
                self.aws_region = config_data.get('aws_region')
                self.aws_service = config_data.get('aws_service', 'es')
                self.credentials = None
            else:
                self.credentials = None
            
            # 确保布尔值和数值类型正确
            self.use_ssl = bool(config_data.get('use_ssl', True))
            self.verify_certs = bool(config_data.get('verify_certs', False))
            self.ssl_show_warn = bool(config_data.get('ssl_show_warn', False))
            self.http_compress = bool(config_data.get('http_compress', True))
            
            # 确保timeout是整数
            timeout = config_data.get('timeout', 30)
            self.timeout = int(timeout) if timeout is not None else 30
        else:
            self.host = host
            self.port = port
            self.credentials = credentials
            self.use_ssl = use_ssl
            self.verify_certs = verify_certs
            self.ssl_show_warn = ssl_show_warn
            self.http_compress = http_compress
            self.timeout = timeout
        
        # 初始化Elasticsearch客户端
        try:
            # 确保timeout是整数类型
            timeout_int = int(self.timeout) if self.timeout is not None else 30
            
            # 确保port是整数类型
            port_int = int(self.port) if self.port is not None else 443
            
            # 构建连接配置
            es_config = {
                'hosts': [{'host': self.host, 'port': port_int}],
                'use_ssl': self.use_ssl,
                'verify_certs': self.verify_certs,
                'ssl_show_warn': self.ssl_show_warn,
                'http_compress': self.http_compress,
                'timeout': timeout_int
            }
            
            # 添加认证信息
            if self.credentials:
                es_config['http_auth'] = self.credentials
            
            self.client = Elasticsearch(**es_config)
            
        except Exception as e:
            logger.error(f"初始化Elasticsearch客户端失败: {str(e)}")
            self.client = None
            raise
    
    def test_connection(self) -> bool:
        """
        测试连接
        
        Returns:
            bool: 是否连接成功
        """
        try:
            if self.client is None:
                return False
            
            # 尝试获取集群信息
            info = self.client.info()
            return True
        except Exception as e:
            logger.error(f"测试连接失败: {str(e)}")
            return False
    
    def get_indices_list(self) -> List[Dict[str, Any]]:
        """
        接口1：获取索引列表
        
        Returns:
            List[Dict]: 索引信息列表
        """
        try:
            # 获取所有索引信息
            indices_info = self.client.cat.indices(format='json', v=True)
            
            result = []
            for index in indices_info:
                # 确保索引名称是准确的
                index_name = index.get('index', '')
                if not index_name:
                    # 尝试其他可能的字段名
                    for key in ['index', 'i', 'idx', 'name']:
                        if key in index and index[key]:
                            index_name = index[key]
                            break
                
                # 添加更多索引信息
                index_data = {
                    'index_name': index_name,
                    'docs_count': index.get('docs.count', '0'),
                    'store_size': index.get('store.size', '0'),
                    'health': index.get('health', 'unknown'),
                    'status': index.get('status', 'unknown')
                }
                
                # 只添加有效的索引
                if index_name and not index_name.startswith('.'):  # 排除系统索引
                    result.append(index_data)
            
            return result
            
        except ElasticsearchException as e:
            logger.error(f"获取索引列表失败: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"获取索引列表时发生未知错误: {str(e)}")
            raise
    
    def get_index_mapping(self, index_name: str) -> Dict[str, Any]:
        """
        接口2：获取索引中的字段信息
        
        Args:
            index_name: 索引名称
            
        Returns:
            Dict: 索引字段映射信息
        """
        try:
            # 首先尝试获取所有索引，检查索引是否存在
            try:
                indices_list = self.get_indices_list()
                actual_index_name = None
                
                # 检查索引是否存在（不区分大小写）
                for idx in indices_list:
                    idx_name = idx.get('index_name', '')
                    if idx_name.lower() == index_name.lower():
                        actual_index_name = idx_name
                        break
                
                if actual_index_name:
                    # 如果找到匹配的索引，使用实际的索引名称
                    index_name = actual_index_name
            except Exception as e:
                logger.warning(f"获取索引列表失败，将使用原始索引名称: {str(e)}")
            
            # 获取索引映射信息
            try:
                mapping_response = self.client.indices.get_mapping(index=index_name)
            except Exception as e:
                # 如果获取失败，尝试使用通配符
                logger.warning(f"获取索引 {index_name} 的映射信息失败，尝试使用通配符: {str(e)}")
                try:
                    # 尝试使用通配符匹配索引
                    wildcard_pattern = f"*{index_name}*"
                    mapping_response = self.client.indices.get_mapping(index=wildcard_pattern)
                    
                    # 如果找到匹配的索引，使用第一个
                    if mapping_response:
                        actual_index_name = list(mapping_response.keys())[0]
                        index_name = actual_index_name
                    else:
                        raise ValueError(f"使用通配符 {wildcard_pattern} 未找到匹配的索引")
                except Exception as wild_e:
                    # 如果通配符也失败，尝试使用小写索引名称
                    logger.warning(f"使用通配符获取映射失败，尝试使用小写索引名称: {str(wild_e)}")
                    try:
                        mapping_response = self.client.indices.get_mapping(index=index_name.lower())
                        index_name = index_name.lower()
                    except Exception as lower_e:
                        # 如果小写也失败，尝试使用大写索引名称
                        logger.warning(f"使用小写索引名称获取映射失败，尝试使用大写索引名称: {str(lower_e)}")
                        mapping_response = self.client.indices.get_mapping(index=index_name.upper())
                        index_name = index_name.upper()
            
            # 如果索引名称不在响应中，尝试查找匹配的索引
            actual_index_name = index_name
            if index_name not in mapping_response:
                # 尝试查找可能的匹配（不区分大小写）
                for key in mapping_response.keys():
                    if key.lower() == index_name.lower():
                        actual_index_name = key
                        break
                
                # 如果仍然找不到匹配的索引，则使用第一个索引
                if actual_index_name not in mapping_response and mapping_response:
                    actual_index_name = list(mapping_response.keys())[0]
                    logger.warning(f"未找到精确匹配，使用第一个可用索引: {actual_index_name}")
            
            # 确保索引存在于响应中
            if actual_index_name not in mapping_response:
                raise ValueError(f"索引 {index_name} 不存在于映射响应中")
            
            mapping = mapping_response[actual_index_name]['mappings']
            
            # 解析字段信息
            fields_info = self._parse_mapping_fields(mapping.get('properties', {}))
            
            result = {
                'index_name': index_name,  # 保持原始索引名称
                'actual_index_name': actual_index_name,  # 添加实际索引名称
                'total_fields': len(fields_info),
                'fields': fields_info,  # 使用统一的字段名
                'fields_info': fields_info,  # 添加额外的字段名以兼容不同的代码
                'mapping_meta': mapping.get('_meta', {}),
                'dynamic': mapping.get('dynamic', True)
            }
            
            return result
            
        except ElasticsearchException as e:
            logger.error(f"获取索引 {index_name} 字段信息失败: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"获取索引字段信息时发生未知错误: {str(e)}")
            raise
    
    def _parse_mapping_fields(self, properties: Dict, parent_path: str = '') -> List[Dict[str, Any]]:
        """
        递归解析映射字段
        
        Args:
            properties: 字段属性字典
            parent_path: 父级路径
            
        Returns:
            List[Dict]: 字段信息列表
        """
        fields = []
        
        for field_name, field_config in properties.items():
            current_path = f"{parent_path}.{field_name}" if parent_path else field_name
            
            field_info = {
                'field_name': field_name,
                'field_path': current_path,
                'field_type': field_config.get('type', 'unknown'),
                'analyzer': field_config.get('analyzer', ''),
                'index': field_config.get('index', True),
                'store': field_config.get('store', False),
                'doc_values': field_config.get('doc_values', True),
                'format': field_config.get('format', ''),
                'null_value': field_config.get('null_value', ''),
                'boost': field_config.get('boost', 1.0)
            }
            
            fields.append(field_info)
            
            # 递归处理嵌套字段
            if 'properties' in field_config:
                nested_fields = self._parse_mapping_fields(
                    field_config['properties'], 
                    current_path
                )
                fields.extend(nested_fields)
        
        return fields
    
    def execute_search(self, 
                      index_name: str, 
                      query: Dict[str, Any], 
                      source: Optional[List[str]] = None,
                      output_format: str = 'simplified',
                      required_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        执行索引查询，统一返回格式
        
        Args:
            index_name: 索引名称
            query: 查询DSL
            source: 返回字段列表
            output_format: 输出格式 ('simplified', 'standard', 'raw')
            required_fields: 字段列表，如果指定则只返回这些字段
            
        Returns:
            Dict: 统一简化格式的查询结果
        """
        try:
            # 构建搜索请求体
            search_body = query.copy()
            if source:
                search_body['_source'] = source
            elif required_fields:
                search_body['_source'] = required_fields
            
            # 执行搜索
            response = self.client.search(
                index=index_name,
                body=search_body
            )
            
            return response
            
        except ElasticsearchException as e:
            logger.error(f"查询索引 {index_name} 失败: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"执行查询时发生未知错误: {str(e)}")
            raise
    
    def execute_aggregation(self, 
                           index_name: str, 
                           aggs: Dict[str, Any],
                           query: Optional[Dict[str, Any]] = None,
                           size: int = 0) -> Dict[str, Any]:
        """
        执行聚合查询
        
        Args:
            index_name: 索引名称
            aggs: 聚合查询DSL
            query: 过滤查询（可选）
            size: 返回文档数量（聚合查询通常设为0）
            
        Returns:
            Dict: 聚合结果
        """
        try:
            search_body = {
                'aggs': aggs,
                'size': size
            }
            
            if query:
                search_body['query'] = query
            
            response = self.client.search(
                index=index_name,
                body=search_body
            )
            
            result = {
                'total_hits': response['hits']['total']['value'],
                'took': response['took'],
                'aggregations': response.get('aggregations', {})
            }
            
            return result
            
        except ElasticsearchException as e:
            logger.error(f"聚合查询索引 {index_name} 失败: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"执行聚合查询时发生未知错误: {str(e)}")
            raise
    
    def get_index_stats(self, index_name: str) -> Dict[str, Any]:
        """
        获取索引统计信息
        
        Args:
            index_name: 索引名称
            
        Returns:
            Dict: 索引统计信息
        """
        try:
            stats_response = self.client.indices.stats(index=index_name)
            
            if index_name in stats_response['indices']:
                index_stats = stats_response['indices'][index_name]
                
                result = {
                    'index_name': index_name,
                    'total_docs': index_stats['total']['docs']['count'],
                    'deleted_docs': index_stats['total']['docs']['deleted'],
                    'store_size_bytes': index_stats['total']['store']['size_in_bytes'],
                    'segments_count': index_stats['total']['segments']['count'],
                    'segments_memory_bytes': index_stats['total']['segments']['memory_in_bytes']
                }
                
                return result
            else:
                raise ValueError(f"索引 {index_name} 统计信息不存在")
                
        except ElasticsearchException as e:
            logger.error(f"获取索引 {index_name} 统计信息失败: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"获取索引统计信息时发生未知错误: {str(e)}")
            raise


# 使用示例
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO)
    
    # 初始化客户端
    client = ElasticsearchClient(
        host='your-elasticsearch-host.com',
        credentials=('username', 'password')
    )
    
    # 测试连接
    if client.test_connection():
        # 获取索引列表
        indices = client.get_indices_list()
        print(f"找到 {len(indices)} 个索引")
        
        # 如果有索引，获取第一个索引的字段信息
        if indices:
            first_index = indices[0]['index_name']
            mapping = client.get_index_mapping(first_index)
            print(f"索引 {first_index} 有 {mapping['total_fields']} 个字段")
