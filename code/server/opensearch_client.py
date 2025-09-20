"""
OpenSearch客户端操作模块
提供索引管理和查询功能
"""

import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from opensearchpy import OpenSearch, RequestsHttpConnection, helpers
from opensearchpy.exceptions import OpenSearchException

logger = logging.getLogger(__name__)


class OpenSearchClient:
    """OpenSearch客户端类"""
    
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
        初始化OpenSearch客户端
        
        Args:
            config_data: 配置数据字典，如果提供则使用其中的配置
            host: OpenSearch域名
            credentials: 用户名密码元组 (username, password)
            port: 端口号，默认443
            use_ssl: 是否使用SSL，默认True
            verify_certs: 是否验证证书，默认True
            http_compress: 是否启用gzip压缩，默认True
            timeout: 超时时间（秒），默认30
        """
        # 如果提供了配置数据，则使用其中的配置
        if config_data:
            self.host = config_data.get('host')
            
            # 确保数值类型是整数，处理可能的Decimal类型
            port = config_data.get('port', 443)
            self.port = int(port) if port is not None else 443
            
            # 根据认证类型设置认证信息
            auth_type = config_data.get('auth_type')
            if auth_type == 'basic':
                print(f'{config_data.get('username')}, {config_data.get('password')}')
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
        
        # 初始化OpenSearch客户端
        try:
            # 确保timeout是整数类型
            timeout_int = int(self.timeout) if self.timeout is not None else 30
            
            # 确保port是整数类型
            port_int = int(self.port) if self.port is not None else 443
            
            if self.credentials:
                self.client = OpenSearch(
                    hosts=[{'host': self.host, 'port': port_int}],
                    http_auth=self.credentials,
                    use_ssl=self.use_ssl,
                    verify_certs=self.verify_certs,
                    ssl_show_warn=self.ssl_show_warn,
                    http_compress=self.http_compress,
                    connection_class=RequestsHttpConnection,
                    timeout=timeout_int
                )
            else:
                self.client = OpenSearch(
                    hosts=[{'host': self.host, 'port': port_int}],
                    use_ssl=self.use_ssl,
                    verify_certs=self.verify_certs,
                    ssl_show_warn=self.ssl_show_warn,
                    http_compress=self.http_compress,
                    connection_class=RequestsHttpConnection,
                    timeout=timeout_int
                )
        except Exception as e:
            logger.error(f"初始化OpenSearch客户端失败: {str(e)}")
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
            
            # 打印原始索引信息，便于调试
            
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
            
        except OpenSearchException as e:
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
                
                # 打印所有索引名称，便于调试
                all_indices = [idx.get('index_name', '') for idx in indices_list]
                
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
            
        except OpenSearchException as e:
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
        执行索引查询，统一返回格式 - 优化版本
        
        统一 Elasticsearch 和 OpenSearch 的查询结果结构，只保留所需的查询字段，简化输出内容
        
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
            
        except OpenSearchException as e:
            logger.error(f"查询索引 {index_name} 失败: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"执行查询时发生未知错误: {str(e)}")
            raise
    
    def _format_simplified_response(self, response: Dict[str, Any], index_name: str, 
                                   required_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        简化格式化搜索响应，统一 Elasticsearch 和 OpenSearch 结果结构
        只保留核心查询字段，大幅简化输出内容，适合prompt识别处理
        
        Args:
            response: 原始搜索响应
            index_name: 索引名称
            required_fields: 必需的字段列表
            
        Returns:
            Dict: 简化统一格式的结果
        """
        try:
            # 提取基本查询信息
            total_hits = self._safe_get_total_hits(response)
            took = response.get('took', 0)
            
            # 处理文档结果
            hits = response.get('hits', {}).get('hits', [])
            documents = []
            
            for hit in hits:
                source = hit.get('_source', {})
                if not source:
                    continue
                
                # 清理和简化字段数据
                cleaned_data = self._clean_document_fields(source, required_fields)
                
                if cleaned_data:
                    # 构建简化的文档结构 - 进一步精简
                    document = {'data': cleaned_data}
                    
                    # 只在ID有意义时添加ID
                    doc_id = hit.get('_id', '')
                    if doc_id and not doc_id.startswith('_') and len(doc_id) < 50:
                        document['id'] = doc_id
                    
                    # 只在评分有意义时添加评分
                    score = hit.get('_score')
                    if score is not None and score > 0 and score != 1.0:
                        document['score'] = round(score, 2)
                        
                    documents.append(document)
            
            # 处理聚合结果 - 简化格式
            aggregations = self._simplify_aggregations(response.get('aggregations', {}))
            
            # 构建简化的统一响应格式 - 移除冗余信息
            result = {
                'total': total_hits,
                'documents': documents
            }
            
            # 只在查询时间较长时显示耗时
            if took > 100:  # 只有超过100ms才显示
                result['took_ms'] = took
            
            # 只在有聚合结果时添加聚合信息
            if aggregations:
                result['aggregations'] = aggregations
            
            # 只在有错误或特殊情况时添加success字段
            result['success'] = True
                
            return result
            
        except Exception as e:
            logger.error(f"简化响应格式化失败: {str(e)}")
            return {
                'success': False,
                'error': f'响应格式化失败: {str(e)}',
                'total': 0,
                'documents': []
            }
    
    def _format_standard_response(self, response: Dict[str, Any], index_name: str) -> Dict[str, Any]:
        """
        标准格式化搜索响应，保持较完整的信息但统一结构
        
        Args:
            response: 原始搜索响应
            index_name: 索引名称
            
        Returns:
            Dict: 标准统一格式的结果
        """
        # 提取基本查询信息
        total_hits = self._safe_get_total_hits(response)
        max_score = response.get('hits', {}).get('max_score')
        took = response.get('took', 0)
        timed_out = response.get('timed_out', False)
        
        # 处理文档结果
        hits = response.get('hits', {}).get('hits', [])
        documents = []
        
        for hit in hits:
            document = {
                'id': hit.get('_id', ''),
                'score': hit.get('_score'),
                'index': hit.get('_index', ''),
                'source': hit.get('_source', {}),
                'timestamp': self._extract_timestamp(hit.get('_source', {}))
            }
            documents.append(document)
        
        # 处理聚合结果
        aggregations = response.get('aggregations', {})
        agg_results = self._format_aggregations(aggregations) if aggregations else []
        
        # 构建标准响应格式
        result = {
            'success': True,
            'query_info': {
                'index_name': index_name,
                'total_hits': total_hits,
                'max_score': max_score,
                'took_ms': took,
                'timed_out': timed_out
            },
            'documents': documents,
            'aggregations': agg_results,
            'summary': {
                'document_count': len(documents),
                'aggregation_count': len(agg_results)
            }
        }
        
        return result
    
    def _format_unified_response(self, response: Dict[str, Any], index_name: str, 
                                flatten_results: bool = True, extract_key_info: bool = True,
                                normalize_timestamps: bool = True, include_metadata: bool = True) -> Dict[str, Any]:
        """
        统一格式化搜索响应，提供最完整的结果格式
        
        Args:
            response: OpenSearch原始响应
            index_name: 索引名称
            flatten_results: 是否扁平化结果
            
        Returns:
            Dict: 统一格式的完整结果
        """
        # 提取基本查询信息
        total_hits = self._safe_get_total_hits(response)
        max_score = response.get('hits', {}).get('max_score')
        took = response.get('took', 0)
        timed_out = response.get('timed_out', False)
        
        # 处理聚合结果
        aggregations = response.get('aggregations', {})
        agg_results = self._format_aggregations(aggregations) if aggregations else []
        
        # 处理文档结果
        hits = response.get('hits', {}).get('hits', [])
        documents = self._format_documents(hits, flatten_results, extract_key_info, normalize_timestamps)
        
        # 统一返回格式
        result = {
            'status': 'success',
            'query_info': {
                'index_name': index_name,
                'total_hits': total_hits,
                'max_score': max_score,
                'took_ms': took,
                'timed_out': timed_out,
                'has_aggregations': len(agg_results) > 0,
                'has_documents': len(documents) > 0
            },
            'data': {
                'documents': documents,
                'aggregations': agg_results
            },
            'summary': {
                'document_count': len(documents),
                'aggregation_count': len(agg_results),
                'key_fields': self._extract_key_fields(documents),
                'data_types': self._analyze_data_types(documents)
            },
            'metadata': {
                'format': 'unified',
                'flattened': flatten_results,
                'timestamp': self._get_current_timestamp()
            }
        }
        
        return result
    
    def _format_list_response(self, response: Dict[str, Any], index_name: str) -> Dict[str, Any]:
        """
        格式化为简洁的列表格式，便于快速浏览关键信息
        
        Args:
            response: OpenSearch原始响应
            index_name: 索引名称
            
        Returns:
            Dict: 列表格式的结果
        """
        total_hits = self._safe_get_total_hits(response)
        hits = response.get('hits', {}).get('hits', [])
        
        # 提取关键信息列表
        items = []
        for hit in hits:
            source = hit.get('_source', {})
            
            # 创建简化的条目
            item = {
                'id': hit.get('_id', ''),
                'score': hit.get('_score', 0),
                'key_values': self._extract_key_values(source),
                'timestamp': self._extract_timestamp(source),
                'summary': self._create_item_summary(source)
            }
            items.append(item)
        
        # 处理聚合结果为简单列表
        aggregations = response.get('aggregations', {})
        agg_list = []
        if aggregations:
            for agg_name, agg_data in aggregations.items():
                agg_summary = {
                    'name': agg_name,
                    'type': self._detect_aggregation_type(agg_data),
                    'summary': self._create_agg_summary(agg_data)
                }
                agg_list.append(agg_summary)
        
        result = {
            'status': 'success',
            'total_count': total_hits,
            'returned_count': len(items),
            'items': items,
            'aggregations': agg_list,
            'metadata': {
                'format': 'list',
                'index_name': index_name,
                'timestamp': self._get_current_timestamp()
            }
        }
        
        return result
    
    def _format_table_response(self, response: Dict[str, Any], index_name: str) -> Dict[str, Any]:
        """
        格式化为表格格式，便于结构化展示
        
        Args:
            response: OpenSearch原始响应
            index_name: 索引名称
            
        Returns:
            Dict: 表格格式的结果
        """
        total_hits = self._safe_get_total_hits(response)
        hits = response.get('hits', {}).get('hits', [])
        
        # 分析所有字段以创建表格结构
        all_fields = set()
        rows = []
        
        for hit in hits:
            source = hit.get('_source', {})
            flattened = self._flatten_source(source)
            all_fields.update(flattened.keys())
            
            # 添加元数据字段
            row = {
                '_id': hit.get('_id', ''),
                '_score': hit.get('_score', 0),
                '_index': hit.get('_index', ''),
                **flattened
            }
            rows.append(row)
        
        # 创建列定义
        columns = ['_id', '_score', '_index'] + sorted(list(all_fields))
        
        result = {
            'status': 'success',
            'total_count': total_hits,
            'columns': columns,
            'rows': rows,
            'metadata': {
                'format': 'table',
                'index_name': index_name,
                'column_count': len(columns),
                'row_count': len(rows),
                'timestamp': self._get_current_timestamp()
            }
        }
        
        return result
    
    def _format_simple_response(self, response: Dict[str, Any], index_name: str, 
                               flatten_results: bool = True, extract_key_info: bool = True,
                               normalize_timestamps: bool = True, include_metadata: bool = True) -> Dict[str, Any]:
        """
        格式化为简单响应格式，只包含基本信息
        
        Args:
            response: OpenSearch原始响应
            index_name: 索引名称
            flatten_results: 是否扁平化结果
            extract_key_info: 是否提取关键信息
            normalize_timestamps: 是否标准化时间戳格式
            include_metadata: 是否包含元数据信息
            
        Returns:
            Dict: 简单格式的结果
        """
        total_hits = self._safe_get_total_hits(response)
        hits = response.get('hits', {}).get('hits', [])
        
        # 简化的文档格式
        documents = []
        for hit in hits:
            source = hit.get('_source', {})
            doc = {
                'id': hit.get('_id', ''),
                'score': hit.get('_score', 0),
                'data': self._flatten_source(source) if flatten_results else source
            }
            documents.append(doc)
        
        result = {
            'total_hits': total_hits,
            'hits': documents,
            'took': response.get('took', 0),
            'timed_out': response.get('timed_out', False)
        }
        
        if include_metadata:
            result['metadata'] = {
                'format': 'simple',
                'index_name': index_name,
                'timestamp': self._get_current_timestamp()
            }
        
        return result
    
    def _format_search_response(self, response: Dict[str, Any], index_name: str) -> Dict[str, Any]:
        """
        统一格式化搜索响应，将关键信息提取为列表格式
        
        Args:
            response: OpenSearch原始响应
            index_name: 索引名称
            
        Returns:
            Dict: 统一格式的结果
        """
        # 提取基本查询信息
        total_hits = self._safe_get_total_hits(response)
        max_score = response.get('hits', {}).get('max_score')
        took = response.get('took', 0)
        timed_out = response.get('timed_out', False)
        
        # 处理聚合结果
        aggregations = response.get('aggregations', {})
        agg_results = self._format_aggregations(aggregations) if aggregations else []
        
        # 处理文档结果
        hits = response.get('hits', {}).get('hits', [])
        documents = self._format_documents(hits)
        
        # 统一返回格式
        result = {
            'query_info': {
                'index_name': index_name,
                'total_hits': total_hits,
                'max_score': max_score,
                'took_ms': took,
                'timed_out': timed_out,
                'has_aggregations': len(agg_results) > 0,
                'has_documents': len(documents) > 0
            },
            'documents': documents,
            'aggregations': agg_results,
            'summary': {
                'document_count': len(documents),
                'aggregation_count': len(agg_results),
                'key_fields': self._extract_key_fields(documents),
                'data_types': self._analyze_data_types(documents)
            }
        }
        
        return result
    
    def _format_raw_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """
        格式化原始响应（保持兼容性）
        """
        total_hits = self._safe_get_total_hits(response)
        
        result = {
            'total_hits': total_hits,
            'max_score': response.get('hits', {}).get('max_score'),
            'took': response.get('took', 0),
            'timed_out': response.get('timed_out', False),
            'hits': []
        }
        
        for hit in response.get('hits', {}).get('hits', []):
            hit_data = {
                'id': hit.get('_id', ''),
                'score': hit.get('_score', 0),
                'source': hit.get('_source', {}),
                'index': hit.get('_index', ''),
                'type': hit.get('_type', '')
            }
            result['hits'].append(hit_data)
        
        return result
    
    def _safe_get_total_hits(self, response: Dict[str, Any]) -> int:
        """
        安全获取总命中数，处理不同版本的OpenSearch格式差异
        """
        hits = response.get('hits', {})
        total = hits.get('total', 0)
        
        # 处理不同的total格式
        if isinstance(total, dict):
            return total.get('value', 0)
        elif isinstance(total, int):
            return total
        else:
            return 0
    
    def _format_documents(self, hits: List[Dict[str, Any]], flatten_results: bool = True, 
                         extract_key_info: bool = True, normalize_timestamps: bool = True) -> List[Dict[str, Any]]:
        """
        格式化文档结果为统一列表格式
        
        Args:
            hits: 原始命中结果
            flatten_results: 是否扁平化字段
            extract_key_info: 是否提取关键信息
            normalize_timestamps: 是否标准化时间戳格式
        """
        documents = []
        
        for hit in hits:
            source = hit.get('_source', {})
            
            # 创建统一的文档格式
            doc = {
                'id': hit.get('_id', ''),
                'score': hit.get('_score', 0),
                'index': hit.get('_index', ''),
                'type': hit.get('_type', ''),
                'timestamp': self._extract_timestamp(source),
                'fields': self._flatten_source(source) if flatten_results else source,
                'raw_source': source  # 保留原始数据以备需要
            }
            
            # 添加关键字段快速访问
            doc['key_values'] = self._extract_key_values(source)
            doc['summary'] = self._create_item_summary(source)
            
            documents.append(doc)
        
        return documents
    
    def _flatten_source(self, source: Dict[str, Any], prefix: str = '') -> Dict[str, Any]:
        """
        扁平化嵌套的source字段
        """
        flattened = {}
        
        for key, value in source.items():
            new_key = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                # 递归处理嵌套对象
                flattened.update(self._flatten_source(value, new_key))
            elif isinstance(value, list):
                # 处理数组
                flattened[new_key] = value
                # 如果数组包含对象，也进行扁平化
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        flattened.update(self._flatten_source(item, f"{new_key}[{i}]"))
            else:
                flattened[new_key] = value
        
        return flattened
    
    def _format_aggregations(self, aggregations: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        格式化聚合结果为统一列表格式
        """
        agg_results = []
        
        for agg_name, agg_data in aggregations.items():
            agg_result = {
                'name': agg_name,
                'type': self._detect_aggregation_type(agg_data),
                'data': self._extract_aggregation_data(agg_data)
            }
            agg_results.append(agg_result)
        
        return agg_results
    
    def _detect_aggregation_type(self, agg_data: Dict[str, Any]) -> str:
        """
        检测聚合类型
        """
        if 'buckets' in agg_data:
            return 'bucket'
        elif 'value' in agg_data:
            return 'metric'
        elif 'values' in agg_data:
            return 'multi_metric'
        else:
            return 'unknown'
    
    def _extract_aggregation_data(self, agg_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        提取聚合数据为统一格式
        """
        data = []
        
        if 'buckets' in agg_data:
            # 桶聚合
            for bucket in agg_data['buckets']:
                bucket_info = {
                    'key': bucket.get('key', ''),
                    'doc_count': bucket.get('doc_count', 0),
                    'key_as_string': bucket.get('key_as_string', ''),
                    'sub_aggregations': {}
                }
                
                # 处理子聚合
                for key, value in bucket.items():
                    if key not in ['key', 'doc_count', 'key_as_string']:
                        if isinstance(value, dict) and ('value' in value or 'buckets' in value):
                            bucket_info['sub_aggregations'][key] = self._extract_aggregation_data(value)
                
                data.append(bucket_info)
        
        elif 'value' in agg_data:
            # 指标聚合
            data.append({
                'value': agg_data['value'],
                'value_as_string': agg_data.get('value_as_string', '')
            })
        
        elif 'values' in agg_data:
            # 多值指标聚合
            for key, value in agg_data['values'].items():
                data.append({
                    'metric': key,
                    'value': value
                })
        
        return data
    
    def _get_nested_value(self, data: Dict[str, Any], field_path: str) -> Any:
        """
        获取嵌套字段的值
        
        Args:
            data: 数据字典
            field_path: 字段路径，如 'user.name' 或 'response.headers.content-type'
            
        Returns:
            Any: 字段值，如果不存在返回 None
        """
        try:
            keys = field_path.split('.')
            value = data
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return None
            return value
        except (KeyError, TypeError, AttributeError):
            return None
    
    def _extract_core_fields(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """
        提取文档的核心字段，统一不同数据源的字段结构
        
        Args:
            source: 原始文档数据
            
        Returns:
            Dict: 核心字段数据
        """
        
        core_data = {}
        
        # 常见的核心字段映射
        core_field_mappings = {
            # 时间字段
            'timestamp': ['@timestamp', 'timestamp', 'time', 'datetime', 'created_at', 'updated_at', 'date'],
            # 消息/内容字段
            'message': ['message', 'msg', 'content', 'text', 'description', 'body', 'request', 'uri'],
            # 级别字段
            'level': ['level', 'severity', 'priority', 'log_level', 'loglevel'],
            # 状态字段
            'status': ['status', 'status_code', 'http_status', 'response_code', 'code', 'sc-status'],
            # 用户字段
            'user': ['user', 'username', 'user_id', 'userid', 'user_name'],
            # IP地址字段
            'ip': ['ip', 'client_ip', 'remote_ip', 'source_ip', 'clientip', 'remote_addr', 'c-ip'],
            # 主机字段
            'host': ['host', 'hostname', 'server', 'instance', 'node'],
            # 服务字段
            'service': ['service', 'service_name', 'application', 'app', 'component'],
            # 错误字段
            'error': ['error', 'exception', 'error_message', 'error_msg', 'err'],
            # CloudFront 特定字段
            'method': ['method', 'cs-method', 'http_method'],
            'uri_stem': ['uri-stem', 'cs-uri-stem', 'path'],
            'user_agent': ['user-agent', 'cs(User-Agent)', 'user_agent'],
            'bytes': ['bytes', 'sc-bytes', 'response_size']
        }
        
        # 提取核心字段
        for core_field, possible_fields in core_field_mappings.items():
            for field in possible_fields:
                value = self._get_nested_value(source, field)
                if value is not None:
                    core_data[core_field] = value
                    break
        
        
        # 如果没有找到任何核心字段，返回前几个字段
        if not core_data:
            count = 0
            for key, value in source.items():
                if count >= 5:  # 最多返回5个字段
                    break
                if not key.startswith('_') and value is not None:
                    # 转换值为字符串以避免复杂对象
                    if isinstance(value, (dict, list)):
                        if isinstance(value, dict) and len(value) == 1:
                            # 如果是只有一个键的字典，提取其值
                            core_data[key] = str(list(value.values())[0])
                        else:
                            core_data[key] = str(value)[:200]  # 限制长度
                    else:
                        core_data[key] = value
                    count += 1
        
        return core_data
    
    def _extract_aggregation_summary(self, aggregations: Dict[str, Any]) -> Dict[str, Any]:
        """
        提取聚合结果的简化摘要
        
        Args:
            aggregations: 原始聚合结果
            
        Returns:
            Dict: 简化的聚合摘要
        """
        summary = {}
        
        for agg_name, agg_data in aggregations.items():
            if 'buckets' in agg_data:
                # 桶聚合 - 提取关键统计信息
                buckets = agg_data['buckets']
                if buckets and isinstance(buckets, list):
                    summary[agg_name] = {
                        'type': 'buckets',
                        'count': len(buckets),
                        'top_values': [
                            {'key': bucket.get('key', ''), 'count': bucket.get('doc_count', 0)}
                            for bucket in buckets[:5]  # 只取前5个
                        ]
                    }
                elif buckets:
                    # 如果 buckets 不是列表，尝试转换
                    try:
                        bucket_list = list(buckets) if hasattr(buckets, '__iter__') else [buckets]
                        summary[agg_name] = {
                            'type': 'buckets',
                            'count': len(bucket_list),
                            'top_values': [
                                {'key': bucket.get('key', ''), 'count': bucket.get('doc_count', 0)}
                                for bucket in bucket_list[:5]  # 只取前5个
                            ]
                        }
                    except Exception as e:
                        logger.warning(f"处理聚合桶数据失败: {str(e)}")
                        summary[agg_name] = {
                            'type': 'buckets',
                            'count': 0,
                            'top_values': []
                        }
            elif 'value' in agg_data:
                # 指标聚合
                summary[agg_name] = {
                    'type': 'metric',
                    'value': agg_data['value']
                }
            elif 'values' in agg_data:
                # 多值指标聚合
                summary[agg_name] = {
                    'type': 'multi_metric',
                    'values': agg_data['values']
                }
        
        return summary
    
    def _extract_key_fields(self, documents: List[Dict[str, Any]]) -> List[str]:
        """
        提取文档中的关键字段名
        """
        key_fields = set()
        
        for doc in documents[:10]:  # 只分析前10个文档以提高性能
            fields = doc.get('fields', {})
            key_fields.update(fields.keys())
        
        return sorted(list(key_fields))
    
    def _analyze_data_types(self, documents: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        分析文档字段的数据类型
        """
        type_analysis = {}
        
        # 确保 documents 是列表
        if not isinstance(documents, list):
            logger.warning(f"documents 不是列表类型: {type(documents)}")
            return type_analysis
        
        # 只分析前5个文档
        docs_to_analyze = documents[:5] if len(documents) > 5 else documents
        
        for doc in docs_to_analyze:
            if not isinstance(doc, dict):
                continue
            fields = doc.get('fields', {})
            if isinstance(fields, dict):
                for field_name, field_value in fields.items():
                    if field_name not in type_analysis:
                        type_analysis[field_name] = type(field_value).__name__
        
        return type_analysis
    
    def _extract_key_values(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """
        提取文档中的关键值（常见的重要字段）
        
        Args:
            source: 文档源数据
            
        Returns:
            Dict: 关键字段值
        """
        key_fields = [
            'timestamp', '@timestamp', 'time', 'datetime', 'created_at', 'updated_at',
            'level', 'severity', 'priority', 'status', 'state',
            'message', 'msg', 'content', 'text', 'description',
            'host', 'hostname', 'server', 'service', 'application', 'app',
            'user', 'username', 'user_id', 'client_ip', 'ip',
            'error', 'exception', 'error_code', 'error_message',
            'id', 'uuid', 'request_id', 'trace_id', 'session_id'
        ]
        
        key_values = {}
        flattened = self._flatten_source(source)
        
        for field in key_fields:
            if field in flattened:
                key_values[field] = flattened[field]
            # 也检查部分匹配
            for flat_key, flat_value in flattened.items():
                if field in flat_key.lower() and field not in key_values:
                    key_values[field] = flat_value
                    break
        
        return key_values
    
    def _extract_timestamp(self, source: Dict[str, Any]) -> Optional[str]:
        """
        提取时间戳字段
        
        Args:
            source: 文档源数据
            
        Returns:
            Optional[str]: 时间戳值
        """
        timestamp_fields = [
            '@timestamp', 'timestamp', 'time', 'datetime', 
            'created_at', 'updated_at', 'event_time', 'log_time'
        ]
        
        flattened = self._flatten_source(source)
        
        for field in timestamp_fields:
            if field in flattened:
                return str(flattened[field])
            # 检查包含时间戳关键词的字段
            for key, value in flattened.items():
                if any(ts_field in key.lower() for ts_field in timestamp_fields):
                    return str(value)
        
        return None
    
    def _create_item_summary(self, source: Dict[str, Any], max_length: int = 200) -> str:
        """
        创建文档项目的摘要
        
        Args:
            source: 文档源数据
            max_length: 摘要最大长度
            
        Returns:
            str: 文档摘要
        """
        # 优先级字段用于创建摘要
        summary_fields = [
            'message', 'msg', 'content', 'text', 'description', 'summary',
            'error', 'exception', 'error_message', 'title', 'subject'
        ]
        
        flattened = self._flatten_source(source)
        
        # 尝试从优先级字段创建摘要
        for field in summary_fields:
            if field in flattened:
                value = str(flattened[field])
                if len(value) > max_length:
                    return value[:max_length] + "..."
                return value
        
        # 如果没有找到优先级字段，使用前几个字段
        summary_parts = []
        for key, value in list(flattened.items())[:3]:
            if isinstance(value, (str, int, float)):
                summary_parts.append(f"{key}: {value}")
        
        summary = " | ".join(summary_parts)
        if len(summary) > max_length:
            return summary[:max_length] + "..."
        
        return summary or "No summary available"
    
    def _create_agg_summary(self, agg_data: Dict[str, Any]) -> str:
        """
        创建聚合结果的摘要
        
        Args:
            agg_data: 聚合数据
            
        Returns:
            str: 聚合摘要
        """
        if 'buckets' in agg_data:
            bucket_count = len(agg_data['buckets'])
            return f"{bucket_count} buckets"
        elif 'value' in agg_data:
            return f"Value: {agg_data['value']}"
        elif 'values' in agg_data:
            value_count = len(agg_data['values'])
            return f"{value_count} metrics"
        else:
            return "Unknown aggregation type"
    
    def _get_current_timestamp(self) -> str:
        """
        获取当前时间戳
        
        Returns:
            str: ISO格式的时间戳
        """
        from datetime import datetime
        return datetime.now().isoformat()
    
    def get_search_suggestions(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        基于查询结果提供搜索建议
        
        Args:
            documents: 文档列表
            
        Returns:
            Dict: 搜索建议
        """
        if not documents or not isinstance(documents, list):
            return {'suggestions': [], 'common_fields': [], 'sample_values': {}}
        
        # 分析常见字段
        field_frequency = {}
        sample_values = {}
        
        # 确保安全地切片文档列表
        docs_to_analyze = documents[:20] if len(documents) > 20 else documents
        
        for doc in docs_to_analyze:
            if not isinstance(doc, dict):
                continue
            fields = doc.get('fields', {})
            if not isinstance(fields, dict):
                continue
                
            for field_name, field_value in fields.items():
                # 统计字段频率
                field_frequency[field_name] = field_frequency.get(field_name, 0) + 1
                
                # 收集样本值
                if field_name not in sample_values:
                    sample_values[field_name] = []
                if len(sample_values[field_name]) < 5:  # 每个字段最多5个样本值
                    value_str = str(field_value)[:50]  # 限制长度
                    if value_str not in sample_values[field_name]:
                        sample_values[field_name].append(value_str)
        
        # 生成建议
        if field_frequency:
            common_fields = sorted(field_frequency.keys(), key=lambda x: field_frequency[x], reverse=True)[:10]
            # 确保 common_fields 是列表
            if isinstance(common_fields, list):
                suggestions = [
                    f"按 {field} 字段过滤" for field in common_fields[:5]
                ]
            else:
                suggestions = []
                common_fields = []
        else:
            common_fields = []
            suggestions = []
        
        return {
            'suggestions': suggestions,
            'common_fields': common_fields,
            'sample_values': sample_values
        }
    
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
            
        except OpenSearchException as e:
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
                
        except OpenSearchException as e:
            logger.error(f"获取索引 {index_name} 统计信息失败: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"获取索引统计信息时发生未知错误: {str(e)}")
            raise
    
    
    def test_connection(self) -> bool:
        """
        测试连接
        
        Returns:
            bool: 连接是否成功
        """
        try:
            info = self.client.info()
            logger.info(f"OpenSearch连接成功，版本: {info['version']['number']}")
            return True
        except Exception as e:
            logger.error(f"OpenSearch连接失败: {str(e)}")
            return False
    
    def _clean_document_fields(self, source: Dict[str, Any], required_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        清理文档字段，简化为易读格式，适合prompt处理
        智能识别和优先显示重要字段，屏蔽系统字段和冗余信息
        
        Args:
            source: 文档源数据
            required_fields: 必需的字段列表，如果为None则返回所有清理后的字段
            
        Returns:
            Dict: 清理后的字段数据
        """
        if not source:
            return {}
        
        # 如果指定了必需字段，只提取这些字段
        if required_fields:
            filtered_data = {}
            for field in required_fields:
                value = self._get_nested_field_value(source, field)
                if value is not None:
                    cleaned_value = self._simplify_field_value(value)
                    if cleaned_value is not None:
                        filtered_data[field] = cleaned_value
            return filtered_data
        
        # 定义系统字段和无用字段（需要排除）
        excluded_fields = {
            '_id', '_index', '_type', '_score', '_version', '_seq_no', '_primary_term',
            '_routing', '_parent', '_timestamp', '_ttl', '_size', '_uid', '_all',
            'sort', 'highlight', 'matched_queries', 'inner_hits', '_shards',
            '_explanation', '_nested', '_ignored'
        }
        
        # 定义重要字段的优先级（按重要性排序）
        priority_fields = [
            # 时间相关
            'timestamp', 'time', '@timestamp', 'datetime', 'date', 'created_at', 'updated_at',
            # 日志级别和状态
            'level', 'severity', 'priority', 'status', 'code', 'response_code', 'status_code',
            # 消息内容
            'message', 'msg', 'content', 'text', 'description', 'summary',
            # 来源信息
            'source', 'host', 'hostname', 'ip', 'client_ip', 'remote_addr', 'server_name',
            # 请求信息
            'method', 'url', 'path', 'endpoint', 'api', 'uri', 'request_uri',
            # 用户信息
            'user', 'username', 'user_id', 'account', 'client_id',
            # 错误信息
            'error', 'exception', 'stack_trace', 'error_message', 'error_code',
            # 性能指标
            'duration', 'response_time', 'latency', 'size', 'bytes'
        ]
        
        cleaned = {}
        
        # 首先添加优先字段（按顺序）
        for field in priority_fields:
            if field in source and field not in excluded_fields:
                cleaned_value = self._simplify_field_value(source[field])
                if cleaned_value is not None:
                    cleaned[field] = cleaned_value
        
        # 然后添加其他有用字段（限制数量避免信息过载）
        other_fields_count = 0
        max_other_fields = 10  # 最多添加10个其他字段
        
        for key, value in source.items():
            # 跳过已处理的字段、系统字段和以下划线开头的字段
            if (key in cleaned or 
                key in excluded_fields or 
                key.startswith('_') or
                other_fields_count >= max_other_fields):
                continue
            
            # 跳过一些常见的无用字段
            if key.lower() in ['raw', 'keyword', 'analyzed', 'not_analyzed', 'fields']:
                continue
            
            cleaned_value = self._simplify_field_value(value)
            if cleaned_value is not None:
                cleaned[key] = cleaned_value
                other_fields_count += 1
        
        return cleaned
    
    def _get_nested_field_value(self, source: Dict[str, Any], field_path: str) -> Any:
        """
        获取嵌套字段值，支持点分隔的路径
        
        Args:
            source: 源数据
            field_path: 字段路径，如 'user.profile.email'
            
        Returns:
            Any: 字段值或None
        """
        try:
            if '.' not in field_path:
                return source.get(field_path)
            
            keys = field_path.split('.')
            current = source
            
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return None
            
            return current
        except Exception:
            return None
    
    def _simplify_field_value(self, value: Any) -> Any:
        """
        简化字段值，转换为易读格式，专门优化日志数据显示
        
        Args:
            value: 原始值
            
        Returns:
            Any: 简化后的值
        """
        if value is None or value == '' or value == []:
            return None
        
        # 字符串类型：清理空白字符，限制长度，处理特殊格式
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                return None
            
            # 对于过长的字符串，智能截断
            if len(cleaned) > 200:  # 进一步缩短长度限制
                # 如果是JSON字符串，尝试解析并简化
                if cleaned.startswith('{') or cleaned.startswith('['):
                    try:
                        import json
                        parsed = json.loads(cleaned)
                        return self._simplify_field_value(parsed)
                    except:
                        pass
                
                # 对于日志消息，尝试保留关键信息
                if any(keyword in cleaned.lower() for keyword in ['error', 'exception', 'failed', 'timeout']):
                    # 错误消息保留更多信息
                    cleaned = cleaned[:197] + '...'
                else:
                    # 普通消息截断更短
                    cleaned = cleaned[:147] + '...'
            
            return cleaned
        
        # 数字类型：格式化显示
        elif isinstance(value, (int, float)):
            if isinstance(value, float):
                # 保留合理的小数位数
                if value == int(value):
                    return int(value)
                else:
                    return round(value, 3)
            return value
        
        # 布尔类型：直接返回
        elif isinstance(value, bool):
            return value
        
        # 字典类型：递归简化，智能处理嵌套结构
        elif isinstance(value, dict):
            if len(value) > 5:  # 进一步限制字段数量
                # 尝试提取最重要的字段
                important_keys = ['message', 'error', 'status', 'code', 'name', 'type', 'value', 'host', 'port']
                simplified = {}
                
                for key in important_keys:
                    if key in value:
                        simplified_v = self._simplify_field_value(value[key])
                        if simplified_v is not None:
                            simplified[key] = simplified_v
                
                # 如果没有重要字段，取前3个字段
                if not simplified:
                    for k, v in list(value.items())[:3]:
                        if not k.startswith('_'):
                            simplified_v = self._simplify_field_value(v)
                            if simplified_v is not None:
                                simplified[k] = simplified_v
                
                # 只有在确实省略了字段时才显示"更多"提示
                remaining_count = len([k for k in value.keys() if not k.startswith('_')]) - len(simplified)
                if remaining_count > 0:
                    simplified['_more'] = f"...还有{remaining_count}个字段"
                
                return simplified if simplified else None
            else:
                # 正常处理小字典
                simplified = {}
                for k, v in value.items():
                    if not k.startswith('_'):  # 跳过系统字段
                        simplified_v = self._simplify_field_value(v)
                        if simplified_v is not None:
                            simplified[k] = simplified_v
                return simplified if simplified else None
        
        # 列表类型：限制长度，智能处理
        elif isinstance(value, list):
            if len(value) > 3:  # 进一步限制列表长度
                simplified_items = []
                for item in value[:3]:
                    simplified_item = self._simplify_field_value(item)
                    if simplified_item is not None:
                        simplified_items.append(simplified_item)
                
                if simplified_items:
                    simplified_items.append(f"...还有{len(value)-3}项")
                    return simplified_items
                else:
                    return f"[{len(value)}个项目]"
            else:
                simplified_items = []
                for item in value:
                    simplified_item = self._simplify_field_value(item)
                    if simplified_item is not None:
                        simplified_items.append(simplified_item)
                return simplified_items if simplified_items else None
        
        # 其他类型：转换为字符串并限制长度
        else:
            str_value = str(value)
            if len(str_value) > 100:
                str_value = str_value[:97] + '...'
            return str_value
    
    def _simplify_aggregations(self, raw_aggregations: Dict[str, Any]) -> Dict[str, Any]:
        """
        简化聚合结果，转换为易读格式
        
        Args:
            raw_aggregations: 原始聚合结果
            
        Returns:
            Dict: 简化的聚合结果
        """
        if not raw_aggregations:
            return {}
        
        simplified = {}
        
        for agg_name, agg_data in raw_aggregations.items():
            try:
                # 处理桶聚合（如terms, histogram等）
                if 'buckets' in agg_data:
                    buckets = agg_data['buckets'][:10]  # 只取前10个
                    simplified[agg_name] = {
                        'type': 'buckets',
                        'total': len(agg_data['buckets']),
                        'items': [
                            {
                                'key': str(bucket.get('key', '')),
                                'count': bucket.get('doc_count', 0)
                            }
                            for bucket in buckets
                        ]
                    }
                
                # 处理指标聚合（如avg, sum, max, min等）
                elif 'value' in agg_data:
                    value = agg_data['value']
                    simplified[agg_name] = {
                        'type': 'metric',
                        'value': round(value, 2) if isinstance(value, (int, float)) else value
                    }
                
                # 处理其他复杂聚合
                else:
                    simplified[agg_name] = {
                        'type': 'complex',
                        'data': self._simplify_field_value(agg_data)
                    }
                    
            except Exception as e:
                logger.warning(f"处理聚合 {agg_name} 失败: {str(e)}")
                simplified[agg_name] = {
                    'type': 'error',
                    'error': str(e)
                }
        
        return simplified
    
    def _get_nested_value(self, data: Dict[str, Any], field_path: str) -> Any:
        """
        获取嵌套字段的值
        
        Args:
            data: 数据字典
            field_path: 字段路径，如 'user.profile.email'
            
        Returns:
            Any: 字段值，如果不存在则返回None
        """
        try:
            keys = field_path.split('.')
            current = data
            
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    return None
            
            return current
        except Exception:
            return None
    
    def _extract_core_fields(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """
        智能提取常见的核心字段，统一不同数据源的字段名称
        
        Args:
            source: 文档源数据
            
        Returns:
            Dict: 提取的核心字段数据
        """
        # 定义核心字段映射，支持多种可能的字段名
        core_field_mappings = {
            'timestamp': ['@timestamp', 'timestamp', 'time', 'datetime', 'created_at', 'date', 'event_time'],
            'message': ['message', 'msg', 'content', 'text', 'description', 'log_message', 'body'],
            'level': ['level', 'severity', 'priority', 'log_level', 'loglevel', 'type'],
            'status': ['status', 'status_code', 'http_status', 'response_code', 'code'],
            'user': ['user', 'username', 'user_id', 'userid', 'user_name'],
            'ip': ['ip', 'client_ip', 'remote_ip', 'source_ip', 'clientip', 'remote_addr'],
            'host': ['host', 'hostname', 'server', 'instance', 'node'],
            'service': ['service', 'service_name', 'application', 'app', 'component'],
            'error': ['error', 'exception', 'error_message', 'error_msg', 'err'],
            'method': ['method', 'http_method', 'request_method'],
            'url': ['url', 'uri', 'path', 'request_uri'],
            'response_time': ['response_time', 'duration', 'elapsed', 'took'],
            'bytes': ['bytes', 'size', 'content_length', 'body_bytes_sent']
        }
        
        extracted_data = {}
        
        # 遍历核心字段映射，查找匹配的字段
        for standard_field, possible_fields in core_field_mappings.items():
            for field_name in possible_fields:
                if field_name in source:
                    extracted_data[standard_field] = source[field_name]
                    break  # 找到第一个匹配的字段就停止
        
        # 如果没有提取到任何核心字段，则返回所有字段（但进行简化处理）
        if not extracted_data:
            # 过滤掉一些不重要的系统字段
            excluded_fields = {'_id', '_index', '_type', '_score', '_version', '_seq_no', '_primary_term'}
            extracted_data = {
                k: v for k, v in source.items() 
                if k not in excluded_fields and not k.startswith('_')
            }
        
        return extracted_data
    
    def _extract_aggregation_summary(self, aggregations: Dict[str, Any]) -> Dict[str, Any]:
        """
        提取聚合结果的摘要信息，简化聚合数据结构
        
        Args:
            aggregations: 原始聚合结果
            
        Returns:
            Dict: 简化的聚合摘要
        """
        if not aggregations:
            return {}
        
        summary = {}
        
        for agg_name, agg_data in aggregations.items():
            try:
                # 处理桶聚合（terms, histogram等）
                if 'buckets' in agg_data:
                    buckets = agg_data['buckets']
                    summary[agg_name] = {
                        'type': 'buckets',
                        'count': len(buckets),
                        'top_values': [
                            {
                                'key': bucket.get('key', ''),
                                'count': bucket.get('doc_count', 0)
                            }
                            for bucket in buckets[:10]  # 只取前10个
                        ]
                    }
                
                # 处理指标聚合（avg, sum, max, min等）
                elif 'value' in agg_data:
                    summary[agg_name] = {
                        'type': 'metric',
                        'value': agg_data['value']
                    }
                
                # 处理基数聚合
                elif 'value' in agg_data and agg_name.endswith('_cardinality'):
                    summary[agg_name] = {
                        'type': 'cardinality',
                        'unique_count': agg_data['value']
                    }
                
                # 处理其他类型的聚合
                else:
                    summary[agg_name] = {
                        'type': 'other',
                        'data': agg_data
                    }
                    
            except Exception as e:
                logger.warning(f"处理聚合 {agg_name} 时出错: {str(e)}")
                summary[agg_name] = {
                    'type': 'error',
                    'error': str(e)
                }
        
        return summary
    
    def _safe_get_total_hits(self, response: Dict[str, Any]) -> int:
        """
        安全获取总命中数，兼容不同版本的响应格式
        
        Args:
            response: 搜索响应
            
        Returns:
            int: 总命中数
        """
        try:
            hits = response.get('hits', {})
            total = hits.get('total', 0)
            
            # 新版本格式：{'value': 100, 'relation': 'eq'}
            if isinstance(total, dict):
                return total.get('value', 0)
            
            # 旧版本格式：直接是数字
            elif isinstance(total, (int, float)):
                return int(total)
            
            else:
                return 0
                
        except Exception as e:
            logger.warning(f"获取总命中数时出错: {str(e)}")
            return 0


# 使用示例
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO)
    
    # 初始化客户端
    client = OpenSearchClient(
        host='your-opensearch-host.com',
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
