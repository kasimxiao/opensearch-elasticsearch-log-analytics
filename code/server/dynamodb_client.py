"""
DynamoDB客户端操作模块
用于存储和管理OpenSearch索引字段信息以及搜索引擎配置信息
"""

import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import boto3
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)


class DynamoDBClient:
    """DynamoDB客户端类"""
    
    def __init__(self, 
                 region: str = 'ap-northeast-1',
                 table_name: str = 'log_field_metadata'):
        """
        初始化DynamoDB客户端
        
        Args:
            region: AWS区域
            table_name: DynamoDB表名
        """
        self.region = region
        self.table_name = table_name
        
        # 初始化DynamoDB资源和客户端
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.dynamodb_client = boto3.client('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)
    
    def create_table_if_not_exists(self) -> bool:
        """
        创建DynamoDB表（如果不存在）
        
        Returns:
            bool: 表是否创建成功或已存在
        """
        try:
            # 检查表是否存在
            self.table.load()
            
            # 检查表结构是否正确
            try:
                # 获取表描述
                table_description = self.dynamodb_client.describe_table(TableName=self.table_name)
                key_schema = table_description['Table']['KeySchema']
                
                # 检查是否只有一个主键，且是index_name
                if len(key_schema) == 1 and key_schema[0]['AttributeName'] == 'index_name':
                    return True
                else:
                    logger.warning(f"表 {self.table_name} 结构不正确，需要重新创建")
                    # 删除表
                    self.dynamodb_client.delete_table(TableName=self.table_name)
                    
                    # 等待表被删除
                    waiter = self.dynamodb_client.get_waiter('table_not_exists')
                    waiter.wait(TableName=self.table_name)
                    
                    # 创建新表
                    return self._create_new_table()
            except Exception as e:
                logger.error(f"检查表结构时发生错误: {str(e)}")
                return False
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # 表不存在，创建表
                return self._create_new_table()
            else:
                logger.error(f"检查表存在性时发生错误: {str(e)}")
                return False
        except Exception as e:
            logger.error(f"创建表时发生未知错误: {str(e)}")
            return False
    
    def _create_new_table(self) -> bool:
        """
        创建新表
        
        Returns:
            bool: 表是否创建成功
        """
        try:
            table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {
                        'AttributeName': 'index_name',
                        'KeyType': 'HASH'  # 分区键
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'index_name',
                        'AttributeType': 'S'
                    }
                ],
                BillingMode='PAY_PER_REQUEST'  # 按需付费
            )
            
            # 等待表创建完成
            table.wait_until_exists()
            logger.info(f"表 {self.table_name} 创建成功")
            return True
        except ClientError as create_error:
            logger.error(f"创建表失败: {str(create_error)}")
            return False
        except Exception as e:
            logger.error(f"创建表时发生未知错误: {str(e)}")
            return False
    
    def save_index_fields(self, 
                         index_name: str, 
                         fields_info: List[Dict[str, Any]]) -> bool:
        """
        提取索引中的字段名称、字段类别，预留备注字段等，写入DynamoDB
        
        Args:
            index_name: 查询索引名称（可以是别名或带通配符的模式）
            fields_info: 字段信息列表
            
        Returns:
            bool: 是否保存成功
        """
        try:
            current_time = datetime.utcnow().isoformat()
            
            # 使用提供的索引名称
            query_index_name = index_name
            
            # 将字段信息合并为一个描述对象
            field_descriptions = {}
            for field in fields_info:
                field_name = field.get('field_name', '')
                field_type = field.get('field_type', 'unknown')
                description = field.get('description', '')
                if field_name:
                    field_descriptions[field_name] = {
                        'type': field_type,
                        'description': description
                    }
            
            # 检查索引是否已存在
            response = self.table.get_item(
                Key={
                    'index_name': query_index_name
                }
            )
            
            if 'Item' in response:
                # 索引存在，更新字段描述
                item = response['Item']
                
                # 保留索引描述和其他元数据
                index_description = item.get('index_description', '')
                created_at = item.get('created_at', current_time)
                version = item.get('version', 0) + 1
                
                # 执行更新
                self.table.update_item(
                    Key={
                        'index_name': query_index_name
                    },
                    UpdateExpression="SET description = :description, updated_at = :updated_at, version = :version",
                    ExpressionAttributeValues={
                        ':description': field_descriptions,
                        ':updated_at': current_time,
                        ':version': version
                    }
                )
            else:
                # 索引不存在，创建新记录
                item = {
                    'index_name': query_index_name,
                    'description': field_descriptions,  # 将字段描述保存为一个对象
                    'index_description': '',  # 添加索引描述字段
                    'created_at': current_time,
                    'updated_at': current_time,
                    'version': 1
                }
                
                # 保存到DynamoDB
                self.table.put_item(Item=item)
            
            return True
            
        except ClientError as e:
            logger.error(f"保存字段信息到DynamoDB失败: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"保存字段信息时发生未知错误: {str(e)}")
            return False
    
    def update_field_description(self, 
                               index_name: str, 
                               field_name: str, 
                               description: str = '',
                               field_type: str = 'unknown') -> bool:
        """
        更新索引字段说明中的备注字段信息
        
        Args:
            index_name: 查询索引名称（可以是别名或带通配符的模式）
            field_name: 字段名称
            description: 字段描述
            field_type: 字段类型
            
        Returns:
            bool: 是否更新成功
        """
        try:
            current_time = datetime.utcnow().isoformat()
            
            # 获取索引字段信息
            response = self.table.get_item(
                Key={
                    'index_name': index_name
                }
            )
            
            if 'Item' in response:
                # 索引存在，更新字段描述
                item = response['Item']
                field_descriptions = item.get('description', {})
                
                # 更新或添加字段描述
                if field_name in field_descriptions:
                    field_descriptions[field_name]['description'] = description
                    if field_type != 'unknown':
                        field_descriptions[field_name]['type'] = field_type
                else:
                    field_descriptions[field_name] = {
                        'type': field_type,
                        'description': description
                    }
                
                # 执行更新
                update_expression = "SET description = :description, updated_at = :updated_at, version = version + :inc"
                expression_values = {
                    ':description': field_descriptions,
                    ':updated_at': current_time,
                    ':inc': 1
                }
                
                self.table.update_item(
                    Key={
                        'index_name': index_name
                    },
                    UpdateExpression=update_expression,
                    ExpressionAttributeValues=expression_values,
                    ReturnValues='UPDATED_NEW'
                )
                
                return True
            else:
                # 索引不存在，创建新记录
                field_descriptions = {
                    field_name: {
                        'type': field_type,
                        'description': description
                    }
                }
                
                item = {
                    'index_name': index_name,
                    'description': field_descriptions,
                    'index_description': '',  # 添加索引描述字段
                    'created_at': current_time,
                    'updated_at': current_time,
                    'version': 1
                }
                
                # 保存到DynamoDB
                self.table.put_item(Item=item)
                
                return True
            
        except ClientError as e:
            logger.error(f"更新字段描述失败: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"更新字段描述时发生未知错误: {str(e)}")
            return False
            
    def update_index_metadata(self, index_name: str, metadata: Dict[str, Any]) -> bool:
        """
        更新索引元数据
        
        Args:
            index_name: 索引名称
            metadata: 元数据字典，包含index_description等
            
        Returns:
            bool: 是否更新成功
        """
        try:
            current_time = datetime.utcnow().isoformat()
            
            # 获取索引信息
            response = self.table.get_item(
                Key={
                    'index_name': index_name
                }
            )
            
            if 'Item' in response:
                # 索引存在，更新元数据
                item = response['Item']
                
                # 构建更新表达式
                update_expression = "SET updated_at = :updated_at"
                expression_values = {
                    ':updated_at': current_time
                }
                
                # 添加元数据字段
                for key, value in metadata.items():
                    update_expression += f", {key} = :{key}"
                    expression_values[f":{key}"] = value
                
                # 执行更新
                self.table.update_item(
                    Key={
                        'index_name': index_name
                    },
                    UpdateExpression=update_expression,
                    ExpressionAttributeValues=expression_values,
                    ReturnValues='UPDATED_NEW'
                )
                
                return True
            else:
                # 索引不存在，创建新记录
                item = {
                    'index_name': index_name,
                    'description': {},  # 空字段描述
                    'created_at': current_time,
                    'updated_at': current_time,
                    'version': 1
                }
                
                # 添加元数据字段
                for key, value in metadata.items():
                    item[key] = value
                
                # 保存到DynamoDB
                self.table.put_item(Item=item)
                
                return True
            
        except ClientError as e:
            logger.error(f"更新索引元数据失败: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"更新索引元数据时发生未知错误: {str(e)}")
            return False
    
    def get_index_fields(self, index_name: str) -> List[Dict[str, Any]]:
        """
        获取索引的所有字段信息
        
        Args:
            index_name: 索引名称
            
        Returns:
            List[Dict]: 字段信息列表
        """
        try:
            response = self.table.get_item(
                Key={
                    'index_name': index_name
                }
            )
            
            if 'Item' not in response:
                return []
            
            item = response['Item']
            field_descriptions = item.get('description', {})
            
            # 将字段描述转换为字段列表
            fields = []
            for field_name, field_info in field_descriptions.items():
                fields.append({
                    'index_name': index_name,
                    'field_name': field_name,
                    'field_type': field_info.get('type', 'unknown'),
                    'description': field_info.get('description', '')
                })
            
            return fields
            
        except ClientError as e:
            logger.error(f"获取索引字段信息失败: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"获取索引字段信息时发生未知错误: {str(e)}")
            return []
    
    def get_field_info(self, index_name: str, field_name: str) -> Optional[Dict[str, Any]]:
        """
        获取特定字段的详细信息
        
        Args:
            index_name: 索引名称
            field_name: 字段名称
            
        Returns:
            Optional[Dict]: 字段信息，如果不存在返回None
        """
        try:
            response = self.table.get_item(
                Key={
                    'index_name': index_name
                }
            )
            
            if 'Item' in response:
                item = response['Item']
                field_descriptions = item.get('description', {})
                
                if field_name in field_descriptions:
                    field_info = field_descriptions[field_name]
                    # 构建完整的字段信息
                    result = {
                        'index_name': index_name,
                        'field_name': field_name,
                        'field_type': field_info.get('type', 'unknown'),
                        'description': field_info.get('description', ''),
                        'created_at': item.get('created_at'),
                        'updated_at': item.get('updated_at'),
                        'version': item.get('version')
                    }
                    return result
                else:
                    logger.warning(f"字段 {index_name}.{field_name} 不存在")
                    return None
            else:
                logger.warning(f"索引 {index_name} 不存在")
                return None
                
        except ClientError as e:
            logger.error(f"获取字段信息失败: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"获取字段信息时发生未知错误: {str(e)}")
            return None
    
    def search_fields_by_type(self, field_type: str) -> List[Dict[str, Any]]:
        """
        根据字段类型搜索字段
        
        Args:
            field_type: 字段类型
            
        Returns:
            List[Dict]: 匹配的字段列表
        """
        try:
            # 使用扫描操作搜索（注意：这在大表中可能很慢）
            response = self.table.scan(
                FilterExpression='field_type = :field_type',
                ExpressionAttributeValues={
                    ':field_type': field_type
                }
            )
            
            fields = response.get('Items', [])
            return fields
            
        except ClientError as e:
            logger.error(f"搜索字段失败: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"搜索字段时发生未知错误: {str(e)}")
            return []
    
    def get_all_indices(self) -> List[str]:
        """
        获取所有已存储的索引名称
        
        Returns:
            List[str]: 索引名称列表
        """
        try:
            # 使用扫描操作获取所有唯一的索引名称
            response = self.table.scan(
                ProjectionExpression='index_name'
            )
            
            # 去重获取索引名称
            indices = list(set([item['index_name'] for item in response.get('Items', [])]))
            return indices
            
        except ClientError as e:
            logger.error(f"获取索引列表失败: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"获取索引列表时发生未知错误: {str(e)}")
            return []
    
    def get_index_description(self, index_name: str) -> str:
        """
        获取索引的描述信息
        
        Args:
            index_name: 索引名称
            
        Returns:
            str: 索引描述信息，如果不存在返回空字符串
        """
        try:
            response = self.table.get_item(
                Key={
                    'index_name': index_name
                }
            )
            
            if 'Item' in response:
                item = response['Item']
                return item.get('index_description', '')
            else:
                logger.warning(f"索引 {index_name} 不存在")
                return ''
                
        except ClientError as e:
            logger.error(f"获取索引描述失败: {str(e)}")
            return ''
        except Exception as e:
            logger.error(f"获取索引描述时发生未知错误: {str(e)}")
            return ''
    
    def delete_index_fields(self, index_name: str) -> bool:
        """
        删除索引的所有字段信息
        
        Args:
            index_name: 索引名称
            
        Returns:
            bool: 是否删除成功
        """
        try:
            # 检查索引是否存在
            response = self.table.get_item(
                Key={
                    'index_name': index_name
                }
            )
            
            if 'Item' not in response:
                return True
            
            # 删除整个索引记录（包括所有字段信息）
            self.table.delete_item(
                Key={
                    'index_name': index_name
                }
            )
            
            return True
            
        except ClientError as e:
            logger.error(f"删除索引字段信息失败: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"删除索引字段信息时发生未知错误: {str(e)}")
            return False
    
    def batch_update_descriptions(self, updates: List[Dict[str, Any]]) -> int:
        """
        批量更新字段描述
        
        Args:
            updates: 更新信息列表，每个元素包含index_name, field_name和更新字段
            
        Returns:
            int: 成功更新的字段数量
        """
        success_count = 0
        
        # 按索引名称分组更新
        updates_by_index = {}
        for update in updates:
            index_name = update.get('index_name')
            field_name = update.get('field_name')
            
            if not index_name or not field_name:
                logger.warning("跳过无效的更新项：缺少index_name或field_name")
                continue
            
            if index_name not in updates_by_index:
                updates_by_index[index_name] = []
            
            updates_by_index[index_name].append({
                'field_name': field_name,
                'description': update.get('description', ''),
                'field_type': update.get('field_type', 'unknown')
            })
        
        # 对每个索引进行批量更新
        for index_name, field_updates in updates_by_index.items():
            try:
                # 获取当前索引信息
                response = self.table.get_item(
                    Key={
                        'index_name': index_name
                    }
                )
                
                current_time = datetime.utcnow().isoformat()
                
                # 构建字段描述字典
                field_descriptions = {}
                
                if 'Item' in response:
                    # 索引存在，获取现有字段描述
                    item = response['Item']
                    field_descriptions = item.get('description', {})
                
                # 更新字段描述
                for update in field_updates:
                    field_name = update['field_name']
                    description = update['description']
                    field_type = update['field_type']
                    
                    field_descriptions[field_name] = {
                        'type': field_type,
                        'description': description
                    }
                
                # 保存或更新记录
                if 'Item' in response:
                    # 更新现有记录
                    self.table.update_item(
                        Key={
                            'index_name': index_name
                        },
                        UpdateExpression="SET description = :description, updated_at = :updated_at, version = version + :inc",
                        ExpressionAttributeValues={
                            ':description': field_descriptions,
                            ':updated_at': current_time,
                            ':inc': 1
                        }
                    )
                else:
                    # 创建新记录
                    item = {
                        'index_name': index_name,
                        'description': field_descriptions,
                        'created_at': current_time,
                        'updated_at': current_time,
                        'version': 1
                    }
                    
                    # 保存到DynamoDB
                    self.table.put_item(Item=item)
                
                success_count += len(field_updates)
                
            except Exception as e:
                logger.error(f"更新索引 {index_name} 的字段描述失败: {str(e)}")
        
        return success_count


# 使用示例
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO)
    
    # 初始化客户端
    client = DynamoDBClient(
        region='ap-northeast-1',
        table_name='log-opensearch-field-metadata'
    )
    
    # 创建表
    if client.create_table_if_not_exists():
        print("DynamoDB表准备就绪")
        
        # 示例：保存字段信息
        sample_fields = [
            {
                'field_name': 'timestamp',
                'field_path': 'timestamp',
                'field_type': 'date',
                'format': 'yyyy-MM-dd HH:mm:ss'
            },
            {
                'field_name': 'message',
                'field_path': 'message',
                'field_type': 'text',
                'analyzer': 'standard'
            }
        ]
        
        client.save_index_fields('test-index', sample_fields)
        
        # 更新字段描述
        client.update_field_description(
            'test-index',
            'timestamp',
            description='日志时间戳',
            business_meaning='记录日志产生的时间'
        )
class SearchEngineConfigClient:
    """搜索引擎配置客户端类"""
    
    def __init__(self, 
                 region: str = 'ap-northeast-1',
                 table_name: str = 'log_engine_configs'):
        """
        初始化搜索引擎配置客户端
        
        Args:
            region: AWS区域
            table_name: DynamoDB表名
        """
        self.region = region
        self.table_name = table_name
        
        # 初始化DynamoDB资源和客户端
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.dynamodb_client = boto3.client('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)
    
    def create_table_if_not_exists(self) -> bool:
        """
        创建搜索引擎配置表（如果不存在）
        
        Returns:
            bool: 表是否创建成功或已存在
        """
        try:
            # 检查表是否存在
            self.table.load()
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # 表不存在，创建表
                try:
                    table = self.dynamodb.create_table(
                        TableName=self.table_name,
                        KeySchema=[
                            {
                                'AttributeName': 'config_id',
                                'KeyType': 'HASH'  # 分区键
                            }
                        ],
                        AttributeDefinitions=[
                            {
                                'AttributeName': 'config_id',
                                'AttributeType': 'S'
                            }
                        ],
                        BillingMode='PAY_PER_REQUEST'  # 按需付费
                    )
                    
                    # 等待表创建完成
                    table.wait_until_exists()
                    logger.info(f"表 {self.table_name} 创建成功")
                    return True
                    
                except ClientError as create_error:
                    logger.error(f"创建表失败: {str(create_error)}")
                    return False
            else:
                logger.error(f"检查表存在性时发生错误: {str(e)}")
                return False
        except Exception as e:
            logger.error(f"创建表时发生未知错误: {str(e)}")
            return False
    
    def save_search_engine_config(self, 
                                config_data: Dict[str, Any],
                                config_id: str = None) -> Optional[str]:
        """
        保存搜索引擎配置信息
        
        Args:
            config_data: 配置信息，必须包含type字段和必要的连接信息
            config_id: 配置ID，如果为None则自动生成
            
        Returns:
            Optional[str]: 配置ID，如果保存失败则返回None
        """
        try:
            # 验证配置数据
            if not self._validate_config_data(config_data):
                logger.error("配置数据验证失败")
                return None
            
            current_time = datetime.utcnow().isoformat()
            
            # 如果没有提供配置ID，则生成一个
            if not config_id:
                config_id = f"{config_data.get('type', 'unknown')}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            
            # 准备要保存的项目
            item = {
                'config_id': config_id,
                'type': config_data.get('type'),  # elasticsearch 或 opensearch
                'name': config_data.get('name', ''),
                'description': config_data.get('description', ''),
                'host': config_data.get('host', ''),
                'port': config_data.get('port', 443),
                'use_ssl': config_data.get('use_ssl', True),
                'verify_certs': config_data.get('verify_certs', True),
                'http_compress': config_data.get('http_compress', True),
                'timeout': config_data.get('timeout', 30),
                'created_at': current_time,
                'updated_at': current_time,
                'version': 1
            }
            
            # 添加认证信息（如果提供）
            if 'username' in config_data and 'password' in config_data:
                item['auth_type'] = 'basic'
                item['username'] = config_data.get('username')
                item['password'] = config_data.get('password')
            elif 'api_key' in config_data:
                item['auth_type'] = 'api_key'
                item['api_key'] = config_data.get('api_key')
            elif 'aws_region' in config_data:
                item['auth_type'] = 'aws_sigv4'
                item['aws_region'] = config_data.get('aws_region')
                item['aws_service'] = config_data.get('aws_service', 'es')
            else:
                item['auth_type'] = 'none'
            
            # 添加其他可选配置
            for key, value in config_data.items():
                if key not in item and key not in ['username', 'password', 'api_key']:
                    item[key] = value
            
            # 保存到DynamoDB
            self.table.put_item(Item=item)
            
            return config_id
            
        except ClientError as e:
            logger.error(f"保存搜索引擎配置失败: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"保存搜索引擎配置时发生未知错误: {str(e)}")
            return None
    
    def get_search_engine_config(self, config_id: str) -> Optional[Dict[str, Any]]:
        """
        获取特定搜索引擎配置
        
        Args:
            config_id: 配置ID
            
        Returns:
            Optional[Dict]: 配置信息，如果不存在返回None
        """
        try:
            response = self.table.get_item(
                Key={
                    'config_id': config_id
                }
            )
            
            if 'Item' in response:
                logger.info(f"获取到搜索引擎配置 {config_id}")
                return response['Item']
            else:
                logger.warning(f"搜索引擎配置 {config_id} 不存在")
                return None
                
        except ClientError as e:
            logger.error(f"获取搜索引擎配置失败: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"获取搜索引擎配置时发生未知错误: {str(e)}")
            return None
    
    def list_search_engine_configs(self) -> List[Dict[str, Any]]:
        """
        获取所有搜索引擎配置的摘要信息
        
        Returns:
            List[Dict]: 配置摘要信息列表
        """
        try:
            response = self.table.scan()
            
            # 提取摘要信息
            configs = []
            for item in response.get('Items', []):
                config_summary = {
                    'config_id': item.get('config_id'),
                    'type': item.get('type'),
                    'name': item.get('name', ''),
                    'description': item.get('description', ''),
                    'host': item.get('host', ''),
                    'port': item.get('port', 443),
                    'username': item.get('username', ''),
                    'password': item.get('password', ''),
                    'http_compress': item.get('http_compress', False),
                    'ssl_show_warn': item.get('ssl_show_warn', False),
                    'use_ssl': item.get('use_ssl', True),
                    'verify_certs': item.get('verify_certs', True),
                    'auth_type': item.get('auth_type', 'none'),
                    'created_at': item.get('created_at'),
                    'updated_at': item.get('updated_at')
                }
                configs.append(config_summary)
            
            return configs
            
        except ClientError as e:
            logger.error(f"获取搜索引擎配置列表失败: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"获取搜索引擎配置列表时发生未知错误: {str(e)}")
            return []
    
    def update_search_engine_config(self, 
                                  config_id: str, 
                                  config_data: Dict[str, Any]) -> bool:
        """
        更新搜索引擎配置
        
        Args:
            config_id: 配置ID
            config_data: 更新的配置数据
            
        Returns:
            bool: 是否更新成功
        """
        try:
            # 先检查配置是否存在
            existing_config = self.get_search_engine_config(config_id)
            if not existing_config:
                logger.error(f"搜索引擎配置 {config_id} 不存在，无法更新")
                return False
            
            # 验证更新的配置数据
            if not self._validate_config_data(config_data, update=True):
                logger.error("更新的配置数据验证失败")
                return False
            
            current_time = datetime.utcnow().isoformat()
            
            # 构建更新表达式
            update_expression = "SET updated_at = :updated_at, version = version + :inc"
            expression_values = {
                ':updated_at': current_time,
                ':inc': 1
            }
            
            # 添加更新的字段
            for key, value in config_data.items():
                if key != 'config_id':  # 不允许更新配置ID
                    update_expression += f", {key} = :{key}"
                    expression_values[f":{key}"] = value
            
            # 执行更新
            self.table.update_item(
                Key={
                    'config_id': config_id
                },
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ReturnValues='UPDATED_NEW'
            )
            
            return True
            
        except ClientError as e:
            logger.error(f"更新搜索引擎配置失败: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"更新搜索引擎配置时发生未知错误: {str(e)}")
            return False
    
    def delete_search_engine_config(self, config_id: str) -> bool:
        """
        删除搜索引擎配置
        
        Args:
            config_id: 配置ID
            
        Returns:
            bool: 是否删除成功
        """
        try:
            # 执行删除
            self.table.delete_item(
                Key={
                    'config_id': config_id
                }
            )
            
            return True
            
        except ClientError as e:
            logger.error(f"删除搜索引擎配置失败: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"删除搜索引擎配置时发生未知错误: {str(e)}")
            return False
    
    def get_connection_params(self, config_id: str) -> Optional[Dict[str, Any]]:
        """
        获取搜索引擎连接参数
        
        Args:
            config_id: 配置ID
            
        Returns:
            Optional[Dict]: 连接参数，如果配置不存在则返回None
        """
        try:
            # 获取完整配置
            config = self.get_search_engine_config(config_id)
            if not config:
                return None
            
            # 提取连接参数
            connection_params = {
                'host': config.get('host'),
                'port': config.get('port'),
                'use_ssl': config.get('use_ssl'),
                'verify_certs': config.get('verify_certs'),
                'http_compress': config.get('http_compress'),
                'timeout': config.get('timeout')
            }
            
            # 添加认证信息
            auth_type = config.get('auth_type')
            if auth_type == 'basic':
                connection_params['credentials'] = (config.get('username'), config.get('password'))
            elif auth_type == 'api_key':
                connection_params['api_key'] = config.get('api_key')
            elif auth_type == 'aws_sigv4':
                connection_params['aws_sigv4'] = True
                connection_params['aws_region'] = config.get('aws_region')
                connection_params['aws_service'] = config.get('aws_service')
            
            return connection_params
            
        except Exception as e:
            logger.error(f"获取连接参数时发生错误: {str(e)}")
            return None
    
    def _validate_config_data(self, config_data: Dict[str, Any], update: bool = False) -> bool:
        """
        验证配置数据
        
        Args:
            config_data: 配置数据
            update: 是否为更新操作
            
        Returns:
            bool: 是否验证通过
        """
        # 对于新配置，必须包含类型和主机
        if not update and ('type' not in config_data or 'host' not in config_data):
            logger.error("新配置必须包含type和host字段")
            return False
        
        # 验证类型
        if 'type' in config_data and config_data['type'] not in ['elasticsearch', 'opensearch']:
            logger.error(f"不支持的搜索引擎类型: {config_data.get('type')}")
            return False
        
        # 验证端口
        if 'port' in config_data:
            try:
                port = int(config_data['port'])
                if port <= 0 or port > 65535:
                    logger.error(f"无效的端口号: {port}")
                    return False
            except (ValueError, TypeError):
                logger.error(f"端口必须是整数: {config_data.get('port')}")
                return False
        
        # 验证超时
        if 'timeout' in config_data:
            try:
                timeout = int(config_data['timeout'])
                if timeout <= 0:
                    logger.error(f"超时值必须大于0: {timeout}")
                    return False
            except (ValueError, TypeError):
                logger.error(f"超时值必须是整数: {config_data.get('timeout')}")
                return False
        
        return True


# 使用示例
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO)
    
    # 初始化搜索引擎配置客户端
    config_client = SearchEngineConfigClient(
        region='ap-northeast-1',
        table_name='log-search-engine-configs'
    )
    
    # 创建表
    if config_client.create_table_if_not_exists():
        print("搜索引擎配置表准备就绪")
        
        # 示例：保存OpenSearch配置
        opensearch_config = {
            'type': 'opensearch',
            'name': '生产环境OpenSearch',
            'description': '用于日志分析的OpenSearch集群',
            'host': 'opensearch.example.com',
            'port': 443,
            'use_ssl': True,
            'verify_certs': True,
            'username': 'admin',
            'password': 'secure_password'
        }
        
        config_id = config_client.save_search_engine_config(opensearch_config)
        if config_id:
            print(f"OpenSearch配置已保存，ID: {config_id}")
            
            # 获取连接参数
            connection_params = config_client.get_connection_params(config_id)
            print(f"连接参数: {connection_params}")

class DSLQueryClient:
    """DSL查询语句客户端类"""
    
    def __init__(self, 
                 region: str = 'ap-northeast-1',
                 table_name: str = 'log_query_samples'):
        """
        初始化DSL查询语句客户端
        
        Args:
            region: AWS区域
            table_name: DynamoDB表名
        """
        self.region = region
        self.table_name = table_name
        
        # 初始化DynamoDB资源和客户端
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.dynamodb_client = boto3.client('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)
    
    def create_table_if_not_exists(self) -> bool:
        """
        创建DSL查询语句表（如果不存在）
        
        Returns:
            bool: 表是否创建成功或已存在
        """
        try:
            # 检查表是否存在
            self.table.load()
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                # 表不存在，创建表
                try:
                    table = self.dynamodb.create_table(
                        TableName=self.table_name,
                        KeySchema=[
                            {
                                'AttributeName': 'query_id',
                                'KeyType': 'HASH'  # 分区键
                            }
                        ],
                        AttributeDefinitions=[
                            {
                                'AttributeName': 'query_id',
                                'AttributeType': 'S'
                            },
                            {
                                'AttributeName': 'data_source_id',
                                'AttributeType': 'S'
                            }
                        ],
                        GlobalSecondaryIndexes=[
                            {
                                'IndexName': 'DataSourceIndex',
                                'KeySchema': [
                                    {
                                        'AttributeName': 'data_source_id',
                                        'KeyType': 'HASH'
                                    }
                                ],
                                'Projection': {
                                    'ProjectionType': 'ALL'
                                },
                                'ProvisionedThroughput': {
                                    'ReadCapacityUnits': 5,
                                    'WriteCapacityUnits': 5
                                }
                            }
                        ],
                        BillingMode='PROVISIONED',
                        ProvisionedThroughput={
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    )
                    
                    # 等待表创建完成
                    table.wait_until_exists()
                    logger.info(f"表 {self.table_name} 创建成功")
                    return True
                    
                except ClientError as create_error:
                    logger.error(f"创建表失败: {str(create_error)}")
                    return False
            else:
                logger.error(f"检查表存在性时发生错误: {str(e)}")
                return False
        except Exception as e:
            logger.error(f"创建表时发生未知错误: {str(e)}")
            return False
    
    def save_dsl_query(self, 
                      data_source_id: str,
                      description: str,
                      dsl_query: str,
                      query_id: str = None,
                      tags: List[str] = None,
                      category: str = None,
                      log_field_metadata_index_name: str = None) -> Optional[str]:
        """
        保存DSL查询语句
        
        Args:
            data_source_id: 数据源ID
            description: 查询描述信息
            dsl_query: DSL查询语句
            query_id: 查询ID，如果为None则自动生成
            tags: 标签列表
            category: 查询类别
            log_field_metadata_index_name: log_field_metadata表中的索引名称
            
        Returns:
            Optional[str]: 查询ID，如果保存失败则返回None
        """
        try:
            # 验证必要参数
            if not data_source_id:
                logger.error("数据源ID不能为空")
                return None
            
            if not dsl_query:
                logger.error("DSL查询语句不能为空")
                return None
            
            current_time = datetime.utcnow().isoformat()
            
            # 如果没有提供查询ID，则生成一个
            if not query_id:
                query_id = f"query-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            
            # 准备要保存的项目
            item = {
                'query_id': query_id,
                'data_source_id': data_source_id,
                'description': description or '',
                'dsl_query': dsl_query,
                'created_at': current_time,
                'updated_at': current_time,
                'version': 1
            }
            
            # 添加可选字段
            if tags:
                item['tags'] = tags
            
            if category:
                item['category'] = category
                
            # 添加log_field_metadata索引名称
            if log_field_metadata_index_name:
                item['log_field_metadata_index_name'] = log_field_metadata_index_name
            else:
                # 如果未提供，默认使用data_source_id
                item['log_field_metadata_index_name'] = data_source_id
            
            # 保存到DynamoDB
            self.table.put_item(Item=item)
            
            return query_id
            
        except ClientError as e:
            logger.error(f"保存DSL查询语句失败: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"保存DSL查询语句时发生未知错误: {str(e)}")
            return None
    
    def get_dsl_query(self, query_id: str) -> Optional[Dict[str, Any]]:
        """
        获取特定DSL查询语句
        
        Args:
            query_id: 查询ID
            
        Returns:
            Optional[Dict]: 查询信息，如果不存在返回None
        """
        try:
            response = self.table.get_item(
                Key={
                    'query_id': query_id
                }
            )
            
            if 'Item' in response:
                logger.info(f"获取到DSL查询语句 {query_id}")
                return response['Item']
            else:
                logger.warning(f"DSL查询语句 {query_id} 不存在")
                return None
                
        except ClientError as e:
            logger.error(f"获取DSL查询语句失败: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"获取DSL查询语句时发生未知错误: {str(e)}")
            return None
    
    def list_dsl_queries(self, data_source_id: str = None, log_field_metadata_index_name: str = None) -> List[Dict[str, Any]]:
        """
        获取DSL查询语句列表
        
        Args:
            data_source_id: 数据源ID，如果提供则只返回该数据源的查询语句
            log_field_metadata_index_name: log_field_metadata表中的索引名称，如果提供则优先使用
            
        Returns:
            List[Dict]: 查询语句列表
        """
        try:
            # 如果提供了log_field_metadata_index_name，优先使用它进行过滤
            if log_field_metadata_index_name:
                # 使用扫描操作并过滤log_field_metadata_index_name
                response = self.table.scan(
                    FilterExpression='log_field_metadata_index_name = :index_name',
                    ExpressionAttributeValues={
                        ':index_name': log_field_metadata_index_name
                    }
                )
            elif data_source_id:
                # 使用全局二级索引查询特定数据源的查询语句
                response = self.table.query(
                    IndexName='DataSourceIndex',
                    KeyConditionExpression='data_source_id = :data_source_id',
                    ExpressionAttributeValues={
                        ':data_source_id': data_source_id
                    }
                )
            else:
                # 获取所有查询语句
                response = self.table.scan()
            
            queries = response.get('Items', [])
            
            return queries
            
        except ClientError as e:
            logger.error(f"获取DSL查询语句列表失败: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"获取DSL查询语句列表时发生未知错误: {str(e)}")
            return []
    
    def find_most_similar_query(self, user_query: str, data_source_id: str = None, log_field_metadata_index_name: str = None, bedrock_model=None) -> Optional[Dict[str, Any]]:
        """
        使用LLM agent找到与用户查询语义相似度最高的样本查询
        
        Args:
            user_query: 用户的查询字符串
            data_source_id: 数据源ID，如果提供则只在该数据源的查询中搜索
            log_field_metadata_index_name: log_field_metadata表中的索引名称，如果提供则优先使用
            bedrock_model: Bedrock模型实例，用于语义相似度比较
            
        Returns:
            Optional[Dict]: 最相似的查询样本，如果没有找到或发生错误则返回None
        """
        try:
            # 获取所有样本查询
            sample_queries = self.list_dsl_queries(data_source_id, log_field_metadata_index_name)
            
            if not sample_queries:
                logger.warning("没有找到任何样本查询")
                return None
            
            if len(sample_queries) == 1:
                logger.info("只有一个样本查询，直接返回")
                return sample_queries[0]
            
            # 如果没有提供bedrock_model，尝试创建一个默认的
            if bedrock_model is None:
                try:
                    from strands.models import BedrockModel
                    bedrock_model = BedrockModel(
                        model_id="anthropic.claude-3-sonnet-20240229-v1:0",
                        temperature=0.1,
                        region_name=self.region
                    )
                except Exception as e:
                    logger.error(f"无法创建Bedrock模型: {str(e)}")
                    # 如果无法使用LLM，返回第一个样本
                    return sample_queries[0]
            
            # 构建用于LLM比较的提示词
            sample_descriptions = []
            for i, query in enumerate(sample_queries):
                description = query.get('description', '无描述')
                sample_descriptions.append(f"{i+1}. {description}")
            
            samples_text = "\n".join(sample_descriptions)
            
            prompt = f"""请分析用户查询与以下样本查询的语义相似度，返回最相似的样本编号。

用户查询：{user_query}

样本查询列表：
{samples_text}

请仔细分析用户查询的意图，并与每个样本查询的描述进行语义比较。
只需要返回最相似样本的编号（1到{len(sample_queries)}之间的数字），不需要其他解释。

最相似的样本编号："""

            try:
                # 使用Bedrock模型进行语义相似度分析
                response = bedrock_model.invoke(prompt)
                response_text = str(response).strip()
                
                # 提取编号
                import re
                numbers = re.findall(r'\d+', response_text)
                if numbers:
                    selected_index = int(numbers[0]) - 1  # 转换为0基索引
                    if 0 <= selected_index < len(sample_queries):
                        logger.info(f"LLM选择了样本 {selected_index + 1}: {sample_queries[selected_index].get('description', '无描述')}")
                        return sample_queries[selected_index]
                    else:
                        logger.warning(f"LLM返回的索引 {selected_index + 1} 超出范围，返回第一个样本")
                        return sample_queries[0]
                else:
                    logger.warning("无法从LLM响应中提取有效编号，返回第一个样本")
                    return sample_queries[0]
                    
            except Exception as e:
                logger.error(f"LLM语义相似度分析失败: {str(e)}")
                # 如果LLM分析失败，返回第一个样本
                return sample_queries[0]
            
        except Exception as e:
            logger.error(f"查找最相似查询失败: {str(e)}")
            return None
    
    def update_dsl_query(self, 
                        query_id: str,
                        description: str = None,
                        dsl_query: str = None,
                        tags: List[str] = None,
                        category: str = None,
                        log_field_metadata_index_name: str = None) -> bool:
        """
        更新DSL查询语句
        
        Args:
            query_id: 查询ID
            description: 查询描述信息
            dsl_query: DSL查询语句
            tags: 标签列表
            category: 查询类别
            
        Returns:
            bool: 是否更新成功
        """
        try:
            # 先检查查询是否存在
            existing_query = self.get_dsl_query(query_id)
            if not existing_query:
                logger.error(f"DSL查询语句 {query_id} 不存在，无法更新")
                return False
            
            current_time = datetime.utcnow().isoformat()
            
            # 构建更新表达式
            update_expression = "SET updated_at = :updated_at, version = version + :inc"
            expression_values = {
                ':updated_at': current_time,
                ':inc': 1
            }
            
            # 添加更新的字段
            if description is not None:
                update_expression += ", description = :description"
                expression_values[':description'] = description
            
            if dsl_query is not None:
                update_expression += ", dsl_query = :dsl_query"
                expression_values[':dsl_query'] = dsl_query
            
            if tags is not None:
                update_expression += ", tags = :tags"
                expression_values[':tags'] = tags
            
            if category is not None:
                update_expression += ", category = :category"
                expression_values[':category'] = category
                
            if log_field_metadata_index_name is not None:
                update_expression += ", log_field_metadata_index_name = :log_field_metadata_index_name"
                expression_values[':log_field_metadata_index_name'] = log_field_metadata_index_name
            
            # 执行更新
            self.table.update_item(
                Key={
                    'query_id': query_id
                },
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ReturnValues='UPDATED_NEW'
            )
            
            return True
            
        except ClientError as e:
            logger.error(f"更新DSL查询语句失败: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"更新DSL查询语句时发生未知错误: {str(e)}")
            return False
    
    def delete_dsl_query(self, query_id: str) -> bool:
        """
        删除DSL查询语句
        
        Args:
            query_id: 查询ID
            
        Returns:
            bool: 是否删除成功
        """
        try:
            # 执行删除
            self.table.delete_item(
                Key={
                    'query_id': query_id
                }
            )
            
            return True
            
        except ClientError as e:
            logger.error(f"删除DSL查询语句失败: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"删除DSL查询语句时发生未知错误: {str(e)}")
            return False
    
    def search_dsl_queries_by_tags(self, tags: List[str]) -> List[Dict[str, Any]]:
        """
        根据标签搜索DSL查询语句
        
        Args:
            tags: 标签列表
            
        Returns:
            List[Dict]: 匹配的查询语句列表
        """
        try:
            # 使用扫描操作搜索
            filter_expression = None
            expression_values = {}
            
            for i, tag in enumerate(tags):
                if filter_expression is None:
                    filter_expression = f"contains(tags, :tag{i})"
                else:
                    filter_expression += f" OR contains(tags, :tag{i})"
                
                expression_values[f":tag{i}"] = tag
            
            if filter_expression:
                response = self.table.scan(
                    FilterExpression=filter_expression,
                    ExpressionAttributeValues=expression_values
                )
            else:
                response = self.table.scan()
            
            queries = response.get('Items', [])
            return queries
            
        except ClientError as e:
            logger.error(f"搜索DSL查询语句失败: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"搜索DSL查询语句时发生未知错误: {str(e)}")
            return []


# 使用示例
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO)
    
    # 初始化DSL查询语句客户端
    dsl_client = DSLQueryClient(
        region='us-east-1',
        table_name='dsl_query_samples'
    )
    
    # 创建表
    if dsl_client.create_table_if_not_exists():
        print("DSL查询语句表准备就绪")
        
        # 示例：保存DSL查询语句
        sample_dsl = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "message": "error"
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": "now-1d",
                                    "lt": "now"
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        query_id = dsl_client.save_dsl_query(
            data_source_id="opensearch-20240718123456",
            description="过去24小时内的错误日志查询",
            dsl_query=json.dumps(sample_dsl),
            tags=["error", "daily", "monitoring"],
            category="日志监控"
        )
        
        if query_id:
            print(f"DSL查询语句已保存，ID: {query_id}")
            
            # 获取查询语句
            query = dsl_client.get_dsl_query(query_id)
            print(f"查询描述: {query.get('description')}")
            print(f"查询语句: {query.get('dsl_query')}")

