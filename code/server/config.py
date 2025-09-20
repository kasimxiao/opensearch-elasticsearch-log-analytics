"""
配置文件
统一的全局配置管理
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ModelConfig:
    """模型配置数据类"""
    model_id: str
    region: str
    temperature: float = 0.1
    max_tokens: int = 4000
    top_p: float = 0.9
    name: str = ""
    provider: str = ""
    description: str = ""


# 全局配置字典
CONFIG = {
    # DynamoDB配置
    'DYNAMODB_REGION': 'ap-northeast-1',
    'DYNAMODB_DATASOURCE_TABLE': 'log_engine_configs',
    'DYNAMODB_METADATA_TABLE': 'log_field_metadata',
    'DYNAMODB_DSL_TABLE': 'log_query_samples',
    
    # 日志配置
    'LOG_LEVEL': 'INFO',
    'LOG_FORMAT': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    
    # 查询配置
    'DEFAULT_SEARCH_SIZE': 100,
    'MAX_SEARCH_SIZE': 1000,
    
    # 模型配置
    'MODEL_CONFIGS': {
        'claude_3_7_sonnet': ModelConfig(
            model_id='us.anthropic.claude-3-7-sonnet-20250219-v1:0',
            region='us-east-1',
            name='Claude 3.7 Sonnet',
            provider='Anthropic',
            description='最新的Claude 3.7 Sonnet模型'
        ),
        'claude_3_5_sonnet': ModelConfig(
            model_id='us.anthropic.claude-3-5-sonnet-20241022-v2:0',
            region='us-east-1',
            name='Claude 3.5 Sonnet v2',
            provider='Anthropic',
            description='Claude 3.5 Sonnet v2'
        )
    },
    'DEFAULT_MODEL': 'claude_3_5_sonnet'
}

# 环境特定配置覆盖
ENVIRONMENT_CONFIGS = {
    'development': {
        'LOG_LEVEL': 'DEBUG'
    },
    'production': {
        'LOG_LEVEL': 'WARNING'
    },
    'testing': {
        'LOG_LEVEL': 'DEBUG',
        'DYNAMODB_METADATA_TABLE': 'test_log_field_metadata',
        'DYNAMODB_DATASOURCE_TABLE': 'test_log_engine_configs',
        'DYNAMODB_DSL_TABLE': 'test_log_query_samples',
        'DEFAULT_SEARCH_SIZE': 10,
        'MAX_SEARCH_SIZE': 50
    }
}


def get_config(key: str = None, default=None):
    """获取配置值"""
    if key is None:
        return CONFIG
    return CONFIG.get(key, default)


def set_config(key: str, value):
    """设置配置值"""
    CONFIG[key] = value


def update_config(updates: Dict[str, Any]):
    """批量更新配置"""
    CONFIG.update(updates)


def get_model_config(model_name: Optional[str] = None) -> ModelConfig:
    """获取模型配置"""
    if model_name is None:
        model_name = CONFIG['DEFAULT_MODEL']
    
    model_configs = CONFIG['MODEL_CONFIGS']
    config = model_configs.get(model_name)
    
    if not config:
        config = model_configs.get(CONFIG['DEFAULT_MODEL'])
        if not config and model_configs:
            config = next(iter(model_configs.values()))
        elif not config:
            config = ModelConfig(
                model_id='us.anthropic.claude-3-7-sonnet-20250219-v1:0',
                region='us-east-1',
                name='Claude 3 Sonnet (Fallback)',
                provider='Anthropic'
            )
    return config


def get_dynamodb_config() -> Dict[str, Any]:
    """获取DynamoDB配置"""
    return {
        'region': CONFIG['DYNAMODB_REGION'],
        'datasource_table': CONFIG['DYNAMODB_DATASOURCE_TABLE'],
        'metadata_table': CONFIG['DYNAMODB_METADATA_TABLE'],
        'dsl_table': CONFIG['DYNAMODB_DSL_TABLE']
    }


def validate_config() -> Dict[str, Any]:
    """验证配置"""
    issues = []
    
    if CONFIG['DEFAULT_SEARCH_SIZE'] <= 0:
        issues.append('DEFAULT_SEARCH_SIZE 必须大于0')
    
    if CONFIG['MAX_SEARCH_SIZE'] < CONFIG['DEFAULT_SEARCH_SIZE']:
        issues.append('MAX_SEARCH_SIZE 不能小于 DEFAULT_SEARCH_SIZE')
    
    return {
        'valid': len(issues) == 0,
        'issues': issues
    }


def init_environment_config():
    """根据环境变量初始化配置"""
    env = os.getenv('ENVIRONMENT', 'development').lower()
    env_config = ENVIRONMENT_CONFIGS.get(env, {})
    CONFIG.update(env_config)


class ModelConfigManager:
    """模型配置管理器（兼容性包装）"""
    
    def get_model_config(self, model_name: Optional[str] = None) -> ModelConfig:
        return get_model_config(model_name)
    
    def list_available_models(self) -> List[Dict[str, Any]]:
        """列出所有可用的模型"""
        models = []
        model_configs = CONFIG['MODEL_CONFIGS']
        default_model = CONFIG['DEFAULT_MODEL']
        
        for name, config in model_configs.items():
            models.append({
                'name': name,
                'model_id': config.model_id,
                'display_name': config.name,
                'provider': config.provider,
                'description': config.description,
                'region': config.region,
                'is_default': name == default_model
            })
        return models


def get_model_config_manager() -> ModelConfigManager:
    """获取模型配置管理器（兼容性函数）"""
    return ModelConfigManager()


def get_model_config_path() -> str:
    """获取模型配置文件路径（已废弃）"""
    return ""


# 初始化环境配置
init_environment_config()

# 兼容性别名
config = type('Config', (), {
    'get_dynamodb_config': lambda: get_dynamodb_config(),
    'get_model_config': lambda model_name=None: get_model_config(model_name),
    'validate_config': lambda: validate_config(),
    'get_model_config_path': lambda: get_model_config_path(),
    **{k: v for k, v in CONFIG.items() if not k.startswith('MODEL_')}
})()
