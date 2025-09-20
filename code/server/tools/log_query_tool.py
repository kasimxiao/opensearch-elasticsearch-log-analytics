"""
日志查询工具模块
负责执行高级日志查询功能
"""

import json
import logging
import re
import sys
import os
from typing import Dict, Any, Optional
from datetime import datetime

# 添加父目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from opensearch_client import OpenSearchClient
from elasticsearch_client import ElasticsearchClient

# 导入重试处理器
try:
    from utils.retry_handler import retry_on_rate_limit
except ImportError:
    # 如果导入失败，创建一个空的装饰器
    def retry_on_rate_limit(max_retries=3, wait_time=15):
        def decorator(func):
            return func
        return decorator

logger = logging.getLogger(__name__)


class LogQueryTool:
    """日志查询工具类"""
    
    def __init__(self, model_config_manager, dynamodb_client, config_client, dsl_client, step_callback_system=None):
        """
        初始化日志查询工具
        
        Args:
            model_config_manager: 模型配置管理器
            dynamodb_client: DynamoDB客户端
            config_client: 搜索引擎配置客户端
            dsl_client: DSL查询客户端
            step_callback_system: 步骤回调系统（可选）
        """
        self.model_config_manager = model_config_manager
        self.dynamodb_client = dynamodb_client
        self.config_client = config_client
        self.dsl_client = dsl_client
        self.step_callback_system = step_callback_system
        
    def query_logs(self, query: str, semantic_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        高级日志查询工具，基于语义分析结果进行智能日志查询
        
        Args:
            query: 用户的原始查询字符串
            semantic_result: 语义分析结果字典
            
        Returns:
            Dict[str, Any]: 查询结果
        """
        try:
            # 参数验证 - 更宽松的验证
            if not query or not isinstance(query, str):
                return {
                    "success": False,
                    "error": "query参数必须是非空字符串",
                    "query": str(query) if query else None
                }
            
            query = query.strip()
            if not query:
                return {
                    "success": False,
                    "error": "query参数不能为空",
                    "query": query
                }
            
            # 处理semantic_result - 不能为空，必须是有效的字典
            if semantic_result is None:
                return {
                    "success": False,
                    "error": "semantic_result参数不能为None，请提供有效的语义分析结果",
                    "query": query,
                    "suggestion": "请先调用semantic_analysis工具获取有效的语义分析结果"
                }
            
            if not isinstance(semantic_result, dict):
                return {
                    "success": False,
                    "error": "semantic_result参数必须是字典类型",
                    "query": query,
                    "semantic_result": semantic_result,
                    "suggestion": "请确保semantic_result是有效的字典格式"
                }
            
            if not semantic_result:
                return {
                    "success": False,
                    "error": "semantic_result参数不能为空字典，必须包含有效的语义分析数据",
                    "query": query,
                    "suggestion": "请先调用semantic_analysis工具获取有效的语义分析结果"
                }
            
            # 提取原始查询和改写查询
            rewritten_query = semantic_result.get("rewritten_query", query)
            
            # 检查语义分析结果 - 更宽松的检查
            if not semantic_result.get("success", True):  # 默认为True
                return {
                    "success": False,
                    "error": "语义分析结果无效，请先进行成功的语义分析",
                    "query": query,
                    "semantic_result": semantic_result,
                    "suggestion": "请先调用semantic_analysis工具获取有效的语义分析结果"
                }
            
            # 验证必需字段 - 如果缺失则使用默认值
            if "time_range" not in semantic_result:
                semantic_result["time_range"] = {}
            if "entities" not in semantic_result:
                semantic_result["entities"] = {}
            if "intent_type" not in semantic_result:
                semantic_result["intent_type"] = "log_query"

            # 执行查询流程
            return self._execute_query_pipeline(rewritten_query, semantic_result)
            
        except Exception as e:
            logger.error(f"查询日志失败: {str(e)}")
            return {
                "success": False,
                "error": f"查询日志失败: {str(e)}",
                "rewritten_query": rewritten_query if 'rewritten_query' in locals() else query,
                "query": rewritten_query if 'rewritten_query' in locals() else query,
                "semantic_result": semantic_result if 'semantic_result' in locals() else None
            }
    
    
    def _execute_query_pipeline(self, query: str, semantic_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行查询管道
        
        Args:
            query: 查询字符串
            semantic_result: 语义分析结果
            
        Returns:
            Dict[str, Any]: 查询结果
        """
        try:
            # 查询可用索引
            self._emit_text("正在查询可用的日志索引列表...", "索引查询", "processing")
            
            available_indices = self.dynamodb_client.get_all_indices()
            if not available_indices:
                self._emit_text(
                    {"error": "未找到任何可用索引", "count": 0},
                    "索引查询",
                    "error"
                )
                
                return {
                    "success": False,
                    "error": "未找到任何可用的日志索引",
                    "rewritten_query": query,
                    "query": query,
                    "semantic_result": semantic_result
                }
            
            # 发送索引列表（JSON格式）
            self._emit_json(
                {
                    "count": len(available_indices),
                    "indices": available_indices
                },
                "索引查询",
                "success"
            )
            
            # 选择合适的索引
            self._emit_text("正在分析查询内容，选择最适合的索引...", "索引选择", "processing")
            
            selected_index = self._select_best_index(semantic_result)
            if not selected_index:
                self._emit_text(
                    {"error": "未找到合适的日志索引"},
                    "索引选择",
                    "error"
                )
                return {
                    "success": False,
                    "error": "未找到合适的日志索引",
                    "rewritten_query": query,
                    "query": query,
                    "semantic_result": semantic_result
                }
            
            # 发送选择结果
            self._emit_json(
                {
                    "selected_index": selected_index,
                    "reason": "基于查询内容和索引特征选择最适合的索引",
                    "available_count": len(available_indices)
                },
                "索引选择",
                "success"
            )

            # 继续执行其他步骤...
            return self._continue_query_execution(query, semantic_result, selected_index)
            
        except Exception as e:
            logger.error(f"执行查询管道失败: {str(e)}")
            raise
    
    def _continue_query_execution(self, query: str, semantic_result: Dict[str, Any], selected_index: str) -> Dict[str, Any]:
        """
        继续执行查询的完整逻辑
        
        Args:
            query: 查询字符串
            semantic_result: 语义分析结果
            selected_index: 选择的索引
            
        Returns:
            Dict[str, Any]: 查询结果
        """
        try:
            time_range = semantic_result.get("time_range", {})
            
            # 获取索引字段
            self._emit_text("正在查询索引字段信息和样本数据...", "字段查询", "processing")
            
            try:
                fields = self.dynamodb_client.get_index_fields(selected_index)
                if not fields:
                    logger.warning(f"索引 {selected_index} 没有字段信息")
                fields_prompt = self._convert_fields_str(fields) if fields else "暂无字段信息"
            except Exception as e:
                logger.error(f"获取索引字段信息失败: {str(e)}")
                return {
                    "success": False,
                    "error": f"获取索引字段信息失败: {str(e)}",
                    "rewritten_query": query,
                    "query": query,
                    "index_name": selected_index
                }

            # 字段信息和样本获取完成
            self._emit_json({
                "field_count": len(fields) if fields else 0,
                "main_fields": [field.get('field_name', '') for field in fields[:10]] if fields else []
            }, "字段查询", "success")

            
            # 获取索引描述信息
            try:
                index_description = self.dynamodb_client.get_index_description(selected_index)
                if not index_description:
                    logger.warning(f"索引 {selected_index} 没有描述信息")
                    index_description = ""
            except Exception as e:
                logger.warning(f"获取索引描述信息失败: {str(e)}")
                index_description = ""

            try:
                from decimal import Decimal
                def convert_decimal_to_serializable(obj):
                    """递归转换对象中的Decimal类型为可序列化的类型"""
                    if isinstance(obj, Decimal):
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

                # 首先获取所有样本查询
                self._emit_text("正在查询样本数据...", "样本选择", "processing")

                raw_samples = self.dsl_client.list_dsl_queries(log_field_metadata_index_name=selected_index)

                raw_samples = convert_decimal_to_serializable(raw_samples) if raw_samples else []

                # 如果有多个样本，使用Agent选择最相似的一个
                if len(raw_samples) > 1:
                    try:
                        most_similar_sample = self._select_most_similar_sample(query, raw_samples)
                        
                        if most_similar_sample:
                            # 只使用最相似的查询作为样本
                            raw_samples = [most_similar_sample]
                            logger.info(f"使用语义相似度选择了最相似的样本查询: {most_similar_sample.get('description', '无描述')}")
                        else:
                            logger.warning("语义相似度比较失败，使用所有样本")
                            
                    except Exception as e:
                        logger.error(f"语义相似度比较失败: {str(e)}，使用所有样本")
                
            except Exception as e:
                logger.error(f"获取查询样本失败: {str(e)}")
                samples_prompt = "暂无查询样本参考"

            # 字段信息和样本获取完成
            sample_description = "无描述"
            if raw_samples and isinstance(raw_samples, list) and len(raw_samples) > 0:
                # 如果raw_samples是列表，取第一个样本的描述
                first_sample = raw_samples[0]
                if isinstance(first_sample, dict):
                    sample_description = first_sample.get('description', '无描述')
            elif isinstance(raw_samples, dict):
                # 如果raw_samples是字典，直接获取描述
                sample_description = raw_samples.get('description', '无描述')
            
            self._emit_json({
                "sample_describe": sample_description
            }, "样本选择", "success")
            
            samples_prompt = self._convert_samples_str(raw_samples)

            
            # 获取搜索引擎配置
            self._emit_text("正在查询索引的搜索引擎配置...", "配置查询", "processing")
            
            config = self._get_search_engine_config(selected_index)
            if not config:
                self._emit_text({
                    "error": f"未找到索引 {selected_index} 的搜索引擎配置"
                }, "配置查询", "error")
                return {
                    "success": False,
                    "error": f"未找到索引 {selected_index} 的搜索引擎配置",
                    "rewritten_query": query,
                    "query": query,
                    "index_name": selected_index,
                    "suggestion": "请检查搜索引擎配置，或联系管理员确认配置信息"
                }
            
            # 搜索引擎配置获取完成
            self._emit_json({
                "index_config": config
            }, "配置查询", "success")
            
            # 步骤6: 生成和执行查询DSL（错误重试5次，空结果重试3次）
            search_results = ''
            dsl_query = ''
            error_prompt = ''
            last_error = ''
            query_prompt = query
            error_retry_count = 0  # 错误重试计数器
            empty_retry_count = 0  # 空结果重试计数器
            max_error_retries = 5  # 错误最大重试次数
            max_empty_retries = 3  # 空结果最大重试次数
            
            # 生成DSL查询语句
            self._emit_text("正在生成Elasticsearch DSL查询语句...", "DSL生成", "processing")
            
            while True:
                try:
                    # 生成查询DSL
                    last_query = dsl_query
                    error_prompt = ''
                    if len(last_error) > 0:
                        error_prompt = f"Error message:{last_error}\nProblematic DSL:{last_query}"
                        
                    dsl_query = self._generate_intelligent_query_dsl(
                        query_prompt, fields_prompt, samples_prompt, time_range, error_prompt, index_description
                    )
                    
                    # 执行查询 - 使用引擎检测创建客户端
                    search_client = self._create_search_client(config)
                    search_results = search_client.execute_search(
                        index_name=selected_index,
                        query=dsl_query,
                        output_format='simplified'
                    )
                    
                    logger.info(f"search_results:{search_results}")
                    # 检查查询结果
                    if search_results:
                        # 检查search_results是否包含错误信息
                        has_error = False
                        if isinstance(search_results, dict):
                            has_error = search_results.get("error") is not None or search_results.get("status", 0) >= 400
                        
                        if not has_error:
                            # 检查是否为空结果
                            total_hits = search_results['hits']['total']['value']
                            if total_hits == 0:
                                # 空结果，检查是否需要重试
                                empty_retry_count += 1
                                if empty_retry_count <= max_empty_retries:
                                    last_error = f"查询结果为空，第{empty_retry_count}次重试"
                                    logger.warning(f"查询结果为空，尝试次数: {empty_retry_count}/{max_empty_retries}")
                                    self._emit_text(
                                        f"查询结果为空，正在重试 ({empty_retry_count}/{max_empty_retries})",
                                        "DSL生成",
                                        "processing"
                                    )
                                    continue
                                else:
                                    # 空结果重试次数已达上限
                                    self._emit_text(f"查询结果为空，已重试{max_empty_retries}次", "DSL生成", "error")
                            
                            # 查询成功（有结果或空结果重试已达上限）
                            self._emit_json({"dsl_query": dsl_query, "total_hits": total_hits, "query_result": f"成功执行DSL查询，返回{total_hits}条结果"}, "DSL生成", "success")
                            
                            # 对search_results进行格式优化
                            results_prompt = self._optimize_search_results(search_results)
                            
                            break
                        else:
                            # 查询执行失败，检查是否需要重试
                            error_retry_count += 1
                            error_msg = search_results.get("error", "未知错误")
                            last_error = f"查询执行失败: {error_msg}"
                            
                            if error_retry_count <= max_error_retries:
                                logger.warning(f"查询执行失败，尝试次数: {error_retry_count}/{max_error_retries}, 错误: {error_msg}")
                                self._emit_text(
                                    f"查询执行失败，正在重试 ({error_retry_count}/{max_error_retries}): {error_msg}",
                                    "DSL生成",
                                    "processing"
                                )
                                continue
                            else:
                                # 错误重试次数已达上限
                                self._emit_text(f"查询执行失败，已重试{max_error_retries}次: {error_msg}", "DSL生成", "error")
                                return {
                                    "success": False,
                                    "error": f"查询执行失败，已重试{max_error_retries}次: {error_msg}",
                                    "rewritten_query": query,
                                    "query": query,
                                    "index_name": selected_index,
                                    "dsl_query": dsl_query,
                                    "semantic_result": semantic_result
                                }
                    else:
                        # search_results为空，视为错误情况
                        error_retry_count += 1
                        last_error = "查询返回空结果"
                        
                        if error_retry_count <= max_error_retries:
                            logger.warning(f"查询返回空结果，尝试次数: {error_retry_count}/{max_error_retries}")
                            self._emit_text(
                                f"查询返回空结果，正在重试 ({error_retry_count}/{max_error_retries})",
                                "DSL生成",
                                "processing"
                            )
                            continue
                        else:
                            # 错误重试次数已达上限
                            self._emit_text(f"查询返回空结果，已重试{max_error_retries}次", "DSL生成", "error")
                            return {
                                "success": False,
                                "error": f"查询返回空结果，已重试{max_error_retries}次",
                                "rewritten_query": query,
                                "query": query,
                                "index_name": selected_index,
                                "dsl_query": dsl_query,
                                "semantic_result": semantic_result
                            }
                    
                except Exception as e:
                    # 异常情况，视为错误
                    error_retry_count += 1
                    last_error = str(e)
                    
                    if error_retry_count <= max_error_retries:
                        logger.warning(f"查询异常，尝试次数: {error_retry_count}/{max_error_retries}, 错误: {last_error}")
                        self._emit_text(
                            f"查询异常，正在重试 ({error_retry_count}/{max_error_retries}): {last_error}",
                            "DSL生成",
                            "processing"
                        )
                        continue
                    else:
                        # 错误重试次数已达上限
                        self._emit_text(f"查询异常，已重试{max_error_retries}次: {last_error}", "DSL生成", "error")
                        return {
                            "success": False,
                            "error": f"查询异常，已重试{max_error_retries}次: {last_error}",
                            "rewritten_query": query,
                            "query": query,
                            "index_name": selected_index,
                            "dsl_query": dsl_query,
                            "semantic_result": semantic_result
                        }
            
            try:
                self._emit_text("正在生成图表...", "图表生成", "processing")
                # 执行查询并生成图表
                chart_data = self._generate_intelligent_chart_data(results_prompt, query_prompt)

                if chart_data:
                    self._emit_chart(
                        chart_data,
                        "图表生成",
                        "success"
                    )
                else:
                     self._emit_text(f"未生成图表", "图表生成", "success")


            except Exception as e:
                logger.error(f"生成图表数据失败: {str(e)}")
                return {
                    "success": False,
                    "error": f"生成图表数据失败: {str(e)}",
                    "rewritten_query": query,
                    "query": query,
                    "index_name": selected_index,
                    "search_results": search_results,
                    "semantic_result": semantic_result
                }
            
            # 生成分析报告
            try:
                self._emit_text("正在生成综合分析...", "综合分析", "processing")
                analysis = self._generate_intelligent_analysis(results_prompt, chart_data, query_prompt, semantic_result)
                self._emit_json({
                    analysis},
                    "综合分析",
                    "success"
                )
            except Exception as e:
                logger.error(f"生成分析报告失败: {str(e)}")
                return {
                    "success": False,
                    "error": f"生成分析报告失败: {str(e)}",
                    "rewritten_query": query,
                    "query": query,
                    "index_name": selected_index,
                    "search_results": search_results,
                    "chart_data": chart_data,
                    "semantic_result": semantic_result
                }

            # 构建最终响应
            # response = {
            #     "success": True,
            #     "rewritten_query": query,
            #     "query": query,
            #     "semantic_result": semantic_result,
            #     "time_range": time_range,
            #     "index_name": selected_index,
            #     "dsl_query": dsl_query,
            #     "total_hits": search_results.get("total", 0),
            #     "hits": search_results.get("documents", [])[:10],  # 只返回前10条结果
            #     "chart_data": chart_data,
            #     "analysis": analysis,
            #     "response": self._generate_final_response(query, search_results, chart_data, analysis, time_range)
            # }
            response = {
                "success": True,
                "response": analysis
            }
            
            return response
            
        except Exception as e:
            logger.error(f"继续执行查询失败: {str(e)}")
            return {
                "success": False,
                "error": f"查询执行失败: {str(e)}",
                "rewritten_query": query,
                "query": query,
                "semantic_result": semantic_result
            }
    
    def _select_best_index(self, semantic_result: Dict[str, Any]) -> Optional[str]:
        """
        基于语义分析结果选择最合适的索引
        
        Args:
            semantic_result: 语义分析结果
            
        Returns:
            Optional[str]: 选择的索引名称，如果没有找到合适的索引则返回None
        """
        try:
            # 获取所有可用索引
            indices = self.dynamodb_client.get_all_indices()
            if not indices:
                logger.error("没有找到任何可用索引")
                return None
            
            # 提取语义分析中的关键信息
            entities = semantic_result.get("entities", {})
            log_type = entities.get("log_type", "")
            keywords = entities.get("keywords", [])
            query = semantic_result.get("query", "")
            rewritten_query = semantic_result.get("rewritten_query", query)
            
            # 构建索引选择提示词
            index_selection_prompt = f"""
            你是一个专业的日志分析专家，需要根据用户查询意图选择最合适的索引。

            用户查询: {query}
            日志类型: {log_type}
            关键词: {keywords}

            可用索引列表:
            {json.dumps(indices, indent=2, ensure_ascii=False)}

            请分析用户查询意图，选择最合适的索引。考虑以下因素：
            1. 索引名称与查询内容的匹配度
            2. 日志类型的相关性（AWS服务、应用程序、系统组件等）
            3. 关键词的匹配程度
            4. 索引名称中的时间范围或数据类型信息

            索引选择规则：
            - AWS服务日志匹配：根据查询中的AWS服务名称选择对应索引（如cloudfront、waf、alb、lambda、s3、rds、ec2等）
            - 应用程序日志匹配：根据应用类型、组件名称或业务模块选择相应索引
            - 日志类型匹配：根据日志性质选择索引（如access、error、audit、performance、security等）
            - 时间范围匹配：优先选择时间范围与查询需求相符的索引
            - 关键词权重匹配：综合考虑索引名称、描述中的关键词与查询内容的相关性
            - 数据源匹配：根据数据来源（如不同环境、不同系统）选择对应索引

            请严格按照以下JSON格式返回结果：
            {{
                "selected_index": "选择的索引名称",
                "confidence": 置信度(0-1),
                "reason": "选择理由，说明为什么选择这个索引"
            }}
            """
            
            # 获取索引选择任务的模型配置（使用中等性能模型）
            selection_config = self.model_config_manager.get_model_config('claude_3_5_sonnet')
            
            # 创建专门的索引选择模型，不使用工具
            from strands.models import BedrockModel
            from strands import Agent
            
            selection_model = BedrockModel(
                model_id=selection_config.model_id,
                temperature=selection_config.temperature,
                region_name=selection_config.region
            )
            
            # 创建简单的Agent进行索引选择
            selection_agent = Agent(
                system_prompt="你是一个专业的索引选择助手，根据用户查询选择最合适的日志索引。",
                model=selection_model
            )
            
            @retry_on_rate_limit(max_retries=3, wait_time=15)
            def call_selection_agent():
                return selection_agent(index_selection_prompt)
            
            response = call_selection_agent()
            response_text = str(response)
            
            # 解析AI响应
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                # 清理可能的注释
                json_str = re.sub(r'//.*?\n', '\n', json_str)
                json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
                
                result = json.loads(json_str)
                
                selected_index = result.get("selected_index")
                confidence = result.get("confidence", 0)
                reason = result.get("reason", "")
                
                # 验证选择的索引是否存在
                if selected_index in indices:
                    return selected_index
                else:
                    logger.warning(f"AI选择的索引 {selected_index} 不在可用索引列表中")
            
            # 如果AI选择失败，直接返回None，不使用备选逻辑
            logger.error("AI索引选择失败，未找到合适的索引")
            
            return None
            
        except json.JSONDecodeError as e:
            logger.error(f"索引选择JSON解析失败: {str(e)}")
            return None
            
        except Exception as e:
            logger.error(f"选择索引失败: {str(e)}")
            return None
    
    def _select_most_similar_sample(self, user_query: str, samples: list) -> Optional[Dict[str, Any]]:
        """
        基于用户查询选择最相似的样本查询（简化版本，只针对description进行相似度匹配）
        
        Args:
            user_query: 用户的查询字符串
            samples: 样本查询列表
            
        Returns:
            Optional[Dict[str, Any]]: 最相似的样本查询，如果没有找到合适的样本则返回None
        """
        try:
            if not samples or len(samples) <= 1:
                return samples[0] if samples else None
            
            # 构建简化的样本选择提示词，只使用description
            descriptions = []
            for i, sample in enumerate(samples):
                description = sample.get('description', '无描述')
                descriptions.append(f"{i}: {description}")
            
            similarity_selection_prompt = f"""
            根据用户查询选择最相似的样本描述。

            用户查询: {user_query}

            样本描述列表:
            {chr(10).join(descriptions)}

            请选择与用户查询最相似的样本描述，只需要返回数组序号（从0开始）。

            返回格式：只返回一个数字，例如：0
            """
            
            # 获取样本选择任务的模型配置（使用中等性能模型）
            selection_config = self.model_config_manager.get_model_config('claude_3_5_sonnet')
            
            # 创建专门的样本选择模型，不使用工具
            from strands.models import BedrockModel
            from strands import Agent
            
            selection_model = BedrockModel(
                model_id=selection_config.model_id,
                temperature=0.1,  # 降低温度以获得更确定的结果
                region_name=selection_config.region
            )
            
            # 创建简单的Agent进行样本选择
            selection_agent = Agent(
                system_prompt="你是一个专业的相似度匹配助手，根据用户查询选择最相似的样本描述。",
                model=selection_model
            )
            
            @retry_on_rate_limit(max_retries=3, wait_time=15)
            def call_selection_agent():
                return selection_agent(similarity_selection_prompt)
            
            response = call_selection_agent()
            response_text = str(response).strip()
            
            # 尝试解析响应中的数字
            import re
            number_match = re.search(r'\b(\d+)\b', response_text)
            if number_match:
                selected_index = int(number_match.group(1))
                
                # 验证选择的样本索引是否有效
                if 0 <= selected_index < len(samples):
                    selected_sample = samples[selected_index]
                    logger.info(f"AI选择了样本{selected_index}: {selected_sample.get('description', '无描述')}")
                    return selected_sample
                else:
                    logger.warning(f"AI选择的样本索引 {selected_index} 超出范围")
            
            # 如果AI选择失败，返回第一个样本
            logger.warning("AI样本选择失败，返回第一个样本")
            return samples[0]
            
        except Exception as e:
            logger.error(f"选择最相似样本失败: {str(e)}")
            return samples[0] if samples else None
    
    def _extract_query_info(self, dsl_obj: Dict[str, Any]) -> str:
        """
        从DSL查询对象中提取关键信息
        
        Args:
            dsl_obj: DSL查询对象
            
        Returns:
            str: 查询特征描述
        """
        try:
            features = []
            
            # 提取查询类型
            if 'query' in dsl_obj:
                query_part = dsl_obj['query']
                if 'bool' in query_part:
                    features.append("布尔查询")
                elif 'match' in query_part:
                    features.append("匹配查询")
                elif 'range' in query_part:
                    features.append("范围查询")
                elif 'term' in query_part:
                    features.append("精确查询")
            
            # 提取聚合信息
            if 'aggs' in dsl_obj or 'aggregations' in dsl_obj:
                aggs = dsl_obj.get('aggs', dsl_obj.get('aggregations', {}))
                agg_types = []
                for agg_name, agg_config in aggs.items():
                    if isinstance(agg_config, dict):
                        for agg_type in agg_config.keys():
                            if agg_type not in ['aggs', 'aggregations']:
                                agg_types.append(agg_type)
                if agg_types:
                    features.append(f"聚合({', '.join(agg_types)})")
            
            # 提取排序信息
            if 'sort' in dsl_obj:
                features.append("排序")
            
            # 提取大小限制
            if 'size' in dsl_obj:
                features.append(f"限制{dsl_obj['size']}条")
            
            return ', '.join(features) if features else "基础查询"
            
        except Exception as e:
            logger.error(f"提取查询信息失败: {str(e)}")
            return "查询信息提取失败"

    
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
    
    def _convert_fields_str(self, fields_data):
        """提取必要信息并格式化字段数据"""
        field_info_list = []
        for field in fields_data:
            name = field['field_name']
            field_type = field['field_type']
            description = field['description'].strip()
            
            # 只有当描述不为空时才添加
            if description:
                field_info_list.append(f"{name} ({field_type}): {description}")
            else:
                field_info_list.append(f"{name} ({field_type})")
        
        # 按字段名称排序
        field_info_list.sort()
        
        # 将列表转换为字符串
        field_info = "\n".join(field_info_list)
        
        return field_info
    
    def _convert_samples_str(self, samples: list) -> str:
        """将 samples 数据格式化为易读的文本格式，用于 DSL 生成参考"""
        if not samples:
            return "暂无查询样本参考"
        
        formatted_samples = []
        
        for sample in samples:
            description = sample.get('description', '无描述')
            dsl_query = sample.get('dsl_query', '{}')
            
            # 格式化 DSL 查询，使其更易读
            try:
                # 尝试解析和重新格式化 JSON
                if isinstance(dsl_query, str):
                    dsl_obj = json.loads(dsl_query)
                    dsl_formatted = json.dumps(dsl_obj, indent=2, ensure_ascii=False)
                else:
                    dsl_formatted = json.dumps(dsl_query, indent=2, ensure_ascii=False)
            except (json.JSONDecodeError, TypeError):
                # 如果解析失败，使用原始字符串
                dsl_formatted = str(dsl_query)
            
            # 构建格式化的样本文本
            sample_text = f"query:{description}\nDSL:{dsl_formatted}"
            formatted_samples.append(sample_text)
        
        # 直接返回拼接的样本信息
        return "\n\n".join(formatted_samples)
    
    def _get_search_engine_config(self, index_name: str) -> Optional[Dict[str, Any]]:
        """获取指定索引的搜索引擎配置"""
        try:
            # 获取所有搜索引擎配置
            configs = self.config_client.list_search_engine_configs()
            if not configs:
                logger.warning("没有找到任何搜索引擎配置")
                return None
            
            # 尝试找到与索引名称匹配的配置
            for config in configs:
                config_name = config.get('name', '').lower()
                config_description = config.get('description', '').lower()
                
                # 检查配置名称或描述是否包含索引名称
                if (index_name.lower() in config_name or 
                    index_name.lower() in config_description):
                    return config
            
            # 如果没有找到匹配的配置，返回第一个配置
            return configs[0]
            
        except Exception as e:
            logger.error(f"获取搜索引擎配置失败: {str(e)}")
            return None
    
    def _detect_search_engine_type(self, config: Dict[str, Any]) -> str:
        """
        检测搜索引擎类型
        
        Args:
            config: 搜索引擎配置
            
        Returns:
            str: 引擎类型 ('opensearch' 或 'elasticsearch')
        """
        try:
            # 检查配置中的引擎类型字段
            engine_type = config.get('engine_type', '').lower()
            if engine_type in ['opensearch', 'elasticsearch']:
                return engine_type
            
            # 根据主机名或其他配置信息推断引擎类型
            host = config.get('host', '').lower()
            
            # OpenSearch 特征
            if any(keyword in host for keyword in ['opensearch', 'aoss', 'es.amazonaws.com']):
                return 'opensearch'
            
            # Elasticsearch 特征
            if any(keyword in host for keyword in ['elasticsearch', 'elastic.co', 'es.elastic-cloud.com']):
                return 'elasticsearch'
            
            # 检查端口号（通常OpenSearch使用443，Elasticsearch使用9200或9243）
            port = config.get('port', 443)
            if port == 9200 or port == 9243:
                return 'elasticsearch'
            
            # 默认返回opensearch（向后兼容）
            logger.warning(f"无法确定搜索引擎类型，默认使用opensearch")
            return 'opensearch'
            
        except Exception as e:
            logger.error(f"检测搜索引擎类型失败: {str(e)}")
            return 'opensearch'  # 默认返回opensearch
    
    def _create_search_client(self, config: Dict[str, Any]) -> Any:
        """
        根据配置创建相应的搜索引擎客户端
        
        Args:
            config: 搜索引擎配置
            
        Returns:
            搜索引擎客户端实例
        """
        try:
            engine_type = self._detect_search_engine_type(config)
            
            if engine_type == 'elasticsearch':
                logger.info(f"创建Elasticsearch客户端")
                return ElasticsearchClient(config_data=config)
            else:
                logger.info(f"创建OpenSearch客户端")
                return OpenSearchClient(config_data=config)
                
        except Exception as e:
            logger.error(f"创建搜索引擎客户端失败: {str(e)}")
            raise
    
    def _optimize_search_results(self, search_results: Dict[str, Any]) -> Dict[str, Any]:
        """优化搜索结果格式，屏蔽系统字段，使查询结果更加精简易读"""
        try:
            if not search_results or not isinstance(search_results, dict):
                return search_results
            
            # 创建优化后的结果副本
            optimized_results = search_results.copy()
            
            # 优化文档列表
            if 'documents' in optimized_results and isinstance(optimized_results['documents'], list):
                optimized_documents = []
                
                for doc in optimized_results['documents']:
                    if not isinstance(doc, dict):
                        continue
                    
                    optimized_doc = {}
                    
                    # 保留文档ID（如果存在且有意义）
                    if 'id' in doc and doc['id'] and not doc['id'].startswith('_'):
                        optimized_doc['id'] = doc['id']
                    
                    # 优化文档数据
                    if 'data' in doc and isinstance(doc['data'], dict):
                        optimized_data = self._clean_document_data(doc['data'])
                        if optimized_data:
                            optimized_doc['data'] = optimized_data
                    
                    # 保留评分（如果有意义）
                    if 'score' in doc and isinstance(doc['score'], (int, float)) and doc['score'] > 0:
                        optimized_doc['score'] = round(doc['score'], 2)
                    
                    if optimized_doc:
                        optimized_documents.append(optimized_doc)
                
                optimized_results['documents'] = optimized_documents
            
            return optimized_results
            
        except Exception as e:
            logger.error(f"优化搜索结果失败: {str(e)}")
            return search_results  # 返回原始结果
    
    def _clean_document_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """清理文档数据，移除系统字段和无用信息"""
        if not isinstance(data, dict):
            return data
        
        cleaned_data = {}
        
        # 定义需要排除的系统字段和无用字段
        excluded_fields = {
            '_id', '_index', '_type', '_score', '_version', '_seq_no', '_primary_term',
            '_routing', '_parent', '_timestamp', '_ttl', '_size', '_uid', '_all',
            'sort', 'highlight', 'matched_queries', 'inner_hits',
            'analyzed_field', 'keyword_field', 'raw_log', 'original', 'raw',
            'keyword', 'analyzed', 'not_analyzed', 'fields'
        }
        
        # 定义常见的日志字段优先级（优先显示的字段）
        priority_fields = {
            'timestamp', 'time', '@timestamp', 'datetime', 'date',
            'level', 'severity', 'priority', 'status', 'code',
            'message', 'msg', 'content', 'text', 'description',
            'source', 'host', 'hostname', 'ip', 'client_ip', 'remote_addr',
            'method', 'url', 'path', 'endpoint', 'api',
            'user', 'username', 'user_id', 'account',
            'error', 'exception', 'stack_trace', 'error_message'
        }
        
        # 首先添加优先字段
        for field in priority_fields:
            if field in data and field not in excluded_fields:
                value = self._clean_field_value(data[field])
                if value is not None:
                    cleaned_data[field] = value
        
        # 然后添加其他字段
        for key, value in data.items():
            if (key not in excluded_fields and 
                not key.startswith('_') and 
                key not in cleaned_data):  # 避免重复添加
                
                cleaned_value = self._clean_field_value(value)
                if cleaned_value is not None:
                    cleaned_data[key] = cleaned_value
        
        return cleaned_data
    
    def _clean_field_value(self, value):
        """清理字段值，简化复杂数据结构"""
        if value is None or value == '':
            return None
        
        # 字符串处理
        if isinstance(value, str):
            cleaned = value.strip()
            # 移除过长的字符串
            if len(cleaned) > 500:
                cleaned = cleaned[:497] + '...'
            return cleaned if cleaned else None
        
        # 数字和布尔值直接返回
        elif isinstance(value, (int, float, bool)):
            return value
        
        # 字典处理 - 递归清理但限制深度
        elif isinstance(value, dict):
            if len(value) > 15:  # 限制字段数量
                return f"[复杂对象: {len(value)}个字段]"
            
            cleaned_dict = {}
            for k, v in value.items():
                if not k.startswith('_'):
                    cleaned_v = self._clean_field_value(v)
                    if cleaned_v is not None:
                        cleaned_dict[k] = cleaned_v
            return cleaned_dict if cleaned_dict else None
        
        # 列表处理 - 限制长度
        elif isinstance(value, list):
            if len(value) > 5:  # 限制列表长度
                cleaned_items = []
                for item in value[:5]:
                    cleaned_item = self._clean_field_value(item)
                    if cleaned_item is not None:
                        cleaned_items.append(cleaned_item)
                if cleaned_items:
                    cleaned_items.append(f"...还有{len(value)-5}项")
                return cleaned_items
            else:
                cleaned_items = []
                for item in value:
                    cleaned_item = self._clean_field_value(item)
                    if cleaned_item is not None:
                        cleaned_items.append(cleaned_item)
                return cleaned_items if cleaned_items else None
        
        # 其他类型转换为字符串
        else:
            str_value = str(value)
            if len(str_value) > 200:
                str_value = str_value[:197] + '...'
            return str_value
    
    def _generate_intelligent_query_dsl(self, query_prompt: str, fields_prompt: str, 
                                      samples_prompt: str, time_range: Dict[str, str], 
                                      error_prompt: str, index_description: str = ""):
        """
        使用Bedrock Claude生成智能查询DSL
        
        Args:
            query_prompt (str): 用户的查询提示词，包含用户的原始查询意图和需求
            fields_prompt (str): 索引字段信息提示词，包含目标索引的所有字段名称、类型和描述信息
            samples_prompt (str): 样本查询提示词，包含相关的DSL查询示例，用于参考和学习
            time_range (Dict[str, str]): 时间范围字典，包含查询的开始时间和结束时间
                - start_time: 查询开始时间，格式为 YYYY-MM-DD HH:MM:SS
                - end_time: 查询结束时间，格式为 YYYY-MM-DD HH:MM:SS
            error_prompt (str): 错误信息提示词，包含之前查询失败的错误信息和有问题的DSL，用于错误修正
            index_description (str): 索引描述信息，包含索引的用途、数据来源、业务含义等描述
        
        Returns:
            str: 生成的Elasticsearch/OpenSearch DSL查询语句
        """
        try:
            from strands import Agent
            from strands.models import BedrockModel
            
            dsl_prompt = f"""
You are an expert Elasticsearch/OpenSearch query specialist tasked with generating precise DSL query statements based on user intent.

USER QUERY:
{query_prompt}

INDEX DESCRIPTION:
{index_description if index_description else "暂无索引描述信息"}

TIME RANGE:
- Start time: {time_range.get('start_time')}
- End time: {time_range.get('end_time')}

INDEX FIELD INFORMATION:
{fields_prompt}

REFERENCE EXAMPLES:
{samples_prompt}

ERROR INFORMATION (if applicable):
{error_prompt}
            
CRITICAL RULES:
1. ALWAYS append .keyword to ALL text fields in aggregations, sorting, and term-level queries
2. ONLY use range aggregation with numeric and date fields, NEVER with keyword/text fields
3. For categorical field ranges (status codes, etc.), use filters aggregation with term/prefix queries
4. Include time range filtering using timestamp field (YYYY-MM-DD HH:MM:SS format)
5. NEVER aggregate on text fields without .keyword suffix

FIELD TYPE COMPATIBILITY:
- text fields: Always add .keyword suffix for all operations
- keyword fields: Use terms/filters aggregations (not range aggregation)
- numeric fields: Can use range queries and range aggregations
- date fields: Format as YYYY-MM-DD HH:MM:SS in queries

CATEGORICAL FIELD HANDLING:
For categorical fields like status codes, response types, etc:
- Terms aggregation: {{"terms": {{"field": "field_name", "size": 10}}}}
- For grouping by patterns/prefixes: {{"filters":{{"filters":{{"group1":{{"prefix":{{"field_name":"prefix1"}}}},"group2":{{"prefix":{{"field_name":"prefix2"}}}}}}}}}}

VERIFICATION CHECKLIST:
1. ✓ All text fields have .keyword suffix in all operations
2. ✓ Range aggregations only used on numeric/date fields
3. ✓ Categorical fields handled with terms/filters, not range aggregation
4. ✓ Timestamp field correctly used with proper format
5. ✓ Each aggregation type only used with compatible field types

Generate only the complete, executable DSL query without explanations or comments.
            """

            logger.info(f"dsl_prompt：{dsl_prompt}")
            # 获取DSL生成任务的模型配置（使用高性能模型）
            dsl_config = self.model_config_manager.get_model_config('claude_3_7_sonnet')
            
            # 创建专门的DSL生成模型，不使用工具
            dsl_model = BedrockModel(
                model_id=dsl_config.model_id,
                temperature=dsl_config.temperature,
                region_name=dsl_config.region
            )
            
            # 创建简单的Agent进行DSL生成
            dsl_agent = Agent(
                system_prompt="你是一个专业的Elasticsearch/OpenSearch DSL查询生成专家。",
                model=dsl_model
            )
            
            @retry_on_rate_limit(max_retries=3, wait_time=15)
            def call_dsl_agent():
                return dsl_agent(dsl_prompt)
            
            response = call_dsl_agent()
            response_text = str(response)
            
            # 解析AI响应
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                # 清理可能的注释
                json_str = re.sub(r'//.*?\n', '\n', json_str)
                json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
                
                dsl_query = json.loads(json_str)
                return dsl_query
            else:
                raise Exception("AI响应中未找到有效的JSON格式DSL查询")
            
        except json.JSONDecodeError as e:
            logger.error(f"DSL JSON解析失败: {str(e)}")
            raise Exception(f"生成的DSL查询JSON格式错误: {str(e)}")
        except Exception as e:
            logger.error(f"生成智能DSL查询失败: {str(e)}")
            raise Exception(f"DSL查询生成失败: {str(e)}")
    
    def _generate_intelligent_chart_data(self, results_prompt: str, query_prompt: str) -> Dict[str, Any]:
        """使用Bedrock Claude生成智能图表数据"""
        try:
            from strands import Agent
            from strands.models import BedrockModel
            
            # 构建图表生成提示词
            chart_prompt = f"""
            你是一个专业的数据可视化专家，需要根据用户查询意图和数据结果生成最合适的图表展现形式。

            用户查询: 
            {query_prompt}
            
            数据返回结果:
            {results_prompt}
            
            请分析数据特征和用户意图，智能选择最合适的图表类型和数量：
            
            图表生成策略：
            1. 如果数据适合单一维度展示，生成1个图表
            2. 如果数据包含多个维度或用户查询涉及多个方面，可生成2-5个图表
            3. 每个图表应该有明确的分析目的和价值
            4. 优先生成最能回答用户问题的图表
            
            可选图表类型及适用场景:
            - line: 折线图（时间序列、趋势分析）
            - bar: 柱状图（分类统计、排名对比）
            - pie: 饼图（比例分布、占比分析）
            - scatter: 散点图（相关性分析、分布情况）
            - heatmap: 热力图（二维数据、密度分析）
            - area: 面积图（累积趋势、堆叠数据）
            
            数据质量要求：
            - 确保所有数组长度完全一致
            - 数值类型正确（数字字段使用数字，文本字段使用字符串）
            - 提供有意义的标题和描述
            
            请严格按照以下JSON格式返回：
            {{
                "charts": [
                    {{
                        "chart_type": "图表类型",
                        "title": "具体的图表标题",
                        "x_axis": ["标签1", "标签2", "标签3"],
                        "y_axis": [数值1, 数值2, 数值3],
                        "values": [数值1, 数值2, 数值3],
                        "names": ["名称1", "名称2", "名称3"],
                        "description": "图表用途和洞察",
                        "chart_id": "chart_1",
                        "priority": 1
                    }}
                ]
            }}
            
            重要提醒：
            - 每个图表的数组长度必须一致
            - 图表标题要具体明确，不要使用通用标题
            - 确保JSON格式完全正确，不要包含注释
            - 根据数据实际情况决定图表数量，不要强制生成多个图表
            """
            
            # 获取图表生成任务的模型配置（使用中等性能模型）
            chart_config = self.model_config_manager.get_model_config('claude_3_5_sonnet')
            
            # 创建专门的图表生成模型，不使用工具
            chart_model = BedrockModel(
                model_id=chart_config.model_id,
                temperature=chart_config.temperature,
                region_name=chart_config.region
            )
            
            # 创建简单的Agent进行图表生成
            chart_agent = Agent(
                system_prompt="你是一个专业的数据可视化专家，根据数据特征生成最合适的图表。",
                model=chart_model
            )
            
            @retry_on_rate_limit(max_retries=3, wait_time=15)
            def call_chart_agent():
                return chart_agent(chart_prompt)
            
            response_text = call_chart_agent()
            
            # 使用增强的JSON解析方法
            chart_data = self._extract_and_parse_json(str(response_text))
            
            # 验证并处理多图表数据结构
            processed_chart_data = self._process_multi_chart_data(chart_data)
            
            return processed_chart_data
    
        except json.JSONDecodeError as e:
            logger.error(f"图表数据JSON解析失败: {str(e)}")
            logger.error(f"原始响应: {response_text}")
            
            # 返回默认图表数据
            return self._get_default_multi_chart_data()
            
        except Exception as e:
            logger.error(f"生成智能图表数据失败: {str(e)}")
            
            # 返回默认图表数据
            return self._get_default_multi_chart_data()
    
    def _generate_intelligent_analysis(self, results_prompt: str, chart_data: Dict[str, Any], 
                                     query_prompt: str, semantic_result: Dict[str, Any] = None) -> Dict[str, Any]:
        """使用Bedrock Claude生成智能分析报告 - 支持多种日志分析场景"""
        try:
            from strands import Agent
            from strands.models import BedrockModel
            
            # 分析查询意图和日志类型，确定分析模式
            analysis_mode = self._determine_analysis_mode(query_prompt, semantic_result)
            
            # 安全地获取分析模式信息
            mode_name = analysis_mode.get('mode', 'comprehensive_analysis')
            focus_areas = analysis_mode.get('focus_areas', [])
            
            # 将focus_areas转换为字符串，避免字典格式化问题
            focus_areas_str = ', '.join(focus_areas) if isinstance(focus_areas, list) else str(focus_areas)
            
            # 根据分析模式获取对应的JSON结构模板
            json_template = self._get_analysis_json_template(mode_name)
            
            # 安全地处理chart_data和results_prompt，避免字典格式化问题
            chart_data_str = json.dumps(chart_data, ensure_ascii=False, indent=2) if chart_data else "无图表数据"
            results_prompt_str = str(results_prompt) if results_prompt else "无查询结果"
            
            # 构建灵活的分析提示词
            analysis_prompt = f"""
            你是一个专业的日志分析专家，需要根据用户查询意图和日志类型进行智能分析。

            用户查询: {query_prompt}
            分析模式: {mode_name}
            分析重点: {focus_areas_str}
            
            图表数据:
            {chart_data_str}
            
            日志查询结果:
            {results_prompt_str}
            
            根据分析模式 "{mode_name}" 进行专业分析：
            
            {self._get_analysis_template(mode_name)}
            
            分析要求：
            1. 根据日志类型和查询意图调整分析重点
            2. 提供实用的洞察和建议
            3. 突出最重要的发现和模式
            4. 考虑业务影响和技术影响
            5. 提供可执行的行动建议
            6. 严格按照指定的JSON结构返回结果
            
            请严格按照以下JSON格式返回分析结果：
            {json_template}
            """
            
            # 获取分析报告任务的模型配置（使用高性能模型）
            analysis_config = self.model_config_manager.get_model_config('claude_3_7_sonnet')
            
            # 创建专门的分析报告生成模型，不使用工具
            analysis_model = BedrockModel(
                model_id=analysis_config.model_id,
                temperature=analysis_config.temperature,
                region_name=analysis_config.region
            )
            
            # 创建简单的Agent进行分析报告生成
            analysis_agent = Agent(
                system_prompt="你是一个专业的日志分析专家，根据不同场景提供深入的数据分析和专业建议。",
                model=analysis_model
            )
            
            @retry_on_rate_limit(max_retries=3, wait_time=15)
            def call_analysis_agent():
                return analysis_agent(analysis_prompt)
            
            response = call_analysis_agent()
            response_text = str(response)
            
            # 解析AI响应
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                # 清理可能的注释
                json_str = re.sub(r'//.*?\n', '\n', json_str)
                json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
                
                analysis = json.loads(json_str)
                
                # 添加分析元数据
                analysis["analysis_metadata"] = {
                    "analysis_mode": mode_name,
                    "focus_areas": focus_areas,
                    "timestamp": datetime.now().isoformat(),
                    "query_type": semantic_result.get('intent_type', 'unknown') if semantic_result else 'unknown'
                }
                
                return analysis
            else:
                raise Exception("AI响应中未找到有效的JSON格式分析报告")
            
        except json.JSONDecodeError as e:
            logger.error(f"分析报告JSON解析失败: {str(e)}")
            raise Exception(f"生成的分析报告JSON格式错误: {str(e)}")
        except Exception as e:
            logger.error(f"生成智能分析报告失败: {str(e)}")
            import traceback
            logger.error(f"详细错误信息: {traceback.format_exc()}")
            raise Exception(f"分析报告生成失败: {str(e)}")
    
    def _determine_analysis_mode(self, query_prompt: str, semantic_result: Dict[str, Any] = None) -> Dict[str, Any]:
        """根据查询内容和语义分析结果确定分析模式"""
        try:
            query_lower = query_prompt.lower()
            
            # 获取语义分析中的实体信息
            entities = semantic_result.get('entities', {}) if semantic_result else {}
            log_type = str(entities.get('log_type', '')).lower()
            
            # 安全地处理keywords，确保都是字符串
            raw_keywords = entities.get('keywords', [])
            keywords = []
            if isinstance(raw_keywords, list):
                for kw in raw_keywords:
                    if isinstance(kw, str):
                        keywords.append(kw.lower())
                    else:
                        keywords.append(str(kw).lower())
            else:
                keywords = [str(raw_keywords).lower()] if raw_keywords else []
            
            # 定义不同的分析模式
            analysis_modes = {
                'performance_analysis': {
                    'keywords': ['性能', '响应时间', '延迟', 'latency', 'response time', '吞吐量', 'throughput', 'qps', 'tps', '慢查询', 'slow'],
                    'focus_areas': ['性能指标', '响应时间分析', '吞吐量统计', '资源利用率', '性能瓶颈识别']
                },
                'error_analysis': {
                    'keywords': ['错误', '异常', 'error', 'exception', '失败', 'failed', '5xx', '4xx', 'timeout', '超时'],
                    'focus_areas': ['错误统计', '异常模式', '错误分布', '根因分析', '影响评估']
                },
                'security_analysis': {
                    'keywords': ['安全', '攻击', '威胁', 'security', 'attack', 'threat', '入侵', 'intrusion', '恶意', 'malicious', 'sql注入', 'xss'],
                    'focus_areas': ['安全事件', '威胁检测', '攻击模式', '风险评估', '安全建议']
                },
                'access_analysis': {
                    'keywords': ['访问', '请求', 'access', 'request', '用户', 'user', '流量', 'traffic', 'ip', '地理位置'],
                    'focus_areas': ['访问统计', '用户行为', '流量分析', '地理分布', '访问模式']
                },
                'business_analysis': {
                    'keywords': ['业务', '订单', '交易', 'business', 'order', 'transaction', '转化', 'conversion', '收入', 'revenue'],
                    'focus_areas': ['业务指标', '转化分析', '用户行为', '业务趋势', '收入影响']
                },
                'system_analysis': {
                    'keywords': ['系统', '服务', 'system', 'service', '资源', 'resource', 'cpu', 'memory', '磁盘', 'disk', '网络', 'network'],
                    'focus_areas': ['系统状态', '资源监控', '服务健康', '容量规划', '系统优化']
                },
                'audit_analysis': {
                    'keywords': ['审计', '合规', 'audit', 'compliance', '权限', 'permission', '操作记录', 'operation log'],
                    'focus_areas': ['操作审计', '权限分析', '合规检查', '操作统计', '风险识别']
                },
                'application_analysis': {
                    'keywords': ['应用', '功能', 'application', 'function', '模块', 'module', '接口', 'api', '服务调用'],
                    'focus_areas': ['应用性能', '功能使用', '接口调用', '模块分析', '应用健康']
                }
            }
            
            # 计算每种模式的匹配分数
            mode_scores = {}
            for mode, config in analysis_modes.items():
                score = 0
                for keyword in config['keywords']:
                    if keyword in query_lower:
                        score += 2  # 查询中直接匹配的关键词权重更高
                    if keyword in log_type:
                        score += 1.5  # 日志类型匹配
                    # 安全地检查keywords列表
                    keywords_str = ' '.join(keywords) if keywords else ''
                    if keyword in keywords_str:
                        score += 1  # 语义关键词匹配
                
                mode_scores[mode] = score
            
            # 选择得分最高的模式
            if mode_scores:
                best_mode = max(mode_scores.items(), key=lambda x: x[1])
                if best_mode[1] > 0:  # 有匹配的关键词
                    selected_mode = best_mode[0]
                    return {
                        'mode': selected_mode,
                        'focus_areas': analysis_modes[selected_mode]['focus_areas'],
                        'confidence': min(best_mode[1] / 5.0, 1.0)  # 归一化置信度
                    }
            
            # 默认使用综合分析模式
            return {
                'mode': 'comprehensive_analysis',
                'focus_areas': ['数据概览', '关键指标', '趋势分析', '异常检测', '模式识别', '业务影响'],
                'confidence': 0.5
            }
            
        except Exception as e:
            logger.error(f"确定分析模式失败: {str(e)}")
            import traceback
            logger.error(f"详细错误信息: {traceback.format_exc()}")
            return {
                'mode': 'comprehensive_analysis',
                'focus_areas': ['数据概览', '关键指标', '趋势分析', '异常检测', '模式识别'],
                'confidence': 0.3
            }
    
    def _get_analysis_template(self, analysis_mode: str) -> str:
        """根据分析模式获取对应的分析模板"""
        templates = {
            'performance_analysis': """
            性能分析重点：
            - 响应时间分布和趋势
            - 吞吐量和并发度分析
            - 性能瓶颈识别
            - 资源利用率评估
            - 性能优化建议
            """,
            'error_analysis': """
            错误分析重点：
            - 错误类型和频率统计
            - 错误发生的时间模式
            - 错误影响范围评估
            - 根本原因分析
            - 错误预防和修复建议
            """,
            'security_analysis': """
            安全分析重点：
            - 安全事件识别和分类
            - 攻击模式和威胁分析
            - 异常行为检测
            - 风险等级评估
            - 安全加固建议
            """,
            'access_analysis': """
            访问分析重点：
            - 访问量统计和趋势
            - 用户行为模式分析
            - 流量来源和分布
            - 异常访问检测
            - 用户体验优化建议
            """,
            'business_analysis': """
            业务分析重点：
            - 业务指标统计和趋势
            - 用户转化和留存分析
            - 业务流程效率评估
            - 收入和成本影响
            - 业务优化建议
            """,
            'system_analysis': """
            系统分析重点：
            - 系统健康状态评估
            - 资源使用情况分析
            - 服务可用性统计
            - 容量规划建议
            - 系统优化方案
            """,
            'audit_analysis': """
            审计分析重点：
            - 操作记录统计和分析
            - 权限使用情况评估
            - 合规性检查
            - 异常操作识别
            - 审计改进建议
            """,
            'application_analysis': """
            应用分析重点：
            - 应用功能使用统计
            - 接口调用分析
            - 应用性能评估
            - 用户交互模式
            - 应用优化建议
            """,
            'comprehensive_analysis': """
            综合分析重点：
            - 全面的数据概览
            - 多维度指标分析
            - 跨领域关联分析
            - 综合风险评估
            - 整体优化建议
            """
        }
        
        return templates.get(analysis_mode, templates['comprehensive_analysis'])
    
    def _get_analysis_json_template(self, analysis_mode: str) -> str:
        """根据分析模式获取对应的JSON结构模板"""
        templates = {
            'performance_analysis': '''
            {
                "analysis_mode": "performance_analysis",
                "summary": "性能分析总结",
                "performance_metrics": {
                    "avg_response_time": "平均响应时间",
                    "max_response_time": "最大响应时间",
                    "throughput": "吞吐量",
                    "error_rate": "错误率",
                    "concurrent_users": "并发用户数"
                },
                "performance_trends": [
                    "性能趋势1",
                    "性能趋势2"
                ],
                "bottlenecks": [
                    "性能瓶颈1",
                    "性能瓶颈2"
                ],
                "resource_utilization": {
                    "cpu_usage": "CPU使用率",
                    "memory_usage": "内存使用率",
                    "disk_io": "磁盘IO",
                    "network_io": "网络IO"
                },
                "optimization_recommendations": [
                    "优化建议1",
                    "优化建议2"
                ],
                "severity": "严重程度",
                "confidence": "分析置信度"
            }
            ''',
            'error_analysis': '''
            {
                "analysis_mode": "error_analysis",
                "summary": "错误分析总结",
                "error_statistics": {
                    "total_errors": "错误总数",
                    "error_rate": "错误率",
                    "most_common_error": "最常见错误",
                    "error_trend": "错误趋势"
                },
                "error_categories": [
                    {
                        "category": "错误类别",
                        "count": "数量",
                        "percentage": "占比"
                    }
                ],
                "error_patterns": [
                    "错误模式1",
                    "错误模式2"
                ],
                "root_causes": [
                    "根本原因1",
                    "根本原因2"
                ],
                "impact_assessment": {
                    "affected_users": "受影响用户数",
                    "business_impact": "业务影响",
                    "downtime": "停机时间"
                },
                "remediation_steps": [
                    "修复步骤1",
                    "修复步骤2"
                ],
                "prevention_measures": [
                    "预防措施1",
                    "预防措施2"
                ],
                "severity": "严重程度",
                "confidence": "分析置信度"
            }
            ''',
            'security_analysis': '''
            {
                "analysis_mode": "security_analysis",
                "summary": "安全分析总结",
                "security_events": {
                    "total_events": "安全事件总数",
                    "high_risk_events": "高风险事件数",
                    "attack_attempts": "攻击尝试次数",
                    "blocked_attempts": "已阻止攻击数"
                },
                "threat_indicators": [
                    "威胁指标1",
                    "威胁指标2"
                ],
                "attack_patterns": [
                    "攻击模式1",
                    "攻击模式2"
                ],
                "vulnerability_assessment": [
                    "漏洞评估1",
                    "漏洞评估2"
                ],
                "risk_level": "风险等级",
                "affected_systems": [
                    "受影响系统1",
                    "受影响系统2"
                ],
                "security_recommendations": [
                    "安全建议1",
                    "安全建议2"
                ],
                "immediate_actions": [
                    "立即行动1",
                    "立即行动2"
                ],
                "severity": "严重程度",
                "confidence": "分析置信度"
            }
            ''',
            'access_analysis': '''
            {
                "analysis_mode": "access_analysis",
                "summary": "访问分析总结",
                "access_statistics": {
                    "total_requests": "总请求数",
                    "unique_visitors": "独立访客数",
                    "peak_traffic_time": "流量高峰时间",
                    "avg_session_duration": "平均会话时长"
                },
                "traffic_patterns": [
                    "流量模式1",
                    "流量模式2"
                ],
                "user_behavior": {
                    "most_visited_pages": ["页面1", "页面2"],
                    "bounce_rate": "跳出率",
                    "conversion_rate": "转化率"
                },
                "geographic_distribution": [
                    {
                        "region": "地区",
                        "requests": "请求数",
                        "percentage": "占比"
                    }
                ],
                "anomalous_access": [
                    "异常访问1",
                    "异常访问2"
                ],
                "recommendations": [
                    "优化建议1",
                    "优化建议2"
                ],
                "severity": "严重程度",
                "confidence": "分析置信度"
            }
            ''',
            'business_analysis': '''
            {
                "analysis_mode": "business_analysis",
                "summary": "业务分析总结",
                "business_metrics": {
                    "total_transactions": "总交易数",
                    "revenue": "收入",
                    "conversion_rate": "转化率",
                    "customer_acquisition_cost": "客户获取成本"
                },
                "business_trends": [
                    "业务趋势1",
                    "业务趋势2"
                ],
                "customer_insights": {
                    "active_users": "活跃用户数",
                    "retention_rate": "留存率",
                    "churn_rate": "流失率"
                },
                "revenue_analysis": {
                    "revenue_growth": "收入增长",
                    "top_revenue_sources": ["收入来源1", "收入来源2"]
                },
                "business_impact": [
                    "业务影响1",
                    "业务影响2"
                ],
                "strategic_recommendations": [
                    "战略建议1",
                    "战略建议2"
                ],
                "severity": "严重程度",
                "confidence": "分析置信度"
            }
            ''',
            'system_analysis': '''
            {
                "analysis_mode": "system_analysis",
                "summary": "系统分析总结",
                "system_health": {
                    "overall_status": "整体状态",
                    "uptime": "运行时间",
                    "availability": "可用性",
                    "service_status": "服务状态"
                },
                "resource_metrics": {
                    "cpu_utilization": "CPU利用率",
                    "memory_usage": "内存使用率",
                    "disk_usage": "磁盘使用率",
                    "network_throughput": "网络吞吐量"
                },
                "system_events": [
                    "系统事件1",
                    "系统事件2"
                ],
                "capacity_planning": {
                    "current_capacity": "当前容量",
                    "projected_needs": "预计需求",
                    "scaling_recommendations": ["扩容建议1", "扩容建议2"]
                },
                "maintenance_recommendations": [
                    "维护建议1",
                    "维护建议2"
                ],
                "severity": "严重程度",
                "confidence": "分析置信度"
            }
            ''',
            'audit_analysis': '''
            {
                "analysis_mode": "audit_analysis",
                "summary": "审计分析总结",
                "audit_statistics": {
                    "total_operations": "总操作数",
                    "user_activities": "用户活动数",
                    "admin_operations": "管理员操作数",
                    "failed_attempts": "失败尝试数"
                },
                "compliance_status": {
                    "compliance_score": "合规评分",
                    "violations": ["违规项1", "违规项2"],
                    "compliance_gaps": ["合规缺口1", "合规缺口2"]
                },
                "user_access_patterns": [
                    "用户访问模式1",
                    "用户访问模式2"
                ],
                "privilege_analysis": {
                    "elevated_privileges": "提权操作",
                    "suspicious_activities": ["可疑活动1", "可疑活动2"]
                },
                "audit_recommendations": [
                    "审计建议1",
                    "审计建议2"
                ],
                "severity": "严重程度",
                "confidence": "分析置信度"
            }
            ''',
            'application_analysis': '''
            {
                "analysis_mode": "application_analysis",
                "summary": "应用分析总结",
                "application_metrics": {
                    "active_sessions": "活跃会话数",
                    "feature_usage": "功能使用情况",
                    "api_calls": "API调用次数",
                    "response_times": "响应时间"
                },
                "feature_analysis": [
                    {
                        "feature": "功能名称",
                        "usage_count": "使用次数",
                        "success_rate": "成功率"
                    }
                ],
                "user_journey": [
                    "用户路径1",
                    "用户路径2"
                ],
                "application_health": {
                    "error_rate": "错误率",
                    "crash_rate": "崩溃率",
                    "performance_score": "性能评分"
                },
                "optimization_opportunities": [
                    "优化机会1",
                    "优化机会2"
                ],
                "development_recommendations": [
                    "开发建议1",
                    "开发建议2"
                ],
                "severity": "严重程度",
                "confidence": "分析置信度"
            }
            '''
        }
        
        # 为其他模式提供基础模板
        if analysis_mode not in templates:
            templates[analysis_mode] = '''
            {
                "analysis_mode": "''' + analysis_mode + '''",
                "summary": "分析总结",
                "key_findings": [
                    "关键发现1",
                    "关键发现2"
                ],
                "data_insights": {
                    "total_records": "记录总数",
                    "time_span": "时间跨度"
                },
                "patterns": [
                    "模式1",
                    "模式2"
                ],
                "recommendations": [
                    "建议1",
                    "建议2"
                ],
                "severity": "严重程度",
                "confidence": "分析置信度"
            }
            '''
        
        return templates[analysis_mode]
    
   
    
    def _extract_and_parse_json(self, response_text) -> Dict[str, Any]:
        """增强的JSON提取和解析方法"""
        try:
            # 确保response_text是字符串
            if not isinstance(response_text, str):
                response_text = str(response_text)
                
            # 方法1: 尝试直接解析整个响应
            try:
                return json.loads(response_text.strip())
            except json.JSONDecodeError:
                pass
            
            # 方法2: 使用改进的正则表达式提取JSON
            # 匹配完整的JSON对象，处理嵌套结构
            json_patterns = [
                r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}',  # 简单嵌套
                r'\{(?:[^{}]|(?:\{[^{}]*\}))*\}',    # 更复杂的嵌套
                r'\{.*?\}(?=\s*$|\s*\n\s*[^{])',     # 到行尾或非JSON内容
            ]
            
            for pattern in json_patterns:
                matches = re.findall(pattern, response_text, re.DOTALL)
                for match in matches:
                    try:
                        # 清理JSON字符串
                        cleaned_json = self._clean_json_string(match)
                        return json.loads(cleaned_json)
                    except json.JSONDecodeError:
                        continue
            
            # 方法3: 逐行解析，寻找有效的JSON
            lines = response_text.split('\n')
            json_lines = []
            in_json = False
            brace_count = 0
            
            for line in lines:
                stripped_line = line.strip()
                if not stripped_line:
                    continue
                    
                if stripped_line.startswith('{'):
                    in_json = True
                    json_lines = [line]
                    brace_count = line.count('{') - line.count('}')
                elif in_json:
                    json_lines.append(line)
                    brace_count += line.count('{') - line.count('}')
                    
                    if brace_count == 0:
                        # JSON对象结束
                        json_str = '\n'.join(json_lines)
                        try:
                            cleaned_json = self._clean_json_string(json_str)
                            return json.loads(cleaned_json)
                        except json.JSONDecodeError:
                            pass
                        in_json = False
                        json_lines = []
            
            raise Exception("无法从响应中提取有效的JSON数据")
            
        except Exception as e:
            raise Exception(f"JSON提取和解析失败: {str(e)}")

    def _clean_json_string(self, json_str: str) -> str:
        """清理JSON字符串，移除常见的格式问题"""
        
        # 移除注释
        json_str = re.sub(r'//.*?\n', '\n', json_str)
        json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
        
        # 移除多余的逗号
        json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
        
        # 修复常见的引号问题
        json_str = re.sub(r"'([^']*)':", r'"\1":', json_str)  # 单引号键名
        json_str = re.sub(r':\s*\'([^\']*)\'', r': "\1"', json_str)  # 单引号值
        
        # 移除控制字符
        json_str = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', json_str)
        
        return json_str.strip()
    
    def _process_multi_chart_data(self, chart_data: Dict[str, Any]) -> Dict[str, Any]:
        """处理多图表数据结构，确保格式正确"""
        try:
            # 检查是否已经是多图表格式
            if "charts" in chart_data and isinstance(chart_data["charts"], list):
                # 验证每个图表
                validated_charts = []
                for i, chart in enumerate(chart_data["charts"]):
                    if isinstance(chart, dict):
                        try:
                            # 验证单个图表数据
                            self._validate_chart_data(chart)
                            
                            # 添加默认字段
                            if "chart_id" not in chart:
                                chart["chart_id"] = f"chart_{i+1}"
                            if "priority" not in chart:
                                chart["priority"] = i + 1
                            
                            validated_charts.append(chart)
                        except ValueError as e:
                            logger.warning(f"跳过无效的图表 {i+1}: {str(e)}")
                            continue
                
                if not validated_charts:
                    # 如果没有有效图表，返回默认数据
                    return self._get_default_multi_chart_data()
                
                return {
                    "charts": validated_charts,
                    "total_charts": len(validated_charts),
                    "primary_chart": 0,
                    "analysis_summary": chart_data.get("analysis_summary", "数据可视化分析")
                }
            
            else:
                # 单图表格式，转换为多图表格式
                self._validate_chart_data(chart_data)
                
                # 添加默认字段
                chart_data["chart_id"] = "chart_1"
                chart_data["priority"] = 1
                
                return {
                    "charts": [chart_data],
                    "total_charts": 1,
                    "primary_chart": 0,
                    "analysis_summary": chart_data.get("description", "数据可视化分析")
                }
                
        except Exception as e:
            logger.error(f"处理多图表数据失败: {str(e)}")
            return self._get_default_multi_chart_data()
    
    def _validate_chart_data(self, chart: Dict[str, Any]) -> None:
        """验证单个图表数据的格式是否正确，根据图表类型灵活验证"""
        # 验证必需字段
        required_fields = ["chart_type", "title"]
        missing_fields = [field for field in required_fields if field not in chart]
        if missing_fields:
            logger.warning(f"图表数据缺少必需字段: {missing_fields}")
            raise ValueError(f"图表数据缺少必需字段: {', '.join(missing_fields)}")
        
        # 验证图表类型
        valid_chart_types = ["bar", "line", "pie", "scatter", "histogram", "area", "heatmap", "table", "metric", "text"]
        chart_type = chart.get("chart_type", "").lower()
        if chart_type not in valid_chart_types:
            logger.warning(f"无效的图表类型: {chart_type}")
            chart["chart_type"] = "bar"  # 默认使用柱状图
            chart_type = "bar"
        
        # 确保所有数组字段都是列表类型
        array_fields = ["x_axis", "y_axis", "values", "names"]
        for field in array_fields:
            if field in chart and not isinstance(chart[field], list):
                logger.warning(f"字段 {field} 不是列表类型: {type(chart[field])}")
                try:
                    # 尝试转换为列表
                    if isinstance(chart[field], str):
                        # 如果是字符串，尝试解析为JSON数组
                        try:
                            chart[field] = json.loads(chart[field])
                        except json.JSONDecodeError:
                            chart[field] = [chart[field]]
                    else:
                        chart[field] = [chart[field]]
                except Exception as e:
                    logger.error(f"转换字段 {field} 为列表失败: {str(e)}")
                    chart[field] = []
        
        # 根据图表类型进行特定验证
        try:
            if chart_type in ["bar", "line", "scatter", "area"]:
                self._validate_xy_chart(chart, chart_type)
            elif chart_type == "pie":
                self._validate_pie_chart(chart)
            elif chart_type == "histogram":
                self._validate_histogram_chart(chart)
            elif chart_type == "heatmap":
                self._validate_heatmap_chart(chart)
            elif chart_type in ["table", "metric", "text"]:
                self._validate_info_chart(chart, chart_type)
            else:
                # 对于其他类型，进行基本验证
                self._validate_basic_chart(chart)
        except ValueError as e:
            # 如果验证失败，尝试修复数据或使用默认值
            logger.warning(f"图表验证失败，尝试修复: {str(e)}")
            self._fix_chart_data(chart, chart_type)
    
    def _validate_xy_chart(self, chart: Dict[str, Any], chart_type: str) -> None:
        """验证需要x轴和y轴的图表"""
        has_xy = "x_axis" in chart and "y_axis" in chart
        has_values_names = "values" in chart and "names" in chart
        
        if not has_xy and not has_values_names:
            # 尝试从其他字段生成数据
            if "data" in chart and isinstance(chart["data"], list):
                self._generate_xy_from_data(chart)
            else:
                raise ValueError(f"{chart_type}图表缺少数据字段")
        
        # 验证数组长度一致性（允许为空，但如果有数据则需要一致）
        if has_xy:
            x_len = len(chart.get("x_axis", []))
            y_len = len(chart.get("y_axis", []))
            if x_len > 0 and y_len > 0 and x_len != y_len:
                logger.warning(f"x_axis和y_axis长度不一致: {x_len} vs {y_len}")
                # 调整为相同长度
                min_len = min(x_len, y_len)
                chart["x_axis"] = chart["x_axis"][:min_len]
                chart["y_axis"] = chart["y_axis"][:min_len]
        
        if has_values_names:
            v_len = len(chart.get("values", []))
            n_len = len(chart.get("names", []))
            if v_len > 0 and n_len > 0 and v_len != n_len:
                logger.warning(f"values和names长度不一致: {v_len} vs {n_len}")
                # 调整为相同长度
                min_len = min(v_len, n_len)
                chart["values"] = chart["values"][:min_len]
                chart["names"] = chart["names"][:min_len]
    
    def _validate_pie_chart(self, chart: Dict[str, Any]) -> None:
        """验证饼图数据"""
        if "values" not in chart or "names" not in chart:
            # 尝试从x_axis和y_axis生成
            if "x_axis" in chart and "y_axis" in chart:
                chart["names"] = chart["x_axis"]
                chart["values"] = chart["y_axis"]
            else:
                raise ValueError("饼图缺少values和names字段")
        
        # 验证数组长度一致性
        v_len = len(chart.get("values", []))
        n_len = len(chart.get("names", []))
        if v_len > 0 and n_len > 0 and v_len != n_len:
            logger.warning(f"饼图values和names长度不一致: {v_len} vs {n_len}")
            min_len = min(v_len, n_len)
            chart["values"] = chart["values"][:min_len]
            chart["names"] = chart["names"][:min_len]
    
    def _validate_histogram_chart(self, chart: Dict[str, Any]) -> None:
        """验证直方图数据"""
        if "x_axis" not in chart or not chart["x_axis"]:
            # 尝试从values生成
            if "values" in chart and chart["values"]:
                chart["x_axis"] = chart["values"]
            else:
                raise ValueError("直方图缺少x_axis数据")
    
    def _validate_heatmap_chart(self, chart: Dict[str, Any]) -> None:
        """验证热力图数据"""
        required_fields = ["x_axis", "y_axis", "values"]
        missing = [f for f in required_fields if f not in chart or not chart[f]]
        if missing:
            raise ValueError(f"热力图缺少必需字段: {', '.join(missing)}")
    
    def _validate_info_chart(self, chart: Dict[str, Any], chart_type: str) -> None:
        """验证信息类图表（表格、指标、文本）"""
        if chart_type == "table":
            if "data" not in chart and "x_axis" not in chart:
                raise ValueError("表格缺少数据")
        elif chart_type == "metric":
            if "value" not in chart and "values" not in chart:
                chart["value"] = chart.get("title", "N/A")
        # 文本类型不需要特殊验证
    
    def _validate_basic_chart(self, chart: Dict[str, Any]) -> None:
        """基本图表验证"""
        # 对于未知类型，只要有title就认为有效
        pass
    
    def _fix_chart_data(self, chart: Dict[str, Any], chart_type: str) -> None:
        """修复图表数据"""
        try:
            if chart_type in ["bar", "line", "scatter", "area"]:
                # 为xy图表提供默认数据
                if not chart.get("x_axis") and not chart.get("names"):
                    chart["x_axis"] = ["数据"]
                if not chart.get("y_axis") and not chart.get("values"):
                    chart["y_axis"] = [1]
            elif chart_type == "pie":
                # 为饼图提供默认数据
                if not chart.get("values"):
                    chart["values"] = [1]
                if not chart.get("names"):
                    chart["names"] = ["数据"]
            elif chart_type == "histogram":
                # 为直方图提供默认数据
                if not chart.get("x_axis"):
                    chart["x_axis"] = [1]
        except Exception as e:
            logger.error(f"修复图表数据失败: {str(e)}")
    
    def _generate_xy_from_data(self, chart: Dict[str, Any]) -> None:
        """从data字段生成x_axis和y_axis"""
        try:
            data = chart.get("data", [])
            if isinstance(data, list) and data:
                if isinstance(data[0], dict):
                    # 如果是对象数组，取前两个字段作为x和y
                    keys = list(data[0].keys())
                    if len(keys) >= 2:
                        chart["x_axis"] = [item.get(keys[0], "") for item in data]
                        chart["y_axis"] = [item.get(keys[1], 0) for item in data]
                else:
                    # 如果是简单数组，生成索引作为x轴
                    chart["x_axis"] = list(range(len(data)))
                    chart["y_axis"] = data
        except Exception as e:
            logger.error(f"从data生成xy数据失败: {str(e)}")
    
    def _get_default_multi_chart_data(self) -> Dict[str, Any]:
        """获取默认的多图表数据结构"""
        default_chart = {
            "chart_type": "bar",
            "title": "查询结果统计",
            "x_axis": ["查询结果"],
            "y_axis": [1],
            "values": [1],
            "names": ["查询结果"],
            "description": "默认图表数据",
            "chart_id": "chart_1",
            "priority": 1
        }
        
        return {
            "charts": [default_chart],
            "total_charts": 1,
            "primary_chart": 0,
            "analysis_summary": "默认数据可视化"
        }
