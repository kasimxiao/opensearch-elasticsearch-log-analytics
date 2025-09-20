"""
智能聊天页面
用于与日志分析系统进行自然语言交互
"""

import os
import warnings
import sys
import streamlit as st
import pandas as pd
import json
import uuid
import boto3
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import time
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass

# 环境配置
os.environ.setdefault("STREAMLIT_SERVER_HEADLESS", "true")
os.environ.setdefault("STREAMLIT_BROWSER_GATHER_USAGE_STATS", "false")
warnings.filterwarnings("ignore", message=".*ScriptRunContext.*")
warnings.filterwarnings("ignore", category=UserWarning)

# 路径配置
current_dir = os.path.dirname(os.path.abspath(__file__))
server_path = os.path.join(current_dir, "..", "..", "server")
if server_path not in sys.path:
    sys.path.insert(0, server_path)

# 导入后端模块
from strands_log_agent import log_query_agent

# 日志配置
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
for log_name in ['botocore', 'boto3', 'urllib3']:
    logging.getLogger(log_name).setLevel(logging.WARNING)

# 全局配置
MCP_AVAILABLE = True

@dataclass
class ChartConfig:
    """图表配置数据类"""
    title: str
    chart_type: str
    x_data: List = None
    y_data: List = None
    values: List = None
    names: List = None
    x_label: str = ""
    y_label: str = ""
    description: str = ""
    chart_id: str = None


class ChartRenderer:
    """统一的图表渲染器"""
    
    @staticmethod
    def render_chart_data(chart_data):
        """主要图表渲染入口"""
        try:
            if not chart_data:
                st.info("📊 暂无图表数据")
                return
            
            st.markdown("### 📊 数据可视化")
            
            # 处理多图表
            if isinstance(chart_data, dict) and "charts" in chart_data:
                charts = chart_data["charts"]
                if isinstance(charts, list) and charts:
                    st.info(f"📊 共 {len(charts)} 个图表")
                    for i, chart in enumerate(charts, 1):
                        if isinstance(chart, dict):
                            chart_title = chart.get("title", f"图表 {i}")
                            st.markdown(f"#### {i}. {chart_title}")
                            ChartRenderer._render_single_chart(chart)
                            if i < len(charts):
                                st.markdown("---")
            # 处理单图表
            elif isinstance(chart_data, dict):
                ChartRenderer._render_single_chart(chart_data)
            else:
                st.warning(f"⚠️ 不支持的图表数据格式: {type(chart_data).__name__}")
                
        except Exception as e:
            logger.error(f"图表渲染失败: {str(e)}")
            st.error(f"❌ 图表渲染失败: {str(e)}")
    
    @staticmethod
    def _render_single_chart(chart_data):
        """渲染单个图表"""
        try:
            config = ChartConfig(
                title=chart_data.get("title", "图表"),
                chart_type=chart_data.get("chart_type", chart_data.get("type", "auto")),
                x_data=chart_data.get("x_axis", []),
                y_data=chart_data.get("y_axis", []),
                values=chart_data.get("values", []),
                names=chart_data.get("names", []),
                x_label=chart_data.get("x_label", ""),
                y_label=chart_data.get("y_label", ""),
                description=chart_data.get("description", ""),
                chart_id=chart_data.get("chart_id", chart_data.get("id"))
            )
            
            # 根据类型渲染
            chart_methods = {
                "bar": ChartRenderer._render_bar,
                "line": ChartRenderer._render_line,
                "pie": ChartRenderer._render_pie,
                "scatter": ChartRenderer._render_scatter,
                "area": ChartRenderer._render_area,
                "heatmap": ChartRenderer._render_heatmap
            }
            
            if config.chart_type in chart_methods:
                chart_methods[config.chart_type](config)
            else:
                ChartRenderer._auto_render(config)
                
        except Exception as e:
            logger.error(f"单个图表渲染失败: {str(e)}")
            st.error(f"❌ 图表渲染失败: {str(e)}")
    
    @staticmethod
    def _auto_render(config: ChartConfig):
        """自动判断图表类型"""
        if config.x_data and config.y_data and len(config.x_data) == len(config.y_data):
            if all(isinstance(x, str) for x in config.x_data):
                ChartRenderer._render_bar(config)
            else:
                ChartRenderer._render_line(config)
        elif config.values and config.names and len(config.values) == len(config.names):
            if len(config.values) <= 5:
                ChartRenderer._render_pie(config)
            else:
                ChartRenderer._render_bar(config)
        elif config.values:
            if len(config.values) <= 5:
                ChartRenderer._render_pie(config)
            else:
                ChartRenderer._render_bar(config)
        else:
            st.error("❌ 无法确定图表类型，数据不完整")
    
    @staticmethod
    def _render_bar(config: ChartConfig):
        """渲染柱状图"""
        try:
            df = ChartRenderer._prepare_dataframe(config)
            if df is None:
                return
            
            if 'series' in df.columns:
                fig = px.bar(df, x='x', y='y', color='series', title=config.title, barmode='group')
            else:
                fig = px.bar(df, x='x', y='y', title=config.title)
            
            ChartRenderer._render_plotly_chart(fig, config)
        except Exception as e:
            st.error(f"❌ 柱状图渲染失败: {str(e)}")
    
    @staticmethod
    def _render_line(config: ChartConfig):
        """渲染折线图"""
        try:
            df = ChartRenderer._prepare_dataframe(config)
            if df is None:
                return
            
            if 'series' in df.columns:
                fig = px.line(df, x='x', y='y', color='series', title=config.title)
            else:
                fig = px.line(df, x='x', y='y', title=config.title)
            
            ChartRenderer._render_plotly_chart(fig, config)
        except Exception as e:
            st.error(f"❌ 折线图渲染失败: {str(e)}")
    
    @staticmethod
    def _render_pie(config: ChartConfig):
        """渲染饼图"""
        try:
            values = config.values or config.y_data
            names = config.names or config.x_data
            
            if not values:
                st.warning("⚠️ 饼图数据不完整")
                return
            
            if not names:
                names = [f"类别{i+1}" for i in range(len(values))]
            
            fig = px.pie(values=values, names=names, title=config.title)
            ChartRenderer._render_plotly_chart(fig, config)
        except Exception as e:
            st.error(f"❌ 饼图渲染失败: {str(e)}")
    
    @staticmethod
    def _render_scatter(config: ChartConfig):
        """渲染散点图"""
        try:
            if not config.x_data or not config.y_data:
                st.warning("⚠️ 散点图数据不完整")
                return
            
            fig = px.scatter(x=config.x_data, y=config.y_data, title=config.title)
            ChartRenderer._render_plotly_chart(fig, config)
        except Exception as e:
            st.error(f"❌ 散点图渲染失败: {str(e)}")
    
    @staticmethod
    def _render_area(config: ChartConfig):
        """渲染面积图"""
        try:
            df = ChartRenderer._prepare_dataframe(config)
            if df is None:
                return
            
            if 'series' in df.columns:
                fig = px.area(df, x='x', y='y', color='series', title=config.title)
            else:
                fig = px.area(df, x='x', y='y', title=config.title)
            
            ChartRenderer._render_plotly_chart(fig, config)
        except Exception as e:
            st.error(f"❌ 面积图渲染失败: {str(e)}")
    
    @staticmethod
    def _render_heatmap(config: ChartConfig):
        """渲染热力图"""
        try:
            if not config.x_data or not config.y_data or not config.values:
                st.warning("⚠️ 热力图数据不完整")
                return
            
            if not isinstance(config.values[0], list):
                st.warning("⚠️ 热力图values必须是二维数组")
                return
            
            z_values = np.array(config.values)
            fig = go.Figure(data=go.Heatmap(
                z=z_values, x=config.x_data, y=config.y_data,
                colorscale='Viridis', showscale=True
            ))
            fig.update_layout(title=config.title, height=500)
            
            ChartRenderer._render_plotly_chart(fig, config)
        except Exception as e:
            st.error(f"❌ 热力图渲染失败: {str(e)}")
    
    @staticmethod
    def _render_plotly_chart(fig, config: ChartConfig):
        """渲染Plotly图表"""
        fig.update_layout(
            height=400,
            margin=dict(l=50, r=50, t=50, b=50),
            xaxis_title=config.x_label or 'X轴',
            yaxis_title=config.y_label or 'Y轴'
        )
        
        chart_key = f"chart_{config.chart_id}" if config.chart_id else None
        st.plotly_chart(fig, use_container_width=True, key=chart_key)
        
        if config.description:
            st.markdown(f"*📝 {config.description}*")
    
    @staticmethod
    def _prepare_dataframe(config: ChartConfig):
        """准备DataFrame数据"""
        try:
            # 多系列数据
            if config.values and isinstance(config.values[0], list):
                if not config.names or not config.x_data:
                    return None
                
                df_data = []
                for i, series_values in enumerate(config.values):
                    series_name = config.names[i] if i < len(config.names) else f"系列{i+1}"
                    for j, value in enumerate(series_values):
                        x_val = config.x_data[j] if j < len(config.x_data) else j
                        df_data.append({'x': x_val, 'y': value, 'series': series_name})
                return pd.DataFrame(df_data)
            
            # 单系列数据
            elif config.x_data and config.y_data:
                return pd.DataFrame({'x': config.x_data, 'y': config.y_data})
            elif config.x_data and config.values:
                return pd.DataFrame({'x': config.x_data, 'y': config.values})
            elif config.values and config.names:
                return pd.DataFrame({'x': config.names, 'y': config.values})
            elif config.values:
                x_data = [f"类别{i+1}" for i in range(len(config.values))]
                return pd.DataFrame({'x': x_data, 'y': config.values})
            
            return None
            
        except Exception as e:
            logger.error(f"准备DataFrame失败: {str(e)}")
            return None


class QueryDisplay:
    """简化的查询显示管理器"""
    
    def __init__(self):
        self.main_container = None
        self.output_container = None
        self.outputs = []
        self.output_containers = {}
        
    def setup(self):
        """初始化显示界面"""
        try:
            self.main_container = st.container()
            with self.main_container:
                self.output_container = st.container()
            return True
        except Exception as e:
            logger.error(f"QueryDisplay 初始化失败: {str(e)}")
            return False
    
    def add_output(self, data_type: str, content: Any, title: str = None, status: str = "processing"):
        """添加输出项 - 确保相同标题只显示一次"""
        try:
            title = title or f"{data_type.upper()} 输出"
            
            # 查找现有的相同标题输出项
            existing_idx = None
            existing_id = None
            for i, output in enumerate(self.outputs):
                if output.get("title") == title:
                    existing_idx = i
                    existing_id = output.get("id")
                    break
            
            # 创建或更新输出项
            output_item = {
                "id": existing_id or str(uuid.uuid4()),  # 保持相同ID或创建新ID
                "data_type": data_type,
                "content": content,
                "title": title,
                "status": status,
                "timestamp": datetime.now()
            }
            
            if existing_idx is not None:
                # 更新现有项
                self.outputs[existing_idx] = output_item
                logger.info(f"更新输出项: {title} -> {status}")
            else:
                # 添加新项
                self.outputs.append(output_item)
                logger.info(f"添加新输出项: {title} -> {status}")
            
            self._render_output(output_item)
            
        except Exception as e:
            logger.error(f"添加输出项失败: {str(e)}")
    
    def _render_output(self, output_item: Dict[str, Any]):
        """渲染输出项 - 使用标题作为容器键确保唯一性"""
        try:
            if not self.output_container:
                return
            
            # 使用标题作为容器键，确保相同标题使用相同容器
            container_key = output_item["title"]
            if container_key not in self.output_containers:
                with self.output_container:
                    self.output_containers[container_key] = st.empty()
            
            container = self.output_containers[container_key]
            
            with container.container():
                # 状态显示
                status_icons = {"success": "✅", "error": "❌", "processing": "🔄", "waiting": "⏳"}
                icon = status_icons.get(output_item["status"], "📋")
                
                col1, col2 = st.columns([4, 1])
                with col1:
                    title = f"{icon} **{output_item['title']}**"
                    if output_item["status"] == "success":
                        st.success(title)
                    elif output_item["status"] == "error":
                        st.error(title)
                    else:
                        st.info(title)
                
                with col2:
                    if output_item["timestamp"]:
                        st.caption(f"`{output_item['timestamp'].strftime('%H:%M:%S')}`")
                
                # 渲染内容
                self._render_content(output_item["data_type"], output_item["content"])
                        
        except Exception as e:
            logger.error(f"渲染输出项失败: {str(e)}")
    
    def _render_content(self, data_type: str, content: Any):
        """渲染内容"""
        try:
            if data_type == "text":
                self._render_text(content)
            elif data_type == "json":
                with st.expander("查看详细数据", expanded=False):
                    st.json(content)
            elif data_type == "chart":
                ChartRenderer.render_chart_data(content)
            else:
                st.markdown(f"    {str(content)}")
        except Exception as e:
            logger.error(f"渲染{data_type}内容失败: {str(e)}")
            st.error(f"内容渲染失败: {str(e)}")
    
    def _render_text(self, content: Any):
        """渲染文本内容"""
        if isinstance(content, str):
            st.markdown(f"    {content}")
        elif isinstance(content, dict):
            for key in ["message", "text", "description"]:
                if key in content:
                    st.markdown(f"    {content[key]}")
                    return
            # 显示键值对
            for key, value in content.items():
                if isinstance(value, (str, int, float)):
                    st.markdown(f"    - **{key}**: {value}")
        elif isinstance(content, list):
            for i, item in enumerate(content, 1):
                st.markdown(f"    {i}. {str(item)}")
        else:
            st.markdown(f"    {str(content)}")
    
    def reset(self):
        """重置状态"""
        try:
            self.outputs = []
            for container in self.output_containers.values():
                try:
                    container.empty()
                except:
                    pass
            self.output_containers = {}
            if self.output_container:
                try:
                    self.output_container.empty()
                except:
                    pass
            self.main_container = None
            self.output_container = None
        except Exception as e:
            logger.error(f"重置失败: {str(e)}")
    
    def force_reset(self):
        """强制重置"""
        self.reset()
        # 清理session state
        for key in ['pending_thread_outputs', 'has_pending_outputs']:
            if key in st.session_state:
                st.session_state[key] = [] if 'outputs' in key else False


class RealTimeCallback:
    """通用实时回调处理器"""
    
    def __init__(self, display: QueryDisplay):
        self.display = display
        self.updates = []
    
    def __call__(self, callback_data: Dict[str, Any]):
        """处理回调数据"""
        try:
            callback_type = callback_data.get("type", "output")
            
            if callback_type == "output":
                # 通用输出回调
                data_type = callback_data.get("data_type", "text")
                content = callback_data.get("content")
                title = callback_data.get("title")
                status = callback_data.get("status", "processing")
                
                # 获取当前线程信息
                import threading
                current_thread = threading.current_thread()
                thread_name = current_thread.name
                
                # 添加时间戳确保更新顺序
                update_timestamp = datetime.now()
                
                # 简化的线程处理逻辑 - 只关注线程安全
                can_render = self._can_safely_render(current_thread, thread_name)
                
                if can_render:
                    try:
                        # 直接调用add_output，它会处理重复和更新逻辑
                        self.display.add_output(data_type, content, title, status)
                        logger.info(f"在{thread_name}中渲染输出: {title} -> {status}")
                    except Exception as e:
                        logger.error(f"输出项处理失败 (线程: {thread_name}): {str(e)}")
                else:
                    # 在非安全线程中，记录到队列等待后续处理
                    update_info = {
                        "data_type": data_type,
                        "content": content,
                        "title": title,
                        "status": status,
                        "thread_name": thread_name,
                        "timestamp": update_timestamp
                    }
                    self.updates.append(update_info)
                    logger.info(f"输出项在{thread_name}中排队等待渲染: {title}")
                
        except Exception as e:
            logger.error(f"处理回调失败: {str(e)}")
            # 不抛出异常，避免中断整个处理流程
    


    def _can_safely_render(self, current_thread, thread_name):
        """
        判断是否可以安全渲染UI组件
        使用多种方法来判断，提高跨平台兼容性
        """
        import threading
        
        # 方法1: 检查是否是主线程
        if current_thread is threading.main_thread():
            return True
        
        # 方法2: 检查线程名称（兼容不同环境）
        safe_thread_patterns = [
            "MainThread",      # 标准主线程名称
            "ScriptRunner",    # Streamlit脚本运行线程
            "main",            # 某些环境下的主线程名称
            "Thread-1",        # 某些环境下的主线程名称
            "MainProcess"      # 多进程环境下的主进程
        ]
        
        for pattern in safe_thread_patterns:
            if pattern.lower() in thread_name.lower():
                return True
        
        # 方法3: 检查线程ID（主线程通常ID较小）
        try:
            if hasattr(current_thread, 'ident') and current_thread.ident:
                # 主线程通常有较小的ID
                if current_thread.ident <= 10:  # 经验值
                    return True
        except:
            pass
        
        # 方法4: 尝试检查是否在Streamlit上下文中
        try:
            import streamlit as st
            # 如果能访问session_state，通常表示在主线程中
            _ = st.session_state
            return True
        except:
            pass
        
        # 默认情况下不渲染，避免线程安全问题
        return False

    def process_updates(self):
        """处理所有累积的更新"""
        import threading
        current_thread = threading.current_thread()
        thread_name = current_thread.name
        
        # 在主线程中处理排队的更新
        if self._can_safely_render(current_thread, thread_name):
            for update in self.updates:
                try:
                    self.display.add_output(
                        update["data_type"], 
                        update["content"], 
                        update["title"], 
                        update["status"]
                    )
                    logger.info(f"处理排队更新: {update['title']} -> {update['status']}")
                except Exception as e:
                    logger.error(f"处理排队更新失败: {str(e)}")
        
        # 清空队列
        processed_updates = self.updates.copy()
        self.updates = []
        return processed_updates


def render_chart_data(chart_data):
    """渲染图表数据 - 使用统一的ChartRenderer"""
    ChartRenderer.render_chart_data(chart_data)


def render_charts(chart_data):
    """渲染图表 - 兼容性函数"""
    ChartRenderer.render_chart_data(chart_data)


def render_execution_outputs(execution_outputs):
    """渲染执行输出列表"""
    try:
        if not execution_outputs:
            return
        
        for output in execution_outputs:
            data_type = output.get("data_type", "text")
            title = output.get("title", "输出")
            status = output.get("status", "success")
            content = output.get("content")
            timestamp = output.get("timestamp")
            
            # 状态图标
            status_icons = {"success": "✅", "error": "❌", "processing": "🔄", "waiting": "⏳"}
            icon = status_icons.get(status, "📋")
            
            with st.container():
                col1, col2 = st.columns([4, 1])
                
                with col1:
                    if status == "success":
                        st.success(f"{icon} **{title}**")
                    elif status == "error":
                        st.error(f"{icon} **{title}**")
                    elif status == "processing":
                        st.info(f"{icon} **{title}** (进行中...)")
                    else:
                        st.markdown(f"{icon} **{title}**")
                
                with col2:
                    if timestamp:
                        try:
                            if isinstance(timestamp, str):
                                ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                st.caption(f"`{ts.strftime('%H:%M:%S')}`")
                            else:
                                st.caption(f"`{str(timestamp)[:8]}`")
                        except:
                            st.caption(f"`{str(timestamp)[:8]}`")
                
                # 渲染内容
                if content is not None:
                    if data_type == "text":
                        if isinstance(content, str):
                            st.markdown(f"    {content}")
                        elif isinstance(content, dict):
                            for key in ["message", "text", "error"]:
                                if key in content:
                                    if key == "error":
                                        st.error(f"    {content[key]}")
                                    else:
                                        st.markdown(f"    {content[key]}")
                                    break
                            else:
                                for key, value in content.items():
                                    if isinstance(value, (str, int, float)):
                                        st.markdown(f"    - **{key}**: {value}")
                        else:
                            st.markdown(f"    {str(content)}")
                    elif data_type == "json":
                        with st.expander("查看详细数据", expanded=False):
                            st.json(content)
                    elif data_type == "chart":
                        if content:
                            ChartRenderer.render_chart_data(content)
                        else:
                            st.info("暂无图表数据")
                    else:
                        st.markdown(f"    {str(content)}")
                
                if output != execution_outputs[-1]:
                    st.markdown("---")
                    
    except Exception as e:
        logger.error(f"渲染执行输出失败: {str(e)}")
        st.error(f"执行输出渲染失败: {str(e)}")



class SessionManager:
    """会话管理器"""
    
    @staticmethod
    def init_session_state():
        """初始化会话状态"""
        defaults = {
            "chat_messages": [],
            "chat_id": str(uuid.uuid4()),
            "query_display": QueryDisplay(),
            "need_rerun": False,
            "processing_query": False,
            "conversation_history": [],
            "conversation_count": 1,
            "message_counter": 0,
            "bedrock_client": None,
            "strands_available": False
        }
        
        for key, default_value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = default_value
        
        # 初始化 Bedrock 客户端
        if not st.session_state.bedrock_client:
            try:
                st.session_state.bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')
                st.session_state.strands_available = True
            except Exception as e:
                logger.error(f"初始化 Bedrock 客户端失败: {str(e)}")
                st.session_state.strands_available = False
        
        SessionManager._ensure_message_order()
    
    @staticmethod
    def _ensure_message_order():
        """确保消息按正确顺序排列"""
        try:
            if st.session_state.chat_messages:
                for i, message in enumerate(st.session_state.chat_messages):
                    if "timestamp" not in message:
                        message["timestamp"] = f"2024-01-01 00:00:{i:02d}"
                    if "message_id" not in message:
                        message["message_id"] = f"legacy_{i}_{str(uuid.uuid4())[:8]}"
                
                st.session_state.chat_messages.sort(key=lambda x: x.get("timestamp", ""))
        except Exception as e:
            logger.error(f"消息排序失败: {str(e)}")
    
    @staticmethod
    def start_new_conversation():
        """开启新对话"""
        try:
            # 保存当前对话
            if st.session_state.chat_messages:
                conversation_data = {
                    "conversation_id": st.session_state.chat_id,
                    "conversation_number": st.session_state.conversation_count,
                    "messages": st.session_state.chat_messages.copy(),
                    "start_time": datetime.now().isoformat(),
                    "message_count": len(st.session_state.chat_messages)
                }
                st.session_state.conversation_history.append(conversation_data)
            
            # 重置状态
            st.session_state.chat_messages = []
            st.session_state.chat_id = str(uuid.uuid4())
            st.session_state.query_display = QueryDisplay()
            st.session_state.processing_query = False
            st.session_state.conversation_count += 1
            
            # 重置代理会话
            if 'log_query_agent' in globals():
                log_query_agent.set_session_id(st.session_state.chat_id)
            
            st.success(f"🆕 已开启新对话 #{st.session_state.conversation_count}")
            st.rerun()
            
        except Exception as e:
            logger.error(f"开启新对话失败: {str(e)}")
            st.error(f"开启新对话失败: {str(e)}")
    
    @staticmethod
    def clear_chat_history():
        """清空当前对话记录"""
        try:
            message_count = len(st.session_state.chat_messages)
            st.session_state.chat_messages = []
            st.session_state.query_display = QueryDisplay()
            st.session_state.processing_query = False
            
            st.success(f"🗑️ 已清空 {message_count} 条消息")
            st.rerun()
            
        except Exception as e:
            logger.error(f"清空对话记录失败: {str(e)}")
            st.error(f"清空对话记录失败: {str(e)}")
    
    @staticmethod
    def restore_conversation(conversation_data):
        """恢复指定对话"""
        try:
            # 保存当前对话
            if st.session_state.chat_messages:
                current_conv = {
                    "conversation_id": st.session_state.chat_id,
                    "conversation_number": st.session_state.conversation_count,
                    "messages": st.session_state.chat_messages.copy(),
                    "start_time": datetime.now().isoformat(),
                    "message_count": len(st.session_state.chat_messages)
                }
                st.session_state.conversation_history.append(current_conv)
            
            # 恢复指定对话
            st.session_state.chat_messages = conversation_data['messages'].copy()
            st.session_state.chat_id = conversation_data['conversation_id']
            st.session_state.query_display = QueryDisplay()
            st.session_state.processing_query = False
            
            # 从历史记录中移除
            st.session_state.conversation_history = [
                conv for conv in st.session_state.conversation_history 
                if conv['conversation_id'] != conversation_data['conversation_id']
            ]
            
            if 'log_query_agent' in globals():
                log_query_agent.set_session_id(st.session_state.chat_id)
            
            st.success(f"🔄 已恢复对话 #{conversation_data['conversation_number']}")
            st.rerun()
            
        except Exception as e:
            logger.error(f"恢复对话失败: {str(e)}")
            st.error(f"恢复对话失败: {str(e)}")


def ensure_message_order():
    """确保消息按正确顺序排列 - 兼容性函数"""
    SessionManager._ensure_message_order()


def init_session_state():
    """初始化会话状态 - 兼容性函数"""
    SessionManager.init_session_state()


def start_new_conversation():
    """开启新对话 - 兼容性函数"""
    SessionManager.start_new_conversation()


def clear_chat_history():
    """清空当前对话记录 - 兼容性函数"""
    SessionManager.clear_chat_history()


def restore_conversation(conversation_data):
    """恢复指定的对话 - 兼容性函数"""
    SessionManager.restore_conversation(conversation_data)


def show_welcome_message():
    """显示欢迎消息"""
    st.markdown("""
    ### 👋 欢迎使用智能日志分析助手！
    
    ### 💡 使用提示：
    - 您可以随时点击侧边栏的 **"🆕 新对话"** 开始全新的对话
    - 使用 **"🗑️ 清空记录"** 清空当前对话内容
    - 历史对话会自动保存，您可以随时恢复之前的对话
    
    ### 🚀 开始提问：
    请在下方输入框中描述您的需求，例如：
    - "分析cloudfront 日志半年内 4xx/5xx 错误情况"
    - "分析WAF日志半年内被Block的情况"
    - "分析WAF日志半年内存在不同的IP使用了相同JA3的情况"
    - "S3如何开启版本控制"
    """)
    
    # 显示当前对话信息
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("当前对话", f"#{st.session_state.conversation_count}")
    with col2:
        st.metric("历史对话", len(st.session_state.conversation_history))
    with col3:
        system_status = "🟢 正常" if st.session_state.get("bedrock_client") else "🔴 异常"
        st.metric("系统状态", system_status)
    
    st.markdown("---")


def render_chat_history():
    """渲染聊天历史"""
    
    # 确保消息按正确顺序显示
    # 首先按message_order排序，如果没有则按时间戳，最后按索引
    sorted_messages = sorted(
        enumerate(st.session_state.chat_messages),
        key=lambda x: (
            x[1].get("message_order", x[0]),  # 优先使用message_order
            x[1].get("timestamp", ""),        # 其次使用时间戳
            x[0]                              # 最后使用原始索引
        )
    )
    
    for msg_index, message in sorted_messages:
        # 添加消息唯一标识符，防止重复渲染
        message_key = f"msg_{msg_index}_{message.get('message_order', msg_index)}"
        
        if message["role"] == "user":
            with st.chat_message("user", avatar="👤"):
                st.write(message["content"])
                # 添加时间戳显示（如果有）
                if "timestamp" in message:
                    st.caption(f"发送时间: {message['timestamp'][:19]}")  # 只显示到秒
        else:
            with st.chat_message("assistant", avatar="🤖"):
                if "error" in message:
                    # 显示执行输出（如果有）
                    if "execution_outputs" in message and message["execution_outputs"]:
                        render_execution_outputs(message["execution_outputs"])
                    
                    st.error(message["error"])
                elif message.get("require_completion", False):
                    # 显示执行输出（如果有）
                    if "execution_outputs" in message and message["execution_outputs"]:
                        render_execution_outputs(message["execution_outputs"])
                    
                    # 显示补全信息的历史消息
                    st.warning(f"⚠️ {message['content']}")
                    
                    missing_info = message.get("missing_info", {})
                    suggestions = message.get("suggestions", [])
                    completion_prompt = message.get("completion_prompt", "")
                    
                    if missing_info:
                        st.markdown("**需要补全的信息：**")
                        if missing_info.get("time_range", False):
                            st.info("🕒 时间段信息缺失")
                        if missing_info.get("log_source", False):
                            st.info("📊 日志源信息缺失")
                    
                    if suggestions:
                        st.markdown("**建议：**")
                        for suggestion in suggestions:
                            st.markdown(f"- {suggestion}")
                    
                    if completion_prompt:
                        st.markdown("**示例：**")
                        st.code(completion_prompt, language="text")
                else:
                    # 显示执行输出（如果有）- 这里包含了完整的图表渲染
                    if "execution_outputs" in message and message["execution_outputs"]:
                        render_execution_outputs(message["execution_outputs"])
                    
                    # 显示主要内容
                    st.write(message["content"])
                    
                    # 检查执行输出中是否已经包含图表
                    has_chart_in_outputs = False
                    if message.get("execution_outputs"):
                        for output in message["execution_outputs"]:
                            if output.get("data_type") == "chart" and output.get("content"):
                                has_chart_in_outputs = True
                                break
                    
                    # 如果执行输出中已经包含图表，就不再单独渲染图表数据
                    # 这样避免了图表的重复显示
                    if not has_chart_in_outputs:
                        # 渲染图表（如果有）
                        chart_rendered = False
                        
                        # 按优先级检查图表字段
                        for chart_field in ["chart_data", "chart", "charts", "visualization"]:
                            if chart_field in message and message[chart_field]:
                                try:
                                    logger.info(f"渲染历史消息中的图表字段: {chart_field}")
                                    render_chart_data(message[chart_field])
                                    chart_rendered = True
                                    break
                                except Exception as e:
                                    logger.error(f"历史消息图表渲染失败: {str(e)}")
                                    continue
                    else:
                        logger.info("跳过图表渲染，因为执行输出中已包含图表")
                
                # 添加时间戳显示（如果有）
                if "timestamp" in message:
                    st.caption(f"回复时间: {message['timestamp'][:19]}")  # 只显示到秒
    
    # 如果正在处理查询，显示实时状态
    if st.session_state.get("processing_query", False):
        with st.chat_message("assistant", avatar="🤖"):
            # 获取查询显示管理器
            display = st.session_state.query_display
            
            # 显示处理中的提示，不需要重复setup
            st.info("🔄 正在处理您的查询，请稍候...")


def process_query(user_input: str, display: QueryDisplay):
    """处理用户查询"""
    try:
        # 强制重置显示状态，确保界面清洁
        display.force_reset()
        
        # 初始化显示界面
        if not display.setup():
            logger.error("QueryDisplay 初始化失败")
            return {
                "success": False,
                "error": "显示界面初始化失败"
            }
        
        # 检查系统状态
        if not MCP_AVAILABLE:
            return {
                "success": False,
                "error": "日志查询代理不可用"
            }
        
        if not st.session_state.get("bedrock_client"):
            return {
                "success": False,
                "error": "Bedrock客户端不可用"
            }
        
        # 创建实时回调处理器
        callback = RealTimeCallback(display)
        
        try:
            # 设置会话ID和回调函数
            log_query_agent.set_session_id(st.session_state.chat_id)
            log_query_agent.set_step_callback(callback)
            
            # 直接调用strands_log_agent处理查询
            response = log_query_agent.process_query(
                query=user_input,
                session_id=st.session_state.chat_id
            )
            
            logger.info(f"response:{response}")
            # 处理响应结果
            if response.get("success", False):
                # 查询成功
                content = response.get("response", "")
                
                # 构建助手回复
                return {
                    "success": True,
                    "content": content,
                    "chart_data": response.get("chart_data", {}),
                    "intent_result": response.get("intent_result", {}),
                    "hits": response.get("hits", []),
                    "analysis": response.get("analysis", {}),
                    "dsl_query": response.get("dsl_query", {}),
                    "index_name": response.get("index_name", ""),
                    "search_config": response.get("search_config", {})
                }
            else:
                # 查询失败
                error_message = response.get("error", "未知错误")
                callback({
                    "type": "output",
                    "data_type": "text",
                    "content": {"error": error_message},
                    "title": "查询失败",
                    "status": "error"
                })
                return {
                    "success": False,
                    "error": error_message
                }
                
        except Exception as e:
            # 处理异常
            error_message = f"查询处理失败: {str(e)}"
            callback({
                "type": "output",
                "data_type": "text",
                "content": {"error": error_message},
                "title": "处理异常",
                "status": "error"
            })
            return {
                "success": False,
                "error": error_message
            }
        finally:
            # 清理回调函数
            try:
                log_query_agent.set_step_callback(None)
            except Exception as e:
                logger.warning(f"清理回调函数失败: {str(e)}")
            
    except Exception as e:
        # 处理顶层异常
        logger.error(f"process_query顶层异常: {str(e)}")
        return {
            "success": False,
            "error": f"处理查询时发生错误: {str(e)}"
        }




def handle_user_input(user_input: str):
    """处理用户输入 - 立即显示用户消息并开始处理"""
    try:
        # 检查是否已经在处理查询，避免重复处理
        if st.session_state.get("processing_query", False):
            logger.warning("已有查询正在处理中，跳过新查询")
            return
        
        # 增加消息计数器
        st.session_state.message_counter += 1
        
        # 立即添加用户消息到聊天历史
        user_message = {
            "role": "user",
            "content": user_input,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "message_id": str(uuid.uuid4()),
            "message_order": st.session_state.message_counter,
            "conversation_id": st.session_state.chat_id
        }
        st.session_state.chat_messages.append(user_message)
        
        # 设置处理状态和待处理输入
        st.session_state.processing_query = True
        st.session_state.pending_user_input = user_input
        
        # 清理之前的待处理输出
        if 'pending_thread_outputs' in st.session_state:
            st.session_state.pending_thread_outputs = []
        if 'has_pending_outputs' in st.session_state:
            st.session_state.has_pending_outputs = False
        
        # 立即重新运行页面以显示用户输入
        st.rerun()
        
    except Exception as e:
        logger.error(f"处理用户输入失败: {str(e)}")
        st.error(f"处理用户输入时发生错误: {str(e)}")
        # 确保在异常情况下也重置处理状态
        st.session_state.processing_query = False


def continue_query_processing(user_input: str):
    """继续处理查询的剩余部分"""
    try:
        
        # 重新创建查询显示管理器，确保状态清洁
        st.session_state.query_display = QueryDisplay()
        display = st.session_state.query_display
        
        # 处理查询
        result = process_query(user_input, display)
        
        # 处理结果
        if result.get("success", False):
            content = result.get("content", "")
            chart_data = result.get("chart_data", {})
            
            # 增加消息计数器
            st.session_state.message_counter += 1
            
            # 收集执行输出信息
            execution_outputs = []
            if hasattr(display, 'outputs'):
                for output in display.outputs:
                    if output.get("status") != "waiting":  # 只保存已执行的输出
                        execution_outputs.append({
                            "id": output.get("id"),
                            "data_type": output.get("data_type"),
                            "title": output.get("title"),
                            "status": output.get("status"),
                            "content": output.get("content"),
                            "timestamp": output.get("timestamp").isoformat() if output.get("timestamp") else None
                        })
            
            # 调试：记录图表数据信息
            logger.info(f"保存图表数据: chart_data类型={type(chart_data)}, 内容={bool(chart_data)}")
            if chart_data:
                logger.info(f"图表数据详情: {list(chart_data.keys()) if isinstance(chart_data, dict) else 'not dict'}")
            
            # 添加助手消息到聊天历史，包含时间戳确保顺序
            assistant_message = {
                "role": "assistant",
                "content": content,
                "chart_data": chart_data,
                "execution_outputs": execution_outputs,  # 保存执行输出信息
                "intent_result": result.get("intent_result", {}),
                "hits": result.get("hits", []),
                "analysis": result.get("analysis", {}),
                "dsl_query": result.get("dsl_query", {}),
                "index_name": result.get("index_name", ""),
                "search_config": result.get("search_config", {}),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),  # 包含微秒确保唯一性
                "message_id": str(uuid.uuid4()),  # 添加唯一标识符
                "message_order": st.session_state.message_counter,  # 添加序号确保顺序
                "conversation_id": st.session_state.chat_id
            }
            st.session_state.chat_messages.append(assistant_message)
            
            # 调试：验证保存的数据
            saved_chart_data = assistant_message.get("chart_data")
            logger.info(f"已保存图表数据: 类型={type(saved_chart_data)}, 内容={bool(saved_chart_data)}")
        else:
            # 查询失败 - 检查是否需要补全信息
            error_message = result.get("error", "未知错误")
            
            # 增加消息计数器
            st.session_state.message_counter += 1
            
            # 收集执行输出信息（即使失败也要保存）
            execution_outputs = []
            if hasattr(display, 'outputs'):
                for output in display.outputs:
                    if output.get("status") != "waiting":  # 只保存已执行的输出
                        execution_outputs.append({
                            "id": output.get("id"),
                            "data_type": output.get("data_type"),
                            "title": output.get("title"),
                            "status": output.get("status"),
                            "content": output.get("content"),
                            "timestamp": output.get("timestamp").isoformat() if output.get("timestamp") else None
                        })
            
            # 检查是否是需要补全信息的情况
            if result.get("require_completion", False):
                missing_info = result.get("missing_info", {})
                suggestions = result.get("suggestions", [])
                completion_prompt = result.get("completion_prompt", "")
                
                # 添加补全提示消息到聊天历史，包含时间戳确保顺序
                completion_message = {
                    "role": "assistant",
                    "content": f"您的查询需要补全信息：{error_message}",
                    "require_completion": True,
                    "missing_info": missing_info,
                    "suggestions": suggestions,
                    "completion_prompt": completion_prompt,
                    "execution_outputs": execution_outputs,  # 保存执行输出信息
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),  # 包含微秒确保唯一性
                    "message_id": str(uuid.uuid4()),  # 添加唯一标识符
                    "message_order": st.session_state.message_counter,  # 添加序号确保顺序
                    "conversation_id": st.session_state.chat_id
                }
                st.session_state.chat_messages.append(completion_message)
            else:
                # 添加错误消息到聊天历史，包含时间戳确保顺序
                error_msg = {
                    "role": "assistant",
                    "content": f"抱歉，处理您的查询时发生错误: {error_message}",
                    "error": error_message,
                    "execution_outputs": execution_outputs,  # 保存执行输出信息
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),  # 包含微秒确保唯一性
                    "message_id": str(uuid.uuid4()),  # 添加唯一标识符
                    "message_order": st.session_state.message_counter,  # 添加序号确保顺序
                    "conversation_id": st.session_state.chat_id
                }
                st.session_state.chat_messages.append(error_msg)
        
    except Exception as e:
        logger.error(f"继续处理查询失败: {str(e)}")
        # 确保在异常情况下也重置处理状态
        st.session_state.processing_query = False
        
        # 添加错误消息
        st.session_state.message_counter += 1
        error_msg = {
            "role": "assistant",
            "content": f"抱歉，处理您的查询时发生系统错误: {str(e)}",
            "error": str(e),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "message_id": str(uuid.uuid4()),
            "message_order": st.session_state.message_counter,
            "conversation_id": st.session_state.chat_id
        }
        st.session_state.chat_messages.append(error_msg)
    
    finally:
        # 确保处理状态被重置
        st.session_state.processing_query = False
        
        # 清理待处理输出
        if 'pending_thread_outputs' in st.session_state:
            st.session_state.pending_thread_outputs = []
        if 'has_pending_outputs' in st.session_state:
            st.session_state.has_pending_outputs = False
        
        # 重新运行页面以显示结果
        st.rerun()
        
        # 触发页面重新渲染以显示新消息
        st.rerun()


def show_chat_interface(api_client=None):
    """显示聊天界面
    
    Args:
        api_client: API客户端实例，可选
    """
    st.markdown("---")
    
    # 初始化会话状态
    init_session_state()
    
    # 简化待处理输出的处理逻辑，避免无限循环
    try:
        if st.session_state.get('has_pending_outputs', False):
            # 清空标志，避免重复处理
            st.session_state.has_pending_outputs = False
            if 'pending_thread_outputs' in st.session_state:
                st.session_state.pending_thread_outputs = []
            logger.info("已清理待处理输出标志")
    except Exception as e:
        logger.error(f"处理待处理输出失败: {str(e)}")
        # 确保清空标志
        st.session_state.has_pending_outputs = False
        if 'pending_thread_outputs' in st.session_state:
            st.session_state.pending_thread_outputs = []
    
    # 侧边栏
    with st.sidebar:
        # 对话管理区域
        st.subheader("💬 对话管理")
        
        # 开启新对话按钮
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🆕 新对话", help="开启新对话，重置上下文", use_container_width=True):
                start_new_conversation()
        
        with col2:
            if st.button("🗑️ 清空记录", help="清空当前对话记录", use_container_width=True):
                clear_chat_history()
        
        # 会话信息
        st.markdown("---")
        st.markdown("**当前会话信息:**")
        st.info(f"📝 会话ID: {st.session_state.chat_id[:8]}...")
        
        # 显示对话统计
        message_count = len(st.session_state.chat_messages)
        user_messages = len([msg for msg in st.session_state.chat_messages if msg["role"] == "user"])
        st.caption(f"💬 消息总数: {message_count}")
        st.caption(f"👤 用户消息: {user_messages}")
        
        # 显示系统状态
        st.markdown("---")
        st.subheader("🔧 系统状态")
        
        # MCP状态
        if MCP_AVAILABLE:
            st.success("✅ MCP客户端: 正常")
        else:
            st.error("❌ MCP客户端: 不可用")
        
        # Bedrock状态
        if st.session_state.get("bedrock_client"):
            st.success("✅ Bedrock: 正常")
        else:
            st.error("❌ Bedrock: 不可用")
        
        # 处理状态显示
        if st.session_state.get("processing_query", False):
            st.warning("🔄 正在处理查询...")
        else:
            st.success("✅ 系统就绪")
        
        # 历史对话管理
        if st.session_state.conversation_history:
            st.markdown("---")
            st.subheader("📚 历史对话")
            
            # 显示历史对话数量
            history_count = len(st.session_state.conversation_history)
            st.caption(f"共有 {history_count} 个历史对话")
            
            # 历史对话列表
            for conv in reversed(st.session_state.conversation_history[-5:]):  # 最多显示最近5个
                with st.expander(f"对话 #{conv['conversation_number']}", expanded=False):
                    st.caption(f"ID: {conv['conversation_id'][:8]}...")
                    st.caption(f"消息数: {conv['message_count']}")
                    st.caption(f"时间: {conv['start_time'][:16]}")
                    
                    # 显示第一个用户问题作为预览
                    user_messages = [msg for msg in conv['messages'] if msg['role'] == 'user']
                    if user_messages:
                        preview = user_messages[0]['content'][:60]
                        st.markdown(f"**问题:** {preview}...")
                    
                    # 操作按钮
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("🔄 恢复", key=f"restore_sidebar_{conv['conversation_id']}", use_container_width=True):
                            restore_conversation(conv)
                    with col2:
                        if st.button("🗑️ 删除", key=f"delete_sidebar_{conv['conversation_id']}", use_container_width=True):
                            delete_conversation(conv['conversation_id'])
            
            # 清空所有历史对话按钮
            if history_count > 0:
                st.markdown("---")
                if st.button("🗑️ 清空所有历史", help="删除所有历史对话", use_container_width=True):
                    clear_all_history()
    
    # 显示欢迎信息（仅在新对话开始时显示）
    if not st.session_state.chat_messages:
        show_welcome_message()
    
    # 显示聊天历史
    render_chat_history()
    
    # 用户输入处理
    if st.session_state.get("processing_query", False):
        # 显示处理中的提示
        st.chat_input("🔄 正在处理您的查询，请稍候...", disabled=True)
        
        # 如果正在处理查询，继续处理流程
        if st.session_state.get("pending_user_input"):
            user_input = st.session_state.pending_user_input
            st.session_state.pending_user_input = None  # 清除待处理输入
            
            # 继续处理查询的剩余部分
            continue_query_processing(user_input)
    else:
        # 允许新的用户输入
        user_input = st.chat_input("💭 请输入您的问题...")
        
        if user_input:
            handle_user_input(user_input)


# 主函数
if __name__ == "__main__":
    show_chat_interface()
