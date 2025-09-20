"""
Streamlit前端应用
用于管理OpenSearch/Elasticsearch配置、索引字段信息和DSL查询语句
"""

import streamlit as st
import os
import sys
import json
from pathlib import Path

# 不再需要导入后端模块
# sys.path.append(str(Path(__file__).parent.parent / "server"))

# 导入页面模块
from pages.data_source_config import show_data_source_config
from pages.index_field_management import show_index_field_management
from pages.dsl_query_management import show_dsl_query_management
from pages.chat import show_chat_interface
from utils.api_client import APIClient

# 设置页面配置
st.set_page_config(
    page_title="日志分析平台",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 初始化API客户端
@st.cache_resource
def get_api_client():
    """获取API客户端实例"""
    try:
        return APIClient()
    except Exception as e:
        st.error(f"初始化API客户端失败: {str(e)}")
        # 返回一个空的API客户端
        return None

# 初始化会话状态
if "current_page" not in st.session_state:
    st.session_state.current_page = "数据源配置"
    
# 确保其他必要的会话状态变量已初始化
if "confirm_delete" not in st.session_state:
    st.session_state.confirm_delete = None

# 侧边栏导航
st.sidebar.title("日志分析平台")
st.sidebar.markdown("---")

# 导航菜单
menu_options = ["数据源配置", "索引字段管理", "DSL查询管理", "智能聊天"]
selected_page = st.sidebar.radio("导航菜单", menu_options, index=menu_options.index(st.session_state.current_page))
st.session_state.current_page = selected_page

# 获取API客户端
api_client = get_api_client()

# 显示选中的页面
if api_client is None:
    st.error("无法初始化API客户端，请检查后端服务是否正常运行")
else:
    if selected_page == "数据源配置":
        show_data_source_config(api_client)
    elif selected_page == "索引字段管理":
        show_index_field_management(api_client)
    elif selected_page == "DSL查询管理":
        show_dsl_query_management(api_client)
    elif selected_page == "智能聊天":
        show_chat_interface(api_client)

# 页面底部
st.sidebar.markdown("---")
st.sidebar.info("© 2025 日志分析平台")
