"""
数据源配置页面
用于管理搜索引擎连接配置
"""

import streamlit as st
import json
import pandas as pd
from typing import Dict, List, Any, Optional
from utils.api_client import APIClient

def show_data_source_config(api_client: APIClient):
    """
    显示数据源配置页面
    
    Args:
        api_client: API客户端实例
    """
    
    # 检查API客户端是否初始化成功
    if api_client is None:
        st.error("API客户端初始化失败，请检查后端服务是否正常运行")
        return
    
    try:
        # 尝试访问客户端属性，如果失败则说明客户端初始化失败
        if not hasattr(api_client, 'config_client') or api_client.config_client is None:
            st.error("API客户端初始化失败，请检查后端服务是否正常运行")
            return
    except Exception as e:
        st.error(f"API客户端初始化失败: {str(e)}")
        return
    
    # 创建选项卡，根据是否处于编辑模式选择默认选项卡
    edit_mode = "edit_config" in st.session_state
    
    # 如果是编辑模式，默认显示新建/编辑配置选项卡
    if edit_mode:
        tab_index = 1  # 默认选择第二个选项卡（新建/编辑配置）
    else:
        tab_index = 0  # 默认选择第一个选项卡（配置管理）
    
    tab1, tab2 = st.tabs(["配置管理", "新建/编辑配置"])
    
    # 根据选择的选项卡显示内容
    if tab_index == 1:
        # 新建/编辑配置选项卡
        with tab2:
            show_config_form(api_client)
        
        # 配置管理选项卡
        with tab1:
            show_config_management(api_client)
    else:
        # 配置管理选项卡
        with tab1:
            show_config_management(api_client)
        
        # 新建/编辑配置选项卡
        with tab2:
            show_config_form(api_client)

def show_config_management(api_client: APIClient):
    """
    显示配置管理界面
    
    Args:
        api_client: API客户端实例
    """
    st.header("配置管理")
    
    # 添加刷新按钮
    if st.button("刷新配置列表"):
        st.rerun()
    
    # 获取所有配置
    configs = api_client.list_search_engine_configs()
    
    # 优化数据处理逻辑
    if not configs:
        st.info("暂无配置信息，请先在\"新建配置\"选项卡中创建配置")
        # 创建一个空的DataFrame以显示表头
        df = pd.DataFrame(columns=["config_id", "name", "type", "host", "auth_type", "created_at", "updated_at"])
    else:
        try:
            # 确保configs是列表
            if isinstance(configs, dict):
                if "error" in configs:
                    st.error(f"获取配置失败: {configs['error']}")
                    return
                configs = [configs]
            
            # 过滤有效的配置数据
            valid_configs = [config for config in configs if isinstance(config, dict)]
            
            if not valid_configs:
                st.info("暂无有效的配置信息，请先在\"新建配置\"选项卡中创建配置")
                df = pd.DataFrame(columns=["config_id", "name", "type", "host", "auth_type", "http_compress", "username", "password", "use_ssl", "verify_certs", "port", "created_at", "updated_at"])
            else:
                # 创建配置表格
                df = pd.DataFrame(valid_configs)
        except Exception as e:
            st.error(f"处理配置数据时出错: {str(e)}")
            df = pd.DataFrame(columns=["config_id", "name", "type", "host", "auth_type", "created_at", "updated_at"])
    
    # 重命名列
    columns_mapping = {
        "config_id": "配置ID",
        "name": "名称",
        "type": "类型",
        "host": "主机地址",
        "auth_type": "认证类型",
        "created_at": "创建时间",
        "updated_at": "更新时间"
    }
    
    # 优化表格显示逻辑
    try:
        # 选择要显示的列
        display_columns = [col for col in ["config_id", "name", "type", "host", "auth_type", "created_at"] if col in df.columns]
        
        if not display_columns:
            st.error("没有可显示的列")
            return
        
        # 创建显示表格
        display_df = df[display_columns].rename(columns={col: columns_mapping.get(col, col) for col in display_columns})
        
        # 显示表格
        st.dataframe(display_df, use_container_width=True)
    except Exception as e:
        st.error(f"创建显示表格时出错: {str(e)}")
        # 显示原始数据
        st.write("原始数据:")
        st.write(df)
    
    # 选择配置进行操作
    if len(configs) > 0:
        selected_config_id = st.selectbox(
            "选择配置进行操作",
            options=[config["config_id"] for config in configs],
            format_func=lambda x: next((config["name"] for config in configs if config["config_id"] == x), x)
        )
    else:
        selected_config_id = None
    
    if selected_config_id:
        # 获取选中的配置
        selected_config = next((config for config in configs if config["config_id"] == selected_config_id), None)
        
        if selected_config:
            # 显示配置详情
            with st.expander("配置详情", expanded=True):
                # 隐藏敏感信息
                display_config = selected_config.copy()
                if "password" in display_config:
                    display_config["password"] = "********"
                if "api_key" in display_config:
                    display_config["api_key"] = "********"
                
                # 格式化显示
                st.json(display_config)
            
            # 操作按钮
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("编辑配置", key="edit_config_button"):
                    # 确保 selected_config 是一个字典
                    if not isinstance(selected_config, dict):
                        st.error(f"选中的配置数据格式错误: 期望字典，但得到 {type(selected_config)}")
                    else:
                        # 获取完整的配置信息
                        full_config = api_client.get_search_engine_config(selected_config_id)
                        if full_config:
                            # 设置会话状态
                            st.session_state["edit_config"] = full_config
                            # 重新加载页面
                            st.rerun()
                        else:
                            st.error("无法获取配置详情")
            
            with col2:
                if st.button("测试连接", key="test_connection"):
                    with st.spinner("正在测试连接..."):
                        result = api_client.test_search_engine_connection(selected_config)
                        
                        # 调试信息在开发阶段使用，现在注释掉
                        # st.write(f"测试连接结果类型: {type(result)}")
                        
                        # 确保结果是字典类型
                        if isinstance(result, dict):
                            success = result.get("success", False)
                            message = result.get("message", "未知结果")
                            if success:
                                st.success(message)
                            else:
                                st.error(message)
                        elif isinstance(result, bool):
                            # 如果结果是布尔值
                            if result:
                                st.success("连接成功")
                            else:
                                st.error("连接失败")
                        else:
                            # 其他类型的结果
                            st.error(f"测试连接返回了意外的结果类型: {type(result)}")
            
            with col3:
                if st.button("删除配置", key="delete_config"):
                    if st.session_state.get("confirm_delete") == selected_config_id:
                        # 确认删除
                        success = api_client.delete_search_engine_config(selected_config_id)
                        
                        if success:
                            st.success("配置已删除")
                            st.session_state.pop("confirm_delete", None)
                            st.rerun()
                        else:
                            st.error("删除失败")
                    else:
                        # 显示确认按钮
                        st.session_state.confirm_delete = selected_config_id
                        st.warning("确认删除？此操作不可撤销")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("确认"):
                                success = api_client.delete_search_engine_config(selected_config_id)
                                
                                if success:
                                    st.success("配置已删除")
                                    st.session_state.pop("confirm_delete", None)
                                    st.rerun()
                                else:
                                    st.error("删除失败")
                        
                        with col2:
                            if st.button("取消"):
                                st.session_state.pop("confirm_delete", None)
                                st.rerun()

def show_config_form(api_client: APIClient):
    """
    显示配置表单
    
    Args:
        api_client: API客户端实例
    """
    # 检查是否是编辑模式
    edit_mode = "edit_config" in st.session_state
    
    if edit_mode:
        st.header("编辑配置")
        st.info("请修改以下表单更新搜索引擎连接配置。所有带 * 的字段为必填项。")
    else:
        st.header("新建配置")
        st.info("请填写以下表单创建新的搜索引擎连接配置。所有带 * 的字段为必填项。")
    
    # 检查是否是编辑模式
    edit_mode = "edit_config" in st.session_state
    
    if edit_mode:
        config = st.session_state["edit_config"]
        # 确保 config 是一个字典
        if not isinstance(config, dict):
            st.error(f"编辑配置数据格式错误: 期望字典，但得到 {type(config)}")
            config = {}
        else:
            st.subheader(f"编辑配置: {config.get('name', '未命名')}")
    else:
        config = {}
    
    # 初始化变量，避免未定义错误
    username = ""
    password = ""
    api_key = ""
    aws_region = ""
    aws_service = ""
    
    # 认证类型选择（在表单外）
    st.write("### 认证信息")
    # 确保 config 是一个字典
    if not isinstance(config, dict):
        config = {}
    
    auth_type = st.selectbox(
        "认证类型",
        options=["none", "basic", "api_key", "aws_sigv4"],
        index=["none", "basic", "api_key", "aws_sigv4"].index(config.get("auth_type", "none")),
        key="auth_type_select"
    )
    
    # 根据认证类型显示不同的输入字段（在表单外）
    if auth_type == "basic":
        st.info("请输入基本认证信息（用户名和密码）")
        # 确保 config 是一个字典
        if not isinstance(config, dict):
            config = {}
        
        username = st.text_input("用户名 *", value=config.get("username", ""), key="basic_username_outside")
        # 密码不显示原值，但在编辑模式下可以留空
        password_help = "新建配置时必填，编辑配置时留空表示不修改密码" if edit_mode else "请输入密码"
        password = st.text_input("密码 *", type="password", value="", help=password_help, key="basic_password_outside")
        if not password and edit_mode:
            st.info("留空表示不修改密码")
    
    # API密钥认证
    elif auth_type == "api_key":
        st.info("请输入API密钥")
        # 确保 config 是一个字典
        if not isinstance(config, dict):
            config = {}
        
        api_key = st.text_input("API密钥 *", type="password", value="", key="api_key_input_outside")
        if not api_key and edit_mode:
            st.info("留空表示不修改API密钥")
    
    # AWS SigV4认证
    elif auth_type == "aws_sigv4":
        st.info("请输入AWS认证信息")
        # 确保 config 是一个字典
        if not isinstance(config, dict):
            config = {}
        
        aws_region = st.text_input("AWS区域 *", value=config.get("aws_region", "ap-northeast-1"), key="aws_region_input_outside")
        aws_service = st.text_input("AWS服务", value=config.get("aws_service", "es"), key="aws_service_input_outside")
    
    # 无认证
    elif auth_type == "none":
        st.info("无需认证信息")
    
    # 创建表单
    with st.form("config_form"):
        # 基本信息
        name = st.text_input("配置名称 *", value=config.get("name", ""))
        description = st.text_area("配置描述", value=config.get("description", ""))
        
        # 搜索引擎类型
        engine_type = st.selectbox(
            "搜索引擎类型",
            options=["elasticsearch", "opensearch"],
            index=0 if config.get("type") != "opensearch" else 1
        )
        
        # 连接信息
        host = st.text_input("主机地址 *", value=config.get("host", ""))
        port = st.number_input("端口号", min_value=1, max_value=65535, value=config.get("port", 443))
        
        # 连接选项
        col1, col2, col3 = st.columns(3)
        
        with col1:
            use_ssl = st.checkbox("使用SSL", value=config.get("use_ssl", True))
        
        with col2:
            verify_certs = st.checkbox("验证证书", value=config.get("verify_certs", True))
        
        with col3:
            http_compress = st.checkbox("HTTP压缩", value=config.get("http_compress", True))
        
        timeout = st.number_input("超时时间（秒）", min_value=1, max_value=300, value=config.get("timeout", 30))
        
        # 提交按钮
        submit_button = st.form_submit_button("保存配置")
    
    # 处理表单提交
    if submit_button:
        # 验证必填字段
        if not name or not host:
            st.error("配置名称和主机地址为必填项（标记为 * 的字段）")
            return
        
        # 构建配置数据
        config_data = {
            "name": name,
            "description": description,
            "type": engine_type,
            "host": host,
            "port": port,
            "use_ssl": use_ssl,
            "verify_certs": verify_certs,
            "http_compress": http_compress,
            "timeout": timeout,
            "auth_type": auth_type
        }
        
        # 添加认证信息
        if auth_type == "basic":
            if not username:
                st.error("用户名为必填项")
                return
            
            config_data["username"] = username
            
            # 只有在新建或密码不为空时才设置密码
            if password or not edit_mode:
                if not password and not edit_mode:
                    st.error("密码为必填项")
                    return
                config_data["password"] = password
            
            # 调试信息
            st.write(f"认证类型: {auth_type}")
            st.write(f"用户名: {username}")
            st.write(f"密码长度: {len(password) if password else 0}")
        
        elif auth_type == "api_key":
            # 只有在新建或API密钥不为空时才设置API密钥
            if api_key or not edit_mode:
                if not api_key and not edit_mode:
                    st.error("API密钥为必填项")
                    return
                config_data["api_key"] = api_key
            
            # 调试信息
            st.write(f"认证类型: {auth_type}")
            st.write(f"API密钥长度: {len(api_key) if api_key else 0}")
        
        elif auth_type == "aws_sigv4":
            if not aws_region:
                st.error("AWS区域为必填项")
                return
            
            config_data["aws_region"] = aws_region
            config_data["aws_service"] = aws_service
            
            # 调试信息
            st.write(f"认证类型: {auth_type}")
            st.write(f"AWS区域: {aws_region}")
            st.write(f"AWS服务: {aws_service}")
        
        # 保存配置
        with st.spinner("正在保存配置..."):
            if edit_mode:
                config_id = api_client.save_search_engine_config(config_data, config.get("config_id"))
            else:
                config_id = api_client.save_search_engine_config(config_data)
            
            if config_id:
                st.success(f"配置已保存，ID: {config_id}")
                
                # 清除编辑状态
                if edit_mode:
                    if "edit_config" in st.session_state:
                        del st.session_state["edit_config"]
                
                # 显示测试按钮
                if st.button("测试连接"):
                    with st.spinner("正在测试连接..."):
                        result = api_client.test_search_engine_connection(config_data)
                        
                        # 调试信息在开发阶段使用，现在注释掉
                        # st.write(f"测试连接结果类型: {type(result)}")
                        
                        # 确保结果是字典类型
                        if isinstance(result, dict):
                            success = result.get("success", False)
                            message = result.get("message", "未知结果")
                            if success:
                                st.success(message)
                            else:
                                st.error(message)
                        elif isinstance(result, bool):
                            # 如果结果是布尔值
                            if result:
                                st.success("连接成功")
                            else:
                                st.error("连接失败")
                        else:
                            # 其他类型的结果
                            st.error(f"测试连接返回了意外的结果类型: {type(result)}")
            else:
                st.error("保存失败")