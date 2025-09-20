"""
DSL查询管理页面
用于创建、保存和执行DSL查询语句
"""

import streamlit as st
import pandas as pd
import json
from typing import Dict, List, Any, Optional
from utils.api_client import APIClient
from streamlit_ace import st_ace

def show_dsl_query_management(api_client: APIClient):
    """
    显示DSL查询管理页面
    
    Args:
        api_client: API客户端实例
    """
    
    # 从log_field_metadata表中获取查询索引名称
    with st.spinner("正在获取查询索引列表..."):
        try:
            # 获取所有查询索引名称
            indices = api_client.get_all_query_index_names()
            st.info(f"找到 {len(indices)} 个查询索引")
        except Exception as e:
            st.error(f"获取查询索引列表失败: {str(e)}")
            indices = []
    
    if not indices:
        st.warning("未找到查询索引，请先在索引字段管理页面创建索引")
        return
    
    # 选择查询索引
    selected_index = st.selectbox("选择查询索引名称", options=indices)
    
    if selected_index:
        # 获取索引元数据以显示更多信息
        index_metadata = api_client.get_index_metadata(selected_index)
        if index_metadata and "index_description" in index_metadata and index_metadata["index_description"]:
            st.info(f"索引描述: {index_metadata['index_description']}")
        
        # 创建选项卡
        tab1, tab2 = st.tabs(["查询编辑器", "查询示例"])
        
        # 查询编辑器选项卡
        with tab1:
            show_query_editor(api_client, selected_index)
        
        # 查询示例选项卡
        with tab2:
            show_query_examples(api_client, selected_index)

def show_query_editor(api_client: APIClient, index_name: str):
    """
    显示查询编辑器
    
    Args:
        api_client: API客户端实例
        index_name: 索引名称
    """
    st.header("查询编辑器")
    
    # 获取配置列表
    configs = api_client.list_search_engine_configs()
    
    if not configs:
        st.error("暂无数据源配置，无法执行查询")
        return
    
    # 选择配置
    config_options = {config["config_id"]: f"{config['name']} ({config['host']})" for config in configs}
    selected_config_id = st.selectbox(
        "选择数据源配置",
        options=list(config_options.keys()),
        format_func=lambda x: config_options[x]
    )
    
    # 初始化查询状态
    if "current_query" not in st.session_state:
        st.session_state.current_query = {
            "index_name": index_name,
            "query_dsl": "{\n  \"query\": {\n    \"match_all\": {}\n  }\n}",
            "name": "",
            "description": "",
            "tags": [],
            "config_id": selected_config_id,
            "log_field_metadata_index_name": index_name,  # 添加log_field_metadata索引名称
            "query_id": None  # 添加查询ID字段
        }
    
    # 如果索引变更，更新查询状态
    if st.session_state.current_query["index_name"] != index_name:
        st.session_state.current_query["index_name"] = index_name
        st.session_state.current_query["log_field_metadata_index_name"] = index_name  # 同时更新log_field_metadata索引名称
    
    # 如果配置变更，更新查询状态
    if "config_id" not in st.session_state.current_query or st.session_state.current_query["config_id"] != selected_config_id:
        st.session_state.current_query["config_id"] = selected_config_id
    
    # 查询基本信息
    col1, col2 = st.columns(2)
    
    with col1:
        query_name = st.text_input(
            "查询名称",
            value=st.session_state.current_query.get("name", ""),
            help="为查询起一个简短的名称"
        )
        st.session_state.current_query["name"] = query_name
    
    with col2:
        # 标签输入
        current_tags = st.session_state.current_query.get("tags", [])
        user_tags = [tag for tag in current_tags if not tag.startswith("config:")]
        
        tags_input = st.text_input(
            "标签（用逗号分隔）",
            value=", ".join(user_tags),
            help="输入标签，用逗号分隔，便于分类和搜索"
        )
        
        # 处理标签
        new_user_tags = []
        if tags_input.strip():
            new_user_tags = [tag.strip() for tag in tags_input.split(",") if tag.strip()]
        
        # 保留config:开头的标签并添加新的用户标签
        config_tags = [tag for tag in current_tags if tag.startswith("config:")]
        st.session_state.current_query["tags"] = new_user_tags + config_tags
    
    # 查询说明字段
    query_description = st.text_area(
        "查询说明",
        value=st.session_state.current_query.get("description", ""),
        height=100,
        help="请输入查询的用途、目标或其他说明信息"
    )
    
    # 更新查询状态中的说明
    st.session_state.current_query["description"] = query_description
    
    # 查询编辑器
    query_dsl = st_ace(
        value=st.session_state.current_query["query_dsl"],
        language="json",
        theme="monokai",
        key="query_editor",
        height=300,
        auto_update=True
    )
    
    # 更新查询状态
    st.session_state.current_query["query_dsl"] = query_dsl
    
    # 操作按钮
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("执行查询", key="execute_query", type="primary"):
            execute_query(api_client, index_name, query_dsl, selected_config_id)
    
    with col2:
        if st.button("保存查询", key="save_query"):
            save_current_query(api_client, index_name, query_dsl, selected_config_id)
    
    with col3:
        if st.button("清空编辑器", key="clear_query"):
            # 重置查询状态
            st.session_state.current_query = {
                "index_name": index_name,
                "query_dsl": "{\n  \"query\": {\n    \"match_all\": {}\n  }\n}",
                "name": "",
                "description": "",
                "tags": [],
                "config_id": selected_config_id,
                "log_field_metadata_index_name": index_name,
                "query_id": None
            }
            st.success("编辑器已清空")
            st.rerun()

def save_current_query(api_client: APIClient, index_name: str, query_dsl: str, selected_config_id: str):
    """保存当前查询"""
    # 验证必填字段
    if not st.session_state.current_query.get("name", "").strip():
        st.error("请输入查询名称")
        return
    
    if not st.session_state.current_query.get("description", "").strip():
        st.error("请输入查询说明")
        return
    
    # 验证DSL语句
    try:
        json.loads(query_dsl)
    except json.JSONDecodeError:
        st.error("DSL查询语句格式错误，请检查JSON语法")
        return
    
    with st.spinner("正在保存..."):
        # 创建包含config_id的标签
        tags = st.session_state.current_query.get("tags", [])
        config_tag = f"config:{selected_config_id}"
        
        # 确保不重复添加config标签
        if not any(tag.startswith("config:") for tag in tags):
            tags.append(config_tag)
        else:
            # 更新现有的config标签
            tags = [tag for tag in tags if not tag.startswith("config:")]
            tags.append(config_tag)
        
        # 保存查询时，添加log_field_metadata索引名称
        query_data = {
            "data_source_id": index_name,  # 使用索引名称作为数据源ID
            "name": st.session_state.current_query.get("name", ""),  # 查询名称
            "description": st.session_state.current_query.get("description", ""),  # 使用查询说明
            "dsl_query": query_dsl,  # 使用DSL语句
            "tags": tags,  # 包含config_id的标签
            "log_field_metadata_index_name": index_name  # 添加log_field_metadata索引名称
        }
        
        query_id = api_client.save_dsl_query(
            query_data,
            query_id=st.session_state.current_query.get("query_id")
        )
        
        if query_id:
            is_update = st.session_state.current_query.get("query_id") is not None
            action = "更新" if is_update else "保存"
            st.success(f"查询已{action}成功，ID: {query_id}")
            
            # 更新查询状态
            st.session_state.current_query["query_id"] = query_id
            st.session_state.current_query["tags"] = tags
        else:
            st.error("保存失败，请检查输入并重试")

def execute_query(api_client: APIClient, index_name: str, query_dsl: str, config_id: str = None):
    """
    执行查询
    
    Args:
        api_client: API客户端实例
        index_name: 索引名称（实际查询的索引名称）
        query_dsl: DSL查询语句
        config_id: 配置ID，如果为None则使用第一个可用配置
    """
    try:
        # 验证查询语句
        json.loads(query_dsl)
        
        # 如果没有提供config_id，获取配置列表并使用第一个
        if not config_id:
            configs = api_client.list_search_engine_configs()
            
            if not configs:
                st.error("暂无数据源配置，无法执行查询")
                return
            
            # 使用第一个配置
            config_id = configs[0]["config_id"]
        
        # 执行查询
        with st.spinner("正在执行查询..."):
            result = api_client.execute_dsl_query(config_id, index_name, query_dsl)
        
        # 显示结果
        if "error" in result:
            st.error(f"查询失败: {result['error']}")
        else:
            st.success("查询成功")
            
            # 显示查询统计
            if "hits" in result and "total" in result["hits"]:
                total = result["hits"]["total"]
                if isinstance(total, dict) and "value" in total:
                    st.info(f"找到 {total['value']} 条匹配记录")
                else:
                    st.info(f"找到 {total} 条匹配记录")
            
            # 显示查询结果
            with st.expander("查询结果", expanded=True):
                st.json(result)
            
            # 提取并显示匹配文档
            if "hits" in result and "hits" in result["hits"]:
                hits = result["hits"]["hits"]
                
                if hits:
                    st.subheader("匹配文档")
                    
                    # 创建表格数据
                    table_data = []
                    
                    for hit in hits:
                        row = {
                            "_id": hit.get("_id", ""),
                            "_score": hit.get("_score", 0)
                        }
                        
                        # 添加文档字段
                        source = hit.get("_source", {})
                        for key, value in source.items():
                            if isinstance(value, (str, int, float, bool)):
                                row[key] = value
                        
                        table_data.append(row)
                    
                    # 显示表格
                    if table_data:
                        df = pd.DataFrame(table_data)
                        st.dataframe(df, use_container_width=True)
    
    except json.JSONDecodeError:
        st.error("查询语句格式错误，请检查JSON语法")
    except Exception as e:
        st.error(f"执行查询时发生错误: {str(e)}")

# 移除show_save_query_form函数，因为现在直接在查询编辑器中保存

def show_query_examples(api_client: APIClient, index_name: str):
    """
    显示查询示例列表
    
    Args:
        api_client: API客户端实例
        index_name: 索引名称（用作log_field_metadata_index_name）
    """
    st.header("查询示例管理")
    
    # 添加搜索和过滤功能
    col1, col2 = st.columns([2, 1])
    with col1:
        search_term = st.text_input("搜索查询（按描述或标签）", placeholder="输入关键词搜索...")
    with col2:
        show_all = st.checkbox("显示所有索引的查询", value=False)
    
    # 获取查询示例列表
    with st.spinner("正在获取查询示例..."):
        if show_all:
            queries = api_client.list_dsl_queries()  # 获取所有查询
        else:
            queries = api_client.list_dsl_queries(index_name=index_name)  # 只获取当前索引的查询
    
    # 应用搜索过滤
    if search_term:
        filtered_queries = []
        for query in queries:
            description = query.get("description", "").lower()
            tags = " ".join(query.get("tags", [])).lower()
            if search_term.lower() in description or search_term.lower() in tags:
                filtered_queries.append(query)
        queries = filtered_queries
    
    if not queries:
        if search_term:
            st.info("未找到匹配的查询示例")
        else:
            st.info("暂无查询示例，请先创建并保存查询")
        return
    
    st.info(f"找到 {len(queries)} 个查询示例")
    
    # 创建查询表格
    df = pd.DataFrame(queries)
    
    # 处理显示列
    display_data = []
    for query in queries:
        row = {
            "查询ID": query.get("query_id", "")[:8] + "...",  # 显示前8位
            "描述": query.get("description", "")[:50] + ("..." if len(query.get("description", "")) > 50 else ""),
            "标签": ", ".join(query.get("tags", [])[:3]) + ("..." if len(query.get("tags", [])) > 3 else ""),
            "索引": query.get("log_field_metadata_index_name", query.get("data_source_id", "")),
            "创建时间": query.get("created_at", "")[:19] if query.get("created_at") else "",
            "完整ID": query.get("query_id", "")  # 用于选择
        }
        display_data.append(row)
    
    if display_data:
        display_df = pd.DataFrame(display_data)
        
        # 显示表格（不包含完整ID列）
        st.dataframe(display_df.drop(columns=["完整ID"]), use_container_width=True)
        
        # 选择查询示例
        query_options = {row["完整ID"]: f"{row['描述'][:30]}..." if len(row['描述']) > 30 else row['描述'] 
                        for row in display_data}
        
        selected_query_id = st.selectbox(
            "选择要操作的查询示例",
            options=list(query_options.keys()),
            format_func=lambda x: query_options[x],
            key="query_selector"
        )
        
        if selected_query_id:
            # 获取选中的查询
            selected_query = next((query for query in queries if query["query_id"] == selected_query_id), None)
            
            if selected_query:
                # 显示查询详情和编辑功能
                show_query_details_and_edit(api_client, selected_query, index_name)
    else:
        st.error("查询数据格式不正确")

def show_query_details_and_edit(api_client: APIClient, selected_query: Dict[str, Any], current_index_name: str):
    """
    显示查询详情和编辑功能
    
    Args:
        api_client: API客户端实例
        selected_query: 选中的查询
        current_index_name: 当前索引名称
    """
    # 创建选项卡
    tab1, tab2 = st.tabs(["查询详情", "编辑查询"])
    
    with tab1:
        # 显示查询详情
        st.subheader("查询详情")
        
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**查询ID:** {selected_query.get('query_id', '')}")
            st.write(f"**描述:** {selected_query.get('description', '')}")
            st.write(f"**索引:** {selected_query.get('log_field_metadata_index_name', selected_query.get('data_source_id', ''))}")
        
        with col2:
            if "tags" in selected_query and selected_query["tags"]:
                st.write(f"**标签:** {', '.join(selected_query['tags'])}")
            st.write(f"**创建时间:** {selected_query.get('created_at', '')}")
            st.write(f"**更新时间:** {selected_query.get('updated_at', '')}")
        
        st.subheader("DSL查询语句")
        st.code(selected_query.get("dsl_query", ""), language="json")
        
        # 操作按钮
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("加载到编辑器", key="load_query"):
                load_query_to_editor(api_client, selected_query, current_index_name)
        
        with col2:
            if st.button("执行查询", key="execute_example"):
                execute_selected_query(api_client, selected_query, current_index_name)
        
        with col3:
            if st.button("复制查询", key="copy_query"):
                copy_query(api_client, selected_query, current_index_name)
        
        with col4:
            if st.button("删除查询", key="delete_query", type="secondary"):
                delete_query_with_confirmation(api_client, selected_query)
    
    with tab2:
        # 编辑查询
        edit_query_form(api_client, selected_query, current_index_name)

def load_query_to_editor(api_client: APIClient, selected_query: Dict[str, Any], current_index_name: str):
    """加载查询到编辑器"""
    # 从标签中提取config_id
    config_id = None
    tags = selected_query.get("tags", [])
    for tag in tags:
        if tag.startswith("config:"):
            config_id = tag.replace("config:", "")
            break
    
    # 如果没有找到config_id，使用当前选择的配置
    if not config_id:
        configs = api_client.list_search_engine_configs()
        if configs:
            config_id = configs[0]["config_id"]
    
    # 更新查询状态
    metadata_index_name = selected_query.get("log_field_metadata_index_name", 
                                           selected_query.get("data_source_id", current_index_name))
    
    st.session_state.current_query = {
        "index_name": selected_query.get("data_source_id", current_index_name),
        "query_dsl": selected_query.get("dsl_query", ""),
        "name": selected_query.get("name", ""),
        "description": selected_query.get("description", ""),
        "tags": selected_query.get("tags", []),
        "query_id": selected_query.get("query_id"),
        "config_id": config_id,
        "log_field_metadata_index_name": metadata_index_name
    }

    st.success("查询已加载到编辑器")
    st.rerun()

def execute_selected_query(api_client: APIClient, selected_query: Dict[str, Any], current_index_name: str):
    """执行选中的查询"""
    # 从标签中提取config_id
    config_id = None
    tags = selected_query.get("tags", [])
    for tag in tags:
        if tag.startswith("config:"):
            config_id = tag.replace("config:", "")
            break
    
    # 使用查询中保存的索引名称
    query_index_name = selected_query.get("data_source_id", current_index_name)
    
    execute_query(api_client, query_index_name, selected_query.get("dsl_query", ""), config_id)

def copy_query(api_client: APIClient, selected_query: Dict[str, Any], current_index_name: str):
    """复制查询"""
    # 创建查询副本
    new_query_data = {
        "data_source_id": selected_query.get("data_source_id", current_index_name),
        "description": f"[副本] {selected_query.get('description', '')}",
        "dsl_query": selected_query.get("dsl_query", ""),
        "tags": selected_query.get("tags", []),
        "log_field_metadata_index_name": selected_query.get("log_field_metadata_index_name", current_index_name)
    }
    
    with st.spinner("正在复制查询..."):
        query_id = api_client.save_dsl_query(new_query_data)
        
        if query_id:
            st.success(f"查询已复制，新查询ID: {query_id}")
            st.rerun()
        else:
            st.error("复制失败")

def delete_query_with_confirmation(api_client: APIClient, selected_query: Dict[str, Any]):
    """删除查询（带确认）"""
    query_id = selected_query.get("query_id")
    
    if st.session_state.get("confirm_delete") == query_id:
        # 确认删除
        success = api_client.delete_dsl_query(query_id)
        
        if success:
            st.success("查询已删除")
            st.session_state.pop("confirm_delete", None)
            st.rerun()
        else:
            st.error("删除失败")
    else:
        # 显示确认
        st.session_state.confirm_delete = query_id
        st.warning("⚠️ 确认删除此查询？此操作不可撤销")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("确认删除", key="confirm_delete_btn", type="primary"):
                success = api_client.delete_dsl_query(query_id)
                
                if success:
                    st.success("查询已删除")
                    st.session_state.pop("confirm_delete", None)
                    st.rerun()
                else:
                    st.error("删除失败")
        
        with col2:
            if st.button("取消", key="cancel_delete_btn"):
                st.session_state.pop("confirm_delete", None)
                st.rerun()

def edit_query_form(api_client: APIClient, selected_query: Dict[str, Any], current_index_name: str):
    """编辑查询表单"""
    st.subheader("编辑查询")
    
    # 初始化编辑状态
    if "edit_query" not in st.session_state:
        st.session_state.edit_query = {
            "description": selected_query.get("description", ""),
            "dsl_query": selected_query.get("dsl_query", ""),
            "tags": selected_query.get("tags", [])
        }
    
    # 编辑表单
    with st.form("edit_query_form"):
        # 查询描述
        new_description = st.text_area(
            "查询描述",
            value=st.session_state.edit_query["description"],
            height=100
        )
        
        # DSL查询语句
        new_dsl_query = st_ace(
            value=st.session_state.edit_query["dsl_query"],
            language="json",
            theme="monokai",
            key="edit_query_editor",
            height=300,
            auto_update=True
        )
        
        # 标签编辑
        current_tags = st.session_state.edit_query["tags"]
        # 过滤掉config:开头的标签，这些由系统管理
        user_tags = [tag for tag in current_tags if not tag.startswith("config:")]
        
        tags_input = st.text_input(
            "标签（用逗号分隔）",
            value=", ".join(user_tags),
            help="输入标签，用逗号分隔。config:开头的标签由系统自动管理。"
        )
        
        # 提交按钮
        col1, col2 = st.columns(2)
        with col1:
            submitted = st.form_submit_button("保存修改", type="primary")
        with col2:
            reset = st.form_submit_button("重置")
        
        if submitted:
            # 验证DSL语句
            try:
                json.loads(new_dsl_query)
                
                # 处理标签
                new_tags = []
                if tags_input.strip():
                    new_tags = [tag.strip() for tag in tags_input.split(",") if tag.strip()]
                
                # 保留config:开头的标签
                config_tags = [tag for tag in current_tags if tag.startswith("config:")]
                new_tags.extend(config_tags)
                
                # 更新查询
                query_data = {
                    "data_source_id": selected_query.get("data_source_id", current_index_name),
                    "description": new_description,
                    "dsl_query": new_dsl_query,
                    "tags": new_tags,
                    "log_field_metadata_index_name": selected_query.get("log_field_metadata_index_name", current_index_name)
                }
                
                with st.spinner("正在保存修改..."):
                    query_id = api_client.save_dsl_query(query_data, query_id=selected_query.get("query_id"))
                    
                    if query_id:
                        st.success("查询已更新")
                        # 清除编辑状态
                        st.session_state.pop("edit_query", None)
                        st.rerun()
                    else:
                        st.error("保存失败")
            
            except json.JSONDecodeError:
                st.error("DSL查询语句格式错误，请检查JSON语法")
        
        if reset:
            # 重置为原始值
            st.session_state.edit_query = {
                "description": selected_query.get("description", ""),
                "dsl_query": selected_query.get("dsl_query", ""),
                "tags": selected_query.get("tags", [])
            }
            st.rerun()
