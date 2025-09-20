"""
索引字段管理页面
用于查看和编辑索引字段信息
"""

import streamlit as st
import pandas as pd
import json
from typing import Dict, List, Any, Optional
from utils.api_client import APIClient

def show_index_field_management(api_client: APIClient):
    """
    显示索引字段管理页面
    
    Args:
        api_client: API客户端实例
    """
    
    # 初始化会话状态
    if 'opensearch_fields' not in st.session_state:
        st.session_state.opensearch_fields = []
    
    if 'dynamodb_fields' not in st.session_state:
        st.session_state.dynamodb_fields = []
    
    if 'display_fields' not in st.session_state:
        st.session_state.display_fields = []
        
    # 添加一个状态来跟踪是否已经从OpenSearch加载了字段
    if 'has_loaded_from_opensearch' not in st.session_state:
        st.session_state.has_loaded_from_opensearch = False
    
    # 添加手工编辑字段的状态
    if 'custom_fields' not in st.session_state:
        st.session_state.custom_fields = []
    
    if 'edit_mode' not in st.session_state:
        st.session_state.edit_mode = False
    
    # 1. 选择数据源
    configs = api_client.list_search_engine_configs()
    
    if not configs:
        st.info("暂无数据源配置，请先在数据源配置页面创建配置")
        return
    
    config_options = {config["config_id"]: f"{config['name']} ({config['host']})" for config in configs}
    selected_config_id = st.selectbox("选择数据源", options=list(config_options.keys()), format_func=lambda x: config_options[x])
    
    if not selected_config_id:
        st.info("请选择数据源以继续")
        return
    
    # 同时加载数据源、索引和查询索引名称
    col1, col2 = st.columns(2)
    
    # 并行加载索引列表和查询索引名称
    indices = []
    query_index_names = []
    
    # 使用两个并行的spinner来同时加载数据
    with st.container():
        loading_col1, loading_col2 = st.columns(2)
        
        with loading_col1:
            with st.spinner("正在获取索引列表..."):
                try:
                    indices = api_client.get_indices(selected_config_id)
                except Exception as e:
                    st.error(f"获取索引列表时发生错误: {str(e)}")
                    indices = []
        
        with loading_col2:
            with st.spinner("正在加载查询索引名称..."):
                try:
                    query_index_names = api_client.get_all_indices()
                except Exception as e:
                    st.error(f"获取查询索引名称时发生错误: {str(e)}")
                    query_index_names = []
    
    with col1:
        # 2. 基于数据源选择索引
        if not indices:
            st.warning("未找到索引。可能的原因：\n- 数据源连接失败（网络问题或SSL证书问题）\n- 数据源配置不正确\n- 没有可访问的索引")
            st.info("💡 提示：即使无法连接到OpenSearch，您仍然可以使用查询索引名称功能来管理已保存的字段配置")
            index_options = ["请选择索引"]
        else:
            # 添加一个空选项作为默认值
            index_options = ["请选择索引"] + indices
        
        selected_index_option = st.selectbox("选择索引", options=index_options, index=0)
        
        # 处理索引选择
        selected_index = None
        if selected_index_option != "请选择索引":
            selected_index = selected_index_option
            # 如果索引选择发生变化，重置已加载标志
            if 'last_selected_index' not in st.session_state or st.session_state.last_selected_index != selected_index:
                st.session_state.has_loaded_from_opensearch = False
                st.session_state.last_selected_index = selected_index
    
    with col2:
        # 3. 选择查询索引名称（已并行加载）
        # 添加"新建查询索引"和"请选择"选项
        query_index_options = ["请选择查询索引名称", "新建查询索引"] + query_index_names
        selected_query_index = st.selectbox(
            "选择查询索引名称",
            options=query_index_options,
            index=0,
            key="query_index_selector"
        )
        
        # 添加删除查询索引名称的按钮
        if selected_query_index not in ["请选择查询索引名称", "新建查询索引"]:
            if st.button(f"删除查询索引 '{selected_query_index}'", key="delete_query_index", type="secondary"):
                if st.session_state.get('confirm_delete', False):
                    # 执行删除操作
                    with st.spinner(f"正在删除查询索引 '{selected_query_index}'..."):
                        success = api_client.delete_query_index(selected_query_index)
                        if success:
                            st.success(f"成功删除查询索引 '{selected_query_index}'")
                            # 重置确认状态并刷新页面
                            st.session_state.confirm_delete = False
                            st.rerun()
                        else:
                            st.error(f"删除查询索引 '{selected_query_index}' 失败")
                    st.session_state.confirm_delete = False
                else:
                    # 显示确认对话框
                    st.session_state.confirm_delete = True
                    st.warning(f"确认要删除查询索引 '{selected_query_index}' 吗？这将删除所有相关的字段配置。请再次点击删除按钮确认。")
    
    # 处理字段加载逻辑
    query_index_name = ""
    index_description = ""
    
    # 根据选择的查询索引名称处理
    if selected_query_index == "新建查询索引":
        # 如果选择了"新建查询索引"，清空输入框
        query_index_name = ""
        index_description = ""
    elif selected_query_index != "请选择查询索引名称":
        # 如果选择了现有的查询索引名称，获取对应的描述
        query_index_name = selected_query_index
        
        # 从log_field_metadata表中获取索引信息
        with st.spinner(f"正在加载查询索引 {query_index_name} 的信息..."):
            # 获取字段信息，其中包含索引描述
            dynamodb_fields = api_client.get_index_fields(config_id=selected_config_id, selected_index=None, query_index_name=query_index_name)
            st.session_state.dynamodb_fields = dynamodb_fields
            
            # 从字段元数据中获取索引描述
            index_metadata = api_client.get_index_metadata(query_index_name)
            if index_metadata and "index_description" in index_metadata:
                index_description = index_metadata.get("index_description", "")
            else:
                index_description = ""
            
            # 如果从DynamoDB获取到了字段信息，则使用它
            if dynamodb_fields:
                st.session_state.display_fields = dynamodb_fields
            else:
                # 如果没有从DynamoDB获取到字段信息，但已经从OpenSearch加载了字段，则保留OpenSearch的字段
                if st.session_state.has_loaded_from_opensearch and len(st.session_state.opensearch_fields) > 0:
                    # 保持display_fields不变，继续使用OpenSearch加载的字段
                    pass
                else:
                    st.session_state.display_fields = []
    
    # 根据选择的索引处理OpenSearch字段加载
    if selected_index:
        # 自动从OpenSearch加载字段信息
        with st.spinner(f"正在从OpenSearch加载索引 {selected_index} 的字段信息..."):
            try:
                opensearch_fields = api_client.get_index_fields(selected_config_id, selected_index)
                if opensearch_fields:
                    st.session_state.opensearch_fields = opensearch_fields
                    # 如果没有从DynamoDB加载字段，则使用OpenSearch字段
                    if not st.session_state.display_fields:
                        st.session_state.display_fields = opensearch_fields
                    st.session_state.has_loaded_from_opensearch = True
                else:
                    st.warning(f"索引 {selected_index} 没有可用的字段信息")
            except Exception as e:
                st.error(f"从OpenSearch加载字段信息时发生错误: {str(e)}")
                st.info("💡 提示：您仍然可以使用查询索引名称功能来管理已保存的字段配置")
    
    # 添加查询索引输入框和索引描述输入框
    # 查询索引名称输入框（必填）
    input_query_index_name = st.text_input(
        "查询索引名称（必填，支持别名或通配符，如 logs-* 或 app-logs）", 
        value=query_index_name,
        placeholder="请输入查询索引名称或从上方选择"
    )
    
    # 添加提示信息
    st.info("由于索引会按大小和时间滚动，可以使用别名或通配符来查询索引，例如 'logs-*' 或 'app-logs'")
    
    # 索引描述
    input_index_description = st.text_area(
        "索引描述", 
        value=index_description,
        height=100,
        placeholder="请输入索引描述",
        key="index_description_input"
    )
    
    # 显示字段列表
    show_field_list(api_client, selected_index, st.session_state.display_fields, input_query_index_name, selected_config_id, input_index_description)

def show_field_list(api_client: APIClient, selected_index: str, fields: List[Dict[str, Any]], query_index_name: str, selected_config_id: str = None, index_description: str = ""):
    """
    显示字段列表
    
    Args:
        api_client: API客户端实例
        selected_index: 选择的索引名称
        fields: 字段信息列表
        query_index_name: 查询索引名称
        selected_config_id: 选择的配置ID
        index_description: 索引描述
    """
    st.header("字段列表管理")
    
    # 添加编辑模式切换
    col1, col2, col3 = st.columns(3)
    
    with col1:
        edit_mode = st.toggle("启用手工编辑模式", value=st.session_state.edit_mode, key="edit_mode_toggle")
        st.session_state.edit_mode = edit_mode
    
    with col2:
        if st.button("重置为原始字段", key="reset_to_original"):
            st.session_state.custom_fields = fields.copy() if fields else []
            st.session_state.display_fields = st.session_state.custom_fields.copy()
            st.success("已重置为原始字段")
            st.rerun()
    
    with col3:
        if st.button("清空所有字段", key="clear_all_fields"):
            st.session_state.custom_fields = []
            st.session_state.display_fields = []
            st.success("已清空所有字段")
            st.rerun()
    
    # 根据编辑模式显示不同的界面
    if edit_mode:
        # 手工编辑模式
        show_manual_edit_interface(api_client, query_index_name, index_description, selected_config_id, fields)
    else:
        # 原有的批量编辑模式
        if not fields:
            st.warning("未找到字段信息")
            return
        
        # 显示当前使用的索引名称
        if query_index_name:
            st.info(f"当前使用的查询索引名称: {query_index_name}")
            # 创建可编辑的数据框
            show_batch_edit_form(api_client, query_index_name, selected_index, fields, selected_config_id)
        else:
            # 提示用户输入查询索引名称
            st.warning("请输入查询索引名称才能保存字段信息")
            # 仍然显示可编辑的表单，但保存时会要求输入查询索引名称
            show_batch_edit_form(api_client, "", selected_index, fields, selected_config_id)

def show_batch_edit_form(api_client: APIClient, query_index_name: str, selected_index: str, fields: List[Dict[str, Any]], selected_config_id: str = None):
    """
    显示批量编辑表单
    
    Args:
        api_client: API客户端实例
        query_index_name: 查询索引名称
        selected_index: 选择的索引名称
        fields: 字段信息列表
        selected_config_id: 选择的配置ID
    """
    st.subheader("字段列表")
    
    if not fields:
        st.info("没有可编辑的字段")
        return
    
    # 创建一个可编辑的数据框
    edited_data = []
    for field in fields:
        edited_data.append({
            "field_name": field.get("field_name", ""),
            "名称": field.get("field_name", ""),
            "类型": field.get("field_type", ""),
            "描述": field.get("description", "")
        })
    
    # 创建可编辑的数据框
    edited_df = pd.DataFrame(edited_data)
    
    # 只显示名称、类型和描述列，但保留field_name作为标识符
    display_columns = ["名称", "类型", "描述"]
    
    # 使用Streamlit的可编辑数据框
    edited_df_result = st.data_editor(
        edited_df,
        column_config={
            "field_name": None,  # 隐藏field_name列
            "名称": st.column_config.TextColumn("名称", disabled=True),
            "类型": st.column_config.TextColumn("类型", disabled=True),
            "描述": st.column_config.TextColumn("描述")
        },
        use_container_width=True,
        hide_index=True,
        num_rows="fixed"
    )
    
    # 添加保存按钮
    if st.button("保存字段描述"):
        # 验证查询索引名称是否存在
        save_index_name = query_index_name
        if not save_index_name:
            # 如果没有提供查询索引名称，提示用户输入
            st.error("查询索引名称不能为空，请在上方输入查询索引名称")
        else:
            # 获取索引描述
            input_index_description = st.session_state.get("index_description_input", "")
            
            # 构建完整的字段信息，包括类型和描述
            field_info_list = []
            for i, row in edited_df_result.iterrows():
                field_name = row["field_name"]
                field_type = row["类型"]  # 从表格中获取字段类型
                description = row["描述"]
                if field_name:
                    field_info_list.append({
                        "field_name": field_name,
                        "field_type": field_type,
                        "description": description
                    })
            
            # 保存操作状态到session_state
            if 'save_status' not in st.session_state:
                st.session_state.save_status = None
                st.session_state.save_message = ""
            
            with st.spinner(f"正在保存字段到查询索引 '{save_index_name}'..."):
                # 使用save_index_with_fields方法保存完整的字段信息
                success = api_client.save_index_with_fields(save_index_name, input_index_description, field_info_list)
                
                # 更新保存状态
                st.session_state.save_status = success
                if success:
                    st.session_state.save_message = f"成功更新 {len(field_info_list)} 个字段到查询索引 '{save_index_name}'"
                else:
                    st.session_state.save_message = "保存失败"
            
            # 显示保存状态
            if st.session_state.save_status is not None:
                if st.session_state.save_status:
                    st.success(st.session_state.save_message)
                    # 保存成功后刷新页面以更新查询索引名称列表
                    st.rerun()
                else:
                    st.error(st.session_state.save_message)

def show_manual_edit_interface(api_client: APIClient, query_index_name: str, index_description: str, selected_config_id: str, original_fields: List[Dict[str, Any]]):
    """
    显示手工编辑界面
    
    Args:
        api_client: API客户端实例
        query_index_name: 查询索引名称
        index_description: 索引描述
        selected_config_id: 选择的配置ID
        original_fields: 原始字段列表
    """
    st.subheader("字段编辑器")
    
    # 初始化自定义字段列表
    if not st.session_state.custom_fields:
        st.session_state.custom_fields = original_fields.copy() if original_fields else []
    
    # 快速操作按钮
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("从OpenSearch同步", key="sync_from_opensearch"):
            if original_fields:
                st.session_state.custom_fields = original_fields.copy()
                st.success(f"已同步 {len(original_fields)} 个字段")
                st.rerun()
            else:
                st.warning("没有可同步的OpenSearch字段")
    
    with col2:
        if st.button("导出字段配置", key="export_fields"):
            if st.session_state.custom_fields:
                export_data = json.dumps(st.session_state.custom_fields, ensure_ascii=False, indent=2)
                st.download_button(
                    label="下载字段配置",
                    data=export_data,
                    file_name="fields_config.json",
                    mime="application/json"
                )
            else:
                st.info("暂无字段可导出")
    
    with col3:
        # 批量导入功能
        uploaded_file = st.file_uploader(
            "导入字段配置",
            type=['json'],
            help="上传JSON格式的字段配置文件"
        )
        
        if uploaded_file is not None:
            try:
                file_content = uploaded_file.read().decode('utf-8')
                imported_fields = json.loads(file_content)
                
                if isinstance(imported_fields, list):
                    valid_fields = []
                    for field in imported_fields:
                        if isinstance(field, dict) and "field_name" in field:
                            valid_fields.append({
                                "field_name": field.get("field_name", ""),
                                "field_type": field.get("field_type", "text"),
                                "description": field.get("description", "")
                            })
                    
                    if valid_fields:
                        st.session_state.custom_fields.extend(valid_fields)
                        st.success(f"成功导入 {len(valid_fields)} 个字段")
                        st.rerun()
                else:
                    st.error("文件格式错误，需要JSON数组格式")
            except Exception as e:
                st.error(f"导入失败: {str(e)}")
    
    # 统一的字段编辑表格 - 支持增加和删除
    show_unified_fields_table(api_client, query_index_name, index_description, selected_config_id)

def show_unified_fields_table(api_client: APIClient, query_index_name: str, index_description: str, selected_config_id: str):
    """
    显示统一的字段编辑表格，支持增加、删除、编辑
    
    Args:
        api_client: API客户端实例
        query_index_name: 查询索引名称
        index_description: 索引描述
        selected_config_id: 选择的配置ID
    """
    st.write("### 字段管理表格")
    st.info("💡 提示：可以直接在表格中编辑字段，添加新行来增加字段，删除行来删除字段")
    
    # 准备表格数据
    if not st.session_state.custom_fields:
        # 如果没有字段，添加一个空行供用户开始编辑
        st.session_state.custom_fields = [{"field_name": "", "field_type": "text", "description": ""}]
    
    field_data = []
    for i, field in enumerate(st.session_state.custom_fields):
        field_data.append({
            "字段名称": field.get("field_name", ""),
            "字段类型": field.get("field_type", "text"),
            "描述": field.get("description", "")
        })
    
    # 创建DataFrame
    df = pd.DataFrame(field_data)
    
    # 使用可编辑的数据编辑器 - 支持动态行数
    edited_df = st.data_editor(
        df,
        column_config={
            "字段名称": st.column_config.TextColumn(
                "字段名称",
                help="输入字段名称，如：user_id, timestamp",
                required=True,
                width="medium"
            ),
            "字段类型": st.column_config.SelectboxColumn(
                "字段类型",
                options=[
                    "text", "keyword", "long", "integer", "double", "float", "boolean", "date", 
                    "object", "nested", "ip", "geo_point", "geo_shape", "binary", "range",
                    "alias", "flattened", "search_as_you_type", "token_count", "dense_vector",
                    "sparse_vector", "rank_feature", "rank_features", "completion", "percolator",
                    "join", "histogram", "constant_keyword", "wildcard", "version", "aggregate_metric_double"
                ],
                default="text",
                help="选择合适的字段类型，支持所有 Elasticsearch/OpenSearch 类型",
                width="small"
            ),
            "描述": st.column_config.TextColumn(
                "描述",
                help="输入字段描述信息",
                width="large"
            )
        },
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",  # 允许动态添加/删除行
        key="unified_fields_table"
    )
    
    # 处理字段更新
    updated_fields = []
    for _, row in edited_df.iterrows():
        field_name = str(row["字段名称"]).strip()
        field_type = str(row["字段类型"]).strip()
        description = str(row["描述"]).strip()
        
        # 只保留有名称的字段
        if field_name:
            updated_fields.append({
                "field_name": field_name,
                "field_type": field_type,
                "description": description
            })
    
    # 更新会话状态
    st.session_state.custom_fields = updated_fields
    
    # 显示字段统计
    valid_fields_count = len([f for f in updated_fields if f["field_name"]])
    st.info(f"当前有效字段数量: {valid_fields_count}")
    
    # 操作按钮
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("保存字段配置", key="save_unified_fields", type="primary"):
            if not query_index_name:
                st.error("请输入查询索引名称")
                return
            
            # 验证字段
            errors = validate_fields(updated_fields)
            if errors:
                st.error("字段验证失败:")
                for error in errors:
                    st.write(f"❌ {error}")
                return
            
            # 保存字段配置
            with st.spinner("正在保存字段配置..."):
                # 构建完整的字段信息，包括类型和描述
                field_info_list = []
                for field in updated_fields:
                    field_name = field.get("field_name", "")
                    field_type = field.get("field_type", "text")
                    description = field.get("description", "")
                    if field_name:
                        field_info_list.append({
                            "field_name": field_name,
                            "field_type": field_type,
                            "description": description
                        })
                
                # 显示将要保存的字段列表
                st.info(f"将保存以下 {len(field_info_list)} 个字段: {', '.join([f['field_name'] for f in field_info_list])}")
                
                success = api_client.save_index_with_fields(
                    query_index_name, 
                    index_description, 
                    field_info_list
                )
                
                if success:
                    st.success(f"✅ 成功保存 {len(field_info_list)} 个字段到查询索引 '{query_index_name}'")
                    st.info("💡 已完全替换DynamoDB中的字段配置，删除的字段已从数据库中移除")
                    # 更新显示字段
                    st.session_state.display_fields = st.session_state.custom_fields.copy()
                    # 保存成功后刷新页面以更新查询索引名称列表
                    st.rerun()
                else:
                    st.error("❌ 保存失败")
    
    with col2:
        if st.button("验证字段", key="validate_unified_fields"):
            errors = validate_fields(updated_fields)
            warnings = []
            
            # 检查描述
            for i, field in enumerate(updated_fields):
                if field["field_name"] and not field["description"]:
                    warnings.append(f"字段 '{field['field_name']}' 缺少描述")
            
            if errors:
                st.error("发现错误:")
                for error in errors:
                    st.write(f"❌ {error}")
            elif warnings:
                st.warning("发现警告:")
                for warning in warnings:
                    st.write(f"⚠️ {warning}")
                st.success("✅ 基本验证通过")
            else:
                st.success("✅ 所有字段验证通过")
    
    with col3:
        if st.button("添加常用字段", key="add_common_fields"):
            show_common_fields_dialog()

def validate_fields(fields: List[Dict[str, Any]]) -> List[str]:
    """
    验证字段配置
    
    Args:
        fields: 字段列表
        
    Returns:
        List[str]: 错误信息列表
    """
    errors = []
    field_names = []
    
    for i, field in enumerate(fields):
        field_name = field.get("field_name", "").strip()
        field_type = field.get("field_type", "").strip()
        
        if not field_name:
            continue  # 跳过空字段名的行
        
        # 检查重复
        if field_name in field_names:
            errors.append(f"字段名称 '{field_name}' 重复")
        else:
            field_names.append(field_name)
        
        # 检查字段类型 - 支持所有 Elasticsearch/OpenSearch 字段类型
        valid_types = [
            "text", "keyword", "long", "integer", "double", "float", "boolean", "date", 
            "object", "nested", "ip", "geo_point", "geo_shape", "binary", "range",
            "alias", "flattened", "search_as_you_type", "token_count", "dense_vector",
            "sparse_vector", "rank_feature", "rank_features", "completion", "percolator",
            "join", "histogram", "constant_keyword", "wildcard", "version", "aggregate_metric_double"
        ]
        if field_type not in valid_types:
            errors.append(f"字段 '{field_name}' 的类型 '{field_type}' 无效")
    
    return errors

def show_common_fields_dialog():
    """
    显示常用字段添加对话框
    """
    st.write("### 添加常用字段")
    
    # 常用字段模板
    common_fields = {
        "基础日志字段": [
            {"field_name": "timestamp", "field_type": "date", "description": "日志时间戳"},
            {"field_name": "log_level", "field_type": "keyword", "description": "日志级别 (DEBUG/INFO/WARN/ERROR)"},
            {"field_name": "message", "field_type": "text", "description": "日志消息内容"},
            {"field_name": "source", "field_type": "keyword", "description": "日志来源"}
        ],
        "用户相关字段": [
            {"field_name": "user_id", "field_type": "keyword", "description": "用户唯一标识符"},
            {"field_name": "session_id", "field_type": "keyword", "description": "会话标识符"},
            {"field_name": "user_agent", "field_type": "text", "description": "用户代理信息"}
        ],
        "网络相关字段": [
            {"field_name": "client_ip", "field_type": "ip", "description": "客户端IP地址"},
            {"field_name": "server_ip", "field_type": "ip", "description": "服务器IP地址"},
            {"field_name": "port", "field_type": "integer", "description": "端口号"}
        ],
        "HTTP相关字段": [
            {"field_name": "method", "field_type": "keyword", "description": "HTTP请求方法"},
            {"field_name": "url", "field_type": "keyword", "description": "请求URL"},
            {"field_name": "status_code", "field_type": "integer", "description": "HTTP状态码"},
            {"field_name": "response_size", "field_type": "long", "description": "响应大小（字节）"}
        ],
        "结构化数据字段": [
            {"field_name": "request", "field_type": "object", "description": "HTTP请求对象"},
            {"field_name": "response", "field_type": "object", "description": "HTTP响应对象"},
            {"field_name": "user_info", "field_type": "object", "description": "用户信息对象"},
            {"field_name": "metadata", "field_type": "object", "description": "元数据对象"},
            {"field_name": "tags", "field_type": "nested", "description": "标签数组（嵌套对象）"}
        ]
    }
    
    # 选择字段类别
    selected_category = st.selectbox("选择字段类别", options=list(common_fields.keys()))
    
    if selected_category:
        fields_to_add = common_fields[selected_category]
        
        st.write(f"**{selected_category}包含以下字段:**")
        for field in fields_to_add:
            st.write(f"- {field['field_name']} ({field['field_type']}): {field['description']}")
        
        if st.button(f"添加{selected_category}", key=f"add_{selected_category}"):
            # 检查重复
            existing_names = [f.get("field_name", "") for f in st.session_state.custom_fields]
            new_fields = []
            skipped_fields = []
            
            for field in fields_to_add:
                if field["field_name"] not in existing_names:
                    new_fields.append(field)
                    existing_names.append(field["field_name"])
                else:
                    skipped_fields.append(field["field_name"])
            
            if new_fields:
                st.session_state.custom_fields.extend(new_fields)
                st.success(f"✅ 成功添加 {len(new_fields)} 个字段")
            
            if skipped_fields:
                st.warning(f"⚠️ 以下字段已存在，已跳过: {', '.join(skipped_fields)}")
            
            if new_fields:
                st.rerun()

def show_fields_preview():
    """
    显示字段预览
    """
    st.write("### 字段结构预览")
    
    if st.session_state.custom_fields:
        preview_data = []
        for field in st.session_state.custom_fields:
            preview_data.append({
                "字段名称": field.get("field_name", ""),
                "类型": field.get("field_type", ""),
                "描述": field.get("description", "")
            })
        
        preview_df = pd.DataFrame(preview_data)
        st.dataframe(preview_df, use_container_width=True)
        
        # 统计信息
        type_counts = {}
        for field in st.session_state.custom_fields:
            field_type = field.get("field_type", "unknown")
            type_counts[field_type] = type_counts.get(field_type, 0) + 1
        
        st.write("**字段类型统计:**")
        for field_type, count in type_counts.items():
            st.write(f"- {field_type}: {count} 个")
    else:
        st.info("暂无字段可预览")