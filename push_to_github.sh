#!/bin/bash

echo "🚀 推送代码到GitHub..."

# 进入项目目录
cd /Users/wongxiao/project/log_analytics

# 配置Git用户信息
git config user.email "kasimxiao@gmail.com"
git config user.name "kasimxiao"

# 检查Git状态
echo "📋 检查Git状态..."
git status

# 添加所有文件
echo "📁 添加文件..."
git add .

# 创建提交
echo "💾 创建提交..."
git commit -m "Initial commit: OpenSearch/Elasticsearch Log Analytics System

Features:
- 智能日志分析聊天界面
- 统一图表渲染系统 (ChartRenderer)
- 会话管理系统 (SessionManager)  
- 支持6种图表类型：柱状图、折线图、饼图、散点图、面积图、热力图
- DynamoDB集成和AWS Bedrock模型支持
- Streamlit前端界面
- 完整的错误处理和数据验证"

# 添加远程仓库
echo "🔗 添加远程仓库..."
git remote add origin https://github.com/kasimxiao/opensearch-elasticsearch-log-analytics.git

# 设置主分支
echo "🌿 设置主分支..."
git branch -M main

# 推送到GitHub
echo "⬆️ 推送到GitHub..."
git push -u origin main

echo "✅ 推送完成！"
echo "🌐 访问: https://github.com/kasimxiao/opensearch-elasticsearch-log-analytics"
