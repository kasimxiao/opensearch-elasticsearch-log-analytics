# OpenSearch/Elasticsearch Log Analytics System

智能日志分析系统，支持自然语言查询和数据可视化。

## 🚀 功能特性

- **智能聊天界面** - 自然语言日志查询
- **多种图表支持** - 柱状图、折线图、饼图、散点图、面积图、热力图
- **会话管理** - 支持多对话切换和历史记录
- **AWS集成** - DynamoDB存储、Bedrock模型支持
- **实时渲染** - 统一的图表渲染系统

## 📁 项目结构

```
log_analytics/
├── code/
│   ├── front/           # Streamlit前端
│   │   ├── pages/       # 页面组件
│   │   └── .streamlit/  # Streamlit配置
│   └── server/          # 后端服务
├── README.md
└── .gitignore
```

## 🛠️ 技术栈

- **前端**: Streamlit, Plotly
- **后端**: Python, AWS Bedrock
- **数据库**: DynamoDB
- **图表**: 统一的ChartRenderer系统
- **AI模型**: Claude 3.5/3.7 Sonnet

## 📊 图表系统

支持6种图表类型，统一配置和渲染：
- 柱状图 (Bar Chart)
- 折线图 (Line Chart) 
- 饼图 (Pie Chart)
- 散点图 (Scatter Plot)
- 面积图 (Area Chart)
- 热力图 (Heatmap)

## 🚀 快速开始

1. 安装依赖
2. 配置AWS凭据
3. 运行Streamlit应用

## 📝 更新日志

- 优化图表渲染系统，统一ChartRenderer类
- 简化会话管理，添加SessionManager类
- 修复所有图表类型的渲染问题
- 添加完整的错误处理和数据验证
