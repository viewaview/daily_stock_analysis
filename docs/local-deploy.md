# 本地部署指南

## 环境要求

- Python 3.10+（推荐 3.12.0）
- Node.js 18+
- conda（推荐）或 venv

## 一次性部署步骤

### 1. 创建 conda 环境

```bash
conda create -n stock python=3.12.0 -y
conda activate stock
```

### 2. 安装 Python 依赖

```bash
cd F:/code/personal/stock_daily
pip install -r requirements.txt
```

### 3. 配置 .env

```bash
cp .env.example .env
```

最简配置（以 DeepSeek 为例）：

```env
STOCK_LIST=600519,000001,AAPL

LLM_CHANNELS=primary
LLM_PRIMARY_PROTOCOL=openai
LLM_PRIMARY_BASE_URL=https://api.deepseek.com/v1
LLM_PRIMARY_API_KEY=sk-xxxxxxxx
LLM_PRIMARY_MODELS=deepseek-chat
LITELLM_MODEL=openai/deepseek-chat

TRADING_DAY_CHECK_ENABLED=false
AGENT_MODE=true
MAX_WORKERS=2
WEBUI_PORT=8080
```

> 端口说明：若 8000 被 VS Code 或其他程序占用，通过 `WEBUI_PORT` 换端口。

### 4. 构建前端

```bash
cd apps/dsa-web
npm install
npm run build
cd ../..
```

### 5. 启动服务

```bash
python main.py --webui-only
```

访问 http://127.0.0.1:8080

---

## 日常启动（每次使用）

```bash
conda activate stock
cd F:/code/personal/stock_daily
python main.py --webui-only
```

---

## 常见问题

### 页面 MIME 类型报错（JS 加载失败）

Windows 注册表可能将 `.js` 映射为 `text/plain`，已在 `api/app.py` 中通过自定义路由修复，正常启动即可。

### 端口冲突（pending / 无响应）

```bash
netstat -ano | findstr :8000
```

查到占用进程后在 `.env` 中设置 `WEBUI_PORT=8080`（或其他空闲端口）。

---

## 添加更多 AI 模型

详见 [LLM 配置说明](#llm-多模型配置)。
