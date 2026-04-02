# FB_Group_RPA

基于 BitBrowser 的群组私聊自动化系统，采用“服务器授权/管理 + 客户端本地执行”模式。

## 功能说明
- 服务端提供管理后台（激活码、套餐限制、设备状态、任务下发、日志汇总）
- 客户端负责本地输入 `bit_api` / `api_token`、激活验证、本地运行与接收远程指令
- 支持多窗口并行
- 支持 pause / resume / stop
- 窗口级限制触发顶层弹窗提醒

## 启动方式

### 服务端（后台管理）
```bash
python3 web_ui.py
```
默认读取 `config/server.yaml`。如需自定义运行目录，可设置：
```bash
export FB_RPA_RUNTIME_DIR=/path/to/runtime
```

### 客户端（EXE 统一安装包）
客户端交付形态为安装包 `FB私聊助手安装包.exe`，安装后运行 `FB_RPA_Client.exe`。
客户端本地填写 `bit_api`、`api_token`、激活码后即可运行，不需要本地安装 Python。

若在源码模式下调试：
```bash
python3 client_app.py
```

### Worker（由客户端自动拉起）
客户端完成激活或点击“运行”后会确保本地 worker 在线。
worker 只从服务器拉取统一任务队列，不再读取独立的本地 worker 配置文件。
本地“运行 / 停止 / 暂停 / 继续”与后台控制共用同一条服务端任务链。

## 配置说明

### 服务端配置
`config/server.yaml`
- `database.*`：服务端共享数据库唯一权威来源，当前正式要求为 MySQL 8
- `web_ui.host` / `web_ui.port`：服务端监听
- `admin_account.*`：管理员账号
- `agent_security.*`：token 有效期、心跳与设备限制
- `runtime_dir`：运行时目录（可选）
- 服务端共享状态（用户、会话、设备、任务、统计）走 MySQL 8
- 客户端本地运行态（断点、占位、运行时缓存）继续走本地 SQLite
- 服务端不再维护 `send_interval_seconds`，发送频率只认客户端本地配置

### 客户端配置
`config/client.yaml`
- `server_url`：服务端地址
- `machine_id`：本机自动生成的设备标识
- `agent_token`：激活成功后保存的长期运行凭证
- `bit_api` / `api_token`：本地唯一连接配置来源
- `task_settings.*`：执行参数（本地保存）
  其中 `task_settings.send_interval_seconds` 是发送频率唯一权威来源，默认 `0-0`
- `runtime_dir`：运行时目录（可选）

仓库只跟踪配置模板：
- `config/client.example.yaml`
- `config/server.example.yaml`

本地真实配置文件：
- `config/client.yaml`
- `config/server.yaml`

均属于实例私有文件，不进入源码基线，由运行环境复制模板后人工填写或由激活流程生成。

## 日志位置
程序运行后会自动创建 `runtime` 目录：
- `runtime/logs/ui.log`
- `runtime/logs/worker.log`
- `runtime/logs/runtime_*.log`

## 交付包边界
交付包应只包含运行所需源码与配置模板，以下内容不进入交付包：
- `runtime/`（运行日志与数据库）
- `dist/`（历史打包产物）
- `Output/`
- `.git/`
- `__MACOSX/`
- `.venv/`、`__pycache__/`、`.DS_Store`
- `*.db`、`*.sqlite*`、`*.log`、`*.wal`、`*.shm`、`*.pid`、`*.cookies`、`*.session*`
- `config/server.yaml`
- `config/client.yaml`
- `config/messages.server.yaml`

## 敏感配置填写
请在部署时手动填写以下字段，不要把真实值提交到源码基线：
- `config/server.yaml`：`database.password`、`admin_account.password`、`api_token`
- `config/client.yaml`：`agent_token`、`api_token`

## 干净打包链
当前唯一正式打包链为 GitHub Actions：
- `.github/workflows/build-windows.yml`

打包前后都必须执行：
```bash
python3 tools/release_preflight.py
```

当前客户端交付目录只允许包含：
- `FB_RPA_Client.exe`
- `FB_RPA_Worker.exe`
- `FB_RPA_Main.exe`
- `config/messages.yaml`
- `config/client.example.yaml`

## 设备管理与租期控制
- 设备管理页提供纯设备列表（设备ID / 归属账号 / 在线状态 / 最后心跳 / 客户端版本 / 状态）
- 可对设备进行启用/禁用
- 子后台“设备信息”仅展示客户端同步上来的配置摘要，不再作为正式配置入口
- 用户支持租期控制与剩余天数提示

## 常见问题

### 执行端离线
- 确认 worker 是否启动
- `config/client.yaml` 中 `server_url` 是否正确

### BitBrowser API 访问失败
- 确认 BitBrowser Local API 已开启
- `bit_api` / `api_token` 是否正确

### Playwright 未安装
客户端执行端需要：
```bash
python3 -m playwright install
```

## 安装依赖
```bash
pip install -r requirements.txt
```
