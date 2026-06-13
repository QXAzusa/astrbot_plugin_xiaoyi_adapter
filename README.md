# 小艺 AstrBot 适配器

用于将小艺 OpenClaw 类型通道接入 AstrBot 平台适配器体系的插件，实现小艺协议与 AstrBot 消息模型之间的双向转换与桥接。

## 配置方法

参考华为官方文档的说明，创建智能体，获取Access Key、Secret Key、Agent ID

* [OpenClaw基础配置](https://developer.huawei.com/consumer/cn/doc/service/open-claw-base-0000002518704040)

安装插件后在<机器人>页面选择创建机器人,选择“xiaoyi”消息平台类别，将从华为处获取到的Access Key、Secret Key、Agent ID填入即可。

## 文件结构

* `metadata.yaml`
* `requirements.txt`
* `main.py`
* `__init__.py`
* `xiaoyi_astrbot_adapter.py`
* `xiaoyi_astrbot_event.py`
* `xiaoyi_client.py`
* `xiaoyi_config.py`

## 功能特性

### 协议接入

* 基于 AK/SK 的 WebSocket 鉴权机制
* 支持小艺双 WebSocket 接入端点
* 自动维护协议层与应用层心跳

### 消息处理

* 接收 `message/stream` 流式消息
* 处理 `clearContext` 与 `tasks/cancel` 确认响应（Ack）
* 发送、接收文本消息
* 接受图片消息

### 消息桥接

* 支持基于会话的 Webhook 推送桥接
* 对以下消息类型提供增强映射能力：

  * `data`
  * `reasoningText`
  * `command`

## 已知问题

* Todo
  * 小艺协议规定每个 Task 仅允许发送一次 `final` 消息。当前实现采用基于空闲时间窗口的保守 Finalization 策略，在部分长间隔流式场景下仍可能出现 Final 提前发送，导致后续消息被服务端截断的问题。
  * 尚未支持主动推送消息。
* 华为提供的OpenClaw插件尚未提供对图片消息的发送支持，无法回传图片消息。

## 推荐配置

```json
{
  "wsUrl": "wss://hag.cloud.huawei.com/openclaw/v1/ws/link",
  "wsUrl2": "wss://116.63.174.231/openclaw/v1/ws/link",
  "ak": "your-ak",
  "sk": "your-sk",
  "agentId": "your-agent-id",
  "apiId": "",
  "pushId": "",
  "pushUrl": "https://hag.cloud.huawei.com/open-ability-agent/v1/agent-webhook",
  "pushEnabled": false,
  "pushDefaultMode": "push_only_for_async",
  "sendProcessingStatus": true,
  "streamFinalizeDelayMs": 10000,
  "pushOnFinal": false
}
```

## 连接与握手流程

### WebSocket 接入

* 小艺官方实现默认配置两个 WebSocket 接入端点，而非单一连接地址。
* 备用接入点采用 IP 形式的 `wss` 地址，因此客户端需允许该连接跳过 TLS 证书校验。

### 初始化流程

WebSocket 连接建立后，适配器应立即发送 `clawd_bot_init` 初始化消息，并启动以下保活机制：

* 协议层心跳：30 秒
* 应用层心跳：20 秒

### Finalization 机制

根据小艺协议约束，同一 Task 仅允许发送一次 `final` 消息。

为避免流式输出过程中出现 Final 过早结束的问题，当前实现采用延迟 Finalization 策略：在检测到一定时间内无新增消息后，再发送最终完成状态。该方案能够降低误判概率，但无法完全准确判断任务结束时机。

在实际部署环境中，建议将最终完成延迟时间配置为不少于 **10000ms**，以提高长链路流式任务的稳定性。
