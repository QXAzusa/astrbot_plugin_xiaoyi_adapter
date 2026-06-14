# 小艺 AstrBot 适配器

把小艺 OpenClaw 通道接入 AstrBot ，用于处理基础对话。

## 配置

先参考华为官方文档创建智能体并获取参数：

* [OpenClaw 基础配置](https://developer.huawei.com/consumer/cn/doc/service/open-claw-base-0000002518704040)

安装插件后在<机器人>页面选择创建机器人,选择“xiaoyi”消息平台类别，将从华为处获取到的Access Key、Secret Key、Agent ID填入即可。

## 支持

* 收发文字消息
* 接收图片
* 处理 `清除上下文`

## 暂不支持

* 回传图片
* 完整支持复杂消息类型
* 主动推送

## 注意

* 主动推送除 `ak`、`sk`、`agentId` 外，还需要 `apiId`。但华为并未提供此参数的设置入口，所以主动推送尚不可用
* 小艺协议里同一个任务只能结束一次，所以长间隔流式回复仍可能出现收尾过早的问题；`流式收尾延迟` 建议不要低于 `10000`
