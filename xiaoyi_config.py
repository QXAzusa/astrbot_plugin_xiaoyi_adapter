DEFAULT_CONFIG_TMPL: dict = {
    "id": "default",
    "type": "xiaoyi",
    "enable": False,
    "ak": "",
    "sk": "",
    "agentId": "",
    "sendProcessingStatus": True,
    "streamFinalizeDelayMs": 10000,
    "sessionCleanupDelayMs": 300000,
    "sessionStateTtlMs": 3600000,
}


CONFIG_METADATA: dict = {
    "ak": {
        "description": "Access Key",
        "type": "string",
        "hint": "必填。小艺侧分配的 Access Key。",
    },
    "sk": {
        "description": "Secret Key",
        "type": "string",
        "hint": "必填。小艺侧分配的 Secret Key。",
    },
    "agentId": {
        "description": "Agent ID",
        "type": "string",
        "hint": "必填。小艺侧登记的 Agent ID。",
    },
    "sendProcessingStatus": {
        "description": "发送处理中状态",
        "type": "bool",
        "hint": "在 AstrBot 输出最终回复前，先发送一条 working 状态更新。",
    },
    "streamFinalizeDelayMs": {
        "description": "流式收尾延迟（毫秒）",
        "type": "int",
        "hint": "最后一个增量分片发送后，延迟多久自动补发 final 收尾包。建议不要低于 5000，否则同一轮多段回复可能被过早结束。",
    },
    "sessionCleanupDelayMs": {
        "description": "Session 清理延迟（毫秒）",
        "type": "int",
        "hint": "收到 clearContext 后，延迟多久清理缓存的会话路由状态。",
    },
    "sessionStateTtlMs": {
        "description": "Session 状态 TTL（毫秒）",
        "type": "int",
        "hint": "缓存的会话路由与 push 状态允许保留的最长空闲时间。",
    },
}


I18N_RESOURCES: dict = {
    "zh-CN": {
        "ak": {
            "description": "Access Key",
            "hint": "必填。小艺侧分配的 Access Key。",
        },
        "sk": {
            "description": "Secret Key",
            "hint": "必填。小艺侧分配的 Secret Key。",
        },
        "agentId": {
            "description": "Agent ID",
            "hint": "必填。小艺侧登记的 Agent ID。",
        },
        "sendProcessingStatus": {
            "description": "发送处理中状态",
            "hint": "在 AstrBot 输出最终回复前，先发送一条 working 状态更新。",
        },
        "streamFinalizeDelayMs": {
            "description": "流式收尾延迟（毫秒）",
            "hint": "最后一个增量分片发送后，延迟多久自动补发 final 收尾包。",
        },
        "sessionCleanupDelayMs": {
            "description": "Session 清理延迟（毫秒）",
            "hint": "收到 clearContext 后，延迟多久清理缓存的会话路由状态。",
        },
        "sessionStateTtlMs": {
            "description": "Session 状态 TTL（毫秒）",
            "hint": "缓存的会话路由与 push 状态允许保留的最长空闲时间。",
        },
    },
    "en-US": {
        key: {"description": meta["description"], "hint": meta["hint"]}
        for key, meta in CONFIG_METADATA.items()
    },
}
