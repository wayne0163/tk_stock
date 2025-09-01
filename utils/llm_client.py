"""
简单的 LLM 封装：默认在对话最前添加“简体中文”系统提示。

使用方法：

from utils.llm_client import chat

reply = chat([
    {"role": "user", "content": "介绍一下你的功能"}
])
print(reply)

环境变量：
- OPENAI_API_KEY: OpenAI API Key
- OPENAI_MODEL: 模型名称（可选，默认 gpt-4o-mini）
- LLM_SYSTEM_PROMPT: 覆盖默认系统提示（可选）
"""

from typing import List, Dict, Optional
import os

try:
    # 新版 OpenAI SDK（>=1.x）
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover - 仅在未安装时触发
    OpenAI = None  # type: ignore


DEFAULT_SYSTEM_PROMPT = "请一律用简体中文回答。"


def chat(messages: List[Dict[str, str]], model: Optional[str] = None) -> str:
    """发送对话到 LLM，并返回回复文本。

    会自动在最前追加 system 提示，默认“请一律用简体中文回答。”
    你也可以通过环境变量 LLM_SYSTEM_PROMPT 覆盖。
    """
    sys_prompt = os.getenv("LLM_SYSTEM_PROMPT", DEFAULT_SYSTEM_PROMPT)
    final_messages = [{"role": "system", "content": sys_prompt}] + messages

    if OpenAI is None:
        raise ImportError(
            "未安装 openai 库。请在 requirements.txt 添加 'openai>=1.0.0' 并安装。"
        )

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise EnvironmentError("请设置环境变量 OPENAI_API_KEY 用于访问 OpenAI API。")

    client = OpenAI(api_key=api_key)
    mdl = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    # 兼容 Chat Completions 接口
    resp = client.chat.completions.create(model=mdl, messages=final_messages)
    return resp.choices[0].message.content or ""

