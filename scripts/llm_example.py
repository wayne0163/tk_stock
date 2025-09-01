"""示例：调用 utils.llm_client.chat，并默认使用简体中文系统提示。

运行前准备：
- pip install -r requirements.txt（需包含 openai）
- 设置环境变量：OPENAI_API_KEY（以及可选的 OPENAI_MODEL）

运行：
python -m scripts.llm_example
"""

import os
import sys

CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from utils.llm_client import chat


def main():
    answer = chat([
        {"role": "user", "content": "请用一句话介绍你自己。"}
    ])
    print("模型回复：", answer)


if __name__ == "__main__":
    main()

