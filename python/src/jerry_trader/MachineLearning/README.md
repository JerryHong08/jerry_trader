# Machine Learning Roadmap

Previously I'm working on factor_engine.py and state_engine.py to generate signals.
Now I want to try to develop machine learning system to generate signals.
But I have no experience of building a machine learning system before, so I want to start with a simple roadmap for me to get me used to the machine learning development system.

in stage 1, you only need to focus on build a simple Context v0 based ml structure.
later I want to build a Replay Dataset 生成器, 把真实 replay 数据变成：
parquet / csv / numpy 用于训练。

Now let's only focus on stage 1.

## Stage 1

我们现在不谈 alpha。我们先搭一个：

可反复重放、可训练、可评估的最小 ML 工程骨架

raw → dataset → train → evaluate → inference

### 一、初级目标

你接下来要做这一件事：

1️⃣ Context v0（极简）

先用 mock data 跑通：

数据生成

Dataset 构建

训练

保存模型

推理

输出概率

### 二、Context v0 设计（极简版）

不要 window × 多 feature。

我们做：

固定 60s window
每 10s 一个 bucket
只用 price

Context v0 schema

    ContextV0 = {
        "meta": {
            "seconds_from_4am": int,
        },

        "state_window": {
            "return_10s": [r1, r2, r3, r4, r5, r6],
        }
    }

就这样。

只有：

6 维 return 序列 + 1 维时间位置

总共 7 维。

Label v0

    max_return_30s

或者

    continuation_1pct (0/1)

建议先做回归：

    max_return_30s

### 三、Mock Data 训练脚手架

先不接 replay。

写一个：

generate_mock_sample()

逻辑：

随机生成 6 个 return

构造一个“隐藏规则”

例如：

if last 3 returns sum > 0.01:
    label = +0.02
else:
    label = -0.01

然后训练 LightGBM 看能不能学出来。

目标不是 accuracy

目标是：

你熟悉 Dataset

熟悉 train loop

熟悉 validation

熟悉 inference
