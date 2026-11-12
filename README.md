# 03-changeflow（变流）

Changeflow — CDC / 变更流落库：事件入账 → 序/缺口裁决 → 幂等应用到 sink → 游标提交与对账。

**不是** Feed 多源归并（Feedstitch），**不是** 作业依赖编排（Batchgate）。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5300 |
| API | http://localhost:10300 |

本地开发（可选）：

```bash
# backend
cd backend && pip install -r requirements.txt
set DATA_DIR=..\data
uvicorn app.main:app --reload --port 10300

# frontend
cd frontend && npm install && npm run dev
```

## 主链

配置变更流与夹具 → 触发 Apply → 按序应用 / 缺口隔离 → Sink + 游标 + 运行报告对账

## 种子说明

- **Orders CDC（clean）**：JSONL，seq 1→5 连续 upsert/delete，首跑应全部应用、游标推进到 5。
- **Inventory CDC（clean）**：JSONL，seq 1→3 连续 upsert，独立实体键，用于第二路流演示。
- **Orders TXN CDC（clean，事务批）**：seq 1→6；T-100 / T-101 / T-102 三个事务批（含批内 delete），整批提交，游标推进到 6。
- **Orders TXN Broken CDC（broken，事务批）**：T-900 整批提交后，T-901 批内缺 seq 4 → 整批回滚，两条事件全进隔离区，游标停在批前（2）。

打开「应用台」选流跑一次，再看账本 / 隔离区 / Sink / 游标页计数是否与本次报告一致；事务批次结果见应用台「事务批次」面板与记录表。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口

当前仓库已实现「连续序幂等应用」主链与「事务边界整批提交」（`backend/app/modules/txn_boundary_commit`：事件携带 `txn_id` / `is_last`，同一事务整批提交或整批回滚，批次结果落 `txn_batches` 表并入报告）。缺口等待/跳过、乱序缓冲、双 sink 部分失败、毒丸选择性跳过等仍在 `backend/app/modules/*` 留空包，按题单逐题换主链路实现。
