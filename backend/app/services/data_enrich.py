"""数据补全领域服务 — 从外部数据源填充缺失字段，输出 domain_snapshot。

数据工程核心：数据聚合计算与指标产出 — 跨模块数据产物

关键约束：补全一旦真正改写字段，必须在同一把锁内让旧校验结论失效。
字段改写与校验状态失效同步发生，同一记录只保留当前补全版本对应的校验回执；
发生过补全的记录不能沿用补全前的通过结论。
"""
from __future__ import annotations

import random
import threading
from datetime import datetime, timezone
from typing import Any, Callable

from app.services.base import BaseModuleService
from app.store import (
    ENRICH_REVALIDATION_ITEM,
    ENRICH_REVALIDATION_NO,
    STATUS_CONFLICT,
    STATUS_NOT_VALIDATED,
    STATUS_PENDING_REVALIDATION,
    STATUS_REVALIDATION_FAILED,
    STATUS_VALIDATED,
    store,
)


class DataEnrichService(BaseModuleService):
    MODULE = "data_enrich"
    ENTITY = "补全规则"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["规则名称", "目标字段", "补全来源", "匹配键", "补全方式", "缺失处理", "数据时效"]
    REQUIRED_FIELDS = ["规则名称", "目标字段", "补全来源"]
    STATUS_ORDER = ["草稿", "已生效", "已停用"]
    ACTION_RULES = {"启用规则": "草稿", "停用规则": "已生效"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # 补全改写字段的并发闸门：同一批次的补全写入只接受一个版本。
    _write_lock = threading.Lock()

    def enrich_records(
        self,
        batch_ref: str,
        *,
        expected_versions: dict[int, int] | None = None,
        values: dict[str, str] | None = None,
        fill_value: Callable[[dict[str, Any]], str] | None = None,
    ) -> dict[str, Any]:
        """对指定批次的采集记录执行数据补全。

        模拟从外部数据源（如 API、缓存库）补全缺失字段。只有真正发生字段改写
        的记录才会递增补全版本、清除 validated 标记与旧质量分，并作废旧校验回执。

        Args:
            batch_ref: 批次编号
            expected_versions: 可选，{entry_id: 调用方读到的补全版本}，
                用于并发写入控制，版本不一致的写入会被判为冲突。
            values: 可选，按字段指定补全值（便于稳定复现）。
            fill_value: 可选，自定义补全值生成器。

        Returns:
            {
                "batch_ref", "enriched", "skipped", "rewritten",
                "affected_entry_ids", "pending_revalidation", "conflicts",
                "items": [...]
            }
        """
        records = store.collect_records.list_by_batch(batch_ref)
        active_rules = self._get_active_rules()
        expected_versions = expected_versions or {}

        items: list[dict[str, Any]] = []
        affected: list[int] = []
        conflicts: list[int] = []
        skipped = 0

        with self._write_lock:
            for record in records:
                # 版本检查提前到改写之前，保证同一记录只落一个补全版本。
                version = record.enrich_version
                expected = expected_versions.get(record.entry_id)
                if expected is not None and expected != version:
                    ok, message, payload = store.collect_records.apply_rewrite(
                        record.entry_id, {}, expected_version=expected)
                    conflicts.append(record.entry_id)
                    items.append({
                        "entry_id": record.entry_id,
                        "ok": False,
                        "message": message,
                        "enrich_version": version,
                    })
                    continue

                updates: dict[str, Any] = {}
                for rule in active_rules:
                    field = rule["field"]
                    current = record.fields.get(field)
                    if field in record.fields and str(current or "").strip():
                        # 已有值不覆盖：补全只处理缺失字段，避免无谓改写。
                        continue
                    if fill_value is not None:
                        updates[field] = fill_value(rule)
                    elif values and field in values:
                        updates[field] = values[field]
                    else:
                        updates[field] = self._generate_fill_value(rule)

                if not updates:
                    skipped += 1
                    continue

                ok, message, payload = store.collect_records.apply_rewrite(
                    record.entry_id,
                    updates,
                    expected_version=record.enrich_version,
                    source=self.MODULE,
                )
                if not ok:
                    conflicts.append(record.entry_id)
                    items.append({
                        "entry_id": record.entry_id,
                        "ok": False,
                        "message": message,
                    })
                    continue

                changed = bool(payload and payload.get("rewritten_fields"))
                if changed:
                    affected.append(record.entry_id)
                else:
                    skipped += 1
                items.append({
                    "entry_id": record.entry_id,
                    "ok": True,
                    "message": message,
                    "enrich_version": payload.get("enrich_version") if payload else None,
                    "rewritten_fields": payload.get("rewritten_fields", []) if payload else [],
                    "superseded_receipts": payload.get("superseded_receipts", []) if payload else [],
                })

            if affected:
                store.channel.publish(
                    topic="rpa.data_enriched",
                    source=self.MODULE,
                    payload={
                        "batch_ref": batch_ref,
                        "entry_ids": affected,
                        "item": ENRICH_REVALIDATION_ITEM,
                        "item_no": ENRICH_REVALIDATION_NO,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                )

        return {
            "batch_ref": batch_ref,
            "enriched": len(affected),
            "skipped": skipped,
            "rewritten": len(affected),
            "affected_entry_ids": affected,
            "pending_revalidation": len(
                store.collect_records.list_pending_revalidation(batch_ref)),
            "conflicts": conflicts,
            "items": items,
        }

    def _get_active_rules(self) -> list[dict[str, Any]]:
        rows = store.rows(self.MODULE)
        return [
            {"id": r.get("id"), "name": r.get("规则名称"),
             "field": r.get("目标字段"), "source": r.get("补全来源"),
             "match_key": r.get("匹配键"), "method": r.get("补全方式")}
            for r in rows if r.get("status") == "已生效"
        ]

    @staticmethod
    def _generate_fill_value(rule: dict[str, Any]) -> str:
        """根据规则生成模拟补全值。"""
        source = rule.get("source", "")
        templates = {
            "名称": f"{source}-补全",
            "类型": random.choice(["标准", "特殊", "通用"]),
            "状态": "已补全",
            "金额": str(round(random.uniform(100, 5000), 2)),
            "日期": "2026-01-01",
            "标签": "补全数据",
        }
        field = rule.get("field", "")
        return templates.get(field, f"{source}-补全值")

    def enrichment_workbench(self, batch_ref: str | None = None) -> dict[str, Any]:
        """补全工作台：按批次检索被改写字段、当前校验状态和补全版本。"""
        records = (
            store.collect_records.list_by_batch(batch_ref)
            if batch_ref else list(store.collect_records.list_enriched())
        )
        items: list[dict[str, Any]] = []
        for record in records:
            if record.enrich_version <= 0 and batch_ref is None:
                continue
            receipt = store.validation_receipts.active_for(record.entry_id)
            items.append({
                "entry_id": record.entry_id,
                "batch_ref": record.batch_ref,
                "source_module": record.source_module,
                "enrich_version": record.enrich_version,
                "enriched_at": record.enriched_at,
                "rewritten_fields": list(record.rewritten_fields),
                "rewrite_diff": {
                    name: dict(change)
                    for name, change in record.rewrite_diff.items()
                },
                "validation_status": self._status_label(record),
                "validation_version": record.validation_version,
                "validated": record.validated,
                "quality_score": record.quality_score,
                "receipt": receipt.to_dict() if receipt else None,
                "stale_receipt": bool(
                    receipt and receipt.enrich_version != record.enrich_version),
            })
        return {
            "batch_ref": batch_ref,
            "item_no": ENRICH_REVALIDATION_NO,
            "item": ENRICH_REVALIDATION_ITEM,
            "count": len(items),
            "pending_revalidation": sum(
                1 for i in items
                if i["validation_status"] == STATUS_PENDING_REVALIDATION),
            "items": items,
        }

    @staticmethod
    def _status_label(record: Any) -> str:
        if record.validation_status == STATUS_CONFLICT:
            return STATUS_CONFLICT
        if record.validation_status == STATUS_PENDING_REVALIDATION:
            return STATUS_PENDING_REVALIDATION
        if record.validation_status == STATUS_REVALIDATION_FAILED:
            return STATUS_REVALIDATION_FAILED
        if record.validation_status == STATUS_VALIDATED:
            return STATUS_VALIDATED
        if record.enrich_version <= 0:
            return STATUS_NOT_VALIDATED
        return STATUS_PENDING_REVALIDATION

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        collect = store.collect_records.snapshot()
        return {
            **base,
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "stopped_rules": sum(1 for r in rows if r.get("status") == "已停用"),
            "enriched_records": collect["enriched"],
            "pending_revalidation": collect["pending_revalidation"],
            "validation_receipts": store.validation_receipts.snapshot(),
        }
