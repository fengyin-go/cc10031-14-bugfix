"""结果校验领域服务 — 校验采集结果并触发回调。

数据工程核心：数据校验与去重策略执行 — 跨模块数据产物

数据流:
    channel("rpa.data_collected") ← data_extract
    → result_verify 消费校验 + DirtyDataVault
    → channel("rpa.validation_complete") → callback_hook

补全二次校验（14 号处理）:
    channel("rpa.data_enriched") ← data_enrich
    → result_verify 载入补全后的明细数据集
    → 只重算实际发生字段改写的记录（或整批重校验）
    → 校验回执绑定当前补全版本，旧回执立即失效
"""
from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import (
    ENRICH_REVALIDATION_ITEM,
    ENRICH_REVALIDATION_NO,
    STATUS_CONFLICT,
    STATUS_PENDING_REVALIDATION,
    STATUS_REVALIDATION_FAILED,
    STATUS_VALIDATED,
    TERMINAL_CONFLICT,
    TERMINAL_FAILED,
    TERMINAL_PASSED,
    store,
)


def _safe_float(value: str, default: float = 1.0) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


class ResultVerifyService(BaseModuleService):
    MODULE = "result_verify"
    ENTITY = "校验规则"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["规则名称", "校验字段", "校验条件", "期望模式", "异常动作", "抽样比例", "关联流程"]
    REQUIRED_FIELDS = ["规则名称", "校验字段", "校验条件"]
    STATUS_ORDER = ["草稿", "已启用", "已关闭"]
    ACTION_RULES = {"启用规则": "草稿", "关闭规则": "已启用"}
    NEGATIVE_ACTIONS = []

    REVALIDATION_MODE_INCREMENTAL = "incremental"
    REVALIDATION_MODE_FULL_BATCH = "full_batch"

    # ---- 跨模块业务方法 ----

    def verify_collected_data(self) -> dict[str, Any]:
        """消费 data_extract 发布的采集结果，执行校验。

        从 channel("rpa.data_collected") 获取采集结果批次，
        按已启用的校验规则进行逐条校验。

        不通过的记录移入 DirtyDataVault。

        Returns:
            {"verified": int, "passed": int, "failed": int, "batches": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.data_collected",
        )
        if not msgs:
            return {"verified": 0, "passed": 0, "failed": 0, "batches": []}

        batches = []
        total_passed = 0
        total_failed = 0

        for msg in msgs:
            batch_data = msg.payload
            batch_ref = batch_data.get("batch_ref", "")

            # 获取已启用的校验规则
            verify_rules = self._get_active_rules()
            if not verify_rules:
                store.channel.confirm(msg.msg_id, self.MODULE)
                continue

            # 对批次中的记录进行校验
            records = store.collect_records.list_by_batch(batch_ref)
            passed, failed = 0, 0

            for record_entry in records:
                quality = self._validate_record(
                    record_entry.fields, verify_rules)
                stored, _, _ = store.collect_records.record_validation(
                    record_entry.entry_id,
                    quality["passed"],
                    quality["score"],
                    expected_version=record_entry.enrich_version,
                    source=self.MODULE,
                    rules_applied=[{"id": r["id"], "field": r["field"]}
                                   for r in verify_rules],
                    failures=quality.get("failures", []),
                    reason=quality["reason"],
                )
                if not stored:
                    failed += 1
                    continue
                if quality["passed"]:
                    passed += 1
                else:
                    failed += 1
                    # 移入脏数据区
                    store.dirty_vault.quarantine(
                        source=self.MODULE,
                        batch_ref=batch_ref,
                        raw_data=dict(record_entry.fields),
                        reason=quality["reason"],
                    )

            total_passed += passed
            total_failed += failed

            # 发布校验完成事件
            store.channel.publish(
                topic="rpa.validation_complete",
                source=self.MODULE,
                payload={
                    "batch_ref": batch_ref,
                    "site": batch_data.get("site"),
                    "passed": passed,
                    "failed": failed,
                    "rules_applied": len(verify_rules),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
            store.channel.confirm(msg.msg_id, self.MODULE)

            batches.append({
                "batch_ref": batch_ref,
                "passed": passed,
                "failed": failed,
                "total": len(records),
            })

        return {
            "verified": total_passed + total_failed,
            "passed": total_passed,
            "failed": total_failed,
            "batches": batches,
        }

    # ---- 补全二次校验（14 号处理） ----

    def revalidate_enriched(
        self,
        batch_ref: str,
        *,
        mode: str = REVALIDATION_MODE_INCREMENTAL,
        entry_ids: list[int] | None = None,
        expected_versions: dict[int, int] | None = None,
        threshold: float = 0.6,
        deterministic: bool = False,
    ) -> dict[str, Any]:
        """载入补全后的明细数据集，重算受影响记录的校验回执。

        两个可选处理方案都保证报告里不存在旧字段配新校验分：
        - ``incremental``: 只重验实际发生字段改写的记录；
        - ``full_batch``: 整批重新校验，未改写记录也会换发当前版本回执。

        Args:
            batch_ref: 批次编号
            mode: incremental | full_batch
            entry_ids: 仅重验指定记录（可选，缩小范围）
            expected_versions: {entry_id: 期望补全版本}，版本不一致判为冲突
            threshold: 记录得分达到该阈值才算通过（阈值口径）
            deterministic: 关闭抽样/随机异常，便于核对
        """
        if mode not in (
            self.REVALIDATION_MODE_INCREMENTAL,
            self.REVALIDATION_MODE_FULL_BATCH,
        ):
            return {"ok": False, "message": f"未知处理方案：{mode}"}

        records = store.collect_records.list_by_batch(batch_ref)
        if not records:
            return {
                "ok": False,
                "message": f"批次 {batch_ref} 无采集记录",
                "batch_ref": batch_ref,
                "item_no": ENRICH_REVALIDATION_NO,
                "item": ENRICH_REVALIDATION_ITEM,
            }

        rules = self._get_active_rules()
        if not rules:
            return {
                "ok": False,
                "message": "无已启用的校验规则，补全二次校验无法执行",
                "batch_ref": batch_ref,
                "item_no": ENRICH_REVALIDATION_NO,
                "item": ENRICH_REVALIDATION_ITEM,
            }

        if entry_ids is not None:
            targets = [r for r in records if r.entry_id in set(entry_ids)]
        else:
            # 增量方案只重算实际发生字段改写的记录，其余计入 skipped；
            # 整批方案则对批内所有记录换发当前版本回执。
            targets = list(records)

        expected_versions = expected_versions or {}
        results: list[dict[str, Any]] = []
        passed = failed = 0
        conflicts: list[int] = []
        skipped: list[int] = []

        for record in targets:
            expected = expected_versions.get(record.entry_id)
            if expected is not None and expected != record.enrich_version:
                store.collect_records.mark_conflict(record.entry_id)
                conflicts.append(record.entry_id)
                results.append({
                    "entry_id": record.entry_id,
                    "ok": False,
                    "status": TERMINAL_CONFLICT,
                    "message": TERMINAL_CONFLICT,
                    "enrich_version": record.enrich_version,
                    "expected_version": expected,
                })
                continue

            # 增量方案下，没有发生字段改写的记录不重算，也不换发回执。
            if mode == self.REVALIDATION_MODE_INCREMENTAL and (
                record.enrich_version <= 0
                or record.validation_status not in (
                    STATUS_PENDING_REVALIDATION, STATUS_REVALIDATION_FAILED)
            ):
                skipped.append(record.entry_id)
                continue

            quality = self._validate_record(
                record.fields, rules, threshold=threshold,
                deterministic=deterministic,
            )
            stored, message, payload = store.collect_records.record_validation(
                record.entry_id,
                quality["passed"],
                quality["score"],
                expected_version=record.enrich_version,
                source=self.MODULE,
                rules_applied=[{"id": r["id"], "field": r["field"]}
                               for r in rules],
                failures=quality.get("failures", []),
                reason=quality["reason"],
            )
            if not stored:
                store.collect_records.mark_conflict(record.entry_id)
                conflicts.append(record.entry_id)
                results.append({
                    "entry_id": record.entry_id,
                    "ok": False,
                    "status": TERMINAL_CONFLICT,
                    "message": message,
                    "enrich_version": record.enrich_version,
                })
                continue

            if quality["passed"]:
                passed += 1
            else:
                failed += 1
                store.dirty_vault.quarantine(
                    source=self.MODULE,
                    batch_ref=batch_ref,
                    raw_data=dict(record.fields),
                    reason=quality["reason"],
                )
            results.append({
                "entry_id": record.entry_id,
                "ok": True,
                "status": TERMINAL_PASSED if quality["passed"] else TERMINAL_FAILED,
                "passed": quality["passed"],
                "score": quality["score"],
                "enrich_version": record.enrich_version,
                "rewritten_fields": list(record.rewritten_fields),
                "receipt": (payload or {}).get("validation_receipt"),
                "failed_rules": quality.get("failed_rules", []),
                "message": quality["reason"],
            })

        terminal = self._terminal_state(passed, failed, conflicts, results)
        self._sync_batch_tracker(batch_ref, terminal, passed, failed, len(targets))
        todo = self._sync_revalidation_todo(batch_ref, terminal, failed)

        report = {
            "ok": terminal != TERMINAL_CONFLICT,
            "batch_ref": batch_ref,
            "item_no": ENRICH_REVALIDATION_NO,
            "item": ENRICH_REVALIDATION_ITEM,
            "mode": mode,
            "threshold": threshold,
            "terminal_state": terminal,
            "total_records": len(records),
            "revalidated": passed + failed,
            "passed": passed,
            "failed": failed,
            "conflicts": len(conflicts),
            "conflict_entry_ids": conflicts,
            "skipped": skipped,
            "results": results,
            "todo": todo,
            "consistency": store.collect_records.consistency_report(batch_ref),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        store.channel.publish(
            topic="rpa.revalidation_complete",
            source=self.MODULE,
            payload={
                "batch_ref": batch_ref,
                "item_no": ENRICH_REVALIDATION_NO,
                "mode": mode,
                "terminal_state": terminal,
                "passed": passed,
                "failed": failed,
                "conflicts": len(conflicts),
                "timestamp": report["timestamp"],
            },
        )
        return report

    @staticmethod
    def _terminal_state(
        passed: int,
        failed: int,
        conflicts: list[int],
        results: list[dict[str, Any]],
    ) -> str:
        """冲突会改变 14 号处理终态；未重算到任何记录也不能假装通过。"""
        if conflicts:
            return TERMINAL_CONFLICT
        if failed:
            return TERMINAL_FAILED
        if passed == 0 and not results:
            return TERMINAL_FAILED
        return TERMINAL_PASSED

    def _sync_batch_tracker(
        self,
        batch_ref: str,
        terminal: str,
        passed: int,
        failed: int,
        revalidated: int,
    ) -> dict[str, Any] | None:
        """跨业务面同步批次校验状态，保持业务清单与结果台账一致。"""
        for row in store.rows("batch_tracker"):
            if row.get("批次编号") != batch_ref:
                continue
            row["校验状态"] = terminal
            row["补全版本"] = max(
                (e.enrich_version for e in store.collect_records.list_by_batch(batch_ref)),
                default=0,
            )
            row["复验通过数"] = passed
            row["复验失败数"] = failed
            row["复验条数"] = revalidated
            if terminal == TERMINAL_PASSED and not store.collect_records.list_pending_revalidation(batch_ref):
                row["status"] = "已完成"
            elif terminal in (TERMINAL_FAILED, TERMINAL_CONFLICT):
                # 仍需处理，不能落到“已完成”。
                row["status"] = "校验中"
            row["pending"] = terminal != TERMINAL_PASSED
            return row
        return None

    def _sync_revalidation_todo(
        self,
        batch_ref: str,
        terminal: str,
        failed: int,
    ) -> dict[str, Any]:
        """跨业务面同步待办：失败/冲突挂一条复验待办，通过则关闭旧待办。"""
        task_no = f"VERI-{batch_ref}"
        tasks = store.rows("task_queue")
        existing = next((t for t in tasks if t.get("任务编号") == task_no), None)

        if terminal == TERMINAL_PASSED and not failed:
            if existing is not None and existing.get("status") != "已完成":
                existing["status"] = "已完成"
                existing["pending"] = False
                existing["当前状态"] = "已完成"
            return {"task_number": task_no, "status": "已完成", "action": "closed"}

        target_status = "排队中" if terminal == TERMINAL_CONFLICT else "执行失败"
        if existing is None:
            new_id = max((int(t.get("id", 0)) for t in tasks), default=0) + 1
            existing = {
                "id": new_id,
                "任务编号": task_no,
                "流程名称": f"补全复验-{batch_ref}",
                "提交时间": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "优先级": "高",
                "预计执行": "立即",
                "排队时长": "0s",
                "当前状态": target_status,
                "status": target_status,
                "pending": True,
                "abnormal": terminal == TERMINAL_FAILED,
                "batch_ref": batch_ref,
                "item_no": ENRICH_REVALIDATION_NO,
            }
            tasks.append(existing)
            action = "created"
        else:
            existing["status"] = target_status
            existing["当前状态"] = target_status
            existing["pending"] = True
            existing["abnormal"] = terminal == TERMINAL_FAILED
            action = "updated"
        return {"task_number": task_no, "status": target_status, "action": action,
                "terminal_state": terminal}

    def revalidation_report(self, batch_ref: str) -> dict[str, Any]:
        """结果校验工作台：定位具体失败规则与失败记录。"""
        records = store.collect_records.list_by_batch(batch_ref)
        rules = self._get_active_rules()
        rule_by_id = {r["id"]: r for r in rules}
        entries: list[dict[str, Any]] = []
        failed_rule_hits: dict[str, int] = {}

        for record in records:
            receipt = store.validation_receipts.active_for(record.entry_id)
            failed_rules = []
            if receipt is not None:
                for failure in receipt.failures:
                    rule_id = failure.get("rule_id")
                    name = failure.get("rule_name") or (
                        rule_by_id.get(rule_id, {}).get("name") if rule_id else None
                    ) or failure.get("field")
                    if name:
                        failed_rules.append(name)
                        failed_rule_hits[name] = failed_rule_hits.get(name, 0) + 1
            entries.append({
                "entry_id": record.entry_id,
                "enrich_version": record.enrich_version,
                "rewritten_fields": list(record.rewritten_fields),
                "validation_status": record.validation_status,
                "validated": record.validated,
                "quality_score": record.quality_score,
                "receipt": receipt.to_dict() if receipt else None,
                "failed_rules": failed_rules,
            })

        has_pending = any(
            e["validation_status"] == STATUS_PENDING_REVALIDATION for e in entries)
        has_conflict = any(e["validation_status"] == STATUS_CONFLICT for e in entries)
        if not entries:
            terminal = "无待复验记录"
        elif has_conflict:
            terminal = TERMINAL_CONFLICT
        elif has_pending:
            terminal = "待复验"
        elif any(e["validation_status"] == STATUS_REVALIDATION_FAILED for e in entries):
            terminal = TERMINAL_FAILED
        elif entries and all(e["validation_status"] == STATUS_VALIDATED for e in entries):
            terminal = TERMINAL_PASSED
        else:
            terminal = "待复验"

        return {
            "batch_ref": batch_ref,
            "item_no": ENRICH_REVALIDATION_NO,
            "item": ENRICH_REVALIDATION_ITEM,
            "terminal_state": terminal,
            "active_rules": [
                {"id": r["id"], "name": r["name"], "field": r["field"],
                 "condition": r["condition"], "threshold": r["sample_rate"]}
                for r in rules
            ],
            "failed_rule_hits": failed_rule_hits,
            "records": entries,
            "consistency": store.collect_records.consistency_report(batch_ref),
        }

    def _get_active_rules(self) -> list[dict[str, Any]]:
        """获取已启用的校验规则。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"),
                "name": r.get("规则名称"),
                "field": r.get("校验字段"),
                "condition": r.get("校验条件"),
                "pattern": r.get("期望模式", "non_empty"),
                "action_on_fail": r.get("异常动作", "quarantine"),
                "sample_rate": _safe_float(r.get("抽样比例", "1.0")),
                "bot_ref": r.get("关联流程", ""),
            }
            for r in rows if r.get("status") == "已启用"
        ]

    def _validate_record(
        self,
        fields: dict[str, Any],
        rules: list[dict[str, Any]],
        *,
        threshold: float = 0.6,
        deterministic: bool = False,
    ) -> dict[str, Any]:
        """按规则校验单条采集记录。

        阈值口径：得分达到 ``threshold`` 才判通过；任一规则硬失败也会拉低得分，
        但不再用“抽样跳过”把未覆盖的字段当成通过。

        Returns:
            {"passed": bool, "score": float, "reason": str,
             "failures": [...], "failed_rules": [...]}
        """
        if not fields:
            return {
                "passed": False, "score": 0.0, "reason": "空记录",
                "failures": [{"field": None, "reason": "空记录"}],
                "failed_rules": [],
            }

        applied = 0
        failed = 0
        reasons: list[str] = []
        failures: list[dict[str, Any]] = []
        failed_rules: list[dict[str, Any]] = []

        for rule in rules:
            if not deterministic and random.random() > rule["sample_rate"]:
                continue  # 抽样跳过

            field_name = rule["field"]
            condition = rule["condition"]

            def _fail(reason: str) -> None:
                nonlocal failed
                failed += 1
                reasons.append(reason)
                failures.append({
                    "rule_id": rule.get("id"),
                    "rule_name": rule.get("name"),
                    "field": field_name,
                    "condition": condition,
                    "reason": reason,
                })
                failed_rules.append({
                    "rule_id": rule.get("id"),
                    "rule_name": rule.get("name"),
                    "field": field_name,
                })

            # 模拟校验逻辑
            if field_name in fields:
                value = fields[field_name]
                applied += 1

                if condition == "non_empty" and not str(value).strip():
                    _fail(f"字段[{field_name}]为空")
                elif condition == "numeric" and not str(value).replace(".", "").isdigit():
                    _fail(f"字段[{field_name}]不是数值")
                elif condition == "length_gt_0" and len(str(value)) == 0:
                    _fail(f"字段[{field_name}]长度为零")
                # 模拟额外场景（可复现模式下关闭随机异常）
                elif not deterministic and random.random() < 0.08:
                    _fail(f"字段[{field_name}]格式异常")
            else:
                # 字段缺失
                _fail(f"缺少字段[{field_name}]")

        if applied == 0 and failed == 0:
            # 无匹配字段视为通过
            return {
                "passed": True, "score": 1.0, "reason": "无校验规则匹配",
                "failures": [], "failed_rules": [],
            }

        score = 1.0 - (failed / max(applied + failed, 1))
        # 阈值口径：任一规则硬失败直接判不通过；无硬失败时再按质量分阈值兜底。
        passed = failed == 0 and score >= threshold
        if not passed and failed == 0:
            reasons.append(f"质量分 {round(score, 2)} 低于阈值 {threshold}")
        return {
            "passed": passed,
            "score": round(score, 2),
            "reason": "; ".join(reasons) if reasons else "通过",
            "failures": failures,
            "failed_rules": failed_rules,
        }

    def run_single_validate(
        self,
        record: dict[str, Any],
        rule_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        """对单条记录执行指定规则校验。

        Args:
            record: 待校验记录
            rule_ids: 要使用的规则 ID 列表（None = 所有已启用）

        Returns:
            校验结果
        """
        all_rules = self._get_active_rules()
        if rule_ids:
            rules = [r for r in all_rules if r["id"] in rule_ids]
        else:
            rules = all_rules

        return self._validate_record(record, rules)

    def get_validation_stats(self) -> dict[str, Any]:
        """获取校验统计概览。"""
        validate_events = store.channel.snapshot(topic="rpa.validation_complete")
        total_passed = sum(
            e.get("payload", {}).get("passed", 0) for e in validate_events)
        total_failed = sum(
            e.get("payload", {}).get("failed", 0) for e in validate_events)
        revalidation_events = store.channel.snapshot(
            topic="rpa.revalidation_complete")
        return {
            "total_validation_events": len(validate_events),
            "total_passed": total_passed,
            "total_failed": total_failed,
            "revalidation_events": len(revalidation_events),
            "pending_revalidation": store.collect_records.snapshot()[
                "pending_revalidation"],
            "consistency": store.collect_records.consistency_report(),
            "receipts": store.validation_receipts.snapshot(),
            "dirty_vault": store.dirty_vault.snapshot(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        stats = self.get_validation_stats()
        return {**base, **stats}