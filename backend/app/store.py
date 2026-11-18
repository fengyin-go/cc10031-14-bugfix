"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

跨模块数据流能力：
- DataChannel: 服务间数据发布/消费通道，支持 topic 订阅、快照读、消费确认
- DirtyDataVault: 脏数据隔离区，存储校验不通过/解析失败的脏数据记录
- FailureRecovery: 失败恢复管理器，记录失败步骤并支持重试
- CollectRecord: 采集记录存储，记录每次采集产出的结构化数据
- ValidationReceipt: 补全校验回执台账，校验结论与补全版本绑定
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_ROWS

# ---------------------------------------------------------------------------
# 跨模块数据通道 (DataChannel) — 服务间发布/消费
# ---------------------------------------------------------------------------

@dataclass
class ChannelMessage:
    """数据通道中的单条消息，带 topic、来源、负载和确认状态。"""
    msg_id: int
    topic: str
    source_module: str
    payload: dict[str, Any]
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    consumed_by: list[str] = field(default_factory=list)
    confirmed: bool = False

    def mark_consumed(self, consumer: str) -> None:
        if consumer not in self.consumed_by:
            self.consumed_by.append(consumer)


class DataChannel:
    """跨模块数据通道：支持 topic 发布/订阅，服务间数据流转的桥梁。

    用法::

        store.channel.publish("rpa.bot_created", "bot_definition", bot_data)
        msgs = store.channel.consume("step_editor", "rpa.bot_created")
        store.channel.confirm(msg_id, "step_editor")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._messages: list[ChannelMessage] = []

    @property
    def total_messages(self) -> int:
        with self._lock:
            return len(self._messages)

    def publish(self, topic: str, source: str, payload: dict[str, Any]) -> ChannelMessage:
        with self._lock:
            self._seq += 1
            msg = ChannelMessage(
                msg_id=self._seq, topic=topic,
                source_module=source, payload=payload,
            )
            self._messages.append(msg)
            return msg

    def consume(self, consumer: str, topic: str | None = None,
                since_msg_id: int | None = None) -> list[ChannelMessage]:
        """获取指定 consumer 尚未消费的消息。"""
        with self._lock:
            results: list[ChannelMessage] = []
            for msg in self._messages:
                if topic and msg.topic != topic:
                    continue
                if since_msg_id is not None and msg.msg_id <= since_msg_id:
                    continue
                if consumer not in msg.consumed_by:
                    results.append(msg)
                    msg.mark_consumed(consumer)
            return results

    def confirm(self, msg_id: int, consumer: str) -> bool:
        with self._lock:
            for msg in self._messages:
                if msg.msg_id == msg_id:
                    msg.confirmed = True
                    return True
            return False

    def snapshot(self, topic: str | None = None) -> list[dict[str, Any]]:
        """返回消息快照，供跨模块分析/看板消费。"""
        with self._lock:
            msgs = self._messages if topic is None else [
                m for m in self._messages if m.topic == topic
            ]
            return [{
                "msg_id": m.msg_id, "topic": m.topic,
                "source": m.source_module, "payload": m.payload,
                "created_at": m.created_at, "consumed_by": m.consumed_by,
                "confirmed": m.confirmed,
            } for m in msgs]

    def latest(self, topic: str) -> dict[str, Any] | None:
        with self._lock:
            for msg in reversed(self._messages):
                if msg.topic == topic:
                    return {
                        "msg_id": msg.msg_id, "topic": msg.topic,
                        "source": msg.source_module, "payload": msg.payload,
                        "created_at": msg.created_at, "confirmed": msg.confirmed,
                    }
            return None

    def uncomfirmed_count(self) -> int:
        with self._lock:
            return sum(1 for m in self._messages if not m.confirmed)


# ---------------------------------------------------------------------------
# 脏数据隔离区 (DirtyDataVault)
# ---------------------------------------------------------------------------

@dataclass
class DirtyRecord:
    """脏数据记录：校验失败或解析异常的数据行。"""
    record_id: int
    source_module: str
    batch_ref: str
    raw_data: dict[str, Any]
    reason: str  # 如 "字段缺失", "格式非法", "解析超时"
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    quarantined: bool = True
    recovered_to: str | None = None


class DirtyDataVault:
    """脏数据隔离区：被校验规则或解析器标记为问题数据后移入此处。

    用法::

        vault = store.dirty_vault
        vault.quarantine("data_extract", "BATC-001", row, "缺少必填字段")
        dirty = vault.list_by_source("data_extract")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._records: list[DirtyRecord] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._records)

    def quarantine(self, source: str, batch_ref: str,
                   raw_data: dict[str, Any], reason: str) -> DirtyRecord:
        with self._lock:
            self._seq += 1
            rec = DirtyRecord(
                record_id=self._seq, source_module=source,
                batch_ref=batch_ref, raw_data=raw_data, reason=reason,
            )
            self._records.append(rec)
            return rec

    def list_by_source(self, source: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.source_module == source]

    def list_by_batch(self, batch_ref: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.batch_ref == batch_ref]

    def mark_recovered(self, record_id: int, recovered_to: str) -> bool:
        with self._lock:
            for r in self._records:
                if r.record_id == record_id:
                    r.quarantined = False
                    r.recovered_to = recovered_to
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            quarantined = sum(1 for r in self._records if r.quarantined)
            by_reason: dict[str, int] = {}
            for r in self._records:
                by_reason[r.reason] = by_reason.get(r.reason, 0) + 1
            return {
                "total": len(self._records),
                "quarantined": quarantined,
                "recovered": len(self._records) - quarantined,
                "by_reason": by_reason,
            }


# ---------------------------------------------------------------------------
# 采集记录 (CollectRecord) — 每次采集产出的结构化数据
# ---------------------------------------------------------------------------

# 补全二次校验的处理项编号与终态。冲突会改变 14 号处理的终态，
# 未通过时也不能退回“补全前通过”的旧结论。
ENRICH_REVALIDATION_NO = 14
ENRICH_REVALIDATION_ITEM = "补全二次校验"
TERMINAL_PASSED = "已复验通过"
TERMINAL_FAILED = "复验未通过"
TERMINAL_CONFLICT = "版本冲突"
STATUS_NOT_VALIDATED = "未校验"
STATUS_VALIDATED = "已校验"
STATUS_PENDING_REVALIDATION = "待复验"
STATUS_REVALIDATION_FAILED = "复验未通过"
STATUS_CONFLICT = "校验冲突"


@dataclass
class CollectEntry:
    entry_id: int
    batch_ref: str
    source_module: str
    fields: dict[str, Any]
    collected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    validated: bool = False
    quality_score: float | None = None
    # 补全版本：0 表示从未发生补全；每次真正改写字段才 +1。
    enrich_version: int = 0
    enriched_at: str | None = None
    # 当前补全版本实际改写的字段及其新旧值。
    rewritten_fields: list[str] = field(default_factory=list)
    rewrite_diff: dict[str, dict[str, Any]] = field(default_factory=dict)
    rewrite_history: list[dict[str, Any]] = field(default_factory=list)
    # 校验状态必须与补全版本绑定，不能只靠一个布尔标记。
    validation_status: str = STATUS_NOT_VALIDATED
    validation_version: int = 0
    validation_receipt: dict[str, Any] | None = None
    conflict: bool = False

    @property
    def pending_revalidation(self) -> bool:
        return self.validation_status == STATUS_PENDING_REVALIDATION

    def _reset_validation(self) -> None:
        self.validated = False
        self.quality_score = None
        self.validation_receipt = None
        self.validation_status = STATUS_PENDING_REVALIDATION

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "batch_ref": self.batch_ref,
            "source_module": self.source_module,
            "fields": dict(self.fields),
            "collected_at": self.collected_at,
            "validated": self.validated,
            "quality_score": self.quality_score,
            "enrich_version": self.enrich_version,
            "enriched_at": self.enriched_at,
            "rewritten_fields": list(self.rewritten_fields),
            "rewrite_diff": {k: dict(v) for k, v in self.rewrite_diff.items()},
            "validation_status": self.validation_status,
            "validation_version": self.validation_version,
            "conflict": self.conflict,
        }


@dataclass
class EnrichValidationReceipt:
    """补全校验回执：把一次校验结论钉死在某一个补全版本上。

    同一条采集记录只保留一份 effective 回执，字段一旦再次被补全改写，
    旧回执立即 superseded，绝不允许“旧字段配新校验分”。
    """
    receipt_id: int
    entry_id: int
    batch_ref: str
    enrich_version: int
    source: str
    passed: bool
    score: float
    rules_applied: list[dict[str, Any]]
    failures: list[dict[str, Any]]
    reason: str
    rewritten_fields: list[str] = field(default_factory=list)
    status: str = "effective"  # effective | superseded | conflict
    superseded_by: int | None = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "entry_id": self.entry_id,
            "batch_ref": self.batch_ref,
            "enrich_version": self.enrich_version,
            "source": self.source,
            "passed": self.passed,
            "score": self.score,
            "rules_applied": list(self.rules_applied),
            "failures": list(self.failures),
            "reason": self.reason,
            "rewritten_fields": list(self.rewritten_fields),
            "status": self.status,
            "superseded_by": self.superseded_by,
            "created_at": self.created_at,
        }


class ValidationReceiptStore:
    """校验回执台账：每条记录同一时刻只有当前补全版本对应的那份回执有效。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._receipts: list[EnrichValidationReceipt] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._receipts)

    def issue(
        self,
        *,
        entry_id: int,
        batch_ref: str,
        enrich_version: int,
        source: str,
        passed: bool,
        score: float,
        rules_applied: list[dict[str, Any]],
        failures: list[dict[str, Any]],
        reason: str,
        rewritten_fields: list[str] | None = None,
    ) -> EnrichValidationReceipt:
        with self._lock:
            # 新回执生效前，同记录的旧回执全部作废，保证只留一份有效回执。
            for old in self._receipts:
                if old.entry_id == entry_id and old.status == "effective":
                    old.status = "superseded"
            self._seq += 1
            receipt = EnrichValidationReceipt(
                receipt_id=self._seq,
                entry_id=entry_id,
                batch_ref=batch_ref,
                enrich_version=enrich_version,
                source=source,
                passed=passed,
                score=score,
                rules_applied=list(rules_applied),
                failures=list(failures),
                reason=reason,
                rewritten_fields=list(rewritten_fields or []),
            )
            self._receipts.append(receipt)
            return receipt

    def supersede_for_entry(self, entry_id: int, *, superseded_by: int | None = None,
                            only_versions_lt: int | None = None) -> list[int]:
        """字段被改写时作废旧回执，返回被作废的回执号。"""
        with self._lock:
            superseded: list[int] = []
            for receipt in self._receipts:
                if receipt.entry_id != entry_id or receipt.status != "effective":
                    continue
                if only_versions_lt is not None and receipt.enrich_version >= only_versions_lt:
                    continue
                receipt.status = "superseded"
                receipt.superseded_by = superseded_by
                superseded.append(receipt.receipt_id)
            return superseded

    def active_for(self, entry_id: int) -> EnrichValidationReceipt | None:
        with self._lock:
            for receipt in reversed(self._receipts):
                if receipt.entry_id == entry_id and receipt.status == "effective":
                    return receipt
            return None

    def list_by_entry(self, entry_id: int) -> list[EnrichValidationReceipt]:
        with self._lock:
            return [r for r in self._receipts if r.entry_id == entry_id]

    def list_by_batch(self, batch_ref: str) -> list[EnrichValidationReceipt]:
        with self._lock:
            return [r for r in self._receipts if r.batch_ref == batch_ref]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            effective = sum(1 for r in self._receipts if r.status == "effective")
            return {
                "total": len(self._receipts),
                "effective": effective,
                "superseded": sum(1 for r in self._receipts if r.status == "superseded"),
                "conflict": sum(1 for r in self._receipts if r.status == "conflict"),
            }


class CollectRecordStore:
    """采集记录存储：每次采集流程产出的结构化数据行。

    字段改写与校验失效在同一把锁内完成：一旦补全真正改了字段，
    ``enrich_version`` 立即 +1，``validated``/``quality_score`` 同步清空，
    旧校验回执同步作废，避免下游把补全后的记录当成补全前的已验证数据。

    用法::

        rec = store.collect_records.add("BATC-001", "data_extract", {"字段1": "值1", ...})
        store.collect_records.mark_validated(rec.entry_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[CollectEntry] = []
        self._receipts: ValidationReceiptStore | None = None

    def attach_receipt_store(self, receipts: ValidationReceiptStore) -> None:
        self._receipts = receipts

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def add(self, batch_ref: str, source: str,
            fields: dict[str, Any]) -> CollectEntry:
        with self._lock:
            self._seq += 1
            entry = CollectEntry(
                entry_id=self._seq, batch_ref=batch_ref,
                source_module=source, fields=dict(fields),
            )
            self._entries.append(entry)
            return entry

    def get(self, entry_id: int) -> CollectEntry | None:
        with self._lock:
            for entry in self._entries:
                if entry.entry_id == entry_id:
                    return entry
            return None

    def list_by_batch(self, batch_ref: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.batch_ref == batch_ref]

    def list_by_source(self, source: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.source_module == source]

    def list_enriched(self, batch_ref: str | None = None) -> list[CollectEntry]:
        with self._lock:
            return [
                e for e in self._entries
                if e.enrich_version > 0
                and (batch_ref is None or e.batch_ref == batch_ref)
            ]

    def list_pending_revalidation(self, batch_ref: str | None = None) -> list[CollectEntry]:
        with self._lock:
            return [
                e for e in self._entries
                if e.validation_status == STATUS_PENDING_REVALIDATION
                and (batch_ref is None or e.batch_ref == batch_ref)
            ]

    def apply_rewrite(
        self,
        entry_id: int,
        updates: dict[str, Any],
        *,
        expected_version: int | None = None,
        source: str = "data_enrich",
    ) -> tuple[bool, str, dict[str, Any] | None]:
        """原子改写字段：同一个锁内完成改写、版本递增、校验失效、旧回执作废。

        ``expected_version`` 用于并发控制：只有版本仍与调用方读到的一致才允许写入，
        后到的写入直接判为版本冲突，不产生第二份可用的校验结论。
        """
        with self._lock:
            entry = next(
                (e for e in self._entries if e.entry_id == entry_id), None)
            if entry is None:
                return False, "采集记录不存在", None
            if expected_version is not None and entry.enrich_version != expected_version:
                # 后到的写入不落库，也不覆盖先到版本的字段和回执。
                return False, TERMINAL_CONFLICT, entry.to_dict()

            diff: dict[str, dict[str, Any]] = {}
            for name, value in updates.items():
                old = entry.fields.get(name)
                if old != value:
                    diff[name] = {"old": old, "new": value}
            if not diff:
                # 没有真正改写字段，就不允许动版本和校验状态。
                return True, "无字段改写", entry.to_dict()

            previous_version = entry.enrich_version
            entry.enrich_version = previous_version + 1
            entry.enriched_at = datetime.now(timezone.utc).isoformat()
            entry.fields.update({name: change["new"] for name, change in diff.items()})
            entry.rewritten_fields = list(diff)
            entry.rewrite_diff = diff
            entry.rewrite_history.append({
                "version": entry.enrich_version,
                "previous_version": previous_version,
                "diff": {k: dict(v) for k, v in diff.items()},
                "source": source,
                "at": entry.enriched_at,
            })

            superseded: list[int] = []
            if self._receipts is not None:
                superseded = self._receipts.supersede_for_entry(
                    entry_id, only_versions_lt=entry.enrich_version)
            entry._reset_validation()
            entry.validation_version = entry.enrich_version
            entry.conflict = False
            result = entry.to_dict()
            result["previous_version"] = previous_version
            result["superseded_receipts"] = superseded
            return True, "字段已改写，旧校验结论已失效", result

    def record_validation(
        self,
        entry_id: int,
        passed: bool,
        score: float,
        *,
        expected_version: int | None = None,
        source: str = "initial",
        rules_applied: list[dict[str, Any]] | None = None,
        failures: list[dict[str, Any]] | None = None,
        reason: str = "",
    ) -> tuple[bool, str, dict[str, Any] | None]:
        """把校验结论写成绑定补全版本的回执；版本不匹配时拒绝落库。"""
        with self._lock:
            entry = next(
                (e for e in self._entries if e.entry_id == entry_id), None)
            if entry is None:
                return False, "采集记录不存在", None
            version = entry.enrich_version if expected_version is None else expected_version
            if entry.enrich_version != version:
                # 迟到的校验结论对应的是旧字段版本，直接拒收，不覆盖新版本状态。
                return False, TERMINAL_CONFLICT, entry.to_dict()

            receipt: EnrichValidationReceipt | None = None
            if self._receipts is not None:
                receipt = self._receipts.issue(
                    entry_id=entry_id,
                    batch_ref=entry.batch_ref,
                    enrich_version=version,
                    source=source,
                    passed=passed,
                    score=score,
                    rules_applied=list(rules_applied or []),
                    failures=list(failures or []),
                    reason=reason,
                    rewritten_fields=list(entry.rewritten_fields),
                )
            entry.validated = passed
            entry.quality_score = score
            entry.validation_version = version
            entry.validation_receipt = receipt.to_dict() if receipt else None
            entry.validation_status = (
                STATUS_VALIDATED if passed else STATUS_REVALIDATION_FAILED
            )
            entry.conflict = False
            return True, "校验回执已生成", entry.to_dict()

    def mark_conflict(self, entry_id: int, *, preserve_receipt: bool = True) -> bool:
        """并发/版本冲突终态：冲突记录不得保留任何“通过”结论。"""
        with self._lock:
            entry = next(
                (e for e in self._entries if e.entry_id == entry_id), None)
            if entry is None:
                return False
            entry.conflict = True
            entry.validation_status = STATUS_CONFLICT
            entry.validated = False
            entry.quality_score = None
            if not preserve_receipt and self._receipts is not None:
                self._receipts.supersede_for_entry(entry_id)
                entry.validation_receipt = None
            return True

    def mark_validated(self, entry_id: int, score: float = 1.0) -> bool:
        """兼容旧调用：为当前版本补一份通过回执。

        发生过补全且尚未二次校验的记录不会被直接标记为已验证。
        """
        with self._lock:
            entry = next(
                (e for e in self._entries if e.entry_id == entry_id), None)
            if entry is None:
                return False
            if entry.validation_status == STATUS_PENDING_REVALIDATION:
                return False
        ok, _, _ = self.record_validation(
            entry_id, True, score, source="initial",
            reason="通过", rules_applied=[], failures=[])
        return ok
    def consistency_report(self, batch_ref: str | None = None) -> dict[str, Any]:
        """核对业务清单与校验台账：是否存在旧字段配新校验分的情况。"""
        with self._lock:
            entries = [
                e for e in self._entries
                if batch_ref is None or e.batch_ref == batch_ref
            ]
            stale: list[dict[str, Any]] = []
            for entry in entries:
                receipt = None
                if self._receipts is not None:
                    receipt = self._receipts.active_for(entry.entry_id)
                receipt_version = receipt.enrich_version if receipt else None
                if entry.validated and (
                    entry.validation_version != entry.enrich_version
                    or (receipt is not None and receipt_version != entry.enrich_version)
                ):
                    stale.append({
                        "entry_id": entry.entry_id,
                        "batch_ref": entry.batch_ref,
                        "enrich_version": entry.enrich_version,
                        "validation_version": entry.validation_version,
                        "receipt_version": receipt_version,
                    })
            return {
                "count": len(entries),
                "validated": sum(1 for e in entries if e.validated),
                "pending_revalidation": sum(
                    1 for e in entries
                    if e.validation_status == STATUS_PENDING_REVALIDATION),
                "conflicts": sum(1 for e in entries if e.conflict),
                "stale_conclusions": stale,
                "consistent": not stale,
            }

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            validated = sum(1 for e in self._entries if e.validated)
            enriched = sum(1 for e in self._entries if e.enrich_version > 0)
            pending = sum(
                1 for e in self._entries
                if e.validation_status == STATUS_PENDING_REVALIDATION)
            conflicts = sum(1 for e in self._entries if e.conflict)
            by_source: dict[str, int] = {}
            for e in self._entries:
                by_source[e.source_module] = by_source.get(e.source_module, 0) + 1
            return {
                "total": len(self._entries),
                "validated": validated,
                "unvalidated": len(self._entries) - validated,
                "enriched": enriched,
                "pending_revalidation": pending,
                "conflicts": conflicts,
                "by_source": by_source,
            }


# ---------------------------------------------------------------------------
# 失败恢复管理 (FailureRecovery)
# ---------------------------------------------------------------------------

@dataclass
class RecoveryEntry:
    recovery_id: int
    module: str
    batch_ref: str
    step: str
    error_detail: str
    attempt: int = 0
    max_attempts: int = 3
    resolved: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class FailureRecovery:
    """失败恢复管理器：记录失败步骤并支持自动重试。

    用法::

        rec = store.recovery.log("step_editor", "BATC-001", "元素定位",
                                  "选择器超时未匹配")
        store.recovery.retry(rec.recovery_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[RecoveryEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def log(self, module: str, batch_ref: str, step: str,
            error_detail: str, max_attempts: int = 3) -> RecoveryEntry:
        with self._lock:
            self._seq += 1
            entry = RecoveryEntry(
                recovery_id=self._seq, module=module,
                batch_ref=batch_ref, step=step,
                error_detail=error_detail, max_attempts=max_attempts,
            )
            self._entries.append(entry)
            return entry

    def retry(self, recovery_id: int) -> tuple[bool, str]:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    if e.resolved:
                        return False, "已恢复，无需重试"
                    e.attempt += 1
                    if e.attempt > e.max_attempts:
                        return False, f"已达最大重试次数 {e.max_attempts}"
                    return True, f"重试 {e.attempt}/{e.max_attempts}"
            return False, "恢复记录不存在"

    def resolve(self, recovery_id: int) -> bool:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    e.resolved = True
                    return True
            return False

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries if not e.resolved)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            pending = sum(1 for e in self._entries if not e.resolved)
            return {
                "total": len(self._entries),
                "resolved": len(self._entries) - pending,
                "pending": pending,
            }


# ---------------------------------------------------------------------------
# 增强版 Store — 整合通道、脏数据、采集记录、恢复
# ---------------------------------------------------------------------------

class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块基础设施
        self.channel = DataChannel()
        self.dirty_vault = DirtyDataVault()
        self.collect_records = CollectRecordStore()
        self.validation_receipts = ValidationReceiptStore()
        self.collect_records.attach_receipt_store(self.validation_receipts)
        self.recovery = FailureRecovery()

    # ---- 基础数据操作 ----

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()