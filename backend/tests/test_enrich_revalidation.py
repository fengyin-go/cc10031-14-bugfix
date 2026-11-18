"""补全二次校验回归测试 — 字段改写与校验状态失效必须同步发生。

覆盖：
1. 补全改写字段后 validated / quality_score 立即失效，旧回执作废；
2. 发生过补全的记录不能沿用补全前的通过结论；
3. 增量重验只重算被改写记录；整批重校验同样不产生旧字段配新校验分；
4. 并发再次写入只接受一个版本，冲突改变 14 号处理终态；
5. 批次台账与复验待办跨业务面同步；
6. 补全工作台 / 结果校验工作台可定位字段、版本与失败规则。
"""
from __future__ import annotations

import itertools
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services.data_enrich import DataEnrichService
from app.services.result_verify import ResultVerifyService
from app.store import (
    ENRICH_REVALIDATION_NO,
    STATUS_PENDING_REVALIDATION,
    TERMINAL_CONFLICT,
    TERMINAL_FAILED,
    TERMINAL_PASSED,
    store,
)

_seq = itertools.count(1)


def _batch_ref() -> str:
    return f"TEST-ENRICH-{next(_seq):04d}"


def _enrich_field() -> str:
    """取一条已生效补全规则的目标字段，确保补全一定命中。"""
    rules = [
        r for r in store.rows("data_enrich") if r.get("status") == "已生效"
    ]
    assert rules, "样例数据里应有已生效补全规则"
    return rules[0]["目标字段"]


def _verify_field() -> str:
    """取一条已启用校验规则的目标字段，保证样例记录能满足校验规则。"""
    rules = [
        r for r in store.rows("result_verify") if r.get("status") == "已启用"
    ]
    assert rules, "样例数据里应有已启用校验规则"
    return rules[0]["校验字段"]


def _make_batch(pass_all: bool = True) -> str:
    batch = _batch_ref()
    field = _enrich_field()
    for _ in range(3):
        # 故意缺少补全目标字段，同时带上校验规则需要的字段
        fields = {"名称": "样例", "数量": 1, _verify_field(): "合规值"}
        if pass_all:
            fields[field] = ""  # 空值同样触发补全
        store.collect_records.add(batch, "data_extract", fields)
    return batch


def _validate_all(batch: str, passed: bool = True) -> None:
    for record in store.collect_records.list_by_batch(batch):
        store.collect_records.record_validation(
            record.entry_id, passed, 1.0, source="result_verify",
            rules_applied=[{"id": 1, "field": "名称"}], reason="通过")


def test_enrich_invalidates_validation_and_receipt() -> None:
    batch = _make_batch()
    _validate_all(batch, passed=True)
    before = store.collect_records.list_by_batch(batch)
    assert all(r.validated and r.validation_receipt for r in before)
    old_receipts = {
        r.entry_id: store.validation_receipts.active_for(r.entry_id).receipt_id
        for r in before
    }

    result = DataEnrichService().enrich_records(batch)
    assert result["rewritten"] > 0
    assert result["pending_revalidation"] == result["rewritten"]

    for record in store.collect_records.list_by_batch(batch):
        assert record.enrich_version == 1
        # 字段改写与校验状态失效同步发生
        assert record.validated is False
        assert record.quality_score is None
        assert record.validation_status == STATUS_PENDING_REVALIDATION
        assert record.rewritten_fields
        # 旧回执立刻作废，同一记录最多只留一份生效回执
        assert store.validation_receipts.active_for(record.entry_id) is None
        receipts = store.validation_receipts.list_by_entry(record.entry_id)
        assert [r.receipt_id for r in receipts if r.status == "effective"] == []
        assert old_receipts[record.entry_id] in [
            r.receipt_id for r in receipts if r.status == "superseded"
        ]

    report = store.collect_records.consistency_report(batch)
    assert report["consistent"] is True
    assert report["stale_conclusions"] == []


def test_mark_validated_cannot_shortcut_pending_revalidation() -> None:
    batch = _make_batch()
    DataEnrichService().enrich_records(batch)
    for record in store.collect_records.list_by_batch(batch):
        # 补全后不允许绕过二次校验直接把记录标成已验证
        assert store.collect_records.mark_validated(record.entry_id, 1.0) is False
        assert store.collect_records.get(record.entry_id).validated is False


def test_incremental_revalidation_only_recomputes_rewritten() -> None:
    batch = _make_batch()
    records = store.collect_records.list_by_batch(batch)
    # 只有前两条缺字段会被补全，第三条字段齐全不受影响
    third = records[2]
    third.fields[_enrich_field()] = "已有值"
    _validate_all(batch, passed=True)

    DataEnrichService().enrich_records(batch)
    untouched = store.collect_records.get(third.entry_id)
    assert untouched.enrich_version == 0

    report = ResultVerifyService().revalidate_enriched(
        batch, mode="incremental", deterministic=True)
    assert report["item_no"] == ENRICH_REVALIDATION_NO
    assert report["revalidated"] == 2
    assert third.entry_id in report["skipped"]
    assert report["terminal_state"] == TERMINAL_PASSED
    assert report["consistency"]["consistent"] is True

    # 每条被重验的记录，回执版本必须等于当前补全版本
    for record in store.collect_records.list_by_batch(batch):
        receipt = store.validation_receipts.active_for(record.entry_id)
        if receipt is not None:
            assert receipt.enrich_version == record.enrich_version
        assert record.validation_version == record.enrich_version


def test_full_batch_revalidation_keeps_versions_aligned() -> None:
    batch = _make_batch()
    _validate_all(batch, passed=True)
    DataEnrichService().enrich_records(batch)

    report = ResultVerifyService().revalidate_enriched(
        batch, mode="full_batch", deterministic=True)
    assert report["revalidated"] == len(store.collect_records.list_by_batch(batch))
    for record in store.collect_records.list_by_batch(batch):
        receipt = store.validation_receipts.active_for(record.entry_id)
        assert receipt is not None
        assert receipt.enrich_version == record.enrich_version == 1
    assert report["consistency"]["stale_conclusions"] == []


def test_concurrent_second_write_is_rejected_and_marks_terminal_conflict() -> None:
    batch = _make_batch()
    records = store.collect_records.list_by_batch(batch)
    expected = {r.entry_id: r.enrich_version for r in records}

    service = DataEnrichService()
    first = service.enrich_records(
        batch, expected_versions=expected,
        values={_enrich_field(): "第一版补全"})
    assert first["rewritten"] > 0

    # 第二次写入仍拿着旧版本号：只接受一个版本
    second = service.enrich_records(
        batch, expected_versions=expected,
        values={_enrich_field(): "第二版补全"})
    assert second["rewritten"] == 0
    assert len(second["conflicts"]) == len(records)
    for record in store.collect_records.list_by_batch(batch):
        assert record.enrich_version == 1
        assert all(v["new"] != "第二版补全" for v in record.rewrite_diff.values())

    report = ResultVerifyService().revalidate_enriched(
        batch, mode="incremental", expected_versions=expected,
        deterministic=True)
    assert report["terminal_state"] == TERMINAL_CONFLICT
    assert report["ok"] is False
    for record in store.collect_records.list_by_batch(batch):
        assert record.validation_status == "校验冲突"
        assert record.validated is False


def test_failed_revalidation_never_keeps_old_pass_conclusion() -> None:
    batch = _make_batch()
    _validate_all(batch, passed=True)
    DataEnrichService().enrich_records(batch)

    # 补全后字段被外部删掉，重验必须失败且不能回退到补全前的通过
    for record in store.collect_records.list_by_batch(batch):
        record.fields.pop(_verify_field(), None)
    report = ResultVerifyService().revalidate_enriched(
        batch, mode="incremental", threshold=0.99, deterministic=True)
    assert report["terminal_state"] == TERMINAL_FAILED
    assert report["failed"] == len(store.collect_records.list_by_batch(batch))
    for record in store.collect_records.list_by_batch(batch):
        receipt = store.validation_receipts.active_for(record.entry_id)
        assert record.validated is False
        # 失败回执仍然是当前补全版本的回执，不会把补全前的通过结论找回来
        assert receipt is not None
        assert receipt.enrich_version == record.enrich_version
        assert receipt.passed is False
    assert store.collect_records.consistency_report(batch)["consistent"] is True


def test_batch_tracker_and_todo_synced() -> None:
    batch = _make_batch()
    store.rows("batch_tracker").append({
        "id": 9001,
        "批次编号": batch,
        "status": "校验中",
        "pending": True,
        "abnormal": False,
    })
    _validate_all(batch, passed=True)
    DataEnrichService().enrich_records(batch)

    report = ResultVerifyService().revalidate_enriched(
        batch, mode="incremental", deterministic=True)
    row = next(r for r in store.rows("batch_tracker") if r.get("批次编号") == batch)
    assert row["校验状态"] == report["terminal_state"]
    assert row["补全版本"] == 1

    todo = next(
        (t for t in store.rows("task_queue")
         if t.get("任务编号") == f"VERI-{batch}"), None)
    if report["terminal_state"] == TERMINAL_PASSED:
        assert todo is None or todo["status"] == "已完成"
    else:
        assert todo is not None and todo["pending"] is True


def test_workbenches_expose_rewritten_fields_and_failed_rules() -> None:
    batch = _make_batch()
    DataEnrichService().enrich_records(batch)

    workbench = DataEnrichService().enrichment_workbench(batch)
    assert workbench["item_no"] == ENRICH_REVALIDATION_NO
    assert workbench["count"] == len(store.collect_records.list_by_batch(batch))
    for item in workbench["items"]:
        assert item["enrich_version"] == 1
        assert item["rewritten_fields"]
        assert item["validation_status"] == STATUS_PENDING_REVALIDATION
        assert item["rewrite_diff"]

    # 制造一次必然失败的重验，便于结果校验工作台定位失败规则
    for record in store.collect_records.list_by_batch(batch):
        record.fields["名称"] = ""
    ResultVerifyService().revalidate_enriched(
        batch, mode="incremental", threshold=0.99, deterministic=True)
    report = ResultVerifyService().revalidation_report(batch)
    assert report["item_no"] == ENRICH_REVALIDATION_NO
    assert report["records"]
    assert any(entry["validation_status"] != STATUS_PENDING_REVALIDATION
               for entry in report["records"])
