"""未入组设备缩短允许迟到秒数后的窗口封存修复测试（PUT /devices/{id}/config）。

- 缩短 allowed_lateness_seconds：同一写事务内封存满足
  watermark >= window_end + 新迟到秒数 的已有 open 窗口并记录 sealed_at（边界取等）；
  只有实际封存才推进窗口检索的状态变更版本——status 筛选游标翻页返回 409，
  未筛状态的游标照常翻页；未封存任何窗口（调长、无窗口越线、幂等重复提交）
  时游标不失效。
- 调长 allowed_lateness_seconds：不重开已封存窗口（sealed_at 不变）。
- 后续写入落入新封存窗口的事件按 late 修正处理（计入聚合、生成修正记录）。
- 调长迟到期限后，即使按新期限水位线尚未越线，落入仍为 sealed 的旧窗口的
  补报事件仍按 late 入库（不能因 deadline 变长而记为 accepted 漏掉修正）。
"""


def put_config(client, device_id, size, lateness):
    return client.put(
        f"/devices/{device_id}/config",
        json={"window_size_seconds": size, "allowed_lateness_seconds": lateness},
    )


def ingest(client, device_id, event_id, ts, value=1.0):
    r = client.post(
        "/ingest",
        json={"device_id": device_id, "events": [
            {"event_id": event_id, "event_time": ts, "value": value}
        ]},
    )
    assert r.status_code == 200, r.text
    return r


def windows(client, device_id):
    return client.get(f"/devices/{device_id}/windows").json()


def search(client, **params):
    return client.get("/windows/search", params=params)


def test_shorten_lateness_seals_crossed_open_windows(client):
    # 60s 窗口、允许迟到 300s：水位线推进到 00:02:00 时两个窗口都仍 open
    assert put_config(client, "d1", 60, 300).status_code == 200
    ingest(client, "d1", "e1", "2026-09-22T00:00:10Z")
    ingest(client, "d1", "e2", "2026-09-22T00:02:00Z")
    assert [w["status"] for w in windows(client, "d1")] == ["open", "open"]

    # 缩短到 30s：[00:00,00:01) 满足 00:01:00+30 <= 00:02:00 → 封存；
    # [00:02,00:03)（00:03:00+30 > 00:02:00）仍 open
    r = put_config(client, "d1", 60, 30)
    assert r.status_code == 200
    assert r.json()["allowed_lateness_seconds"] == 30

    wins = windows(client, "d1")
    assert [w["status"] for w in wins] == ["sealed", "open"]
    assert wins[0]["sealed_at"] is not None
    assert wins[1]["sealed_at"] is None

    info = client.get("/devices/d1").json()
    assert (info["sealed_windows"], info["open_windows"]) == (1, 1)


def test_shorten_lateness_seals_window_at_exact_threshold(client):
    # 边界取等：watermark == window_end + 新迟到秒数 即封存
    assert put_config(client, "d1", 60, 300).status_code == 200
    ingest(client, "d1", "e1", "2026-09-22T00:00:10Z")
    ingest(client, "d1", "e2", "2026-09-22T00:02:00Z")  # 水位线 00:02:00

    # 00:01:00 + 60 == 00:02:00 == 水位线 → [00:00,00:01) 封存
    assert put_config(client, "d1", 60, 60).status_code == 200
    assert [w["status"] for w in windows(client, "d1")] == ["sealed", "open"]


def test_shorten_lateness_sealing_invalidates_status_cursor_only(client):
    assert put_config(client, "d1", 60, 300).status_code == 200
    ingest(client, "d1", "e1", "2026-09-22T00:00:10Z")
    ingest(client, "d1", "e2", "2026-09-22T00:02:00Z")

    # status=open 筛选与无筛选各取一个首页游标（两个 open 窗口，limit=1 可翻页）
    status_cursor = search(client, status="open", limit=1).json()["next_cursor"]
    plain_cursor = search(client, limit=1).json()["next_cursor"]
    assert status_cursor and plain_cursor

    # 缩短迟到秒数并实际封存一个窗口
    assert put_config(client, "d1", 60, 30).status_code == 200

    # status 筛选游标：相关筛选结果已变化 → 409 提示重新检索
    r = search(client, status="open", limit=1, cursor=status_cursor)
    assert r.status_code == 409
    assert "restart" in r.json()["detail"]

    # 未筛状态的游标：不绑定状态版本，照常翻完首页时刻的快照
    r = search(client, limit=1, cursor=plain_cursor)
    assert r.status_code == 200
    assert [w["window_start"] for w in r.json()["windows"]] == ["2026-09-22T00:02:00Z"]
    assert r.json()["next_cursor"] is None

    # 不带游标重新检索：按最新状态返回
    fresh = search(client, status="open").json()
    assert [w["window_start"] for w in fresh["windows"]] == ["2026-09-22T00:02:00Z"]


def test_config_update_without_sealing_keeps_status_cursor_valid(client):
    assert put_config(client, "d1", 60, 30).status_code == 200
    ingest(client, "d1", "e1", "2026-09-22T00:00:10Z")
    ingest(client, "d1", "e2", "2026-09-22T00:01:10Z")
    # 水位线 00:01:10；lateness=30 时两窗口均 open（00:01:00+30=00:01:30 > 00:01:10）

    cursor = search(client, status="open", limit=1).json()["next_cursor"]
    assert cursor is not None

    # 缩短到 20s：00:01:00+20=00:01:20 > 00:01:10，无窗口越过新阈值 → 不封存、不推进版本
    assert put_config(client, "d1", 60, 20).status_code == 200
    r = search(client, status="open", limit=1, cursor=cursor)
    assert r.status_code == 200
    assert [w["window_start"] for w in r.json()["windows"]] == ["2026-09-22T00:01:00Z"]
    assert r.json()["next_cursor"] is None

    # 调长到 300s 与幂等重复提交同样不封存任何窗口，游标保持有效
    cursor = search(client, status="open", limit=1).json()["next_cursor"]
    assert cursor is not None
    assert put_config(client, "d1", 60, 300).status_code == 200
    assert put_config(client, "d1", 60, 300).status_code == 200
    r = search(client, status="open", limit=1, cursor=cursor)
    assert r.status_code == 200
    assert [w["window_start"] for w in r.json()["windows"]] == ["2026-09-22T00:01:00Z"]


def test_widen_lateness_does_not_reopen_sealed_windows(client):
    assert put_config(client, "d1", 60, 30).status_code == 200
    ingest(client, "d1", "e1", "2026-09-22T00:00:10Z")
    ingest(client, "d1", "e2", "2026-09-22T00:02:00Z")  # 水位线封存 [00:00,00:01)
    before = windows(client, "d1")
    assert [w["status"] for w in before] == ["sealed", "open"]
    sealed_at = before[0]["sealed_at"]
    assert sealed_at is not None

    # 调长允许迟到：已封存窗口保持 sealed 且 sealed_at 不变，open 窗口不受影响
    assert put_config(client, "d1", 60, 3600).status_code == 200
    after = windows(client, "d1")
    assert [w["status"] for w in after] == ["sealed", "open"]
    assert after[0]["sealed_at"] == sealed_at
    assert after[1]["sealed_at"] is None


def test_events_into_newly_sealed_window_are_late_corrections(client):
    assert put_config(client, "d1", 60, 300).status_code == 200
    ingest(client, "d1", "e1", "2026-09-22T00:00:10Z", value=1.0)
    ingest(client, "d1", "e2", "2026-09-22T00:02:00Z", value=2.0)
    assert put_config(client, "d1", 60, 30).status_code == 200  # 封存 [00:00,00:01)

    # 落入新封存窗口的事件按 late 修正处理：计入聚合并生成修正记录
    body = ingest(client, "d1", "e3", "2026-09-22T00:00:40Z", value=5.0).json()
    assert body["late"] == 1 and body["accepted"] == 0
    assert body["results"][0]["status"] == "late"
    assert body["results"][0]["window_start"] == "2026-09-22T00:00:00Z"

    w = windows(client, "d1")[0]
    assert w["status"] == "sealed"
    assert (w["count"], w["sum"]) == (2, 6.0)
    assert (w["correction_count"], w["late_event_count"]) == (1, 1)

    corrections = client.get("/devices/d1/corrections").json()
    assert len(corrections) == 1
    assert corrections[0]["event_id"] == "e3"
    assert corrections[0]["window_start"] == "2026-09-22T00:00:00Z"


def test_widen_lateness_keeps_events_in_still_sealed_window_late(client):
    # 短迟到期限（30s）下封存 [00:00,00:01)：水位线 00:02:00 越过 00:01:00+30
    assert put_config(client, "d1", 60, 30).status_code == 200
    ingest(client, "d1", "e1", "2026-09-22T00:00:10Z", value=1.0)
    ingest(client, "d1", "e2", "2026-09-22T00:02:00Z", value=2.0)
    assert [w["status"] for w in windows(client, "d1")] == ["sealed", "open"]

    # 调长到 3600s：按新期限 00:01:00+3600 > 00:02:00，水位线尚未越线，
    # 但旧窗口仍为 sealed —— 落入其中的补报必须按 late 修正而非 accepted
    assert put_config(client, "d1", 60, 3600).status_code == 200

    body = ingest(client, "d1", "e3", "2026-09-22T00:00:40Z", value=5.0).json()
    assert (body["accepted"], body["late"]) == (0, 1)
    assert body["results"][0]["status"] == "late"
    assert body["results"][0]["window_start"] == "2026-09-22T00:00:00Z"

    w = windows(client, "d1")[0]
    assert w["status"] == "sealed"
    assert (w["count"], w["sum"]) == (2, 6.0)
    assert (w["correction_count"], w["late_event_count"]) == (1, 1)

    corrections = client.get("/devices/d1/corrections").json()
    assert [c["event_id"] for c in corrections] == ["e3"]

    # 同一 event_id 重试仍幂等去重，不重复累计修正
    again = ingest(client, "d1", "e3", "2026-09-22T00:00:40Z", value=5.0).json()
    assert (again["accepted"], again["late"], again["duplicates"]) == (0, 0, 1)
    w = windows(client, "d1")[0]
    assert (w["count"], w["correction_count"], w["late_event_count"]) == (2, 1, 1)


def test_widen_lateness_preview_matches_commit_for_still_sealed_window(client):
    assert put_config(client, "d1", 60, 30).status_code == 200
    ingest(client, "d1", "e1", "2026-09-22T00:00:10Z", value=1.0)
    ingest(client, "d1", "e2", "2026-09-22T00:02:00Z", value=2.0)
    assert put_config(client, "d1", 60, 3600).status_code == 200

    batch = [
        {"event_id": "e3", "event_time": "2026-09-22T00:00:40Z", "value": 5.0},  # sealed 窗口 => late
        {"event_id": "e4", "event_time": "2026-09-22T00:02:10Z", "value": 3.0},  # open 窗口 => accepted
    ]
    prev = client.post("/ingest/preview", json={"device_id": "d1", "events": batch})
    assert prev.status_code == 200
    pbody = prev.json()
    assert [(x["event_id"], x["status"]) for x in pbody["results"]] == [
        ("e3", "late"), ("e4", "accepted"),
    ]
    sealed = next(w for w in pbody["windows"] if w["window_start"] == "2026-09-22T00:00:00Z")
    assert (sealed["status"], sealed["count"], sealed["sum"], sealed["correction_count"]) == (
        "sealed", 2, 6.0, 1,
    )

    committed = client.post("/ingest", json={"device_id": "d1", "events": batch}).json()
    assert [(x["event_id"], x["status"], x["window_start"]) for x in committed["results"]] == [
        (x["event_id"], x["status"], x["window_start"]) for x in pbody["results"]
    ]
    assert (committed["accepted"], committed["late"]) == (pbody["accepted"], pbody["late"])

    w = windows(client, "d1")[0]
    assert (w["status"], w["count"], w["sum"], w["correction_count"]) == (
        "sealed", 2, 6.0, 1,
    )


def test_widen_lateness_still_uses_watermark_when_window_absent(client):
    # 尚无窗口时继续按水位线阈值判定：调长后旧事件时间位置无窗口 => accepted 新建
    assert put_config(client, "d1", 60, 30).status_code == 200
    ingest(client, "d1", "e2", "2026-09-22T00:05:00Z", value=2.0)  # 水位线 00:05:00
    assert put_config(client, "d1", 60, 3600).status_code == 200

    # 窗口 [00:02,00:03) 此前无任何事件（无记录）；按新期限 00:03:00+3600 > 水位线
    # => accepted 并新建 open 窗口，而不是仅因水位线较晚就记 late
    body = ingest(client, "d1", "e3", "2026-09-22T00:02:10Z", value=4.0).json()
    assert (body["accepted"], body["late"]) == (1, 0)
    by_start = {w["window_start"]: w for w in windows(client, "d1")}
    w = by_start["2026-09-22T00:02:00Z"]
    assert (w["status"], w["count"], w["sum"]) == ("open", 1, 4.0)
    assert (w["correction_count"], w["late_event_count"]) == (0, 0)


def test_config_update_on_device_without_data(client):
    # 无水位线/无窗口：缩短与调长都正常更新配置，不发生任何封存
    assert put_config(client, "d1", 60, 300).status_code == 200
    assert put_config(client, "d1", 60, 30).status_code == 200
    assert put_config(client, "d1", 60, 0).status_code == 200
    assert windows(client, "d1") == []
    info = client.get("/devices/d1").json()
    assert info["allowed_lateness_seconds"] == 0
    assert info["watermark"] is None
