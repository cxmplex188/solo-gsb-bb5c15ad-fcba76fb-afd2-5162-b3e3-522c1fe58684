"""调长 allowed_lateness_seconds 后已封存窗口的事件归类回归测试。

缺陷：旧 /ingest 仅按"当前水位线 + 当前（调长后的）期限"判断迟到。窗口在旧期限下
已封存后调长 allowed_lateness_seconds，新期限尚未到达，落入该 sealed 窗口的新事件
被错误记为 accepted，漏掉迟到修正（不更新 correction_count/late_event_count、不追加
corrections 记录、组窗口不升版）。

修复口径（去重后逐事件判定，满足其一即 late）：
- 目标窗口行已为 sealed：一律按 late 入库（封存不可逆，即使按新期限尚未越线），
  更新窗口聚合与 late_event_count/correction_count，追加修正记录；
- 窗口尚无行或仍 open：继续按水位线阈值 watermark >= window_end + lateness 判定。

POST /ingest/preview 复用同一份逐事件处理逻辑，与正式提交结果一致（且不留写入）。
"""


def put_config(client, device_id, size, lateness):
    return client.put(
        f"/devices/{device_id}/config",
        json={"window_size_seconds": size, "allowed_lateness_seconds": lateness},
    )


def ingest(client, device_id, events):
    return client.post("/ingest", json={"device_id": device_id, "events": events})


def preview(client, device_id, events):
    return client.post("/ingest/preview", json={"device_id": device_id, "events": events})


def get_window(client, device_id, start):
    return next(
        w for w in client.get(f"/devices/{device_id}/windows").json()
        if w["window_start"] == start
    )


def _seal_first_window(client, device_id="d1"):
    # lateness=30：00:02:00 的水位线封存 [00:00,00:01)
    assert put_config(client, device_id, 60, 30).status_code == 200
    ingest(client, device_id, [
        {"event_id": "e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
    ])
    ingest(client, device_id, [
        {"event_id": "e2", "event_time": "2026-09-22T00:02:00Z", "value": 2.0},
    ])
    w = get_window(client, device_id, "2026-09-22T00:00:00Z")
    assert w["status"] == "sealed"


def test_event_into_sealed_window_is_late_after_widening_lateness(client):
    _seal_first_window(client)

    # 调长迟到期限：sealed 窗口不重开；新期限（01:01:00）远未被水位线（00:02:00）越过
    assert put_config(client, "d1", 60, 3600).status_code == 200
    assert get_window(client, "d1", "2026-09-22T00:00:00Z")["status"] == "sealed"

    r = ingest(client, "d1", [
        {"event_id": "e3", "event_time": "2026-09-22T00:00:40Z", "value": 5.0},
    ])
    assert r.status_code == 200
    body = r.json()
    assert (body["accepted"], body["late"], body["duplicates"]) == (0, 1, 0)
    assert body["results"][0] == {
        "event_id": "e3",
        "status": "late",
        "window_start": "2026-09-22T00:00:00Z",
    }

    # 窗口聚合与迟到/修正计数均更新，状态保持 sealed
    w = get_window(client, "d1", "2026-09-22T00:00:00Z")
    assert w["status"] == "sealed"
    assert (w["count"], w["sum"], w["min"], w["max"]) == (2, 6.0, 1.0, 5.0)
    assert (w["correction_count"], w["late_event_count"]) == (1, 1)

    # 追加一条修正记录
    corrections = client.get("/devices/d1/corrections").json()
    assert len(corrections) == 1
    assert corrections[0]["event_id"] == "e3"
    assert corrections[0]["window_start"] == "2026-09-22T00:00:00Z"
    assert corrections[0]["event_time"] == "2026-09-22T00:00:40Z"
    assert corrections[0]["value"] == 5.0


def test_preview_matches_commit_for_sealed_window_after_widening(client):
    _seal_first_window(client)
    assert put_config(client, "d1", 60, 3600).status_code == 200

    batch = [
        {"event_id": "e3", "event_time": "2026-09-22T00:00:40Z", "value": 5.0},
        {"event_id": "e4", "event_time": "2026-09-22T00:00:50Z", "value": 7.0},
    ]

    pr = preview(client, "d1", batch)
    assert pr.status_code == 200
    pbody = pr.json()
    assert [(x["event_id"], x["status"]) for x in pbody["results"]] == [
        ("e3", "late"), ("e4", "late"),
    ]
    assert (pbody["accepted"], pbody["late"], pbody["duplicates"]) == (0, 2, 0)
    assert pbody["windows"] == [
        {
            "window_start": "2026-09-22T00:00:00Z",
            "window_end": "2026-09-22T00:01:00Z",
            "status": "sealed",
            "count": 3,
            "sum": 13.0,
            "correction_count": 2,
        },
    ]

    # 预演不落库
    w = get_window(client, "d1", "2026-09-22T00:00:00Z")
    assert (w["count"], w["correction_count"]) == (1, 0)
    assert client.get("/devices/d1/corrections").json() == []

    # 随即正式提交：逐事件结果与预演完全一致
    cr = ingest(client, "d1", batch)
    cbody = cr.json()
    assert cbody["accepted"] == pbody["accepted"]
    assert cbody["late"] == pbody["late"]
    assert cbody["duplicates"] == pbody["duplicates"]
    assert cbody["watermark"] == pbody["watermark"]
    assert [
        (x["event_id"], x["status"], x["window_start"]) for x in cbody["results"]
    ] == [
        (x["event_id"], x["status"], x["window_start"]) for x in pbody["results"]
    ]

    w = get_window(client, "d1", "2026-09-22T00:00:00Z")
    assert (w["status"], w["count"], w["sum"], w["correction_count"]) == (
        "sealed", 3, 13.0, 2,
    )
    assert {x["event_id"] for x in client.get("/devices/d1/corrections").json()} == {
        "e3", "e4",
    }


def test_duplicate_into_sealed_window_after_widening_is_not_recorrected(client):
    _seal_first_window(client)
    assert put_config(client, "d1", 60, 3600).status_code == 200

    # 新迟到事件产生一条修正
    ingest(client, "d1", [
        {"event_id": "e3", "event_time": "2026-09-22T00:00:40Z", "value": 5.0},
    ])
    # 重试同一 event_id：去重为 duplicate，不重复计数/修正（水位线与聚合均不变）
    r = ingest(client, "d1", [
        {"event_id": "e3", "event_time": "2026-09-22T00:00:40Z", "value": 5.0},
    ])
    assert r.json()["results"][0]["status"] == "duplicate"
    assert r.json()["results"][0]["window_start"] is None

    w = get_window(client, "d1", "2026-09-22T00:00:00Z")
    assert (w["count"], w["correction_count"], w["late_event_count"]) == (2, 1, 1)
    assert len(client.get("/devices/d1/corrections").json()) == 1


def test_missing_window_still_uses_watermark_threshold_after_widening(client):
    # 尚无窗口时继续按水位线阈值判定：调长期限后，新空窗即便时间很旧也不按
    # "已封存"处理（窗口行不存在，永远不会是 sealed）。
    _seal_first_window(client)
    assert put_config(client, "d1", 60, 3600).status_code == 200

    # [00:01,00:02) 此前无任何事件（无窗口行）；水位线 00:02:00，新期限 3600s
    # 远未越线 => accepted，并新建 open 窗口
    r = ingest(client, "d1", [
        {"event_id": "e3", "event_time": "2026-09-22T00:01:30Z", "value": 9.0},
    ])
    assert r.json()["results"][0]["status"] == "accepted"
    w = get_window(client, "d1", "2026-09-22T00:01:00Z")
    assert w["status"] == "open"
    assert (w["count"], w["correction_count"], w["late_event_count"]) == (1, 0, 0)

    # 反过来：窗口仍 open 但批内水位线已越过 window_end + lateness（缩短期限场景），
    # 沿用阈值口径判 late。
    assert put_config(client, "d1", 60, 0).status_code == 200
    # [00:04,00:05) 先随水位线 00:05:00 建好并封存其之前窗口；旧空窗 [00:03,00:04)
    # 此时被水位线越过（00:05:00 >= 00:04:00 + 0），首个事件按 late 建窗。
    r = ingest(client, "d1", [
        {"event_id": "e4", "event_time": "2026-09-22T00:05:00Z", "value": 1.0},
        {"event_id": "e5", "event_time": "2026-09-22T00:03:20Z", "value": 4.0},
    ])
    statuses = {x["event_id"]: x["status"] for x in r.json()["results"]}
    assert statuses == {"e4": "accepted", "e5": "late"}
    w = get_window(client, "d1", "2026-09-22T00:03:00Z")
    assert w["status"] == "sealed"
    assert (w["count"], w["late_event_count"], w["correction_count"]) == (1, 1, 1)


def test_ready_group_window_is_revised_on_late_event_after_seal(client):
    # 组场景：两成员窗口均封存、组窗口 ready 后，未入组口径不适用（配置锁定），
    # 但成员窗口在 ready 后收到任何 late 修正（含窗口早为 sealed 的情形）都应令
    # 已 ready 的组汇总按现有修订机制原位升版并追加新 revision 快照。
    for did in ("d1", "d2"):
        put_config(client, did, 60, 30)
        ingest(client, did, [
            {"event_id": f"{did}-e1", "event_time": "2026-09-22T00:00:10Z", "value": 1.0},
        ])
        ingest(client, did, [
            {"event_id": f"{did}-e2", "event_time": "2026-09-22T00:02:00Z", "value": 2.0},
        ])
    assert client.post("/groups", json={
        "group_id": "g1", "member_ids": ["d1", "d2"], "aggregation": "sum",
        "window_size_seconds": 60, "allowed_lateness_seconds": 30,
    }).status_code == 201

    gw = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert gw["status"] == "ready" and gw["revision"] == 1 and gw["value"] == 2.0

    # d1 的 sealed 窗口再收一条迟到事件（水位线未变，窗口仍 sealed）=> revision 2
    r = ingest(client, "d1", [
        {"event_id": "d1-e3", "event_time": "2026-09-22T00:00:40Z", "value": 5.0},
    ])
    assert r.json()["results"][0]["status"] == "late"

    gw = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert gw["status"] == "ready" and gw["revision"] == 2 and gw["value"] == 7.0

    revisions = client.get(
        "/groups/g1/windows/2026-09-22T00:00:00Z/revisions"
    ).json()
    assert [snap["revision"] for snap in revisions] == [1, 2]

    # 幂等重试不重复升版
    ingest(client, "d1", [
        {"event_id": "d1-e3", "event_time": "2026-09-22T00:00:40Z", "value": 5.0},
    ])
    gw = client.get("/groups/g1/windows/2026-09-22T00:00:00Z").json()
    assert gw["revision"] == 2
