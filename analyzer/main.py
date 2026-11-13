"""本地 HTTP 分析库（下游）：接收上游同步页投递的快照与增量变更。

故障注入开关（仅用于演示/验收）：
- accepting=0：停收，所有投递返回 503，不写入
- lose_receipt=1：保存成功但不返回 200（模拟保存后丢回执），重投走幂等去重
"""

from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from . import db

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="物种分布分析库")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@app.on_event("startup")
def startup():
    db.init_db()


def _redirect(url: str, msg: str = "") -> RedirectResponse:
    if msg:
        from urllib.parse import quote

        url = f"{url}?msg={quote(msg)}"
    return RedirectResponse(url, status_code=303)


# ---------------- 同步接收接口 ----------------


@app.post("/sync/snapshot")
def sync_snapshot(body: dict):
    state = db.get_state()
    if state.get("accepting") != "1":
        return JSONResponse({"error": "分析库已停收，投递被拒绝"}, status_code=503)
    delivery_id = body.get("delivery_id")
    payload = body.get("data") or {}
    if not delivery_id or "records" not in payload:
        return JSONResponse({"error": "投递格式错误：缺少 delivery_id 或 records"}, status_code=400)
    try:
        result = db.apply_snapshot(delivery_id, payload)
    except Exception as e:
        return JSONResponse({"error": f"应用快照失败（已回滚）：{e}"}, status_code=400)

    if state.get("lose_receipt") == "1":
        # 数据已落盘，故意不返回成功回执，逼上游重投以验证幂等
        return JSONResponse({"error": "模拟故障：已保存但回执丢失"}, status_code=500)
    return JSONResponse({"ok": True, **result})


@app.post("/sync/changes")
def sync_changes(body: dict):
    state = db.get_state()
    if state.get("accepting") != "1":
        return JSONResponse({"error": "分析库已停收，投递被拒绝"}, status_code=503)
    delivery_id = body.get("delivery_id")
    changes = body.get("data")
    if not delivery_id or not isinstance(changes, list):
        return JSONResponse({"error": "投递格式错误：缺少 delivery_id 或变更列表"}, status_code=400)
    try:
        result = db.apply_changes(delivery_id, changes)
    except db.GapError as e:
        return JSONResponse({"error": str(e)}, status_code=409)
    except Exception as e:
        return JSONResponse({"error": f"应用变更失败（整批未生效）：{e}"}, status_code=400)

    if state.get("lose_receipt") == "1":
        return JSONResponse({"error": "模拟故障：已保存但回执丢失"}, status_code=500)
    return JSONResponse({"ok": True, **result})


# ---------------- 开关与页面 ----------------


@app.post("/admin/accepting")
def set_accepting(value: str = Form("1")):
    on = value in ("1", "true", "on")
    db.set_state("accepting", "1" if on else "0")
    state = db.get_state()
    return _redirect("/", f"分析库已{'恢复接收' if state['accepting'] == '1' else '停收'}")


@app.post("/admin/lose_receipt")
def set_lose_receipt(value: str = Form("0")):
    on = value in ("1", "true", "on")
    db.set_state("lose_receipt", "1" if on else "0")
    state = db.get_state()
    return _redirect("/", f"保存后丢回执模拟已{'开启' if state['lose_receipt'] == '1' else '关闭'}")

@app.get("/", response_class=HTMLResponse)
def index(request: Request, msg: str = ""):
    state = db.get_state()
    records = db.get_records()
    deliveries = db.list_deliveries()
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "request": request,
            "state": state,
            "records": records,
            "deliveries": deliveries,
            "max_version": db.max_version(),
            "msg": msg,
        },
    )


@app.get("/health")
def health():
    state = db.get_state()
    return {"ok": True, "accepting": state.get("accepting") == "1"}


@app.get("/api/state")
def api_state():
    return db.get_state()


@app.get("/api/records")
def api_records():
    return db.get_records()


@app.get("/api/deliveries")
def api_deliveries():
    return db.list_deliveries()
