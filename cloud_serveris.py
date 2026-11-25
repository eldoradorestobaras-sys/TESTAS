# -*- coding: utf-8 -*-
"""
Model MI — Nemokamas Cloud Kasos Simuliatorius + Kaupiklis (Render.com / 0 EUR)
Palaiko:
  - GET  /               : Mobilioji Touch + PLU kasa (veikia per 4G/5G/Wi-Fi net kai PC išjungtas)
  - POST /api/uzsakymas  : Priima užsakymus ir grąžinimus iš kasos UI, kaupia ir siunčia į SSE
  - GET  /api/uzsakymai  : HTTP Pull kaupiklis Model MI programai (?nuo=YYYY-MM-DD HH:MM:SS)
  - GET  /api/stream     : Realaus laiko SSE srautas Model MI programai (text/event-stream)
  - GET  /health         : Serverio būsena
"""

import hashlib
import json
import os
import queue
import threading
import urllib.parse
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List

PORT = int(os.environ.get("PORT", os.environ.get("CLOUD_PORT", "10000")))
DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cloud_data.json")
API_KEY_REQUIRED = os.environ.get("CLOUD_API_KEY", "").strip()

_LOCK = threading.Lock()
_SSE_CLIENTS: List[queue.Queue] = []

DEFAULT_MENU = [
    {"plu": "101", "kat": "Virtuvė", "pavadinimas": "Jautienos mėsainis su čederio sūriu", "kaina": 14.00, "ingredientai": ["Jautienos kumpis", "Mėsainio bandelė", "Čederio sūris", "Pomidorai", "Svogūnai"]},
    {"plu": "102", "kat": "Virtuvė", "pavadinimas": "Traškūs vištienos kepsneliai (paniruoti)", "kaina": 9.00, "ingredientai": ["Vištienos krūtinėlė", "Kiaušiniai", "Kvietiniai miltai", "Džiūvėsėliai"]},
    {"plu": "103", "kat": "Virtuvė", "pavadinimas": "Vištienos kepsnys su bulvių koše", "kaina": 12.00, "ingredientai": ["Vištienos krūtinėlė", "Bulvės", "Sviestas", "Pienas"]},
    {"plu": "104", "kat": "Virtuvė", "pavadinimas": "Cezario salotos su vištiena ir kiaušiniu", "kaina": 9.00, "ingredientai": ["Vištienos krūtinėlė", "Romaninės salotos", "Kiaušiniai", "Parmezano sūris", "Cezario padažas"]},
    {"plu": "105", "kat": "Virtuvė", "pavadinimas": "Keptos tigrinės krevetės česnakiniame svieste", "kaina": 12.00, "ingredientai": ["Tigrinės krevetės", "Sviestas", "Česnakai", "Citrina"]},
    {"plu": "106", "kat": "Virtuvė", "pavadinimas": "Kiaulienos guliašas su morkomis", "kaina": 9.50, "ingredientai": ["Kiaulienos sprandinė", "Morkos", "Svogūnai", "Pomidorų pasta"]},
    {"plu": "201", "kat": "Iškraipyti (Mapping)", "pavadinimas": "Mėsainis čed. 1vnt", "kaina": 14.00, "ingredientai": []},
    {"plu": "202", "kat": "Iškraipyti (Mapping)", "pavadinimas": "Višt. kepsnel. panir.", "kaina": 9.00, "ingredientai": []},
    {"plu": "203", "kat": "Iškraipyti (Mapping)", "pavadinimas": "Cezario sal. višt.", "kaina": 9.00, "ingredientai": []},
    {"plu": "206", "kat": "Iškraipyti (Mapping)", "pavadinimas": "Nežinomas Patiekalas TEST", "kaina": 6.50, "ingredientai": []},
]


def _load_state() -> Dict:
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
                if isinstance(d, dict):
                    d.setdefault("orders", [])
                    d.setdefault("next_id", 1)
                    d.setdefault("menu", DEFAULT_MENU)
                    d.setdefault("sandelis", [])
                    d.setdefault("tinklas", {})
                    return d
        except Exception:
            pass
    return {"orders": [], "next_id": 1, "menu": list(DEFAULT_MENU), "sandelis": [], "tinklas": {}}


def _tinklas_key_hash(raw_key: str) -> str:
    return hashlib.sha256((raw_key or "").strip().encode("utf-8")).hexdigest()[:32]


def _save_state(state: Dict) -> None:
    try:
        tmp = DATA_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        os.replace(tmp, DATA_FILE)
    except Exception:
        pass


_STATE = _load_state()


def _svc_state() -> Dict:
    svc = _STATE.get("service")
    if not isinstance(svc, dict):
        svc = {
            "staff": [
                {"id": "w1", "vardas": "Laura", "pin": "1111", "role": "padavejas", "aktyvus": True},
                {"id": "w2", "vardas": "Tomas", "pin": "2222", "role": "padavejas", "aktyvus": True},
                {"id": "mgr", "vardas": "Vadybininkas", "pin": "0000", "role": "vadybininkas", "aktyvus": True},
            ],
            "work_hours": {"ijungta": True, "pradzia": "08:00", "pabaiga": "22:00"},
            "tables": [
                {
                    "id": f"t{i}", "nr": i, "pavadinimas": f"Stalas {i}",
                    "zona": "Salė" if i <= 7 else "Terasa",
                    "padavejas_id": "w1" if i <= 5 else "w2",
                    "atidare_id": "", "status": "laisvas", "sveciu_sk": 0, "opened_at": "", "checks": [],
                }
                for i in range(1, 11)
            ],
        }
        _STATE["service"] = svc
    return svc


def _svc_after_hours(wh: dict) -> tuple:
    if not wh or not wh.get("ijungta", True):
        return False, ""
    try:
        sh, sm = [int(x) for x in str(wh.get("pradzia", "08:00")).split(":")]
        eh, em = [int(x) for x in str(wh.get("pabaiga", "22:00")).split(":")]
        dt = datetime.now()
        cur = dt.hour * 60 + dt.minute
        s_m, e_m = sh * 60 + sm, eh * 60 + em
        inside = (s_m <= cur < e_m) if s_m <= e_m else (cur >= s_m or cur < e_m)
        if not inside:
            return True, f"Darbo laikas ({wh.get('pradzia')}–{wh.get('pabaiga')}) baigėsi. Galima tik apmokėti/uždaryti pradėtus stalus."
    except Exception:
        pass
    return False, ""


def _svc_user(svc: dict, pin: str):
    p = str(pin or "").strip()
    for s in svc.get("staff") or []:
        if s.get("aktyvus", True) and str(s.get("pin") or "").strip() == p:
            return s
    return None


def _svc_serialize_table(t: dict, staff_map: dict) -> dict:
    checks_out = []
    tot, unp, dcnt = 0.0, 0.0, 0
    for chk in t.get("checks") or []:
        ds, ss, dc, sc = 0.0, 0.0, 0, 0
        for it in chk.get("items") or []:
            if it.get("status") == "voided":
                continue
            ls = float(it.get("kiekis") or 0) * float(it.get("kaina") or 0)
            if it.get("status", "draft") == "draft":
                ds += ls; dc += 1
            else:
                ss += ls; sc += 1
        c_tot = round(ds + ss, 2)
        cp = dict(chk)
        cp["totals"] = {"draft_sum": round(ds, 2), "sent_sum": round(ss, 2), "total": c_tot, "draft_cnt": dc, "sent_cnt": sc}
        checks_out.append(cp)
        tot += c_tot
        if chk.get("status") != "apmoketas":
            unp += c_tot
            dcnt += dc
    return {
        "id": t["id"], "nr": t["nr"], "pavadinimas": t["pavadinimas"],
        "zona": t.get("zona", "Salė"), "padavejas_id": t.get("padavejas_id", ""),
        "padavejas_vardas": staff_map.get(t.get("padavejas_id", ""), "Nepriskirtas"),
        "atidare_id": t.get("atidare_id", ""),
        "status": t.get("status", "laisvas"), "sveciu_sk": int(t.get("sveciu_sk") or 0),
        "opened_at": t.get("opened_at", ""), "total_sum": round(tot, 2),
        "unpaid_sum": round(unp, 2), "draft_cnt": dcnt, "checks": checks_out,
    }


def _broadcast_sse(ev_id: int, payload: dict) -> None:
    msg = f"id: {ev_id}\nevent: uzsakymas\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")
    dead = []
    with _LOCK:
        for q in _SSE_CLIENTS:
            try:
                q.put_nowait(msg)
            except Exception:
                dead.append(q)
        for q in dead:
            if q in _SSE_CLIENTS:
                _SSE_CLIENTS.remove(q)


CLOUD_HTML = r"""<!DOCTYPE html>
<html lang="lt">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>Model MI — Cloud Kasa (0 €)</title>
<style>
  :root {
    --bg:#0B0E14; --panel:#151A24; --card:#1C2331; --border:#2A3346;
    --amber:#FFB300; --blue:#38BDF8; --green:#22C55E; --red:#EF4444;
    --text:#F8FAFC; --muted:#94A3B8;
  }
  * { box-sizing:border-box; -webkit-tap-highlight-color:transparent; touch-action:manipulation; }
  body { margin:0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; background:var(--bg); color:var(--text); min-height:100vh; display:flex; flex-direction:column; }
  header { background:#11151F; border-bottom:1px solid var(--border); padding:10px 14px; display:flex; align-items:center; justify-content:space-between; gap:8px; position:sticky; top:0; z-index:30; }
  .brand { font-weight:800; font-size:15px; color:var(--amber); }
  .pill { font-size:12px; font-weight:700; padding:4px 10px; border-radius:999px; background:rgba(34,197,94,0.14); color:var(--green); border:1px solid rgba(34,197,94,0.45); }
  .tabs { display:flex; gap:6px; padding:8px 12px; background:#10141D; border-bottom:1px solid var(--border); }
  .tab-btn { flex:1; padding:10px; border-radius:10px; border:1px solid var(--border); background:var(--panel); color:var(--muted); font-weight:700; font-size:13px; cursor:pointer; }
  .tab-btn.active { background:rgba(255,179,0,0.16); color:var(--amber); border-color:var(--amber); }
  .main-wrap { flex:1; display:grid; grid-template-columns:1.35fr 1fr; gap:10px; padding:10px; max-width:1280px; width:100%; margin:0 auto; }
  @media (max-width: 820px) { .main-wrap { grid-template-columns:1fr; } }
  .panel { background:var(--panel); border:1px solid var(--border); border-radius:14px; padding:12px; display:flex; flex-direction:column; gap:10px; }
  .row { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
  input, select { background:#0F141D; color:var(--text); border:1px solid var(--border); border-radius:8px; padding:9px 10px; font-size:13px; outline:none; }
  .btn { padding:9px 12px; border-radius:9px; border:1px solid var(--border); background:var(--card); color:var(--text); font-weight:700; font-size:13px; cursor:pointer; }
  .btn-amber { background:var(--amber); color:#111827; border-color:var(--amber); }
  .btn-green { background:var(--green); color:#052E16; border-color:var(--green); }
  .btn-red { background:rgba(239,68,68,0.18); color:#FCA5A5; border-color:rgba(239,68,68,0.5); }
  .btn-blue { background:rgba(56,189,248,0.18); color:var(--blue); border-color:rgba(56,189,248,0.5); }
  .cat-bar { display:flex; gap:6px; overflow-x:auto; padding-bottom:4px; }
  .cat-chip { padding:7px 12px; border-radius:999px; border:1px solid var(--border); background:var(--card); color:var(--muted); font-size:12px; font-weight:700; white-space:nowrap; cursor:pointer; }
  .cat-chip.active { background:rgba(56,189,248,0.18); color:var(--blue); border-color:var(--blue); }
  .tiles { display:grid; grid-template-columns:repeat(auto-fill, minmax(135px, 1fr)); gap:8px; }
  .tile { background:var(--card); border:1px solid var(--border); border-radius:12px; padding:12px 10px; text-align:left; cursor:pointer; display:flex; flex-direction:column; justify-content:space-between; min-height:88px; }
  .tile .plu { font-size:11px; color:var(--blue); font-weight:800; }
  .tile .name { font-size:13px; font-weight:700; color:var(--text); margin:4px 0; line-height:1.25; }
  .tile .price { font-size:14px; font-weight:800; color:var(--amber); }
  .plu-screen { background:#0B0E14; border:1px solid var(--border); border-radius:12px; padding:12px; text-align:right; }
  .plu-big { font-size:28px; font-weight:900; color:var(--amber); font-family:monospace; }
  .keypad { display:grid; grid-template-columns:repeat(4, 1fr); gap:8px; }
  .key { padding:16px 8px; border-radius:12px; border:1px solid var(--border); background:var(--card); color:var(--text); font-size:20px; font-weight:800; cursor:pointer; }
  .key-enter { background:var(--green); color:#052E16; border-color:var(--green); }
  .key-clr { background:rgba(239,68,68,0.2); color:#FCA5A5; }
  .cart-row { background:#111622; border:1px solid var(--border); border-radius:10px; padding:8px 10px; display:flex; flex-direction:column; gap:6px; }
  .qty-btn { width:32px; height:32px; border-radius:8px; border:1px solid var(--border); background:#121722; color:var(--text); font-weight:800; font-size:16px; cursor:pointer; }
  .receipt-row { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:10px; display:flex; flex-direction:column; gap:6px; }
  .toast { position:fixed; bottom:16px; left:50%; transform:translateX(-50%); background:#1E293B; color:#fff; border:1px solid var(--amber); padding:10px 18px; border-radius:12px; font-weight:700; font-size:13px; z-index:99; display:none; }
  .modal-bg { position:fixed; inset:0; background:rgba(0,0,0,0.72); z-index:90; display:none; align-items:center; justify-content:center; padding:12px; }
  .modal-card { background:#151A24; border:1px solid var(--border); border-radius:16px; width:100%; max-width:460px; max-height:90vh; overflow-y:auto; padding:16px; display:flex; flex-direction:column; gap:10px; }
  .ing-chip { padding:6px 10px; border-radius:8px; border:1px solid var(--border); background:var(--card); color:var(--text); font-size:12px; cursor:pointer; }
  .ing-chip.on { background:rgba(239,68,68,0.22); border-color:var(--red); color:#FCA5A5; text-decoration:line-through; }
</style>
</head>
<body>
<header>
  <div class="brand">☁️ Model MI Cloud Kasa <span style="font-size:11px;color:#94A3B8;">(0 €)</span></div>
  <div class="row">
    <div class="pill">🟢 Cloud Online</div>
    <button class="btn" style="padding:6px 10px;" onclick="document.getElementById('infoBox').style.display=document.getElementById('infoBox').style.display==='none'?'block':'none'">ℹ️</button>
  </div>
</header>

<div id="infoBox" style="display:none;background:#131823;border-bottom:1px solid var(--border);padding:12px 14px;font-size:12.5px;color:#CBD5E1;">
  <b>Model MI nustatymams (Kasos jungties langas):</b><br>
  • <b>SSE adresas:</b> <code id="sseUrl" style="color:#38BDF8;"></code><br>
  • <b>HTTP Pull adresas:</b> <code id="pullUrl" style="color:#FFB300;"></code>
</div>

<div class="tabs">
  <button id="tabTouch" class="tab-btn active" onclick="setMode('touch')">📱 Touch Kasa</button>
  <button id="tabButtons" class="tab-btn" onclick="setMode('buttons')">🔢 PLU</button>
  <button id="tabReceipts" class="tab-btn" onclick="setMode('receipts')">🧾 Čekiai (<span id="recCnt">0</span>)</button>
</div>

<main class="main-wrap">
  <section id="leftTouch" class="panel">
    <div id="catBar" class="cat-bar"></div>
    <div id="tilesGrid" class="tiles"></div>
  </section>

  <section id="leftButtons" class="panel" style="display:none;">
    <div class="plu-screen"><div class="plu-big" id="pluDisplay">PLU: _</div></div>
    <div class="keypad">
      <button class="key" onclick="kp('7')">7</button><button class="key" onclick="kp('8')">8</button><button class="key" onclick="kp('9')">9</button><button class="key key-clr" onclick="kp('C')">CLR</button>
      <button class="key" onclick="kp('4')">4</button><button class="key" onclick="kp('5')">5</button><button class="key" onclick="kp('6')">6</button><button class="key" onclick="kp('*')">×</button>
      <button class="key" onclick="kp('1')">1</button><button class="key" onclick="kp('2')">2</button><button class="key" onclick="kp('3')">3</button><button class="key" onclick="kp('BS')">⌫</button>
      <button class="key" onclick="kp('0')">0</button><button class="key" onclick="kp('00')">00</button><button class="key key-enter" style="grid-column:span 2;" onclick="kpEnter()">PLU ↵</button>
    </div>
  </section>

  <section id="leftReceipts" class="panel" style="display:none;">
    <div class="row" style="justify-content:space-between;">
      <b style="color:var(--amber);">Cloud čekiai</b>
      <button class="btn" style="padding:5px 10px;font-size:12px;" onclick="receipts=receipts.filter(r=>r.status==='Aktyvus');localStorage.setItem('mi_cloud_rec',JSON.stringify(receipts));renderReceipts()">🧹 Neaktyvius</button>
    </div>
    <div id="receiptsList" style="display:flex;flex-direction:column;gap:8px;"></div>
  </section>

  <aside class="panel">
    <div class="row" style="justify-content:space-between;">
      <b>🧾 Aktyvus čekis</b>
      <button class="btn" style="padding:5px 10px;font-size:12px;" onclick="cart=[];renderCart()">Išvalyti</button>
    </div>
    <div id="cartItems" style="display:flex;flex-direction:column;gap:6px;max-height:46vh;overflow-y:auto;"></div>
    <input id="inpNote" placeholder="Pastaba virtuvei (pvz. Be svogūnų)">
    <div class="row" style="gap:6px;">
      <input id="inpOrderPromo" placeholder="🏷️ Akcija čekiui" style="flex:2;">
      <input id="inpOrderDisc" type="number" min="0" max="100" step="5" placeholder="Nuol. %" style="flex:1;min-width:80px;" oninput="renderCart()">
    </div>
    <div class="row" style="justify-content:space-between;padding-top:8px;border-top:1px solid var(--border);">
      <span style="color:var(--muted);font-weight:700;">SUMA:</span>
      <span id="cartTotal" style="font-size:22px;font-weight:900;color:var(--amber);">0.00 €</span>
    </div>
    <div class="row">
      <button class="btn btn-green" style="flex:1;padding:14px;font-size:15px;" onclick="sendOrder('kortele')">💳 KORTELE</button>
      <button class="btn btn-amber" style="flex:1;padding:14px;font-size:15px;" onclick="sendOrder('grynais')">💵 GRYNAIS</button>
    </div>
  </aside>
</main>

<div id="modModal" class="modal-bg" onclick="if(event.target===this)this.style.display='none'">
  <div class="modal-card">
    <div class="row" style="justify-content:space-between;"><b id="modTitle" style="color:var(--amber);">Modifikuoti</b><button class="btn" onclick="document.getElementById('modModal').style.display='none'">✕</button></div>
    <div style="font-size:12px;color:var(--muted);">🚫 Nedėti:</div>
    <div id="modIngChips" class="row"></div>
    <div class="row"><input id="inpCustomNo" placeholder="Laisvai ko nedėti..." style="flex:1;"><button class="btn" onclick="addCustomNo()">+ Nedėti</button></div>
    <div style="font-size:12px;color:var(--muted);">🏷️ Nuolaida %:</div>
    <div class="row"><input id="inpItemDisc" type="number" min="0" max="100" step="5" style="width:100px;" placeholder="%"><input id="inpItemPromo" placeholder="Akcijos pav." style="flex:1;"></div>
    <button class="btn btn-amber" onclick="saveMod()">✅ Išsaugoti</button>
  </div>
</div>

<div id="toast" class="toast"></div>

<script>
const menu = __MENU_JSON__;
let activeCat = "Visi";
let cart = [];
let receipts = JSON.parse(localStorage.getItem("mi_cloud_rec") || "[]");
let pluBuf = "";
let seq = Number(localStorage.getItem("mi_cloud_seq") || "5001");
let modIdx = -1;
let modNo = [];

document.getElementById("sseUrl").textContent = location.origin + "/api/stream";
document.getElementById("pullUrl").textContent = location.origin + "/api/uzsakymai?nuo={since}";

function toast(m, err=false) {
  const t = document.getElementById("toast");
  t.textContent = m; t.style.borderColor = err ? "#EF4444" : "#FFB300"; t.style.display = "block";
  clearTimeout(window._tt); window._tt = setTimeout(() => t.style.display = "none", 3000);
}
function setMode(m) {
  ["Touch","Buttons","Receipts"].forEach(x => {
    document.getElementById("left"+x).style.display = (m === x.toLowerCase()) ? "flex" : "none";
    document.getElementById("tab"+x).classList.toggle("active", m === x.toLowerCase());
  });
  if (m === "receipts") renderReceipts();
}
function renderMenu() {
  const cats = ["Visi", ...new Set(menu.map(x => x.kat || "Kita"))];
  document.getElementById("catBar").innerHTML = cats.map(c => `<button class="cat-chip ${c===activeCat?'active':''}" onclick='activeCat=${JSON.stringify(c)};renderMenu()'>${c}</button>`).join("");
  const list = activeCat === "Visi" ? menu : menu.filter(x => (x.kat || "Kita") === activeCat);
  document.getElementById("tilesGrid").innerHTML = list.map(it => {
    const idx = menu.indexOf(it);
    return `<button class="tile" onclick="addToCart(menu[${idx}])"><div class="plu">PLU ${it.plu}</div><div class="name">${it.pavadinimas}</div><div class="price">${Number(it.kaina).toFixed(2)} €</div></button>`;
  }).join("");
}
function addToCart(it, qty=1) {
  const ex = cart.find(c => c.pavadinimas === it.pavadinimas && !c.neideti.length && !c.nuolaida_proc);
  if (ex) ex.kiekis += qty;
  else cart.push({plu:it.plu, pavadinimas:it.pavadinimas, kiekis:qty, kaina:Number(it.kaina), ingredientai:it.ingredientai||[], neideti:[], nuolaida_proc:0, akcija:""});
  renderCart();
}
function calcUnit(c) {
  const d = Number(c.nuolaida_proc || 0) || Number(document.getElementById("inpOrderDisc").value || 0);
  return d > 0 ? Math.max(0, c.kaina * (1 - d / 100)) : c.kaina;
}
function renderCart() {
  const box = document.getElementById("cartItems");
  if (!cart.length) { box.innerHTML = `<div style="color:#64748B;text-align:center;padding:18px;">Čekis tuščias</div>`; document.getElementById("cartTotal").textContent = "0.00 €"; return; }
  let sum = 0;
  box.innerHTML = cart.map((c, i) => {
    const u = calcUnit(c); sum += u * c.kiekis;
    return `<div class="cart-row"><div class="row" style="justify-content:space-between;"><div><b>${c.pavadinimas}</b><div style="font-size:12px;color:#94A3B8;">${u.toFixed(2)} € × ${c.kiekis}</div></div><div class="row"><button class="qty-btn" onclick="cart[${i}].kiekis--;if(cart[${i}].kiekis<=0)cart.splice(${i},1);renderCart()">-</button><b>${c.kiekis}</b><button class="qty-btn" onclick="cart[${i}].kiekis++;renderCart()">+</button></div></div><button class="btn" style="padding:4px 8px;font-size:11.5px;" onclick="openMod(${i})">⚙️ Modifikuoti / Akcija</button></div>`;
  }).join("");
  document.getElementById("cartTotal").textContent = sum.toFixed(2) + " €";
}
function openMod(i) {
  modIdx = i; const c = cart[i]; modNo = [...c.neideti];
  document.getElementById("modTitle").textContent = c.pavadinimas;
  document.getElementById("inpItemDisc").value = c.nuolaida_proc || "";
  document.getElementById("inpItemPromo").value = c.akcija || "";
  renderModChips(); document.getElementById("modModal").style.display = "flex";
}
function renderModChips() {
  const all = [...new Set([...(cart[modIdx].ingredientai || []), ...modNo])];
  document.getElementById("modIngChips").innerHTML = all.map(n => `<button class="ing-chip ${modNo.includes(n)?'on':''}" onclick='const idx=modNo.indexOf(${JSON.stringify(n)});if(idx>=0)modNo.splice(idx,1);else modNo.push(${JSON.stringify(n)});renderModChips()'>${n}</button>`).join("");
}
function addCustomNo() { const v = document.getElementById("inpCustomNo").value.trim(); if (v && !modNo.includes(v)) modNo.push(v); document.getElementById("inpCustomNo").value=""; renderModChips(); }
function saveMod() {
  const c = cart[modIdx]; c.neideti = modNo;
  c.nuolaida_proc = Math.max(0, Math.min(100, Number(document.getElementById("inpItemDisc").value || 0)));
  c.akcija = document.getElementById("inpItemPromo").value.trim();
  document.getElementById("modModal").style.display = "none"; renderCart();
}
function kp(ch) { if(ch==='C')pluBuf=''; else if(ch==='BS')pluBuf=pluBuf.slice(0,-1); else pluBuf+=ch; document.getElementById("pluDisplay").textContent="PLU: "+(pluBuf||"_"); }
function kpEnter() {
  const p = pluBuf.split("*"); const code = p.length>1?p[1]:p[0]; const qty = p.length>1?Math.max(1,parseInt(p[0]||"1")):1;
  const f = menu.find(m => m.plu === code); if (!f) { toast("PLU nerastas: "+code, true); return; }
  addToCart(f, qty); pluBuf=""; document.getElementById("pluDisplay").textContent="PLU: _";
}
async function sendOrder(pay) {
  if (!cart.length) return;
  const cekis = seq++; localStorage.setItem("mi_cloud_seq", String(seq));
  const nowStr = new Date(Date.now() - new Date().getTimezoneOffset() * 60000).toISOString().slice(0, 19).replace("T", " ");
  const note = document.getElementById("inpNote").value.trim();
  const op = document.getElementById("inpOrderPromo").value.trim();
  const od = Number(document.getElementById("inpOrderDisc").value || 0);
  const payload = {
    cekio_nr: cekis, data: nowStr.slice(0, 10), laikas: nowStr.slice(11, 19), timestamp: nowStr,
    mokejimo_budas: pay, nuolaida_proc: od, akcija: op,
    elementai: cart.map(c => ({
      kodas: c.plu, pavadinimas: c.pavadinimas, kiekis: c.kiekis, kaina: calcUnit(c), pradine_kaina: c.kaina,
      komentaras: note, modifikacijos: c.neideti.map(x => ({pavadinimas: "Be " + x, tipas: "isimti"})),
      nuolaida_proc: c.nuolaida_proc || od, akcija: c.akcija || op
    }))
  };
  const res = await fetch("/api/uzsakymas", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(payload)});
  const js = await res.json();
  if (res.ok && js.sekme) {
    const total = cart.reduce((a,b) => a + b.kiekis * calcUnit(b), 0);
    receipts.unshift({cekis, data: nowStr, total, items:[...cart], status:"Aktyvus"});
    receipts = receipts.slice(0, 30); localStorage.setItem("mi_cloud_rec", JSON.stringify(receipts));
    cart = []; document.getElementById("inpNote").value=""; renderCart(); renderReceipts();
    toast(`☁️ Čekis #${cekis} įrašytas į Cloud!`);
  } else toast("Klaida: "+JSON.stringify(js), true);
}
async function refundRec(i) {
  const r = receipts[i];
  const d = new Date();
  const pad = n => String(n).padStart(2, "0");
  const dateStr = `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
  const timeStr = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  const res = await fetch("/api/uzsakymas", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({veiksmas:"grazinti", cekio_nr:r.cekis, priezastis:"Cloud kasos grąžinimas", data:dateStr, laikas:timeStr, timestamp:`${dateStr} ${timeStr}`})});
  if (res.ok) { r.status = "Grąžinta"; localStorage.setItem("mi_cloud_rec", JSON.stringify(receipts)); renderReceipts(); toast(`↩️ #${r.cekis} grąžinta!`); }
}
function renderReceipts() {
  document.getElementById("recCnt").textContent = receipts.length;
  document.getElementById("receiptsList").innerHTML = receipts.map((r,i) => `
    <div class="receipt-row">
      <div class="row" style="justify-content:space-between;"><b>#${r.cekis} (${r.data})</b><b style="color:${r.status==='Aktyvus'?'#22C55E':'#F59E0B'}">${r.status} • ${r.total.toFixed(2)} €</b></div>
      <div style="font-size:12px;color:#CBD5E1;">${r.items.map(x=>x.kiekis+'× '+x.pavadinimas).join(', ')}</div>
      <button class="btn btn-amber" ${r.status!=='Aktyvus'?'disabled style="opacity:0.4"':''} onclick="refundRec(${i})">↩️ Grąžinti</button>
    </div>`).join("") || `<div style="color:#64748B;">Čekių nėra.</div>`;
}
renderMenu(); renderCart(); renderReceipts();
</script>
</body>
</html>
"""


class CloudHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _check_auth(self, parsed) -> bool:
        if not API_KEY_REQUIRED:
            return True
        hk = str(self.headers.get("X-API-Key") or "").strip()
        ak = str(self.headers.get("Authorization") or "").replace("Bearer ", "").strip()
        qk = (urllib.parse.parse_qs(parsed.query).get("key") or [""])[0].strip()
        return API_KEY_REQUIRED in (hk, ak, qk)

    def _send(self, code: int, body: bytes, ctype: str):
        try:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key, X-POS-Source")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError):
            pass

    def do_OPTIONS(self):
        self._send(200, b"", "text/plain")

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path in ("/health", "/api/status"):
            self._send(200, json.dumps({"status": "online", "servisas": "Model MI Cloud Kasa"}).encode("utf-8"), "application/json; charset=utf-8")
            return

        if path == "/api/stream":
            if not self._check_auth(parsed):
                self._send(401, b'{"klaida":"Unauthorized"}', "application/json")
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            q: queue.Queue = queue.Queue(maxsize=200)
            last_id_hdr = str(self.headers.get("Last-Event-ID") or "").strip()
            last_id = int(last_id_hdr) if last_id_hdr.isdigit() else None

            with _LOCK:
                _SSE_CLIENTS.append(q)
                if last_id is not None:
                    missed = [o for o in _STATE["orders"] if int(o.get("_ev_id", 0)) > last_id]
                    for o in missed:
                        ev_id = int(o.get("_ev_id", 0))
                        pay = {k: v for k, v in o.items() if k != "_ev_id"}
                        q.put_nowait(f"id: {ev_id}\nevent: uzsakymas\ndata: {json.dumps(pay, ensure_ascii=False)}\n\n".encode("utf-8"))

            try:
                self.wfile.write(b": connected\n\n")
                self.wfile.flush()
                while True:
                    try:
                        chunk = q.get(timeout=20)
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    except queue.Empty:
                        self.wfile.write(b"event: ping\ndata: {}\n\n")
                        self.wfile.flush()
            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError):
                pass
            finally:
                with _LOCK:
                    if q in _SSE_CLIENTS:
                        _SSE_CLIENTS.remove(q)
            return

        if path in ("/api/uzsakymai", "/api/orders"):
            if not self._check_auth(parsed):
                self._send(401, b'{"klaida":"Unauthorized"}', "application/json")
                return
            qs = urllib.parse.parse_qs(parsed.query)
            nuo = str((qs.get("nuo") or qs.get("since") or [""])[0]).strip()
            with _LOCK:
                orders = list(_STATE["orders"])
            if nuo and "{since}" not in nuo:
                filtered = [
                    {k: v for k, v in o.items() if k != "_ev_id"}
                    for o in orders
                    if str(o.get("timestamp") or o.get("data") or "") > nuo
                ]
            else:
                filtered = [{k: v for k, v in o.items() if k != "_ev_id"} for o in orders[-50:]]
            self._send(200, json.dumps({"uzsakymai": filtered}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
            return

        if path == "/app" or path.startswith("/app/"):
            rel = path[len("/app"):].lstrip("/") or "index.html"
            app_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "mobile_app", "www"))
            target = os.path.abspath(os.path.join(app_root, rel))
            if target.startswith(app_root) and os.path.isfile(target):
                ext = os.path.splitext(target)[1].lower()
                ctype = {
                    ".html": "text/html; charset=utf-8",
                    ".json": "application/json; charset=utf-8",
                    ".png": "image/png",
                    ".svg": "image/svg+xml",
                    ".js": "application/javascript; charset=utf-8",
                    ".css": "text/css; charset=utf-8",
                }.get(ext, "application/octet-stream")
                with open(target, "rb") as f:
                    data = f.read()
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(data)
                return
            self._send(404, b'{"klaida":"App failas nerastas"}', "application/json")
            return

        if path == "/api/meniu":
            qs = urllib.parse.parse_qs(parsed.query)
            src = str((qs.get("source") or [""])[0]).strip()
            meniu_out = None
            if src == "model_mi":
                db_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "model_mi.db"))
                if os.path.exists(db_path):
                    try:
                        import sys as _sys
                        parent_dir = os.path.dirname(db_path)
                        if parent_dir not in _sys.path:
                            _sys.path.insert(0, parent_dir)
                        from pos_integracija import POSOrderProcessor
                        meniu_out = POSOrderProcessor.gauti_visus_meniu_patiekalus()
                    except Exception:
                        meniu_out = None
            if not meniu_out:
                with _LOCK:
                    meniu_out = _STATE.get("menu") or DEFAULT_MENU
            self._send(200, json.dumps({"viso_patiekalu": len(meniu_out), "meniu": meniu_out}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
            return

        if path == "/api/sandelis":
            items_out = None
            db_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "model_mi.db"))
            if os.path.exists(db_path):
                try:
                    import sys as _sys
                    parent_dir = os.path.dirname(db_path)
                    if parent_dir not in _sys.path:
                        _sys.path.insert(0, parent_dir)
                    from db_engine import DBManager, get_connection
                    sand = DBManager.gauti_sandeli() or {}
                    kainos_map = {}
                    conn = get_connection()
                    for kr, kk, kpvm in conn.execute("SELECT lower(trim(raktas)), kaina, pvm FROM produktai_kainos;").fetchall():
                        if kr and kk is not None:
                            kainos_map[kr] = (float(kk or 0.0), float(kpvm if kpvm is not None else 21.0))
                    items_out = []
                    for k, v in sand.items():
                        lik = float(v.get("kiekis") or 0.0) if isinstance(v, dict) else 0.0
                        vnt = str(v.get("vnt") or "kg") if isinstance(v, dict) else "kg"
                        kp = kainos_map.get(str(k).strip().lower(), (0.0, 21.0))
                        items_out.append({"pavadinimas": k, "kiekis": lik, "mat_vnt": vnt, "kaina": kp[0], "pvm": kp[1]})
                except Exception:
                    items_out = None
            if items_out is None:
                with _LOCK:
                    items_out = _STATE.get("sandelis") or []
            self._send(200, json.dumps({"viso_prekiu": len(items_out), "sandelis": items_out}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
            return

        if path == "/api/aktyvus":
            akt_out = []
            db_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "model_mi.db"))
            if os.path.exists(db_path):
                try:
                    import sys as _sys
                    parent_dir = os.path.dirname(db_path)
                    if parent_dir not in _sys.path:
                        _sys.path.insert(0, parent_dir)
                    from db_engine import DBManager
                    akt_out = DBManager.gauti_aktyvius_uzsakymus() or []
                except Exception:
                    akt_out = []
            self._send(200, json.dumps({"viso_aktyviu": len(akt_out), "uzsakymai": akt_out}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
            return

        if path == "/api/tinklas/suvestine":
            if not self._check_auth(parsed):
                self._send(401, b'{"klaida":"Unauthorized"}', "application/json")
                return
            qs = urllib.parse.parse_qs(parsed.query)
            net_key = str(self.headers.get("X-Network-Key") or (qs.get("tinklo_raktas") or [""])[0]).strip()
            if len(net_key) < 4:
                self._send(400, json.dumps({"klaida": "Reikalingas tinklo raktas (min 4 simb.)."}).encode("utf-8"), "application/json")
                return
            kh = _tinklas_key_hash(net_key)
            now_ts = datetime.now().timestamp()
            with _LOCK:
                tinklas_map = _STATE.get("tinklas") or {}
                branches_raw = dict(tinklas_map.get(kh) or {})
            padaliniai = []
            for pid, snap in branches_raw.items():
                if not isinstance(snap, dict):
                    continue
                item = dict(snap)
                item["padalinio_id"] = pid
                srv_ts = float(item.get("_server_ts", 0.0) or 0.0)
                item["online"] = (now_ts - srv_ts) <= 95.0
                item["amzius_s"] = max(0, int(now_ts - srv_ts)) if srv_ts > 0 else 999999
                padaliniai.append(item)
            padaliniai.sort(key=lambda x: str(x.get("pavadinimas", x.get("padalinio_id", ""))).lower())
            self._send(200, json.dumps({"padaliniai": padaliniai}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
            return

        if path == "/api/stalai":
            if not self._check_auth(parsed):
                self._send(401, b'{"klaida":"Unauthorized"}', "application/json")
                return
            qs = urllib.parse.parse_qs(parsed.query)
            pin = str((qs.get("pin") or [self.headers.get("X-Waiter-PIN") or ""])[0]).strip()
            with _LOCK:
                svc = _svc_state()
                user = _svc_user(svc, pin)
                if not user:
                    self._send(401, json.dumps({"klaida": "Reikalingas galiojantis PIN."}).encode("utf-8"), "application/json")
                    return
                smap = {s["id"]: s["vardas"] for s in svc["staff"]}
                is_mgr = (user.get("role") == "vadybininkas")
                tables_out = [
                    _svc_serialize_table(t, smap) for t in svc["tables"]
                    if is_mgr or t.get("padavejas_id") == user["id"] or t.get("atidare_id") == user["id"]
                ]
                ah, ah_msg = _svc_after_hours(svc.get("work_hours", {}))
                res = {
                    "user": {"id": user["id"], "vardas": user["vardas"], "role": user["role"]},
                    "tables": tables_out, "work_hours": svc.get("work_hours", {}),
                    "after_hours": ah, "after_hours_msg": ah_msg,
                    "staff_names": [{"id": s["id"], "vardas": s["vardas"], "role": s["role"]} for s in svc["staff"] if s.get("aktyvus", True)],
                }
                if is_mgr:
                    res["staff_admin"] = [dict(s) for s in svc["staff"]]
                    res["all_tables"] = [_svc_serialize_table(t, smap) for t in svc["tables"]]
            self._send(200, json.dumps(res, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
            return

        with _LOCK:
            menu_cur = _STATE.get("menu") or DEFAULT_MENU
        html = CLOUD_HTML.replace("__MENU_JSON__", json.dumps(menu_cur, ensure_ascii=False))
        self._send(200, html.encode("utf-8"), "text/html; charset=utf-8")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if not self._check_auth(parsed):
            self._send(401, b'{"klaida":"Unauthorized"}', "application/json")
            return
        ln = int(self.headers.get("Content-Length", 0) or 0)
        payload = json.loads(self.rfile.read(ln).decode("utf-8")) if ln > 0 else {}

        if parsed.path in ("/api/uzsakymas", "/api/order", "/webhook"):
            with _LOCK:
                ev_id = int(_STATE.get("next_id", 1))
                _STATE["next_id"] = ev_id + 1
                rec = dict(payload)
                rec["_ev_id"] = ev_id
                rec.setdefault("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                _STATE["orders"].append(rec)
                _STATE["orders"] = _STATE["orders"][-300:]
                _save_state(_STATE)
            _broadcast_sse(ev_id, payload)
            self._send(200, json.dumps({"sekme": True, "ev_id": ev_id, "zinute": "Priimta į Cloud"}).encode("utf-8"), "application/json; charset=utf-8")
            return

        if parsed.path == "/api/tinklas/suvestine":
            if not isinstance(payload, dict):
                self._send(400, b'{"klaida":"Neteisingas formatas"}', "application/json")
                return
            net_key = str(self.headers.get("X-Network-Key") or payload.get("tinklo_raktas") or "").strip()
            pid = str(payload.get("padalinio_id") or "").strip()
            if len(net_key) < 4 or not pid:
                self._send(400, json.dumps({"klaida": "Reikalingas tinklo_raktas ir padalinio_id"}).encode("utf-8"), "application/json")
                return
            kh = _tinklas_key_hash(net_key)
            clean_snap = {
                "padalinio_id": pid,
                "pavadinimas": str(payload.get("pavadinimas") or pid)[:80],
                "timestamp": str(payload.get("timestamp") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                "siandien_apyvarta": round(float(payload.get("siandien_apyvarta") or 0.0), 2),
                "siandien_savikaina": round(float(payload.get("siandien_savikaina") or 0.0), 2),
                "siandien_pelnas": round(float(payload.get("siandien_pelnas") or 0.0), 2),
                "siandien_marza_pct": round(float(payload.get("siandien_marza_pct") or 0.0), 1),
                "siandien_cekiai": int(payload.get("siandien_cekiai") or 0),
                "vidutinis_cekis": round(float(payload.get("vidutinis_cekis") or 0.0), 2),
                "aktyvus_uzsakymai": int(payload.get("aktyvus_uzsakymai") or 0),
                "kritiniai_likuciai_kiekis": int(payload.get("kritiniai_likuciai_kiekis") or 0),
                "kritiniai_likuciai_sarasas": list(payload.get("kritiniai_likuciai_sarasas") or [])[:15],
                "top_patiekalai": list(payload.get("top_patiekalai") or [])[:8],
                "_server_ts": datetime.now().timestamp(),
            }
            with _LOCK:
                tinklas_map = _STATE.setdefault("tinklas", {})
                group = tinklas_map.setdefault(kh, {})
                group[pid] = clean_snap
                _save_state(_STATE)
            self._send(200, json.dumps({"sekme": True, "padalinio_id": pid}).encode("utf-8"), "application/json; charset=utf-8")
            return

        if parsed.path == "/api/meniu":
            if isinstance(payload, dict) and isinstance(payload.get("meniu"), list):
                with _LOCK:
                    _STATE["menu"] = payload["meniu"]
                    _save_state(_STATE)
                self._send(200, b'{"sekme":true}', "application/json")
                return

        if parsed.path == "/api/personalas/login":
            with _LOCK:
                svc = _svc_state()
                user = _svc_user(svc, str(payload.get("pin") or ""))
                if not user:
                    self._send(401, json.dumps({"klaida": "Neteisingas PIN kodas."}).encode("utf-8"), "application/json")
                    return
                ah, ah_msg = _svc_after_hours(svc.get("work_hours", {}))
                res = {
                    "user": {"id": user["id"], "vardas": user["vardas"], "role": user["role"]},
                    "work_hours": svc.get("work_hours", {}), "after_hours": ah, "after_hours_msg": ah_msg,
                }
            self._send(200, json.dumps(res, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
            return

        if parsed.path.startswith("/api/stalai/") or parsed.path == "/api/personalas/nustatymai":
            import uuid
            with _LOCK:
                svc = _svc_state()
                user = _svc_user(svc, str(payload.get("pin") or ""))
                if not user:
                    self._send(401, json.dumps({"klaida": "Neteisingas PIN."}).encode("utf-8"), "application/json")
                    return
                smap = {s["id"]: s["vardas"] for s in svc["staff"]}
                is_mgr = (user.get("role") == "vadybininkas")
                ah, ah_msg = _svc_after_hours(svc.get("work_hours", {}))
                mgr_ok = is_mgr or (lambda m: m and m.get("role") == "vadybininkas")(_svc_user(svc, str(payload.get("manager_pin") or "")))

                if parsed.path == "/api/personalas/nustatymai":
                    if not is_mgr:
                        self._send(403, json.dumps({"klaida": "Prieiga tik vadybininkui."}).encode("utf-8"), "application/json")
                        return
                    if isinstance(payload.get("work_hours"), dict):
                        svc["work_hours"] = payload["work_hours"]
                    if isinstance(payload.get("staff"), list):
                        svc["staff"] = [s for s in payload["staff"] if s.get("vardas") and s.get("pin")]
                    if isinstance(payload.get("table_assignments"), dict):
                        for tid, pid in payload["table_assignments"].items():
                            for t in svc["tables"]:
                                if t["id"] == tid:
                                    t["padavejas_id"] = str(pid or "")
                    _save_state(_STATE)
                    self._send(200, json.dumps({"zinute": "Nustatymai išsaugoti."}).encode("utf-8"), "application/json; charset=utf-8")
                    return

                tid = str(payload.get("table_id") or "")
                t = next((x for x in svc["tables"] if x["id"] == tid or str(x["nr"]) == tid), None)
                if not t:
                    self._send(400, json.dumps({"klaida": "Stalas nerastas."}).encode("utf-8"), "application/json")
                    return

                if parsed.path == "/api/stalai/atidaryti":
                    if ah and not mgr_ok:
                        self._send(403, json.dumps({"klaida": ah_msg, "after_hours_blocked": True}).encode("utf-8"), "application/json")
                        return
                    if t["status"] == "laisvas" or not t["checks"]:
                        t["status"] = "uzimtas"
                        t["atidare_id"] = user["id"]
                        t["sveciu_sk"] = max(1, int(payload.get("sveciu_sk") or 2))
                        t["opened_at"] = datetime.now().strftime("%H:%M")
                        t["checks"] = [{"id": f"chk_{uuid.uuid4().hex[:6]}", "pavadinimas": "Sąskaita 1", "status": "atviras", "items": []}]
                        _save_state(_STATE)
                    self._send(200, json.dumps({"stalas": _svc_serialize_table(t, smap)}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
                    return

                if parsed.path == "/api/stalai/prideti":
                    if ah and not mgr_ok:
                        self._send(403, json.dumps({"klaida": ah_msg, "after_hours_blocked": True}).encode("utf-8"), "application/json")
                        return
                    if t["status"] == "laisvas" or not t["checks"]:
                        t["status"] = "uzimtas"
                        t["atidare_id"] = user["id"]
                        t["sveciu_sk"] = 2
                        t["opened_at"] = datetime.now().strftime("%H:%M")
                        t["checks"] = [{"id": f"chk_{uuid.uuid4().hex[:6]}", "pavadinimas": "Sąskaita 1", "status": "atviras", "items": []}]
                    chk = next((c for c in t["checks"] if c["id"] == payload.get("check_id")), None) or t["checks"][0]
                    qty = float(payload.get("kiekis") or 1)
                    pav_in = str(payload.get("pavadinimas") or "").strip()
                    kaina_in = float(payload.get("kaina") or 0)
                    merged = False
                    for i_it, it in enumerate(chk["items"]):
                        if it.get("status") == "draft" and str(it.get("pavadinimas", "")).lower() == pav_in.lower() and abs(float(it.get("kaina", 0)) - kaina_in) < 0.005:
                            nq = round(float(it["kiekis"]) + qty, 3)
                            if nq <= 0:
                                chk["items"].pop(i_it)
                            else:
                                it["kiekis"] = nq
                            merged = True
                            break
                    if not merged and qty > 0:
                        chk["items"].append({
                            "item_id": f"it_{uuid.uuid4().hex[:8]}",
                            "pavadinimas": pav_in,
                            "kiekis": round(qty, 3),
                            "kaina": round(kaina_in, 2),
                            "tipas": str(payload.get("tipas") or "patiekalas"),
                            "status": "draft", "sent_at": "", "batch_id": "",
                        })
                    _save_state(_STATE)
                    self._send(200, json.dumps({"stalas": _svc_serialize_table(t, smap)}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
                    return

                if parsed.path == "/api/stalai/siusti":
                    if ah and not mgr_ok:
                        self._send(403, json.dumps({"klaida": ah_msg, "after_hours_blocked": True}).encode("utf-8"), "application/json")
                        return
                    drafts = [it for c in t["checks"] if c.get("status") != "apmoketas" for it in c["items"] if it.get("status") == "draft"]
                    if not drafts:
                        self._send(400, json.dumps({"klaida": "Nėra neišsiųstų pozicijų."}).encode("utf-8"), "application/json")
                        return
                    batch_id = uuid.uuid4().hex[:6].upper()
                    cekio_nr = f"T{t['nr']}-{batch_id}"
                    ev_id = int(_STATE.get("next_id", 1)); _STATE["next_id"] = ev_id + 1
                    sse_pay = {
                        "cekio_nr": cekio_nr, "data": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "stalas": t["pavadinimas"], "padavejas": user["vardas"],
                        "elementai": [{"pavadinimas": it["pavadinimas"], "kiekis": it["kiekis"], "kaina": it["kaina"], "tipas": it.get("tipas", "patiekalas")} for it in drafts],
                    }
                    for it in drafts:
                        it["status"] = "sent"; it["sent_at"] = datetime.now().strftime("%H:%M:%S"); it["batch_id"] = cekio_nr
                    rec = dict(sse_pay); rec["_ev_id"] = ev_id; rec["timestamp"] = sse_pay["data"]
                    _STATE["orders"].append(rec); _STATE["orders"] = _STATE["orders"][-300:]
                    _save_state(_STATE)
                    _broadcast_sse(ev_id, sse_pay)
                    self._send(200, json.dumps({"zinute": f"Išsiųsta ({cekio_nr})", "stalas": _svc_serialize_table(t, smap)}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
                    return

                if parsed.path == "/api/stalai/anuliuoti":
                    chk = next((c for c in t["checks"] if c["id"] == payload.get("check_id")), None)
                    it = next((x for x in (chk or {}).get("items", []) if x["item_id"] == payload.get("item_id")), None)
                    if not it:
                        self._send(400, json.dumps({"klaida": "Pozicija nerasta."}).encode("utf-8"), "application/json")
                        return
                    if it.get("status") == "draft":
                        chk["items"] = [x for x in chk["items"] if x["item_id"] != it["item_id"]]
                        _save_state(_STATE)
                        self._send(200, json.dumps({"zinute": "Pašalinta.", "stalas": _svc_serialize_table(t, smap)}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
                        return
                    if not mgr_ok:
                        self._send(400, json.dumps({"klaida": "Reikalingas vadybininko PIN!", "need_manager_pin": True}).encode("utf-8"), "application/json")
                        return
                    it["status"] = "voided"
                    ev_id = int(_STATE.get("next_id", 1)); _STATE["next_id"] = ev_id + 1
                    v_pay = {"veiksmas": "grazinti", "cekio_nr": it.get("batch_id") or f"T{t['nr']}", "pavadinimas": it["pavadinimas"], "kiekis": it["kiekis"], "priezastis": payload.get("priezastis", "")}
                    rec = dict(v_pay); rec["_ev_id"] = ev_id; rec["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    _STATE["orders"].append(rec); _STATE["orders"] = _STATE["orders"][-300:]
                    _save_state(_STATE)
                    _broadcast_sse(ev_id, v_pay)
                    self._send(200, json.dumps({"zinute": "Pozicija anuliuota.", "stalas": _svc_serialize_table(t, smap)}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
                    return

                if parsed.path == "/api/stalai/skelti":
                    src = next((c for c in t["checks"] if c["id"] == payload.get("source_check_id")), None)
                    if not src:
                        self._send(400, json.dumps({"klaida": "Sąskaita nerasta."}).encode("utf-8"), "application/json")
                        return
                    if payload.get("mode") == "equal":
                        n = max(2, min(8, int(payload.get("parts") or 2)))
                        act = [x for x in src["items"] if x.get("status") != "voided"]
                        for x in act:
                            x["kiekis"] = round(float(x["kiekis"]) / n, 3)
                        for _ in range(n - 1):
                            t["checks"].append({"id": f"chk_{uuid.uuid4().hex[:6]}", "pavadinimas": f"Sąskaita {len(t['checks'])+1}", "status": "atviras", "items": [dict(x, item_id=f"it_{uuid.uuid4().hex[:8]}") for x in act]})
                    else:
                        qmap = payload.get("item_qtys") or {}
                        if not qmap and payload.get("item_ids"):
                            qmap = {str(k): 999999.0 for k in payload.get("item_ids")}
                        mv, rem = [], []
                        for x in src["items"]:
                            xid = x["item_id"]
                            if xid in qmap and float(qmap[xid] or 0) > 0 and x.get("status") != "voided":
                                oq = float(x.get("kiekis") or 0)
                                mq = min(oq, float(qmap[xid]))
                                rq = round(oq - mq, 3)
                                if mq > 0:
                                    mv.append(dict(x, item_id=f"it_{uuid.uuid4().hex[:8]}", kiekis=round(mq, 3)))
                                if rq > 0.0001:
                                    rem.append(dict(x, kiekis=rq))
                            else:
                                rem.append(x)
                        if not mv or not any(x.get("status") != "voided" for x in rem):
                            self._send(400, json.dumps({"klaida": "Palikite bent vieną poziciją ir pasirinkite ką perkelti."}).encode("utf-8"), "application/json")
                            return
                        src["items"] = rem
                        tid_tgt = str(payload.get("target_check_id") or "")
                        tgt = next((c for c in t["checks"] if c["id"] == tid_tgt and c["id"] != src["id"] and c.get("status") != "apmoketas"), None)
                        if tgt:
                            tgt["items"].extend(mv)
                        else:
                            t["checks"].append({"id": f"chk_{uuid.uuid4().hex[:6]}", "pavadinimas": f"Sąskaita {len(t['checks'])+1}", "status": "atviras", "items": mv})
                    _save_state(_STATE)
                    self._send(200, json.dumps({"zinute": "Užsakymas padalintas.", "stalas": _svc_serialize_table(t, smap)}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
                    return

                if parsed.path == "/api/stalai/saskaita":
                    t["status"] = "saskaita"
                    _save_state(_STATE)
                    self._send(200, json.dumps({"zinute": "Sąskaita pateikta.", "stalas": _svc_serialize_table(t, smap)}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
                    return

                if parsed.path == "/api/stalai/apmoketi":
                    chk = next((c for c in t["checks"] if c["id"] == payload.get("check_id")), None)
                    if chk:
                        chk["status"] = "apmoketas"
                    if all(c.get("status") == "apmoketas" for c in t["checks"]):
                        t["status"] = "laisvas"; t["atidare_id"] = ""; t["sveciu_sk"] = 0; t["opened_at"] = ""; t["checks"] = []
                    _save_state(_STATE)
                    self._send(200, json.dumps({"zinute": "Apmokėta.", "stalas": _svc_serialize_table(t, smap)}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
                    return

                if parsed.path == "/api/stalai/perleisti":
                    t["padavejas_id"] = str(payload.get("target_staff_id") or "")
                    _save_state(_STATE)
                    self._send(200, json.dumps({"zinute": "Stalas perleistas.", "stalas": _svc_serialize_table(t, smap)}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
                    return

        self._send(404, b'{"klaida":"Not found"}', "application/json")


def main():
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), CloudHandler)
    print(f"Model MI Cloud Kasa veikia ant 0.0.0.0:{PORT}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
