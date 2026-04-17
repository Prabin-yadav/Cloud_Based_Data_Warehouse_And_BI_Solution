import streamlit as st, sys, os
sys.path.insert(0, os.path.dirname(__file__))

st.set_page_config(
    page_title="Zentrik Pharma Intelligence",
    page_icon="💊", layout="wide",
    initial_sidebar_state="expanded"
)

if "theme_mode" not in st.session_state:
    st.session_state["theme_mode"] = "Dark"


def _theme_vars(mode):
    if mode == "Light":
      return {
        "bg": "#f3f7ff",
        "surface": "#eef4ff",
        "card": "#ffffff",
        "border": "#c8d6ef",
        "border2": "#afc3e7",
        "accent": "#0ea5e9",
        "accent2": "#2563eb",
        "green": "#16a34a",
        "amber": "#d97706",
        "red": "#dc2626",
        "purple": "#7c3aed",
        "text": "#0f172a",
        "muted": "#475569",
        "muted2": "#334155",
      }
    return {
      "bg": "#07090f",
      "surface": "#0d1117",
      "card": "#111827",
      "border": "#1e2d45",
      "border2": "#243447",
      "accent": "#38bdf8",
      "accent2": "#818cf8",
      "green": "#22c55e",
      "amber": "#eab308",
      "red": "#ef4444",
      "purple": "#a78bfa",
      "text": "#f1f5f9",
      "muted": "#64748b",
      "muted2": "#94a3b8",
    }

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap');

:root {
  --bg:#07090f; --surface:#0d1117; --card:#111827;
  --border:#1e2d45; --border2:#243447;
  --accent:#38bdf8; --accent2:#818cf8;
  --green:#22c55e; --amber:#eab308;
  --red:#ef4444; --purple:#a78bfa;
  --text:#f1f5f9; --muted:#64748b; --muted2:#94a3b8;
  --font:'Outfit',sans-serif; --mono:'JetBrains Mono',monospace;
}
*, html, body { box-sizing:border-box; }
html, body, [data-testid="stAppViewContainer"] {
  background:var(--bg) !important;
  color:var(--text) !important;
  font-family:var(--font) !important;
}
[data-testid="stAppViewContainer"] .main .block-container {
  padding-top: 0.45rem !important;
}
[data-testid="stSidebar"] {
  background:var(--surface) !important;
  border-right:1px solid var(--border) !important;
}
[data-testid="stSidebar"] * { color:var(--text) !important; }
[data-testid="stHeader"] { background:transparent !important; }
/* Keep Streamlit toolbar visible so the sidebar can be expanded/collapsed. */
#MainMenu { vicasibility:hidden !important; }
footer { visibility:hidden !important; }

/* Sidebar navigation */
[data-testid="stSidebar"] div[role="radiogroup"] {
  gap: 8px;
}
[data-testid="stSidebar"] div[role="radiogroup"] > label {
  background: color-mix(in srgb, var(--card) 78%, transparent) !important;
  border: 1px solid var(--border) !important;
  border-radius: 12px !important;
  padding: 10px 12px !important;
  transition: all .2s ease !important;
}
[data-testid="stSidebar"] div[role="radiogroup"] > label:hover {
  border-color: var(--accent) !important;
  transform: translateY(-1px);
}
[data-testid="stSidebar"] div[role="radiogroup"] > label:has(input:checked) {
  border-color: var(--accent) !important;
  box-shadow: none !important;
  background: color-mix(in srgb, var(--accent) 10%, var(--card)) !important;
}
[data-testid="stSidebar"] div[role="radiogroup"] > label [data-testid="stMarkdownContainer"] p {
  font-size: 18px !important;
  font-weight: 650 !important;
  letter-spacing: .2px !important;
  line-height: 1.25 !important;
}
[data-testid="stSidebar"] div[role="radiogroup"] > label [data-testid="stMarkdownContainer"] {
  margin-left: 4px;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
  background:var(--surface) !important; border-radius:10px;
  padding:4px; gap:4px; border:1px solid var(--border);
}
.stTabs [data-baseweb="tab"] {
  background:transparent !important; color:var(--muted2) !important;
  border-radius:7px !important; font-family:var(--font) !important;
  font-size:14px !important; padding:8px 18px !important;
}
.stTabs [aria-selected="true"] {
  background:var(--card) !important; color:var(--accent) !important;
  border:1px solid var(--border2) !important;
}
[data-testid="stTabContent"] { background:transparent !important; }

/* Inputs */
.stSelectbox > div > div, .stTextInput > div > div,
.stMultiSelect > div > div, .stDateInput > div > div {
  background:var(--card) !important; border:1px solid var(--border2) !important;
  border-radius:8px !important; color:var(--text) !important;
}
.stSelectbox label, .stTextInput label, .stMultiSelect label, .stDateInput label {
  color:var(--muted2) !important; font-size:12px !important;
  font-family:var(--mono) !important; letter-spacing:0.5px !important;
}
.stNumberInput > div > div { background:var(--card) !important; border:1px solid var(--border2) !important; border-radius:8px !important; }

/* Buttons */
.stButton > button {
  background:linear-gradient(135deg,rgba(56,189,248,.15),rgba(129,140,248,.15)) !important;
  border:1px solid var(--accent) !important; color:var(--accent) !important;
  border-radius:8px !important; font-family:var(--mono) !important;
  font-size:13px !important; font-weight:600 !important;
  letter-spacing:0.5px !important; transition:all .2s !important;
}
.stButton > button:hover {
  background:linear-gradient(135deg,rgba(56,189,248,.3),rgba(129,140,248,.3)) !important;
  transform:translateY(-1px) !important;
  box-shadow:none !important;
}
.stButton > button[kind="primary"] {
  background:linear-gradient(135deg,#38bdf8,#818cf8) !important;
  color:#07090f !important; border:none !important; font-weight:700 !important;
}
.stButton > button[kind="primary"]:hover {
  transform:translateY(-1px) !important;
  box-shadow:none !important;
}

/* Dataframe */
[data-testid="stDataFrame"] { border:1px solid var(--border) !important; border-radius:10px !important; overflow:hidden !important; }
[data-testid="stDataFrame"] th { background:var(--card) !important; color:var(--accent) !important; font-family:var(--mono) !important; font-size:12px !important; }

/* Metrics */
div[data-testid="stMetricValue"] { color:var(--accent) !important; font-family:var(--mono) !important; }
div[data-testid="stMetricLabel"] { color:var(--muted2) !important; }

/* File uploader */
[data-testid="stFileUploader"] { background:var(--card) !important; border:1px dashed var(--border2) !important; border-radius:12px !important; }

/* Progress */
.stProgress > div > div { background:linear-gradient(90deg,#38bdf8,#818cf8) !important; }

/* Expander */
[data-testid="stExpander"] { background:var(--card) !important; border:1px solid var(--border) !important; border-radius:10px !important; }
[data-testid="stExpander"] summary { color:var(--text) !important; }

/* Alerts */
[data-testid="stAlert"] { border-radius:10px !important; }

/* Cards */
.kpi-card {
  background:var(--card); border:1px solid var(--border);
  border-radius:14px; padding:22px 20px; text-align:center;
  transition:all .25s; position:relative; overflow:hidden;
}
.kpi-card::before {
  content:''; position:absolute; top:0; left:0; right:0;
  height:2px; background:var(--grad);
}
.kpi-card:hover { border-color:var(--hover-c); transform:translateY(-2px); box-shadow:none; }
.kpi-val { font-size:30px; font-weight:700; font-family:var(--mono); line-height:1.1; }
.kpi-lbl { font-size:11px; color:var(--muted); text-transform:uppercase; letter-spacing:1.5px; margin-top:6px; }
.kpi-sub { font-size:12px; margin-top:8px; }

.sec-hdr {
  font-size:11px; text-transform:uppercase; letter-spacing:2.5px;
  color:var(--accent); font-family:var(--mono);
  margin:28px 0 14px; padding-bottom:8px;
  border-bottom:1px solid var(--border);
}
.page-title {
  font-size:28px; font-weight:700; color:var(--text);
  font-family:var(--font); margin-bottom:4px;
}
.page-sub { font-size:14px; color:var(--muted2); margin-bottom:20px; }

.etl-step {
  background:var(--card); border:1px solid var(--border);
  border-radius:12px; padding:20px; text-align:center;
}
.etl-step-ok  { border-color:var(--green) !important; background:#0d2318 !important; }
.etl-step-err { border-color:var(--red) !important; background:#1f0d0d !important; }

.status-pill {
  display:inline-block; border-radius:99px; padding:3px 12px;
  font-size:11px; font-family:var(--mono); font-weight:600;
}
.pill-ok  { background:#052e16; color:#22c55e; border:1px solid #22c55e; }
.pill-err { background:#1f0d0d; color:#ef4444; border:1px solid #ef4444; }
.pill-wrn { background:#1c1400; color:#eab308; border:1px solid #eab308; }
.pill-inf { background:#0c1a2e; color:#38bdf8; border:1px solid #38bdf8; }

.conn-ok  { background:#052e16; border:1px solid #22c55e; color:#22c55e; border-radius:8px; padding:8px 14px; font-family:var(--mono); font-size:12px; }
.conn-err { background:#1f0d0d; border:1px solid #ef4444; color:#ef4444; border-radius:8px; padding:8px 14px; font-family:var(--mono); font-size:12px; }

.sidebar-brand { text-align:center; padding:8px 0 4px; }
.brand-icon { font-size:36px; margin-bottom:2px; }
.brand-name { font-size:20px; font-weight:700; color:var(--accent); font-family:var(--mono); letter-spacing:2px; }
.brand-sub  { font-size:10px; color:var(--muted); letter-spacing:3px; text-transform:uppercase; }

.top-hero {
  border: 1px solid var(--border);
  border-radius: 16px;
  padding: 16px 18px;
  margin: 0 0 12px;
  background: color-mix(in srgb, var(--surface) 90%, black);
}
.top-hero-kicker {
  font-family: var(--mono);
  font-size: 10px;
  letter-spacing: 2px;
  text-transform: uppercase;
  color: var(--accent);
}
.top-hero-row {
  margin-top: 6px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}
.top-hero-title {
  font-size: 26px;
  font-weight: 700;
  color: var(--text);
  line-height: 1.1;
}
.top-hero-sub {
  margin-top: 4px;
  color: var(--muted2);
  font-size: 13px;
}
.top-chip-wrap {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}
.top-chip {
  border: 1px solid var(--border2);
  border-radius: 999px;
  padding: 5px 10px;
  font-size: 11px;
  color: var(--muted2);
  background: color-mix(in srgb, var(--card) 80%, transparent);
}
.top-chip.active {
  color: var(--text);
  border-color: var(--accent);
  background: color-mix(in srgb, var(--accent) 10%, transparent);
}

h1,h2,h3 { font-family:var(--font) !important; color:var(--text) !important; }
/* Do not force font-family on all spans; Streamlit uses Material icon ligatures in spans. */
p,div { font-family:var(--font) !important; }
code,pre { font-family:var(--mono) !important; }
</style>
""", unsafe_allow_html=True)

tv = _theme_vars(st.session_state["theme_mode"])
st.markdown(f"""
<style>
:root {{
  --bg:{tv['bg']};
  --surface:{tv['surface']};
  --card:{tv['card']};
  --border:{tv['border']};
  --border2:{tv['border2']};
  --accent:{tv['accent']};
  --accent2:{tv['accent2']};
  --green:{tv['green']};
  --amber:{tv['amber']};
  --red:{tv['red']};
  --purple:{tv['purple']};
  --text:{tv['text']};
  --muted:{tv['muted']};
  --muted2:{tv['muted2']};
}}
</style>
""", unsafe_allow_html=True)

from db import ping

with st.sidebar:
    st.markdown("""
    <div class="sidebar-brand">
      <div class="brand-icon">💊</div>
      <div class="brand-name">ZENTRIK</div>
      <div class="brand-sub">Pharma Intelligence</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("---")
    page = st.radio("Navigation", [
      "🏠 Dashboard",
      "📊 Analytics",
      "🔍 Search & Filter",
      "📤 Upload & ETL",
      "📥 Reports",
      "⚙️ ETL Monitor",
    ], label_visibility="collapsed")
    st.markdown("---")
    ok = ping()
    st.markdown(
        f'<div class="{"conn-ok" if ok else "conn-err"}">{"● LIVE — AWS RDS" if ok else "● OFFLINE — AWS RDS"}</div>',
        unsafe_allow_html=True
    )
    st.markdown("---")
    dark_on = st.toggle("Dark Theme", value=(st.session_state["theme_mode"] == "Dark"), key="theme_dark_toggle")
    st.session_state["theme_mode"] = "Dark" if dark_on else "Light"
    st.markdown("---")
    

st.markdown(f"""
<div class="top-hero">
  <div class="top-hero-kicker">Zentrik Command Surface</div>
  <div class="top-hero-row">
    <div>
      <div class="top-hero-title">Pharma Intelligence Workspace</div>
      <div class="top-hero-sub">Unified view for dashboard, analytics, search, ETL, and reporting workflows.</div>
    </div>
    <div class="top-chip-wrap">
      <div class="top-chip active">Active: {page.replace('🏠 ', '').replace('📊 ', '').replace('🔍 ', '').replace('📤 ', '').replace('📥 ', '').replace('⚙️ ', '')}</div>
      <div class="top-chip">Theme: {st.session_state['theme_mode']}</div>
      <div class="top-chip">Data: AWS RDS</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

if   "Dashboard"  in page: from pages import dashboard; dashboard.show()
elif "Analytics"  in page: from pages import analytics; analytics.show()
elif "Search"     in page: from pages import search;    search.show()
elif "Upload"     in page: from pages import upload;    upload.show()
elif "Reports"    in page: from pages import reports;   reports.show()
elif "Monitor"    in page: from pages import monitor;   monitor.show()
