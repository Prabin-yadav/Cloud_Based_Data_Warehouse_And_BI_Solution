import json
import os
import re
import urllib.request
import urllib.error

import pandas as pd
import streamlit as st

import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from db import qry

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


FORBIDDEN_SQL = re.compile(
    r"\b(drop|delete|update|insert|alter|truncate|grant|revoke|create|replace|comment|copy)\b",
    re.IGNORECASE,
)


def _reload_env_from_dotenv() -> None:
    """Reload .env on every rerun so key changes take effect.

    Important: Streamlit reruns the script frequently; relying on process env alone
    can keep an old key around. We intentionally let `.env` override here.
    """

    if load_dotenv is None:
        return
    try:
        load_dotenv(ROOT / ".env", override=True)
    except Exception:
        # Never block the UI because dotenv couldn't be read.
        return


def _get_env_gemini_api_key() -> str:
    """Return Gemini API key from environment (.env loaded)."""

    key = (
        os.getenv("GEMINI_API_KEY")
        or os.getenv("GOOGLE_API_KEY")
        or os.getenv("GOOGLE_AI_API_KEY")
        or ""
    )
    return key.strip()


def _get_gemini_api_key() -> str:
    """Return Gemini API key (always from `.env`)."""

    return _get_env_gemini_api_key()


def _mask_key(key: str) -> str:
    key = (key or "").strip()
    if not key:
        return "(not set)"
    if len(key) <= 8:
        return "*" * len(key)
    return f"{key[:2]}***{key[-4:]}"


def _parse_http_code(message: str) -> int | None:
    if not message:
        return None
    m = re.search(r"HTTP\s+(\d{3})", message)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _looks_like_quota_error(message: str) -> bool:
    msg = (message or "").lower()
    return (
        "resource_exhausted" in msg
        or "quota" in msg
        or "rate limit" in msg
        or "too many requests" in msg
        or "http 429" in msg
    )


def _gemini_list_models_uncached(api_key: str) -> tuple[list[str], str | None]:
    """List models with error detail (no Streamlit caching)."""

    api_key = (api_key or "").strip()
    if not api_key:
        return [], "Missing API key."

    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
    req = urllib.request.Request(url, headers={"Content-Type": "application/json"}, method="GET")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="ignore")
        return [], f"Gemini API error: HTTP {e.code}: {detail}"
    except Exception as e:
        return [], f"Gemini request failed: {e}"

    try:
        data = json.loads(body)
    except Exception:
        return [], f"Could not parse response: {body}"

    models = data.get("models") or []
    out: list[str] = []
    for m in models:
        name = (m.get("name") or "").strip()
        methods = m.get("supportedGenerationMethods") or []
        if "generateContent" not in methods:
            continue
        if name.startswith("models/"):
            name = name[len("models/") :]
        if name:
            out.append(name)

    # Prefer gemini-2.5-flash first, then other flash models.
    def _rank(name: str) -> tuple[int, int, str]:
        n = name.lower()
        is_25_flash = 0 if n == "gemini-2.5-flash" else 1
        is_flash = 0 if "flash" in n else 1
        return (is_25_flash, is_flash, n)

    out.sort(key=_rank)
    return out, None


def _extract_sql(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""

    fenced = re.search(r"```(?:sql)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        return fenced.group(1).strip()

    # Try to cut from the first SELECT/WITH.
    m = re.search(r"\b(select|with)\b", text, flags=re.IGNORECASE)
    if m:
        return text[m.start() :].strip()

    return text


def _sanitize_sql(sql: str) -> str:
    sql = (sql or "").strip()
    sql = re.sub(r"^\s*--.*?$", "", sql, flags=re.MULTILINE).strip()
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL).strip()

    if not sql:
        raise ValueError("Empty SQL generated.")

    # Prevent multi-statement.
    if ";" in sql:
        # Allow a single trailing semicolon.
        if sql.count(";") > 1 or not sql.rstrip().endswith(";"):
            raise ValueError("Only a single SELECT statement is allowed.")
        sql = sql.rstrip().rstrip(";").strip()

    if FORBIDDEN_SQL.search(sql):
        raise ValueError("Blocked: only read-only SELECT queries are allowed.")

    if not re.match(r"^(select|with)\b", sql, flags=re.IGNORECASE):
        raise ValueError("Blocked: query must start with SELECT or WITH.")

    # Enforce a reasonable limit (only if user/model didn't include one).
    if not re.search(r"\blimit\b", sql, flags=re.IGNORECASE):
        sql = sql.rstrip()
        sql += "\nLIMIT 200"

    return sql


@st.cache_data(show_spinner=False, ttl=3600)
def _schema_hint() -> str:
    """Build a lightweight schema hint from information_schema.

    We intentionally keep it small: only public schema, and only tables that exist.
    """
    tables = [
        "fact_drug_sales",
        "fact_inventory",
        "etl_audit_log",
        "dim_date",
        "dim_drug",
        "dim_customer",
        "dim_geography",
        "dim_therapeutic_class",
    ]

    try:
        df = qry(
            """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name = ANY(%s)
            ORDER BY table_name, ordinal_position
            """,
            params=(tables,),
        )
    except Exception:
        df = pd.DataFrame()

    if df.empty:
        return (
            "Tables (PostgreSQL): fact_drug_sales, fact_inventory, etl_audit_log, "
            "dim_date, dim_drug, dim_customer, dim_geography, dim_therapeutic_class."
        )

    out = ["PostgreSQL schema (public):"]
    for tname, g in df.groupby("table_name", sort=True):
        cols = ", ".join(f"{r.column_name}:{r.data_type}" for r in g.itertuples(index=False))
        out.append(f"- {tname}({cols})")
    return "\n".join(out)


def _gemini_generate(api_key: str, model: str, prompt_text: str, temperature: float = 0.2) -> str:
    """Call Gemini via Google Generative Language REST API.

    This is intentionally lightweight (urllib only).
    Endpoint: https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent
    """

    prompt_text = (prompt_text or "").strip()
    if not prompt_text:
        return ""

    model = (model or "").strip()
    if model.startswith("models/"):
        model = model[len("models/") :]
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": prompt_text}],
            }
        ],
        "generationConfig": {
            "temperature": float(temperature),
        },
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Gemini API error: HTTP {e.code}: {detail}") from e
    except Exception as e:
        raise RuntimeError(f"Gemini request failed: {e}") from e

    data = json.loads(body)
    candidates = data.get("candidates") or []
    if not candidates:
        raise RuntimeError(f"Gemini returned no candidates: {body}")

    content = (candidates[0].get("content") or {})
    parts = content.get("parts") or []
    text = "".join((p.get("text") or "") for p in parts)
    return (text or "").strip()


def _extract_retry_seconds(message: str) -> float | None:
    """Best-effort extraction of retry delay from Gemini error text."""

    if not message:
        return None
    m = re.search(r"Please retry in\s+([0-9]+(?:\.[0-9]+)?)s", message)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return None
    m = re.search(r"\"retryDelay\"\s*:\s*\"(\d+)s\"", message)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return None
    return None


@st.cache_data(show_spinner=False, ttl=3600)
def _gemini_list_models(api_key: str) -> list[str]:
    """Return model IDs (without 'models/') that support generateContent."""

    api_key = (api_key or "").strip()
    if not api_key:
        return []

    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
    req = urllib.request.Request(url, headers={"Content-Type": "application/json"}, method="GET")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
    except Exception:
        return []

    try:
        data = json.loads(body)
    except Exception:
        return []

    models = data.get("models") or []
    out: list[str] = []
    for m in models:
        name = (m.get("name") or "").strip()  # e.g. "models/gemini-1.5-flash"
        methods = m.get("supportedGenerationMethods") or []
        if "generateContent" not in methods:
            continue
        if name.startswith("models/"):
            name = name[len("models/") :]
        if name:
            out.append(name)

    # Prefer gemini-2.5-flash first, then other flash models.
    def _rank(name: str) -> tuple[int, int, str]:
        n = name.lower()
        is_25_flash = 0 if n == "gemini-2.5-flash" else 1
        is_flash = 0 if "flash" in n else 1
        return (is_25_flash, is_flash, n)

    out.sort(key=_rank)
    return out


def show():
    _reload_env_from_dotenv()

    st.markdown('<div class="page-title">AI Assistant</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-sub">Ask questions in plain English. The assistant generates a safe read-only SQL query, runs it, and shows results.</div>',
        unsafe_allow_html=True,
    )
    st.markdown("---")

    api_key = _get_gemini_api_key()
    model = os.getenv("GEMINI_MODEL", "").strip()
    if not model:
        # Keep a valid, broadly available default so the page works without manual model refresh.
        model = "gemini-2.5-flash"

    with st.expander("AI settings", expanded=False):
        st.caption(
            "Uses `GEMINI_API_KEY` from `.env`. Choose a model below."
        )

        st.write(f"Active key: `{_mask_key(api_key)}`")

        # Show a model dropdown immediately without making any network call.
        # Users can optionally fetch the live list (Refresh models) if their key supports different models.
        default_models = [
            "gemini-2.5-flash",
            "gemini-2.5-pro",
            "gemini-2.0-flash",
            "gemini-2.0-flash-001",
        ]

        if "_gemini_models" not in st.session_state:
            st.session_state["_gemini_models"] = default_models
        if not st.session_state.get("_gemini_models"):
            st.session_state["_gemini_models"] = default_models

        # Auto-load the full model list once per session (so users don't have to press Refresh).
        if "_gemini_models_autoloaded" not in st.session_state:
            st.session_state["_gemini_models_autoloaded"] = False
        if api_key and not st.session_state.get("_gemini_models_autoloaded"):
            models, err = _gemini_list_models_uncached(api_key)
            if not err and models:
                st.session_state["_gemini_models"] = models
            st.session_state["_gemini_models_autoloaded"] = True

        refresh_models = st.button("Refresh models", use_container_width=True, disabled=not bool(api_key))
        st.caption("Optional: fetch the live model list from Gemini (makes an API call).")

        if refresh_models and api_key:
            models, err = _gemini_list_models_uncached(api_key)
            if err:
                st.error(err)
            st.session_state["_gemini_models"] = models
            st.session_state["_gemini_models_autoloaded"] = True

        available_models = st.session_state.get("_gemini_models") or default_models
        if model.startswith("models/"):
            model = model[len("models/") :]
        if model not in available_models:
            model = "gemini-2.5-flash" if "gemini-2.5-flash" in available_models else available_models[0]
        model = st.selectbox("Model", options=available_models, index=available_models.index(model))

        st.markdown("---")
        if st.button("Test key", use_container_width=True, disabled=not bool(api_key)):
            with st.spinner("Testing Gemini..."):
                try:
                    out = _gemini_generate(
                        api_key=api_key,
                        model=model,
                        prompt_text="Return exactly the string: ok",
                        temperature=0.0,
                    )
                    st.success(f"Gemini OK (model: {model})")
                    st.code(out)
                except Exception as e:
                    st.error("Gemini test failed.")
                    with st.expander("Error details"):
                        st.code(str(e))

    question = st.text_area(
        "Your question",
        placeholder="e.g., Show total revenue and gross margin by therapeutic class for the last 30 days.",
        height=90,
    )

    c1, c2, c3 = st.columns([1.1, 1.1, 2.2])
    with c1:
        show_sql = st.checkbox("Show SQL", value=True)
    with c2:
        explain = st.checkbox("Explain results", value=True)
    with c3:
        run = st.button("Run", type="primary", use_container_width=True)

    if not run:
        return

    # If the user never refreshed models, fetch once on demand (single request) so we can
    # correct invalid defaults and avoid confusing errors.
    if api_key and not (st.session_state.get("_gemini_models") or []):
        models, err = _gemini_list_models_uncached(api_key)
        if not err and models:
            st.session_state["_gemini_models"] = models
            if model.startswith("models/"):
                model = model[len("models/") :]
            if model not in models:
                model = models[0]

    if not api_key:
        st.error("Missing `GEMINI_API_KEY`. Add it to your `.env` or enter it in AI settings.")
        return

    if not question.strip():
        st.warning("Type a question first.")
        return

    schema = _schema_hint()

    system = (
        "You are a helpful data assistant for a PostgreSQL database. "
        "You must output ONLY a single read-only SQL query. "
        "Rules: SELECT/WITH only; never write data; no DDL; no comments; no markdown; "
        "always include LIMIT 200 unless the question clearly needs fewer rows. "
        "Prefer joins to dimensions for names. "
    )

    user = (
        f"{schema}\n\n"
        "User question: " + question.strip() + "\n\n"
        "Return only SQL."
    )

    prompt = system + "\n\n" + user

    with st.spinner("Generating SQL..."):
        try:
            raw = _gemini_generate(api_key=api_key, model=model, prompt_text=prompt, temperature=0.1)
        except RuntimeError as e:
            msg = str(e)
            http_code = _parse_http_code(msg)
            if http_code in (401,):
                st.error(
                    "Gemini rejected the API key (HTTP 401). "
                    "Double-check the key value and ensure the correct project is selected in Google AI Studio."
                )
                with st.expander("Error details"):
                    st.code(msg)
                return
            if http_code in (403,):
                st.error(
                    "Gemini denied access (HTTP 403). This is usually billing/permissions/API enablement for the project, "
                    "not an issue in your app."
                )
                with st.expander("Error details"):
                    st.code(msg)
                return
            if _looks_like_quota_error(msg):
                retry_s = _extract_retry_seconds(msg)
                st.error(
                    "Gemini rate limit / quota exceeded for this API key. "
                    "This is a Gemini account/project issue (not your app)."
                )
                if "limit: 0" in msg:
                    st.info(
                        "Your project shows a quota limit of 0 for the free tier. "
                        "Enable billing / upgrade your plan in Google AI Studio, or use a different API key/project."
                    )
                if retry_s is not None:
                    st.caption(f"Suggested retry after ~{retry_s:.1f}s.")
                st.caption("Docs: https://ai.google.dev/gemini-api/docs/rate-limits")
                with st.expander("Error details"):
                    st.code(msg)
                return
            if "HTTP 404" in msg and "ListModels" in msg:
                models = _gemini_list_models(api_key)
                st.error("Your Gemini model name is not valid for this API key. Pick a model from the list below (or set `GEMINI_MODEL` in `.env`).")
                if models:
                    st.code("\n".join(models))
                else:
                    st.info("Could not fetch available models. Double-check your `GEMINI_API_KEY`.")
                return
            st.error(msg)
            with st.expander("Error details"):
                st.code(msg)
            return

    sql = _extract_sql(raw)
    try:
        sql = _sanitize_sql(sql)
    except Exception as e:
        st.error(f"Could not run generated SQL: {e}")
        if show_sql:
            st.code(raw)
        return

    if show_sql:
        st.markdown('<div class="sec-hdr">GENERATED SQL</div>', unsafe_allow_html=True)
        st.code(sql, language="sql")

    with st.spinner("Running query..."):
        df = qry(sql)

    st.markdown('<div class="sec-hdr">RESULTS</div>', unsafe_allow_html=True)
    if df is None or df.empty:
        st.info("No rows returned.")
    else:
        st.dataframe(df, use_container_width=True)

    if explain and df is not None and not df.empty:
        preview = df.head(30).to_dict(orient="records")
        expl_prompt = (
            "You are a BI analyst. Explain the query result in 4-6 bullet points. "
            "Be concise and business-focused. If you infer something, label it as an inference."\
            "\n\nQuestion: "
            + question.strip()
            + "\n\nSQL: "
            + sql
            + "\n\nResult preview (first rows): "
            + json.dumps(preview, default=str)
            + "\n\nExplain what this means."
        )
        with st.spinner("Explaining..."):
            expl = _gemini_generate(api_key=api_key, model=model, prompt_text=expl_prompt, temperature=0.3)
        st.markdown('<div class="sec-hdr">ANSWER</div>', unsafe_allow_html=True)
        st.write(expl)


if __name__ == "__main__":
    show()
