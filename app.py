# Parte 1/10

# app.py — Pelada (multi-usuário por login)
# Compatível com db_users.AuthManager (retorna (ok, err) em create_user e fornece engine por usuário)
# Cada usuário tem seu próprio banco em ./data/<slug>.sqlite

import streamlit as st
import pandas as pd
from sqlalchemy import text, create_engine
from datetime import date, datetime, timedelta
from io import BytesIO
import calendar, re, urllib.parse, math, random, os, secrets

# --- PDF (ReportLab) opcional ---
try:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet
    HAS_RL = True
except Exception:
    HAS_RL = False

# ---------- Autenticação (db_users) ----------
from db_users import AuthManager
auth = AuthManager()

st.set_page_config(page_title="⚽ Pelada", page_icon="⚽", layout="wide")

# -----------------------------------------------------------------------------------
#  SISTEMA DE MULTI-USUÁRIO + "LEMBRAR-ME" (Link de acesso rápido)
#  - Primeiro mostra login/cadastro
#  - Após login: troca o "engine" para o banco do usuário e garante o schema mínimo
#  - "Lembrar-me": gera token guardado em ./data/_tokens.sqlite e coloca ?t=<token> na URL
#    Reabrindo o app com o mesmo link, o usuário é autenticado automaticamente.
# -----------------------------------------------------------------------------------

# *** Banco global de tokens (independe do usuário logado) ***
os.makedirs("data", exist_ok=True)
TOK_DB_PATH = "sqlite:///data/_tokens.sqlite"
tok_engine = create_engine(TOK_DB_PATH, future=True, echo=False)

def _ensure_tokens_schema():
    with tok_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS auth_tokens (
                token TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            )
        """))
_ensure_tokens_schema()

def _token_insert(username:str, days:int=30) -> str:
    token = secrets.token_urlsafe(32)
    now = datetime.utcnow()
    exp = now + timedelta(days=days)
    with tok_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO auth_tokens(token, username, created_at, expires_at) VALUES(:t,:u,:c,:e)"),
            {"t": token, "u": username, "c": now.isoformat(), "e": exp.isoformat()}
        )
    return token

def _token_get_username(token:str):
    if not token: return None
    with tok_engine.begin() as conn:
        df = pd.read_sql(text("""
            SELECT username, expires_at FROM auth_tokens WHERE token=:t
        """), conn, params={"t": token})
    if df.empty:
        return None
    try:
        exp = datetime.fromisoformat(str(df.iloc[0]["expires_at"]))
    except Exception:
        return None
    if datetime.utcnow() > exp:
        # expirou: limpa
        with tok_engine.begin() as conn:
            conn.execute(text("DELETE FROM auth_tokens WHERE token=:t"), {"t": token})
        return None
    return str(df.iloc[0]["username"])

def _token_delete(token:str):
    if not token: return
    with tok_engine.begin() as conn:
        conn.execute(text("DELETE FROM auth_tokens WHERE token=:t"), {"t": token})

def _token_delete_user(username:str):
    with tok_engine.begin() as conn:
        conn.execute(text("DELETE FROM auth_tokens WHERE username=:u"), {"u": username})

# Engine global, trocado após login
engine = None

def df_query(sql, params=None):
    global engine
    if engine is None:
        return pd.DataFrame()
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def exec_sql(sql, params=None):
    global engine
    if engine is None:
        return
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})

# ---------- Pequenos utilitários ----------
BR_MONTHS = {
    1:"Janeiro",2:"Fevereiro",3:"Março",4:"Abril",5:"Maio",6:"Junho",
    7:"Julho",8:"Agosto",9:"Setembro",10:"Outubro",11:"Novembro",12:"Dezembro"
}

def _expand_in(ids, param_prefix="p"):
    """
    Gera placeholders nomeados para IN do SQLite.
    ids=[10,20] -> (":p0,:p1", {"p0":10,"p1":20})
    Se ids vazio, retorna "NULL" (IN (NULL) => vazio).
    """
    if ids is None:
        ids = []
    ids = [int(x) for x in ids if pd.notna(x)]
    if not ids:
        return "NULL", {}
    placeholders = ",".join([f":{param_prefix}{i}" for i in range(len(ids))])
    params = {f"{param_prefix}{i}": ids[i] for i in range(len(ids))}
    return placeholders, params

def normalize_season(v) -> str:
    """
    Normaliza temporada aceitando apenas anos com 4 dígitos (ex.: '2025').
    Se não houver 4 dígitos, retorna None.
    """
    if v is None:
        return None
    s = re.sub(r"[.,]", "", str(v))
    d = re.findall(r"\d+", s)
    if not d:
        return None
    n = "".join(d)
    if len(n) < 4:
        return None
    return n[:4]

def parse_br_date(s: str):
    s = str(s).strip()
    if not s: return None
    for fmt in ["%d/%m/%Y","%d-%m-%Y"]:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s).date().isoformat()
    except Exception:
        return None

def calc_points(wins:int, draws:int)->int:
    return int(wins)*3 + int(draws)

def to_xlsx_bytes(df: pd.DataFrame, sheet_name="Planilha"):
    buf = BytesIO()
    try:
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            df.to_excel(w, index=False, sheet_name=sheet_name)
    except Exception:
        with pd.ExcelWriter(buf) as w:  # fallback
            df.to_excel(w, index=False, sheet_name=sheet_name)
    return buf.getvalue()

# -----------------------------------------------------------------------------------
#  SCHEMA por usuário (garante tabelas mínimas no banco do usuário logado)
# -----------------------------------------------------------------------------------

def ensure_user_schema(_engine):
    with _engine.begin() as conn:
        # settings
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )"""))

        # rounds
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            notes TEXT,
            closed INTEGER DEFAULT 0,
            four_goalkeepers INTEGER DEFAULT 0,
            season TEXT
        )"""))

        # teams_round
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS teams_round (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round_id INTEGER,
            name TEXT,
            wins INTEGER DEFAULT 0,
            draws INTEGER DEFAULT 0,
            points INTEGER DEFAULT 0
        )"""))

        # players
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            nickname TEXT,
            position TEXT,
            role TEXT,
            is_goalkeeper INTEGER DEFAULT 0,
            plan TEXT,
            active INTEGER DEFAULT 1
        )"""))

        # player_round
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS player_round (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round_id INTEGER,
            player_id INTEGER,
            team_round_id INTEGER,
            presence INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            draws INTEGER DEFAULT 0,
            points INTEGER DEFAULT 0,
            yellow_cards INTEGER DEFAULT 0,
            red_cards INTEGER DEFAULT 0,
            foto_bonus INTEGER DEFAULT 0,
            bola_murcha INTEGER DEFAULT 0,
            individual_override INTEGER DEFAULT 0
        )"""))

        # === FIX de duplicidade: garantir 1 linha por (round_id, player_id) ===
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS _player_round_dedup AS
            SELECT pr.id, pr.round_id, pr.player_id, pr.team_round_id,
                   MAX(pr.presence) AS presence,
                   MAX(pr.wins) AS wins,
                   MAX(pr.draws) AS draws,
                   MAX(pr.points) AS points,
                   MAX(pr.yellow_cards) AS yellow_cards,
                   MAX(pr.red_cards) AS red_cards,
                   MAX(pr.foto_bonus) AS foto_bonus,
                   MAX(pr.bola_murcha) AS bola_murcha,
                   MAX(pr.individual_override) AS individual_override
              FROM player_round pr
          GROUP BY pr.round_id, pr.player_id
        """))
        conn.execute(text("DELETE FROM player_round"))
        conn.execute(text("INSERT INTO player_round SELECT * FROM _player_round_dedup"))
        conn.execute(text("DROP TABLE _player_round_dedup"))
        # índice único para prevenir futuras duplicações
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_player_round_unique
              ON player_round(round_id, player_id)
        """))
        # === FIM DO FIX ===

        # caixa
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cash_month_flags (
          season TEXT NOT NULL,
          player_id INTEGER NOT NULL,
          month INTEGER NOT NULL CHECK(month BETWEEN 1 AND 12),
          paid INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (season, player_id, month)
        )"""))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cash_extra (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          date TEXT,
          season TEXT,
          type TEXT,
          description TEXT,
          value REAL
        )"""))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cash_opening (
          season TEXT PRIMARY KEY,
          opening REAL
        )"""))

    # defaults úteis
    def _maybe_set(k, v):
        df = df_query("SELECT value FROM settings WHERE key=:k", {"k": k})
        if df.empty:
            exec_sql("INSERT INTO settings(key,value) VALUES(:k,:v)", {"k": k, "v": v})
    _maybe_set("league_name", "Pelada do Pivete")
    _maybe_set("use_cards", "1")
    _maybe_set("has_referee", "0")
    _maybe_set("players_per_team_line", "5")

# -----------------------------------------------------------------------------------
#  LOGIN / LOGOUT UI + AUTOLOGIN POR TOKEN (?t=...)
# -----------------------------------------------------------------------------------

def _login_view():
    st.title("⚽ Pelada — Login")

    # Autologin via token na URL (se ainda não autenticado)
    qp = st.query_params
    token_in = qp.get("t")
    if isinstance(token_in, list):
        token_in = token_in[0] if token_in else None
    if token_in and "auth_user" not in st.session_state:
        uname = _token_get_username(str(token_in))
        if uname:
            st.session_state["auth_user"] = uname
            st.success(f"Bem-vindo de volta, {uname}! (login automático)")
            st.rerun()

    tab_login, tab_signup = st.tabs(["Entrar", "Criar conta"])

    with tab_login:
        u = st.text_input("Usuário", key="login_user")
        p = st.text_input("Senha", type="password", key="login_pass")
        remember = st.checkbox("Lembrar-me (gerar link de acesso rápido por 30 dias)", value=True, key="remember_me")
        if st.button("Entrar", type="primary", key="btn_login"):
            user = auth.get_user(u)
            if not user:
                st.error("Usuário não encontrado.")
            else:
                if auth.verify_password(p, user["password_hash"]):
                    st.session_state["auth_user"] = u.strip()
                    if remember:
                        tok = _token_insert(u.strip(), days=30)
                        # coloca o token na URL atual
                        st.query_params.update({"t": tok})
                        st.info("Login salvo neste link (válido por 30 dias). Favor adicionar aos favoritos.")
                    st.rerun()
                else:
                    st.error("Senha incorreta.")

    with tab_signup:
        u2 = st.text_input("Novo usuário", key="signup_user")
        p1 = st.text_input("Senha", type="password", key="signup_p1")
        p2 = st.text_input("Confirmar senha", type="password", key="signup_p2")
        if st.button("Criar conta", key="btn_signup"):
            if not u2 or not p1:
                st.warning("Preencha usuário e senha.")
            elif p1 != p2:
                st.error("As senhas não conferem.")
            else:
                ok, err = auth.create_user(u2.strip(), p1)
                if ok:
                    st.success("Usuário criado! Já pode entrar na aba 'Entrar'.")
                else:
                    st.error(err or "Não foi possível criar usuário.")

def _ensure_user_engine_and_schema():
    global engine
    user = st.session_state.get("auth_user")
    if not user:
        return False
    # cria/pega engine do usuário
    engine = auth.get_or_create_user_engine(user, ensure_base_cb=ensure_user_schema)
    # segurança extra: garante schema sempre que logar
    ensure_user_schema(engine)
    return True

# Se não logado, tenta autologin por token; se falhar, mostra login e sai
if "auth_user" not in st.session_state:
    _login_view()
    st.stop()

if not _ensure_user_engine_and_schema():
    st.error("Falha ao abrir banco do usuário.")
    st.stop()

# Barra lateral: info do usuário + link rápido
with st.sidebar:
    st.markdown(f"**👤 Usuário:** {st.session_state.get('auth_user','-')}")
    colA, colB = st.columns(2)
    with colA:
        if st.button("Sair", use_container_width=True, key="btn_logout"):
            # limpar tokens deste usuário (opcional) e sessão
            _token_delete_user(st.session_state.get("auth_user"))
            st.session_state.pop("auth_user", None)
            # remove token da URL atual
            current = dict(st.query_params)
            if "t" in current: del current["t"]
            st.query_params.clear()
            if current:
                st.query_params.update(current)
            st.rerun()
    with colB:
        if st.button("Gerar link rápido", use_container_width=True, key="btn_make_fastlink"):
            tok = _token_insert(st.session_state.get("auth_user"), days=30)
            st.query_params.update({"t": tok})
            st.success("Link de acesso rápido gerado (válido por 30 dias). Adicione esta página aos favoritos.")

#Parte 2/10

# -------------- Settings ---------------
def get_setting(key, default=""):
    df = df_query("SELECT value FROM settings WHERE key=:k", {"k": key})
    return default if df.empty else df.iloc[0]["value"]

def set_setting(key, value):
    exec_sql(
        "INSERT INTO settings(key,value) VALUES(:k,:v) "
        "ON CONFLICT(key) DO UPDATE SET value=:v",
        {"k": key, "v": str(value)},
    )

def league_name(): return get_setting("league_name", "Pelada do Pivete")

# -------------- Rounds / Teams helpers ---------------
def get_or_create_round_by_date(d_iso: str, season: str = None, four_gk_default: bool = False):
    r = df_query("SELECT id FROM rounds WHERE date=:d", {"d": d_iso})
    if not r.empty:
        rid = int(r.iloc[0]["id"])
        if season is not None:
            exec_sql("UPDATE rounds SET season=:s WHERE id=:r", {"s": season, "r": rid})
        return rid, False
    exec_sql(
        "INSERT INTO rounds(date, notes, closed, four_goalkeepers, season)"
        " VALUES(:d,'',0,:fg,:s)",
        {"d": d_iso, "fg": 1 if four_gk_default else 0, "s": season},
    )
    r2 = df_query("SELECT id FROM rounds WHERE date=:d", {"d": d_iso})
    return int(r2.iloc[0]["id"]), True

def get_or_create_team_round(round_id:int, team_name:str):
    if str(team_name).strip().lower().startswith("tempo"):
        try:
            n = int(str(team_name).strip().split()[-1])
            team_name = f"Time {n}"
        except Exception:
            team_name = "Time 1"
    t = df_query(
        "SELECT id FROM teams_round WHERE round_id=:r AND name=:n",
        {"r": round_id, "n": team_name},
    )
    if not t.empty:
        return int(t.iloc[0]["id"])
    exec_sql(
        "INSERT INTO teams_round(round_id, name, wins, draws, points) "
        "VALUES(:r,:n,0,0,0)",
        {"r": round_id, "n": team_name},
    )
    t2 = df_query(
        "SELECT id FROM teams_round WHERE round_id=:r AND name=:n",
        {"r": round_id, "n": team_name},
    )
    return int(t2.iloc[0]["id"])

def recalc_round(round_id:int):
    teams = df_query("SELECT id, wins, draws FROM teams_round WHERE round_id=:r", {"r": round_id})
    team_pts = {}
    for _, row in teams.iterrows():
        pts = calc_points(int(row["wins"]), int(row["draws"]))
        team_pts[int(row["id"])] = (int(row["wins"]), int(row["draws"]), pts)
        exec_sql("UPDATE teams_round SET points=:p WHERE id=:id", {"p": pts, "id": int(row["id"])})
    if not team_pts: return

    players = df_query(
        "SELECT id, team_round_id, COALESCE(individual_override,0) AS ov "
        "FROM player_round WHERE round_id=:r AND team_round_id IS NOT NULL",
        {"r": round_id},
    )
    for _, pr in players.iterrows():
        tid = pr["team_round_id"]
        if pd.notna(tid) and int(tid) in team_pts and int(pr["ov"]) != 1:
            w,d,pts = team_pts[int(tid)]
            exec_sql(
                "UPDATE player_round SET wins=:w, draws=:d, points=:p, presence=1 WHERE id=:id",
                {"w": w, "d": d, "p": pts, "id": int(pr["id"])},
            )

    # Foto / Bola murcha por time (apenas quando há um único ganhador/perdedor)
    tdf = df_query("SELECT id, points FROM teams_round WHERE round_id=:r", {"r": round_id})
    if tdf.empty: return
    maxp = tdf["points"].max(); minp = tdf["points"].min()
    winners = tdf[tdf["points"]==maxp]["id"].tolist()
    losers  = tdf[tdf["points"]==minp]["id"].tolist()

    exec_sql("UPDATE player_round SET foto_bonus=0, bola_murcha=0 WHERE round_id=:r", {"r": round_id})
    if len(winners)==1:
        exec_sql(
            "UPDATE player_round SET foto_bonus=1 WHERE round_id=:r AND team_round_id=:t",
            {"r": round_id, "t": int(winners[0])},
        )
    if len(losers)==1:
        exec_sql(
            "UPDATE player_round SET bola_murcha=1 WHERE round_id=:r AND team_round_id=:t",
            {"r": round_id, "t": int(losers[0])},
        )

def recalc_all_rounds(close_all: bool = False, regen_notes: bool = True):
    r = df_query("SELECT id FROM rounds ORDER BY date ASC, id ASC")
    for _, row in r.iterrows():
        recalc_round(int(row["id"]))
    if regen_notes:
        generate_round_notes_sequence()
    if close_all:
        exec_sql("UPDATE rounds SET closed=1")

def generate_round_notes_sequence():
    allr = df_query("SELECT id, date FROM rounds ORDER BY date ASC, id ASC")
    for i, row in enumerate(allr.itertuples(index=False), start=1):
        exec_sql("UPDATE rounds SET notes=:n WHERE id=:id", {"n": f"{i}º Rodada", "id": int(row.id)})

# -------------- Players helpers ---------------
def find_player_id_by_name(name: str):
    nm = str(name).strip().lower()
    if not nm: return None
    df = df_query("SELECT id, name, nickname FROM players")
    for _, r in df.iterrows():
        if nm == str(r["name"]).strip().lower() or nm == str(r.get("nickname") or "").strip().lower():
            return int(r["id"])
    return None

def upsert_player_round_for_date(pid: int, d_iso: str):
    rid, _ = get_or_create_round_by_date(d_iso, season=None, four_gk_default=False)
    try:
        exec_sql(
            "INSERT INTO player_round(round_id, player_id, presence, wins, draws, points) "
            "VALUES(:r,:p,1,0,0,0)",
            {"r": rid, "p": pid},
        )
    except Exception:
        pass
    return rid

# Parte 3/10

# -------------- Import helpers ---------------

def import_players_df(df: pd.DataFrame):
    """
    Importa jogadores garantindo:
    - Se 'Apelido' vier vazio, usamos 'Nome' como nickname.
    - Posição 'GOL' => role='GOLEIRO', is_goalkeeper=1; demais => 'JOGADOR', 0.
    Cabeçalhos aceitos: Nome do Jogador, Posição do Jogador, Plano, Tabela, Apelido (opcional)
    """
    # Mapeamento de colunas (aceitando variações)
    cmap = {}
    for c in df.columns:
        s = c.strip().lower()
        if s.startswith("nome do jogador") or s.startswith("nome"):
            cmap[c] = "name"
        elif "apelido" in s:
            cmap[c] = "nickname"
        elif "posição do jogador" in s or "posi" in s:
            cmap[c] = "position"
        elif s == "plano":
            cmap[c] = "plan"
        elif s == "tabela":
            cmap[c] = "table"
    df = df.rename(columns=cmap)

    total, ok = len(df), 0
    for _, r in df.iterrows():
        name = str(r.get("name","")).strip()
        if not name:
            continue

        raw_nk = str(r.get("nickname") or "").strip()
        nk = raw_nk if raw_nk else name  # força nickname = name quando não vier apelido

        pos = str(r.get("position","ATA")).strip().upper()
        is_gk = 1 if pos == "GOL" else 0
        role = "GOLEIRO" if is_gk else "JOGADOR"
        plan = str(r.get("plan","Mensalista")).strip() or "Mensalista"

        exec_sql(
            "INSERT INTO players(name, nickname, position, role, is_goalkeeper, plan, active) "
            "VALUES(:n,:nk,:pos,:role,:gk,:plan,1) "
            "ON CONFLICT(name) DO UPDATE SET nickname=:nk, position=:pos, role=:role, "
            "is_goalkeeper=:gk, plan=:plan, active=1",
            {"n": name, "nk": nk, "pos": pos, "role": role, "gk": is_gk, "plan": plan},
        )
        ok += 1

    # Pós-ajuste idempotente: garante que todos tenham apelido
    exec_sql("UPDATE players SET nickname = name WHERE nickname IS NULL OR TRIM(nickname) = ''")
    return {"linhas": total, "gravadas": ok}


def import_player_links(df: pd.DataFrame):
    """
    Arquivo: VÍNCULO JOGADORES
    - Colunas (qualquer ordem): Data; Nome/Jogador; Time
    - Para cada DATA (rodada), SUBSTITUI os vínculos jogador→time:
        * Limpa todos os team_round_id dessa rodada
        * Garante a existência dos times referenciados (cria com 0-0 se necessário)
        * Faz upsert de player_round (presence=1) e aplica o team_round_id
        * NÃO mexe em vitórias/empates dos times (arquivo 'times' cuida disso)
        * Mantém cartões existentes
    """
    # mapear cabeçalhos
    cols_map = {}
    for col in df.columns:
        c = col.strip().lower()
        if c.startswith("data"): cols_map[col] = "date_br"
        elif c in {"nome","jogador","name"}: cols_map[col] = "player"
        elif c.startswith("time") or c.startswith("tempo"): cols_map[col] = "team"
    df = df.rename(columns=cols_map)

    # saneamento
    for need in ["date_br","player","team"]:
        if need not in df.columns: df[need] = ""
    df = df[
        (df["date_br"].astype(str).str.strip()!="") &
        (df["player"].astype(str).str.strip()!="") &
        (df["team"].astype(str).str.strip()!="")
    ]
    if df.empty: return {"rows":0, "missing_players":0}

    missing, rows = 0, 0
    # processa LOTE por DATA (sobreposição)
    for d, g in df.groupby("date_br"):
        d_iso = parse_br_date(d)
        if not d_iso:
            continue
        rid, _ = get_or_create_round_by_date(d_iso, season=None, four_gk_default=False)

        # 1) limpar todos os vínculos de time da rodada (substitui completamente)
        exec_sql("UPDATE player_round SET team_round_id=NULL WHERE round_id=:r", {"r": rid})

        # 2) aplicar novos vínculos
        for _, row in g.iterrows():
            pid = find_player_id_by_name(row["player"])
            if not pid:
                missing += 1
                continue

            tname = str(row["team"]).strip()
            if tname.lower().startswith("tempo"):
                try: tname = f"Time {int(tname.split()[-1])}"
                except Exception: tname = "Time 1"
            if tname.upper() not in {"TIME 1","TIME 2","TIME 3","TIME 4"}:
                tname = "Time 1"

            tid = get_or_create_team_round(rid, tname)

            # garantir presença e vínculo (sem duplicar)
            try:
                exec_sql(
                    "INSERT INTO player_round(round_id, player_id, presence, team_round_id) "
                    "VALUES(:r,:p,1,:t)",
                    {"r": rid, "p": int(pid), "t": int(tid)},
                )
            except Exception:
                exec_sql(
                    "UPDATE player_round SET presence=1, team_round_id=:t "
                    "WHERE round_id=:r AND player_id=:p",
                    {"r": rid, "p": int(pid), "t": int(tid)},
                )
            rows += 1

        # 3) recálculo (para distribuir pontos dos times aos jogadores sem override)
        recalc_round(rid)

    return {"rows": rows, "missing_players": missing}


def import_times_table(df: pd.DataFrame):
    """
    Arquivo: TIMES
    - Colunas (qualquer ordem): Data, Temporada (opcional), Time, Vitórias, Empates
    - Para cada DATA (rodada), SUBSTITUI os times da rodada:
        * Exclui todos os times existentes da rodada
        * Insere os times do arquivo com vitórias/empates e recalcula pontos
    """
    # mapear cabeçalhos
    cols = {}
    for c in df.columns:
        s = c.strip().lower()
        if s.startswith("data"): cols[c]="date"
        elif "temporada" in s or "season" in s: cols[c]="season"
        elif s.startswith("time") or s.startswith("tempo"): cols[c]="team"
        elif "vit" in s: cols[c]="wins"
        elif "emp" in s: cols[c]="draws"
    df = df.rename(columns=cols)

    miss = [c for c in ["date","team","wins","draws"] if c not in df.columns]
    if miss:
        return {"error": f"Colunas obrigatórias ausentes: {', '.join(miss)}"}

    # processar por DATA
    for d, g in df.groupby("date"):
        d_iso = parse_br_date(d)
        if not d_iso: continue
        season = normalize_season(
            g.get("season").dropna().astype(str).iloc[0] if "season" in g.columns and not g["season"].dropna().empty else None
        )
        rid, _ = get_or_create_round_by_date(d_iso, season=season, four_gk_default=False)

        # 1) excluir times existentes dessa rodada
        exec_sql("DELETE FROM teams_round WHERE round_id=:r", {"r": rid})

        # 2) inserir os times do arquivo
        for _, r in g.iterrows():
            tname = str(r.get("team","Time 1")).strip()
            if tname.lower().startswith("tempo"):
                try: tname = f"Time {int(tname.split()[-1])}"
                except Exception: tname="Time 1"
            wins = int(pd.to_numeric(r.get("wins",0), errors="coerce") or 0)
            draws = int(pd.to_numeric(r.get("draws",0), errors="coerce") or 0)
            pts = calc_points(wins, draws)
            exec_sql(
                "INSERT INTO teams_round(round_id,name,wins,draws,points) VALUES(:r,:n,:w,:d,:p)",
                {"r": rid, "n": tname, "w": wins, "d": draws, "p": pts},
            )

        # 3) recalc para propagar aos jogadores (sem override)
        recalc_round(rid)
    return {"ok": True}


def import_cards_table(df: pd.DataFrame):
    """
    Arquivo: CARTÕES
    - Colunas (qualquer ordem): Data; Jogador/Nome; CA; CV
    - Para cada DATA (rodada), SUBSTITUI os cartões:
        * Zera CA/CV de todos os lançamentos da rodada
        * Aplica os valores agregados por jogador dessa rodada
        * Mantém presença e vínculos/points
    """
    # mapear cabeçalhos
    cols = {}
    for c in df.columns:
        s = c.strip().lower()
        if s.startswith("data"): cols[c] = "date"
        elif ("jog" in s) or ("nome" in s) or s=="jogador" or s=="nome": cols[c] = "player"
        elif s in {"ca","amarelo","amarelos"}: cols[c] = "ca"
        elif s in {"cv","vermelho","vermelhos"}: cols[c] = "cv"
    df = df.rename(columns=cols)

    # colunas padrão e tipos
    for n in ["date","player","ca","cv"]:
        if n not in df.columns: df[n] = 0
    df["ca"] = pd.to_numeric(df["ca"], errors="coerce").fillna(0).astype(int)
    df["cv"] = pd.to_numeric(df["cv"], errors="coerce").fillna(0).astype(int)

    # agregação por data+jogador
    agg = df.groupby(["date","player"], as_index=False)[["ca","cv"]].sum()

    ok, miss = 0, 0
    # processar por DATA
    for d, g in agg.groupby("date"):
        d_iso = parse_br_date(d)
        if not d_iso:
            continue
        rid, _ = get_or_create_round_by_date(d_iso, season=None, four_gk_default=False)

        # 1) zera cartões da rodada (substituição)
        exec_sql("UPDATE player_round SET yellow_cards=0, red_cards=0 WHERE round_id=:r", {"r": rid})

        # 2) aplica cartões dos jogadores listados
        for _, r in g.iterrows():
            pid = find_player_id_by_name(r["player"])
            if not pid:
                miss += 1
                continue

            # garantir presença e atualizar cartões
            try:
                exec_sql(
                    "INSERT INTO player_round(round_id, player_id, presence, yellow_cards, red_cards) "
                    "VALUES(:r,:p,1,:ca,:cv)",
                    {"r": rid, "p": int(pid), "ca": int(r["ca"]), "cv": int(r["cv"])},
                )
            except Exception:
                exec_sql(
                    "UPDATE player_round SET presence=1, yellow_cards=:ca, red_cards=:cv "
                    "WHERE round_id=:r AND player_id=:p",
                    {"r": rid, "p": int(pid), "ca": int(r["ca"]), "cv": int(r["cv"])},
                )
            ok += 1

        # cartões não alteram pontos, mas manter consistente:
        recalc_round(rid)

    return {"gravados": ok, "ignorados": miss}


def save_gk_individual(round_id:int, player_id:int, wins:int, draws:int, team_round_id:int=None, points:int=None):
    """
    Mantém team_round_id para foto/bola murcha; marca override individual.
    Se 'points' vier None, calcula 3*V + 1*E.
    """
    pts = calc_points(wins, draws) if points is None else int(points)
    if team_round_id is None:
        pr = df_query("SELECT team_round_id FROM player_round WHERE round_id=:r AND player_id=:p",
                      {"r": round_id, "p": player_id})
        if not pr.empty and pd.notna(pr.iloc[0]["team_round_id"]):
            team_round_id = int(pr.iloc[0]["team_round_id"])
    try:
        exec_sql(
            "INSERT INTO player_round(round_id, player_id, team_round_id, presence, wins, draws, points, individual_override) "
            "VALUES(:r,:p,:t,1,:w,:d,:pts,1)",
            {"r": round_id, "p": player_id, "t": team_round_id, "w": int(wins), "d": int(draws), "pts": pts},
        )
    except Exception:
        exec_sql(
            "UPDATE player_round SET team_round_id=:t, presence=1, wins=:w, draws=:d, points=:pts, individual_override=1 "
            "WHERE round_id=:r AND player_id=:p",
            {"r": round_id, "p": player_id, "t": team_round_id, "w": int(wins), "d": int(draws), "pts": pts},
        )


def import_goalkeepers_table(df: pd.DataFrame):
    """
    Arquivo: GOLEIROS (pontuação individual)
    - Colunas (qualquer ordem): Data; Goleiro/Nome; Vitórias; Empates; Pontos (opcional)
    - Para cada DATA (rodada), SUBSTITUI os overrides de goleiros:
        * Remove override existente dos goleiros da rodada
        * Aplica vitórias/empates (e pontos, se informado) como override individual
        * Mantém vínculos de time e cartões
    """
    # mapear cabeçalhos
    cols = {}
    for c in df.columns:
        s = c.strip().lower()
        if s.startswith("data"): cols[c] = "date"
        elif ("goleiro" in s) or ("nome" in s) or ("jogador" in s): cols[c] = "gk"
        elif ("vit" in s): cols[c] = "wins"
        elif ("emp" in s): cols[c] = "draws"
        elif ("ponto" in s): cols[c] = "points"
    df = df.rename(columns=cols)

    for need in ["date","gk","wins","draws"]:
        if need not in df.columns:
            df[need] = 0

    # normalizar tipos
    df["wins"]  = pd.to_numeric(df["wins"],  errors="coerce").fillna(0).astype(int)
    df["draws"] = pd.to_numeric(df["draws"], errors="coerce").fillna(0).astype(int)
    if "points" in df.columns:
        df["points"] = pd.to_numeric(df["points"], errors="coerce")

    # agregação por data+gk
    grp_cols = ["date","gk"]
    agg = df.groupby(grp_cols, as_index=False).agg({
        "wins":"sum","draws":"sum", **({"points":"sum"} if "points" in df.columns else {})
    })

    ok, miss = 0, 0
    for d, g in agg.groupby("date"):
        d_iso = parse_br_date(d)
        if not d_iso:
            continue
        rid, _ = get_or_create_round_by_date(d_iso, season=None, four_gk_default=False)

        # 1) limpar overrides de goleiros da rodada
        exec_sql("""
            UPDATE player_round
               SET individual_override=0
             WHERE round_id=:r
               AND player_id IN (
                    SELECT id FROM players WHERE (role='GOLEIRO' OR is_goalkeeper=1)
               )
        """, {"r": rid})

        # 2) aplicar novos overrides
        for _, r in g.iterrows():
            pid = find_player_id_by_name(r["gk"])
            if not pid:
                miss += 1
                continue

            # manter vínculo de time existente, se houver
            pr = df_query(
                "SELECT team_round_id FROM player_round WHERE round_id=:r AND player_id=:p",
                {"r": rid, "p": int(pid)}
            )
            tid = int(pr.iloc[0]["team_round_id"]) if not pr.empty and pd.notna(pr.iloc[0]["team_round_id"]) else None

            pts = None
            if "points" in r and pd.notna(r["points"]):
                try: pts = int(r["points"])
                except Exception: pts = None

            save_gk_individual(rid, int(pid), int(r["wins"]), int(r["draws"]), team_round_id=tid, points=pts)
            ok += 1

        recalc_round(rid)

    return {"gravados": ok, "ignorados": miss}


# Parte 4/10

# -------------- Classificação ---------------
def classificacao_df(period: dict=None):
    where = ""
    params = {}
    if period:
        mode = period.get("mode","all")
        if mode in ("month","window"):
            d1 = period.get("start"); d2 = period.get("end")
            if d1 and d2:
                where = "WHERE r.date >= :d1 AND r.date <= :d2"
                params.update({"d1": d1, "d2": d2})
        elif mode == "season":
            where = "WHERE COALESCE(r.season,'') = :s"
            params.update({"s": period.get("season","")})

    base_sql = f"""
      SELECT
        p.id as player_id,
        COALESCE(p.nickname, p.name) AS jogador,
        UPPER(COALESCE(p.role, CASE WHEN p.is_goalkeeper=1 THEN 'GOLEIRO' ELSE 'JOGADOR' END)) AS tipo,
        COALESCE(SUM(pr.foto_bonus),0)       AS fotos_qtd_total,
        COALESCE(SUM(pr.points),0)           AS pontos_total,
        COALESCE(SUM(pr.wins),0)             AS vitorias_total,
        COALESCE(SUM(pr.draws),0)            AS empates_total,
        COALESCE(SUM(pr.red_cards),0)        AS verm_total,
        COALESCE(SUM(pr.yellow_cards),0)     AS amarelo_total,
        COALESCE(SUM(pr.bola_murcha),0)      AS bola_murcha_total,
        COALESCE(SUM(CASE WHEN pr.presence=1 THEN 1 ELSE 0 END),0) AS presencas_total
      FROM players p
      LEFT JOIN player_round pr ON pr.player_id = p.id
      LEFT JOIN rounds r ON r.id = pr.round_id
      {where}
      GROUP BY p.id, jogador, tipo
    """
    sql = f"SELECT * FROM ({base_sql}) t WHERE t.presencas_total > 0"
    df = df_query(sql, params)
    if df.empty: return df

    df["aproveitamento_fotos"] = df.apply(
        lambda r: (r["fotos_qtd_total"]/r["presencas_total"]) if r["presencas_total"] else 0.0, axis=1
    )
    for c in ["fotos_qtd_total","pontos_total","vitorias_total","empates_total","verm_total","amarelo_total","bola_murcha_total","presencas_total"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    df = df.sort_values(
        ["fotos_qtd_total","pontos_total","vitorias_total","verm_total","amarelo_total","bola_murcha_total","presencas_total","aproveitamento_fotos"],
        ascending=[False,False,False,True,True,True,False,False]
    ).reset_index(drop=True)
    return df

def prepare_class_table(df: pd.DataFrame, hide_cards: bool=False) -> pd.DataFrame:
    if df is None or df.empty: return df
    df2 = df.copy().reset_index(drop=True)
    df2.insert(0, "Posição", [f"{i}º" for i in range(1, len(df2)+1)])
    for col in ["player_id","tipo"]:
        if col in df2.columns: df2 = df2.drop(columns=[col])

    desired = ["Posição","jogador","fotos_qtd_total","pontos_total","vitorias_total","empates_total","verm_total","amarelo_total","bola_murcha_total","presencas_total","aproveitamento_fotos"]
    keep = [c for c in desired if c in df2.columns] + [c for c in df2.columns if c not in desired]
    df2 = df2[keep]

    if hide_cards:
        for c in ["verm_total","amarelo_total"]:
            if c in df2.columns: df2 = df2.drop(columns=[c])

    for c in ["fotos_qtd_total","pontos_total","vitorias_total","empates_total","verm_total","amarelo_total","bola_murcha_total","presencas_total","aproveitamento_fotos"]:
        if c in df2.columns: df2[c] = pd.to_numeric(df2[c], errors="coerce")

    if "aproveitamento_fotos" in df2.columns:
        df2["aproveitamento_fotos"] = (df2["aproveitamento_fotos"].fillna(0.0)*100).round(2).map(lambda v: f"{float(v):.2f}%".replace(".",","))

    for c in ["fotos_qtd_total","pontos_total","vitorias_total","empates_total","verm_total","amarelo_total","bola_murcha_total","presencas_total"]:
        if c in df2.columns: df2[c] = df2[c].fillna(0).astype(int)

    df2["jogador"] = df2["jogador"].astype(str)

    return df2.rename(columns={
        "fotos_qtd_total":"Fotos","pontos_total":"Pontos","vitorias_total":"Vitórias","empates_total":"Empates",
        "verm_total":"Vermelhos","amarelo_total":"Amarelos","bola_murcha_total":"Bola Murcha",
        "presencas_total":"Presenças","aproveitamento_fotos":"%"
    })

def add_delta_and_style(prep: pd.DataFrame, prev_map: dict):
    if prep.empty: return prep, None
    pos_nums = [int(str(v).replace("º","")) for v in prep["Posição"].tolist()]
    arrows=[]
    for i, row in prep.iterrows():
        name = str(row["jogador"])
        prev = prev_map.get(name)
        cur = int(pos_nums[i])
        if prev is None: arrows.append("•")
        else:
            d = prev - cur
            arrows.append(f"▲ {d}" if d>0 else ("▼ "+str(abs(d)) if d<0 else "•"))
    prep = prep.copy()
    prep.insert(1, "Δ", arrows)

    def paint_pos(val):
        p = int(str(val).replace("º",""))
        if p<=4:   return "color:#0b2e4f; font-weight:700;"
        if p<=6:   return "color:#2563eb; font-weight:700;"
        if p<=12:  return "color:#16a34a; font-weight:700;"
        if p<=14:  return "color:#374151; font-weight:700;"
        return "color:#dc2626; font-weight:700;"
    sty = prep.style.map(paint_pos, subset=pd.IndexSlice[:, ["Posição"]])

    def paint_delta(v):
        s=str(v)
        if s.startswith("▲"): return "color:#0a7f2e; font-weight:600;"
        if s.startswith("▼"): return "color:#b00020; font-weight:600;"
        return "color:#6b7280;"
    sty = sty.map(paint_delta, subset=pd.IndexSlice[:, ["Δ"]])

    return prep, sty

def compute_prev_maps(rounds_period, mode, period):
    if rounds_period is None or rounds_period.empty or len(rounds_period) < 2:
        return {}, {}

    prev_end = str(rounds_period.iloc[-2]["date"])

    if mode == "Mês" and period:
        start = str(period["start"])
    elif mode == "Temporada":
        start = str(rounds_period.iloc[0]["date"])
    else:
        start = str(rounds_period.iloc[0]["date"])

    prev_df = classificacao_df({"mode": "window", "start": start, "end": prev_end})
    if prev_df is None or prev_df.empty:
        return {}, {}

    prev_gk = prev_df[prev_df["tipo"] == "GOLEIRO"].reset_index(drop=True)
    prev_pl = prev_df[prev_df["tipo"] != "GOLEIRO"].reset_index(drop=True)

    prev_map_gk = {str(r["jogador"]): i + 1 for i, r in prev_gk.iterrows()}
    prev_map_pl = {str(r["jogador"]): i + 1 for i, r in prev_pl.iterrows()}
    return prev_map_gk, prev_map_pl

# Parte 5/10

# ---- PDF helper ----
def _pos_color_for_pdf(pos_num:int):
    if pos_num<=4:   return colors.HexColor("#0b2e4f")
    if pos_num<=6:   return colors.HexColor("#2563eb")
    if pos_num<=12:  return colors.HexColor("#16a34a")
    if pos_num<=14:  return colors.HexColor("#374151")
    return colors.HexColor("#dc2626")

def build_class_pdf_bytes(title:str, subtitle:str, gk_df:pd.DataFrame, pl_df:pd.DataFrame) -> bytes:
    if not HAS_RL:
        return b""
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=24, rightMargin=24, topMargin=28, bottomMargin=28)
    styles = getSampleStyleSheet()
    story = []
    story.append(Paragraph(title, styles["Title"]))
    if subtitle:
        story.append(Paragraph(subtitle, styles["Heading3"]))
    story.append(Spacer(1, 8))

    def _df_to_table(df: pd.DataFrame, header: str):
        story.append(Spacer(1, 6))
        story.append(Paragraph(header, styles["Heading2"]))

        if df is None or df.empty:
            story.append(Paragraph("Sem dados.", styles["Normal"]))
            return

        data = [list(df.columns)] + [list(map(lambda x: str(x), r)) for r in df.to_numpy()]
        tbl = Table(data, repeatRows=1)

        cmds = [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f3f4f6")),
            ("TEXTCOLOR",  (0, 0), (-1, 0), colors.HexColor("#111827")),
            ("FONTNAME",   (0, 0), (-1, 0), "Helvetica-Bold"),
            ("ALIGN",      (0, 0), (-1, 0), "CENTER"),
            ("GRID",       (0, 0), (-1, -1), 0.25, colors.HexColor("#e5e7eb")),
            ("FONTSIZE",   (0, 0), (-1, -1), 9),
            ("LEFTPADDING",(0, 0), (-1, -1), 6),
            ("RIGHTPADDING",(0, 0), (-1, -1), 6),
        ]

        if len(data) > 1:
            cmds.append(("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#fafafa")]))

        try:
            pos_col_idx = df.columns.get_loc("Posição")
            for ridx in range(1, len(data)):
                try:
                    raw = data[ridx][pos_col_idx]
                    n = int(str(raw).replace("º", "").strip())
                    cmds.append(("TEXTCOLOR", (pos_col_idx, ridx), (pos_col_idx, ridx), _pos_color_for_pdf(n)))
                    cmds.append(("FONTNAME",  (pos_col_idx, ridx), (pos_col_idx, ridx), "Helvetica-Bold"))
                except Exception:
                    pass
        except Exception:
            pass

        tbl.setStyle(TableStyle(cmds))
        story.append(tbl)

    _df_to_table(gk_df, "🧤 Goleiros")
    story.append(Spacer(1, 10))
    _df_to_table(pl_df, "🦵 Jogadores (linha)")
    story.append(Spacer(1, 12))
    gen = datetime.now().strftime("%d/%m/%Y %H:%M")
    story.append(Paragraph(f"Geração: {gen}", styles["Normal"]))
    doc.build(story)
    return buf.getvalue()

def _is_bytes(x): return isinstance(x, (bytes, bytearray)) and len(x) > 0

def safe_build_pdf(title:str, subtitle:str, gk_df:pd.DataFrame, pl_df:pd.DataFrame):
    if not HAS_RL:
        return None
    try:
        b = build_class_pdf_bytes(title, subtitle, gk_df, pl_df)
        return b if _is_bytes(b) else None
    except Exception:
        return None

# Parte 6/10

# ------------------------- Query param: visualização só da classificação -------------------------
def render_only_classification_from_params():
    params = st.query_params
    def _qp_get(k, default=""):
        v = params.get(k, default)
        if isinstance(v, list):
            return v[0] if v else default
        return v

    mode = _qp_get("mode", "Todas")
    season_sel = _qp_get("season", "")
    ym = _qp_get("ym", "")  # yyyy-mm

    desc = ""
    period = {"mode":"all"}
    rounds_period = None
    ysel = msel = None

    if mode == "Mês":
        try:
            ysel, msel = map(int, ym.split("-"))
        except Exception:
            today = date.today(); ysel, msel = today.year, today.month
        start = date(ysel, msel, 1)
        last_day = calendar.monthrange(ysel, msel)[1]
        end_date = date(ysel, msel, last_day)
        period = {"mode":"month","start": start.isoformat(), "end": end_date.isoformat()}
        desc = f"{BR_MONTHS[msel]}/{ysel}"
        rounds_period = df_query("SELECT date FROM rounds WHERE date BETWEEN :a AND :b ORDER BY date", {"a": start.isoformat(), "b": end_date.isoformat()})
    elif mode == "Temporada":
        season_sel = normalize_season(season_sel) or str(date.today().year)
        period = {"mode":"season","season": season_sel}
        desc = f"Temporada: {season_sel}"
        rounds_period = df_query("SELECT date FROM rounds WHERE COALESCE(season,'')=:s ORDER BY date", {"s": season_sel})
    else:
        rounds_period = df_query("SELECT date FROM rounds ORDER BY date")

    st.title(f"⚽ {league_name()} — Classificações")
    use_cards = (get_setting("use_cards","1") == "1")
    has_ref = (get_setting("has_referee","0") == "1")
    hide_cards_cols = (not use_cards) and (not has_ref)

    cdf = classificacao_df(period if mode!="Todas" else None)
    if desc: st.caption(desc)
    if cdf.empty:
        st.info("Sem dados para o período selecionado.")
        st.stop()

    prev_map_gk, prev_map_pl = compute_prev_maps(
        rounds_period,
        mode,
        period if mode != "Todas" else {"mode": "window"}
    )

    df_gk = cdf[cdf["tipo"]=="GOLEIRO"].copy()
    df_pl = cdf[cdf["tipo"]!="GOLEIRO"].copy()

    gk_plain = pd.DataFrame(); pl_plain = pd.DataFrame()
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 🧤 Goleiros")
        show_gk = prepare_class_table(df_gk, hide_cards=hide_cards_cols)
        if not show_gk.empty:
            gk_plain, sty = add_delta_and_style(show_gk, prev_map_gk)
            st.dataframe(sty if sty is not None else gk_plain, use_container_width=True, hide_index=True, key="df_gk_only")
        else:
            st.info("Sem goleiros com lançamentos.")
    with col2:
        st.markdown("### 🦵 Jogadores (linha)")
        show_pl = prepare_class_table(df_pl, hide_cards=hide_cards_cols)
        if not show_pl.empty:
            pl_plain, sty = add_delta_and_style(show_pl, prev_map_pl)
            st.dataframe(sty if sty is not None else pl_plain, use_container_width=True, hide_index=True, key="df_pl_only")
        else:
            st.info("Sem jogadores com lançamentos.")

    # Exportar PDF e link "voltar"
    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        if HAS_RL:
            pdf_bytes = safe_build_pdf(f"{league_name()} — Classificações", desc, gk_plain, pl_plain)
            if pdf_bytes:
                st.download_button("📄 Exportar PDF da classificação", data=pdf_bytes,
                                   file_name=f"classificacao_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
                                   mime="application/pdf", key="dl_pdf_only")
            else:
                st.warning("Não foi possível gerar o PDF agora. Verifique se há dados e se o reportlab está instalado.")
        else:
            st.info("Para exportar PDF, instale:  pip install reportlab")
    with c2:
        st.markdown("[↩️ Voltar ao app completo](?)")

    st.stop()

# Se o link pedir só a classificação, renderiza e encerra
_qp = st.query_params
_view = _qp.get("view", "")
if isinstance(_view, list):
    _view = _view[0] if _view else ""
if str(_view).lower() == "classificacao":
    render_only_classification_from_params()

# Parte 7/10

# ---------- Helpers: Caixa ----------
def _season_list():
    s = df_query("SELECT DISTINCT COALESCE(season,'') AS s FROM rounds ORDER BY s")
    seasons = [normalize_season(x) for x in s["s"].astype(str).tolist() if normalize_season(x)]
    if not seasons:
        seasons = [str(date.today().year)]
    return sorted(set(seasons))

def _month_name_pt(m):  # 1..12
    return BR_MONTHS.get(m, f"M{m}")

def _month_bounds(yy, mm:int):
    try:
        y = int(yy)
    except Exception:
        y = date.today().year
    if y < 1900 or y > 2100:
        y = date.today().year
    start = date(y, mm, 1)
    last = calendar.monthrange(y, mm)[1]
    end = date(y, mm, last)
    return start.isoformat(), end.isoformat()

def _get_opening(season:str) -> float:
    try:
        df = df_query("SELECT opening FROM cash_opening WHERE season=:s", {"s": season})
    except Exception:
        df = df_query("SELECT opening FROM cash_opening WHERE season=:s", {"s": season})
    return float(df.iloc[0]["opening"]) if not df.empty else 0.0

def _set_opening(season:str, val:float):
    exec_sql(
        "INSERT INTO cash_opening(season, opening) VALUES(:s,:v) "
        "ON CONFLICT(season) DO UPDATE SET opening=:v",
        {"s": season, "v": float(val)}
    )

def _mensalistas_df():
    return df_query("""
        SELECT
          id AS player_id,
          COALESCE(NULLIF(TRIM(nickname), ''), name) AS Jogador
        FROM players
        WHERE active = 1
          AND COALESCE(plan,'Mensalista') = 'Mensalista'
        ORDER BY Jogador
    """)

def _flags_to_matrix(season:str):
    base = _mensalistas_df()
    for m in range(1,13):
        base[_month_name_pt(m)] = False
    if base.empty:
        return base
    flags = df_query("SELECT player_id, month, paid FROM cash_month_flags WHERE season=:s", {"s": season})
    if not flags.empty:
        for _, r in flags.iterrows():
            pid = int(r["player_id"]); m = int(r["month"]); paid = int(r["paid"])==1
            col = _month_name_pt(m)
            base.loc[base["player_id"]==pid, col] = paid
    return base

def _matrix_to_flags(season:str, edited_df:pd.DataFrame):
    if edited_df.empty: return 0
    saved = 0
    for _, row in edited_df.iterrows():
        pid = int(row["player_id"])
        for m in range(1,13):
            col = _month_name_pt(m)
            paid = 1 if bool(row.get(col, False)) else 0
            exec_sql("""
                INSERT INTO cash_month_flags(season, player_id, month, paid)
                VALUES(:s,:p,:m,:paid)
                ON CONFLICT(season, player_id, month) DO UPDATE SET paid=:paid
            """, {"s": season, "p": pid, "m": m, "paid": paid})
            saved += 1
    return saved

def _rounds_count_in_month(season:str, month:int):
    y = normalize_season(season) or str(date.today().year)
    d1, d2 = _month_bounds(y, month)
    df = df_query("SELECT COUNT(*) AS n FROM rounds WHERE date BETWEEN :a AND :b", {"a": d1, "b": d2})
    return int(df.iloc[0]["n"]) if not df.empty else 0

def _avulso_presencas_in_month(season:str, month:int):
    y = normalize_season(season) or str(date.today().year)
    d1, d2 = _month_bounds(y, month)
    df = df_query("""
      SELECT COUNT(*) AS n
        FROM player_round pr
        JOIN rounds r  ON r.id=pr.round_id
        JOIN players p ON p.id=pr.player_id
       WHERE pr.presence=1
         AND COALESCE(p.plan,'Mensalista')='Avulso'
         AND r.date BETWEEN :a AND :b
    """, {"a": d1, "b": d2})
    return int(df.iloc[0]["n"]) if not df.empty else 0

def _mensalistas_paid_count(season:str, month:int):
    df = df_query("""
      SELECT COUNT(*) AS n
        FROM cash_month_flags f
        JOIN players p ON p.id=f.player_id
       WHERE f.season=:s AND f.month=:m AND f.paid=1
         AND COALESCE(p.plan,'Mensalista')='Mensalista' AND p.active=1
    """, {"s": season, "m": month})
    return int(df.iloc[0]["n"]) if not df.empty else 0

def _cards_counts_in_month(season:str, month:int):
    y = normalize_season(season) or str(date.today().year)
    d1, d2 = _month_bounds(y, month)
    df = df_query("""
      SELECT
         COALESCE(SUM(pr.yellow_cards),0) AS ca,
         COALESCE(SUM(pr.red_cards),0)    AS cv
      FROM player_round pr
      JOIN rounds r ON r.id=pr.round_id
      WHERE r.date BETWEEN :a AND :b
    """, {"a": d1, "b": d2})
    if df.empty:
        return 0, 0
    return int(df.iloc[0]["ca"] or 0), int(df.iloc[0]["cv"] or 0)

def _cash_extra_month(season:str, month:int):
    y = normalize_season(season) or str(date.today().year)
    d1, d2 = _month_bounds(y, month)
    df = df_query("""
      SELECT COALESCE(type,'') AS type, COALESCE(value,0) AS value
        FROM cash_extra
       WHERE season=:s AND date BETWEEN :a AND :b
    """, {"s": normalize_season(season) or str(date.today().year), "a": d1, "b": d2})
    entradas = float(df[df["type"]=="Entrada"]["value"].sum()) if not df.empty else 0.0
    saidas   = float(df[df["type"]=="Saída"]["value"].sum()) if not df.empty else 0.0
    return entradas, saidas

def _month_summary(season:str, month:int):
    monthly_fee = float(get_setting("monthly_fee","0") or 0)
    single_fee  = float(get_setting("single_fee","0") or 0)
    rent        = float(get_setting("rent_court","0") or 0)   # por mês
    ref_fee     = float(get_setting("referee_fee","0") or 0)  # por rodada
    has_ref     = (get_setting("has_referee","0")=="1")
    yc_fee      = float(get_setting("yellow_card_fee","0") or 0)
    rc_fee      = float(get_setting("red_card_fee","0") or 0)

    m_mensalistas = _mensalistas_paid_count(season, month)
    m_avulsos     = _avulso_presencas_in_month(season, month)
    rounds_m      = _rounds_count_in_month(season, month)
    extra_in, extra_out = _cash_extra_month(season, month)

    ca, cv = _cards_counts_in_month(season, month)
    cards_income = (ca * yc_fee) + (cv * rc_fee)

    entradas = {
        "Mensalidade": m_mensalistas * monthly_fee,
        "Avulsos":     m_avulsos * single_fee,
        "Cartões (Entrada)": cards_income,
        "Extras (Entrada)": extra_in,
    }
    saidas = {
        "Aluguel da quadra": rent,
        "Juiz": (ref_fee * rounds_m) if has_ref else 0.0,
        "Extras (Saída)": extra_out,
    }
    tot_in = sum(entradas.values())
    tot_out = sum(saidas.values())
    saldo = tot_in - tot_out
    return entradas, saidas, tot_in, tot_out, saldo

def _season_running_balance(season:str, up_to_month:int):
    opening = _get_opening(season)
    acc = 0.0
    for m in range(1, up_to_month+1):
        _, _, _, _, saldo = _month_summary(season, m)
        acc += saldo
    return opening + acc

# -------------- UI ---------------
st.title(f"⚽ {league_name()}")

# Barra superior: usuário logado e sair
with st.sidebar:
    st.markdown(f"**👤 Usuário:** {st.session_state.get('auth_user','-')}")
    if st.button("Sair", use_container_width=True):
        st.session_state.pop("auth_user", None)
        st.rerun()

# Parte 8/10

tabs = st.tabs([
    "🏟 Pelada", "👤 Jogadores", "🎲 Presença/Sorteio", "📆 Rodadas & Times",
    "🏆 Classificações", "🛠 Admin (Dados)", "💰 Caixa"
])

# ---- Pelada ----
with tabs[0]:
    st.subheader("Configurações gerais")
    with st.form("pelada_form"):
        ln = st.text_input("Nome da Pelada", value=get_setting("league_name","Pelada do Pivete"), key="ln_set")
        loc = st.text_input("Local da Pelada", value=get_setting("league_location",""), key="loc_set")
        pix = st.text_input("Pix (recebimento)", value=get_setting("pix_key",""), key="pix_set")

        c1,c2 = st.columns(2)
        monthly = c1.number_input("Valor Mensal (R$)", min_value=0.0, step=1.0, value=float(get_setting("monthly_fee","0") or 0), key="mfee")
        single  = c2.number_input("Valor Avulso (R$)", min_value=0.0, step=1.0, value=float(get_setting("single_fee","0") or 0), key="sfee")
        c3,c4 = st.columns(2)
        rent   = c3.number_input("Aluguel da Quadra (R$/mês)", min_value=0.0, step=1.0, value=float(get_setting("rent_court","0") or 0), key="rent")
        refval = c4.number_input("Valor do Juiz por pelada (R$)", min_value=0.0, step=1.0, value=float(get_setting("referee_fee","0") or 0), key="reffee")
        c5,c6 = st.columns(2)
        has_ref = c5.checkbox("Tem Juiz?", value=(get_setting("has_referee","0")=="1"), key="hasref")
        use_cards = c6.checkbox("Aplica Cartões Amarelos e Vermelhos?", value=(get_setting("use_cards","1")=="1"), key="cards")

        c7, c8 = st.columns(2)
        yc_fee = c7.number_input("Valor do Cartão Amarelo (R$)", min_value=0.0, step=1.0, value=float(get_setting("yellow_card_fee","0") or 0), key="ycfee")
        rc_fee = c8.number_input("Valor do Cartão Vermelho (R$)", min_value=0.0, step=1.0, value=float(get_setting("red_card_fee","0") or 0), key="rcfee")

        players_per_team = st.number_input("Quantidade de jogadores de linha por time", min_value=1, max_value=15, step=1, value=int(get_setting("players_per_team_line", "5") or 5), key="ppt")

        if st.form_submit_button("Salvar configurações"):
            set_setting("league_name", ln.strip() or "Pelada")
            set_setting("league_location", loc.strip())
            set_setting("pix_key", pix.strip())
            set_setting("monthly_fee", monthly)
            set_setting("single_fee", single)
            set_setting("rent_court", rent)
            set_setting("referee_fee", refval)
            set_setting("has_referee", "1" if has_ref else "0")
            set_setting("use_cards", "1" if use_cards else "0")
            set_setting("yellow_card_fee", yc_fee)
            set_setting("red_card_fee", rc_fee)
            set_setting("players_per_team_line", int(players_per_team))
            st.success("Configurações salvas! Atualize a página para refletir o nome no topo.")

# ---- Jogadores ----
with tabs[1]:
    st.subheader("Cadastro individual")

    c1, c2 = st.columns(2)
    name = c1.text_input("Nome", value="", key="add_name")
    nickname = c2.text_input("Apelido (opcional)", value="", key="add_nick")

    c3, c4, c5 = st.columns(3)
    pos = c3.selectbox("Posição", ["ATA", "MEIA", "ZAG", "GOL"], key="add_pos")
    plan = c4.selectbox("Plano", ["Mensalista", "Avulso"], key="add_plan")
    table_label = "Goleiros" if pos == "GOL" else "Jogadores"
    c5.text_input("Tabela", value=table_label, disabled=True, key="add_table")

    active = st.checkbox("Ativo?", value=True, key="add_active")

    if st.button("Salvar jogador", key="add_save_btn"):
        try:
            role = "GOLEIRO" if pos == "GOL" else "JOGADOR"
            nk_final = nickname.strip() if nickname.strip() else name.strip()
            exec_sql(
                "INSERT INTO players(name, nickname, position, role, is_goalkeeper, plan, active) "
                "VALUES(:n,:nk,:pos,:role,:gk,:plan,:act) "
                "ON CONFLICT(name) DO UPDATE SET nickname=:nk, position=:pos, role=:role, "
                "is_goalkeeper=:gk, plan=:plan, active=:act",
                {
                    "n": name.strip(),
                    "nk": nk_final,
                    "pos": pos,
                    "role": role,
                    "gk": 1 if role == "GOLEIRO" else 0,
                    "plan": plan,
                    "act": 1 if active else 0,
                },
            )
            exec_sql("UPDATE players SET nickname = name WHERE nickname IS NULL OR TRIM(nickname) = ''")
            st.success("Jogador salvo!")
        except Exception as e:
            st.error(f"Erro ao salvar: {e}")

    st.divider()
    st.markdown("### 📥 Importar jogadores (CSV/Excel)")

    players_tpl = pd.DataFrame({
        "Nome do Jogador": ["Fulano da Silva", "Beltrano Souza"],
        "Apelido": ["Fulano", "Bel"],
        "Posição do Jogador": ["ATA", "GOL"],
        "Plano": ["Mensalista", "Avulso"],
        "Tabela": ["Jogadores", "Goleiros"]
    })

    c_tpl1, c_tpl2 = st.columns(2)
    with c_tpl1:
        st.download_button(
            "⬇️ Modelo CSV (jogadores)",
            data=players_tpl.to_csv(index=False).encode("utf-8"),
            file_name="modelo_jogadores.csv",
            mime="text/csv",
            key="tpl_players_csv"
        )
    with c_tpl2:
        st.download_button(
            "⬇️ Modelo Excel (jogadores)",
            data=to_xlsx_bytes(players_tpl, "Jogadores"),
            file_name="modelo_jogadores.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="tpl_players_xlsx"
        )

    up_players = st.file_uploader(
        "Upload CSV/XLSX de jogadores", type=["csv", "xlsx", "xls"], key="players_import_uploader"
    )

    if up_players is not None:
        try:
            dfimp = (
                pd.read_excel(up_players)
                if up_players.name.lower().endswith((".xlsx", ".xls"))
                else pd.read_csv(up_players)
            )
            res = import_players_df(dfimp)
            st.success(
                f"Importação concluída. Linhas lidas: {res.get('linhas', 0)} · "
                f"Gravadas/atualizadas: {res.get('gravadas', 0)}"
            )
        except Exception as e:
            st.error(f"Erro ao importar jogadores: {e}")


# Parte 9/10

# ---- Presença/Sorteio ----
with tabs[2]:
    st.subheader("Presença & Sorteio")

    # Data/Temporada
    c1, c2 = st.columns([1, 1])
    rdate = c1.date_input("Data da rodada", value=date.today(), key="draw_date")
    season_in = c2.text_input("Temporada (ex.: 2025)", value=str(date.today().year), key="draw_season")
    season_norm = normalize_season(season_in) or str(date.today().year)

    # Carregar jogadores ativos
    jdf = df_query("""
        SELECT id AS player_id,
               COALESCE(NULLIF(TRIM(nickname), ''), name) AS nome,
               CASE WHEN (role='GOLEIRO' OR is_goalkeeper=1) THEN 1 ELSE 0 END AS gk
        FROM players
        WHERE active=1
        ORDER BY nome
    """)
    if jdf.empty:
        st.info("Cadastre jogadores ativos primeiro.")
    else:
        # Mapa id->nome
        id2name = {int(r.player_id): str(r.nome) for r in jdf.itertuples(index=False)}

        st.markdown("#### Selecionados para a rodada")

        sel_key = "presence_selected"
        if sel_key not in st.session_state:
            st.session_state[sel_key] = []

        with st.form("presence_select_form", clear_on_submit=False):
            sel_players_tmp = st.multiselect(
                "Jogadores",
                options=jdf["player_id"].astype(int).tolist(),
                format_func=lambda i: id2name.get(int(i), f"#{i}"),
                default=st.session_state[sel_key],
                key="presence_multiselect",
                placeholder="Digite o nome/apelido e tecle Enter"
            )
            ok_presence = st.form_submit_button("Atualizar seleção")
            if ok_presence:
                st.session_state[sel_key] = list(sel_players_tmp)

        selected_ids = st.session_state[sel_key]
        if selected_ids:
            nomes = [id2name.get(int(i), f"#{i}") for i in selected_ids]
            st.caption(f"**{len(selected_ids)} selecionado(s):** " + ", ".join(nomes))
        else:
            st.caption("_Nenhum jogador selecionado ainda._")

        if st.button("🎲 Sortear Times", key="btn_sortear"):
            chosen = jdf[jdf["player_id"].isin(selected_ids)]
            if chosen.empty:
                st.warning("Selecione pelo menos 1 jogador.")
            else:
                line_ids = chosen[chosen["gk"] == 0]["player_id"].astype(int).tolist()
                gk_ids   = chosen[chosen["gk"] == 1]["player_id"].astype(int).tolist()

                random.shuffle(line_ids)
                random.shuffle(gk_ids)

                per_team = int(get_setting("players_per_team_line", "5") or 5)
                n_teams = max(1, math.ceil(len(line_ids) / per_team))

                teams = {i + 1: [] for i in range(n_teams)}

                for i, pid in enumerate(line_ids):
                    t = (i // per_team) + 1
                    if t > n_teams:
                        t = n_teams
                    teams[t].append(pid)

                team_has_gk = {k: False for k in teams.keys()}
                unassigned_gk = []
                for i, gid in enumerate(gk_ids):
                    t = (i % n_teams) + 1
                    if not team_has_gk[t]:
                        teams[t].append(gid)
                        team_has_gk[t] = True
                    else:
                        unassigned_gk.append(gid)

                st.session_state["draw_preview"] = {
                    "date": rdate.isoformat(),
                    "season": season_norm,
                    "teams": teams,
                    "selected": list(map(int, selected_ids)),
                    "unassigned_gk": unassigned_gk
                }
                st.success("Times sorteados! Veja a prévia abaixo.")

        dr = st.session_state.get("draw_preview")
        if dr and dr.get("date") == rdate.isoformat():
            st.markdown("### Prévia do sorteio")
            cols = st.columns(len(dr["teams"]))
            for idx, (tno, ids) in enumerate(sorted(dr["teams"].items())):
                with cols[idx]:
                    st.markdown(f"**Time {tno}**")
                    if not ids:
                        st.write("- (vazio)")
                    else:
                        for pid in ids:
                            base = jdf[jdf["player_id"] == int(pid)]
                            nm = str(base.iloc[0]["nome"]) if not base.empty else f"#{pid}"
                            is_gk = int(base.iloc[0]["gk"]) if not base.empty else 0
                            st.write(("🧤 " if is_gk == 1 else "• ") + nm)

            if dr["unassigned_gk"]:
                names = [id2name.get(int(x), f"#{x}") for x in dr["unassigned_gk"]]
                st.warning("Goleiros sem time: " + ", ".join(names))

        st.divider()
        if st.button("🧾 Criar/abrir rodada", key="btn_create_round_presence"):
            rid, created = get_or_create_round_by_date(
                rdate.isoformat(),
                season=season_norm,
                four_gk_default=False
            )
            exec_sql("UPDATE rounds SET season=:s WHERE id=:r", {"s": season_norm, "r": rid})

            dr = st.session_state.get("draw_preview") or {}
            if dr.get("date") == rdate.isoformat() and "teams" in dr:
                team_id_map = {}
                for tno in sorted(dr["teams"].keys()):
                    team_id_map[tno] = get_or_create_team_round(rid, f"Time {tno}")

                assigned = set()
                for tno, ids in sorted(dr["teams"].items()):
                    tid = int(team_id_map[tno])
                    for pid in ids:
                        pid = int(pid)
                        assigned.add(pid)
                        try:
                            exec_sql(
                                "INSERT INTO player_round(round_id, player_id, presence, team_round_id) "
                                "VALUES(:r,:p,1,:t)",
                                {"r": rid, "p": pid, "t": tid},
                            )
                        except Exception:
                            exec_sql(
                                "UPDATE player_round SET presence=1, team_round_id=:t "
                                "WHERE round_id=:r AND player_id=:p",
                                {"r": rid, "p": pid, "t": tid},
                            )

                selected_set = set(int(x) for x in (dr.get("selected") or []))
                leftovers = sorted(list(selected_set - assigned))
                for pid in leftovers:
                    try:
                        exec_sql(
                            "INSERT INTO player_round(round_id, player_id, presence) VALUES(:r,:p,1)",
                            {"r": rid, "p": int(pid)}
                        )
                    except Exception:
                        exec_sql(
                            "UPDATE player_round SET presence=1 WHERE round_id=:r AND player_id=:p",
                            {"r": rid, "p": int(pid)}
                        )

                recalc_round(rid)
                generate_round_notes_sequence()

                st.success(
                    f"Rodada #{rid} {'criada' if created else 'aberta'} para {rdate.isoformat()} "
                    f"(temporada {season_norm}) e times/vínculos aplicados."
                )
                st.info("Agora, em 📆 Rodadas & Times, os jogadores já aparecem nos respectivos times.")
            else:
                st.success(f"Rodada #{rid} {'criada' if created else 'aberta'}.")
                st.info("Você ainda não sorteou times para esta data — faça o sorteio e clique novamente para vincular os jogadores.")

# ---- Rodadas & Times ----
with tabs[3]:
    st.subheader("Rodadas & Times")

    with st.form("form_round_new", clear_on_submit=True):
        c1, c2 = st.columns([1, 1])
        rdate = c1.date_input("Data da rodada", value=date.today(), key="rdate")
        season = c2.text_input("Temporada", value=str(date.today().year), key="season_input")
        if st.form_submit_button("Criar/abrir rodada"):
            rid, _ = get_or_create_round_by_date(
                rdate.isoformat(),
                season=normalize_season(season),
                four_gk_default=False
            )
            recalc_round(rid)
            generate_round_notes_sequence()
            st.success("Rodada ok!")

    rounds = df_query("SELECT id, date, season, notes, closed, four_goalkeepers FROM rounds ORDER BY date DESC, id DESC")
    if rounds.empty:
        st.info("Cadastre ao menos uma rodada.")
        st.stop()

    rid = st.selectbox("Selecione a rodada", options=rounds["id"].tolist(),
                       format_func=lambda x: f"#{x}", key="rid_sel")

    if st.button("🔁 Recalcular esta rodada", key=f"recalc_round_quick_{rid}"):
        recalc_round(rid)
        st.success("Rodada recalculada!")

    st.markdown("### Times da Rodada (sorteio) — visão horizontal")

    teams = df_query(
        "SELECT id, name, wins, draws FROM teams_round WHERE round_id=:r ORDER BY name",
        {"r": rid}
    )

    pr_team = df_query("""
        SELECT pr.team_round_id AS team_id,
               COALESCE(p.nickname,p.name) AS nome,
               CASE WHEN (p.role='GOLEIRO' OR p.is_goalkeeper=1) THEN 1 ELSE 0 END AS gk
        FROM player_round pr
        JOIN players p ON p.id=pr.player_id
        WHERE pr.round_id=:r AND pr.team_round_id IS NOT NULL
        ORDER BY nome
    """, {"r": rid})

    cols = st.columns(4)
    save_team_vals = []
    for i, t in enumerate(teams.itertuples(index=False)):
        with cols[i % 4]:
            st.caption(t.name)

            lst = pr_team[pr_team["team_id"] == t.id]
            if lst.empty:
                st.write("- (sem jogadores)")
            else:
                for _, row in lst.iterrows():
                    tag = "🧤 " if int(row["gk"]) == 1 else "• "
                    st.write(tag + str(row["nome"]))

            w = st.number_input(f"Vitórias — {t.name}", min_value=0, step=1, value=int(t.wins), key=f"gw_{t.id}")
            d = st.number_input(f"Empates — {t.name}", min_value=0, step=1, value=int(t.draws), key=f"gd_{t.id}")
            save_team_vals.append((int(t.id), int(w), int(d)))

    if st.button("💾 Salvar vitórias/empates dos times", key=f"save_team_tot_{rid}"):
        for tid, w, d in save_team_vals:
            pts = calc_points(w, d)
            exec_sql("UPDATE teams_round SET wins=:w, draws=:d, points=:p WHERE id=:id",
                     {"w": w, "d": d, "p": pts, "id": tid})
        recalc_round(rid)
        st.success("Times atualizados!")

    # Seções extras da aba 📆

    n_teams = len(teams)

    n_gks_round = int(
        df_query("""
            SELECT COUNT(*) AS n
              FROM player_round pr
              JOIN players p ON p.id=pr.player_id
             WHERE pr.round_id=:r
               AND (p.role='GOLEIRO' OR p.is_goalkeeper=1)
        """, {"r": rid}).iloc[0]["n"] or 0
    )

    if n_teams > 0 and n_gks_round < n_teams:
        st.divider()
        st.markdown("### 🧤 Goleiros — vitórias & empates (individual)")

        df_g = df_query("""
            SELECT pr.id, p.id AS player_id, COALESCE(p.nickname,p.name) AS goleiro,
                   COALESCE(pr.wins,0) AS vitorias, COALESCE(pr.draws,0) AS empates
            FROM player_round pr
            JOIN players p ON p.id=pr.player_id
            WHERE pr.round_id=:r AND (p.role='GOLEIRO' OR p.is_goalkeeper=1)
            ORDER BY goleiro
        """, {"r": rid})

        current_gk = []
        if not df_g.empty:
            gcols = st.columns(2)
            half = (len(df_g) + 1) // 2
            for idx, row in enumerate(df_g.itertuples(index=False)):
                col = gcols[0] if idx < half else gcols[1]
                with col:
                    st.write(f"**{row.goleiro}**")
                    v = st.number_input("Vitórias", min_value=0, step=1, value=int(row.vitorias),
                                        key=f"gk_v_{rid}_{row.id}")
                    e = st.number_input("Empates", min_value=0, step=1, value=int(row.empates),
                                        key=f"gk_e_{rid}_{row.id}")
                    current_gk.append((int(row.id), int(v), int(e)))

            if st.button("💾 Salvar goleiros (existentes)", key=f"save_gk_exist_{rid}"):
                for pr_id, v, e in current_gk:
                    exec_sql("""UPDATE player_round
                                   SET wins=:w, draws=:d, points=:p, individual_override=1
                                 WHERE id=:id""",
                             {"w": v, "d": e, "p": calc_points(v, e), "id": pr_id})
                recalc_round(rid)
                st.success("Goleiros atualizados!")

        current_gk_ids = set(df_g["player_id"].tolist()) if not df_g.empty else set()
        gk_all = df_query("""
            SELECT id, COALESCE(nickname,name) AS nome
            FROM players
            WHERE active=1 AND (role='GOLEIRO' OR is_goalkeeper=1)
            ORDER BY nome
        """)
        remaining = gk_all[~gk_all["id"].isin(list(current_gk_ids))]
        if not remaining.empty:
            st.caption("Adicionar goleiros que não foram lançados nesta rodada:")
            add_count = min(len(teams) - len(current_gk_ids), len(remaining)) if len(teams) > len(current_gk_ids) else len(remaining)
            add_count = max(0, add_count)
            for k in range(add_count):
                c1, c2, c3 = st.columns([2, 1, 1])
                new_gk = c1.selectbox(
                    f"Goleiro novo {k+1}",
                    options=remaining["id"].tolist(),
                    format_func=lambda i: remaining.loc[remaining["id"] == i, "nome"].iloc[0],
                    key=f"new_gk_sel_{rid}_{k}"
                )
                v = c2.number_input("Vitórias", min_value=0, step=1, value=0, key=f"new_gk_v_{rid}_{k}")
                e = c3.number_input("Empates", min_value=0, step=1, value=0, key=f"new_gk_e_{rid}_{k}")
                if st.button("➕ Adicionar goleiro", key=f"btn_add_gk_{rid}_{k}"):
                    save_gk_individual(int(rid), int(new_gk), int(v), int(e))
                    recalc_round(int(rid))
                    st.success("Goleiro adicionado. Atualize a seção para ver na lista.")

    st.divider()
    st.markdown("### 🟨🟥 Cartões — seleção e lançamento")

    round_players = df_query("""
            SELECT p.id, COALESCE(p.nickname,p.name) AS nome,
                   COALESCE(pr.yellow_cards,0) AS ca,
                   COALESCE(pr.red_cards,0) AS cv,
                   pr.id AS pr_id
            FROM player_round pr
            JOIN players p ON p.id=pr.player_id
            WHERE pr.round_id=:r
            ORDER BY nome
        """, {"r": rid})

    if round_players.empty:
        st.info("Nenhum jogador lançado nesta rodada para registrar cartões.")
    else:
        st.caption("Pesquise e selecione quem deseja lançar/editar cartões:")
        rp_options = round_players["id"].tolist()
        rp_format = {int(r["id"]): str(r["nome"]) for _, r in round_players.iterrows()}

        sel_key = f"card_sel_{rid}"
        if sel_key not in st.session_state:
            st.session_state[sel_key] = []

        with st.form(f"card_select_form_{rid}", clear_on_submit=False):
            sel_cards_tmp = st.multiselect(
                "Jogadores",
                options=rp_options,
                format_func=lambda i: rp_format.get(int(i), f"#{i}"),
                default=st.session_state[sel_key],
                key=f"card_selector_{rid}",
                placeholder="Digite o nome/apelido e tecle Enter"
            )
            ok_cards = st.form_submit_button("Atualizar seleção")
            if ok_cards:
                st.session_state[sel_key] = list(sel_cards_tmp)

        sel_cards = st.session_state[sel_key]

        if not sel_cards:
            st.caption("_Nenhum jogador selecionado para cartões._")
        else:
            sub = round_players[round_players["id"].isin(sel_cards)].copy()

            edited_vals = []
            kcols = st.columns(2)
            half = (len(sub) + 1) // 2
            for idx, row in enumerate(sub.itertuples(index=False)):
                col = kcols[0] if idx < half else kcols[1]
                with col:
                    st.write(f"**{row.nome}**")
                    ca = st.number_input("CA", min_value=0, step=1, value=int(row.ca), key=f"ca_{rid}_{row.pr_id}")
                    cv = st.number_input("CV", min_value=0, step=1, value=int(row.cv), key=f"cv_{rid}_{row.pr_id}")
                    edited_vals.append((int(row.pr_id), int(ca), int(cv)))

            if st.button("💾 Salvar cartões selecionados", key=f"save_cards_sel_{rid}"):
                for pr_id, ca, cv in edited_vals:
                    exec_sql(
                        "UPDATE player_round SET yellow_cards=:a, red_cards=:v WHERE id=:id",
                        {"a": ca, "v": cv, "id": pr_id}
                    )
                st.success("Cartões salvos para os jogadores selecionados.")

    st.divider()
    st.markdown("### Lista de Rodadas")
    st.dataframe(rounds, use_container_width=True, hide_index=True, key="rounds_table_guided_v2")

# Parte 10/10

# ---- Classificações ----
with tabs[4]:
    st.subheader("Classificações")
    if st.button("🔄 Atualizar tudo", key="btn_update_all"):
        recalc_all_rounds(close_all=True, regen_notes=True)
        st.success("Tudo recalculado e rodadas fechadas.")

    mode = st.selectbox("Período", ["Todas","Temporada","Mês","Rodada"], index=2, key="per_mode")
    period = {"mode":"all"}
    desc = ""
    rounds_period = None
    ysel = msel = None
    season_sel = None
    round_sel_id = None

    if mode == "Mês":
        months_df = df_query("SELECT DISTINCT substr(date,1,7) as ym FROM rounds WHERE date IS NOT NULL ORDER BY ym DESC")
        months = [(int(str(ym)[:4]), int(str(ym)[5:7])) for ym in months_df["ym"].dropna().tolist()] if not months_df.empty else []
        if not months:
            today = date.today(); months = [(today.year, today.month)]
        labels = [f"{BR_MONTHS[m]}/{y}" for (y,m) in months]
        sel_idx = st.selectbox("Mês de referência", options=list(range(len(months))), index=0, format_func=lambda i: labels[i], key="month_dd")
        ysel, msel = months[sel_idx]
        start = date(ysel, msel, 1)
        last_day = calendar.monthrange(ysel, msel)[1]
        end_date = date(ysel, msel, last_day)
        period = {"mode":"month","start": start.isoformat(), "end": end_date.isoformat()}
        desc = f"{BR_MONTHS[msel]}/{ysel}"
        rounds_period = df_query("SELECT date FROM rounds WHERE date BETWEEN :a AND :b ORDER BY date", {"a": start.isoformat(), "b": end_date.isoformat()})

    elif mode == "Temporada":
        seasons = df_query("SELECT DISTINCT COALESCE(season,'') AS season FROM rounds ORDER BY season")['season'].fillna('').astype(str).tolist()
        seasons = [normalize_season(s) for s in seasons if (normalize_season(s) or '')!='']
        season_sel = st.selectbox("Temporada", options=seasons if seasons else [str(date.today().year)], key="season_sel")
        period = {"mode":"season","season": season_sel}
        desc = f"Temporada: {season_sel}"
        rounds_period = df_query("SELECT date FROM rounds WHERE COALESCE(season,'')=:s ORDER BY date", {"s": season_sel})

    elif mode == "Rodada":
        rlist = df_query("SELECT id, date, COALESCE(notes,'') AS notes FROM rounds ORDER BY date")
        if rlist.empty:
            st.info("Sem rodadas cadastradas.")
            rounds_period = pd.DataFrame(columns=["date"])
        else:
            def _lab(i, row):
                n = i + 1
                tag = str(row["notes"]).strip() or f"{n}ª"
                return f"{tag} — {row['date']}"
            options = list(range(len(rlist)))
            round_idx = st.selectbox("Rodada (classificação até esta rodada)", options=options,
                                     format_func=lambda i: _lab(i, rlist.iloc[i]), index=len(options)-1, key="round_idx_sel")
            round_sel_id = int(rlist.iloc[round_idx]["id"])
            round_sel_date = str(rlist.iloc[round_idx]["date"])
            first_date = str(rlist.iloc[0]["date"])
            period = {"mode": "window", "start": first_date, "end": round_sel_date}
            desc = f"Até a { _lab(round_idx, rlist.iloc[round_idx]) }"
            rounds_period = df_query("SELECT date FROM rounds WHERE date BETWEEN :a AND :b ORDER BY date",
                                     {"a": first_date, "b": round_sel_date})

    else:
        rounds_period = df_query("SELECT date FROM rounds ORDER BY date")

    use_cards = (get_setting("use_cards","1") == "1")
    has_ref = (get_setting("has_referee","0") == "1")
    hide_cards_cols = (not use_cards) and (not has_ref)

    cdf = classificacao_df(period if mode!="Todas" else None)
    if desc: st.caption(desc)
    if cdf.empty:
        st.info("Sem dados para o período selecionado.")
    else:
        prev_map_gk, prev_map_pl = compute_prev_maps(
            rounds_period,
            mode,
            period if mode != "Todas" else {"mode": "window"}
        )

        df_gk = cdf[cdf["tipo"]=="GOLEIRO"].copy()
        df_pl = cdf[cdf["tipo"]!="GOLEIRO"].copy()

        gk_plain = pd.DataFrame(); pl_plain = pd.DataFrame()
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### 🧤 Goleiros")
            show_gk = prepare_class_table(df_gk, hide_cards=hide_cards_cols)
            if not show_gk.empty:
                gk_plain, sty = add_delta_and_style(show_gk, prev_map_gk)
                st.dataframe(sty if sty is not None else gk_plain, use_container_width=True, hide_index=True, key="df_gk")
            else:
                st.info("Sem goleiros com lançamentos.")
        with col2:
            st.markdown("### 🦵 Jogadores (linha)")
            show_pl = prepare_class_table(df_pl, hide_cards=hide_cards_cols)
            if not show_pl.empty:
                pl_plain, sty = add_delta_and_style(show_pl, prev_map_pl)
                st.dataframe(sty if sty is not None else pl_plain, use_container_width=True, hide_index=True, key="df_pl")
            else:
                st.info("Sem jogadores com lançamentos.")

        st.divider()
        cc1, cc2 = st.columns(2)
        with cc1:
            if HAS_RL:
                pdf_bytes = safe_build_pdf(f"{league_name()} — Classificações", desc, gk_plain, pl_plain)
                if pdf_bytes:
                    st.download_button("📄 Exportar PDF da classificação", data=pdf_bytes,
                                       file_name=f"classificacao_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
                                       mime="application/pdf", key="dl_pdf_full")
                else:
                    st.warning("Não foi possível gerar o PDF agora. Verifique se há dados e se o reportlab está instalado.")
            else:
                st.info("Para exportar PDF, instale:  pip install reportlab")
        with cc2:
            share_params = {"view":"classificacao","mode":mode}
            if mode == "Mês" and ysel and msel:
                share_params["ym"] = f"{ysel}-{msel:02d}"
            if mode == "Temporada" and season_sel:
                share_params["season"] = normalize_season(season_sel)
            qs = urllib.parse.urlencode(share_params)
            st.markdown(f"[🔗 Abrir só a Classificação desta visão](?{qs})")
            st.caption("Use o link acima para compartilhar somente a Classificação (respeita o filtro atual).")

# ---- Admin (Dados) ----
with tabs[5]:
    st.subheader("Admin (Dados)")
    st.caption("Importe planilhas das rodadas, inclua lançamentos manualmente e edite as tabelas. Depois, recalcule quando necessário.")

    # ============================
    # 1) IMPORTADORES DE RODADAS
    # ============================
    with st.expander("📥 Importar planilhas das rodadas", expanded=False):

        st.markdown("#### 1. Times por Data (Vitórias/Empates)")
        st.caption("Colunas esperadas (qualquer ordem): **Data**, **Temporada** (opcional), **Time**, **Vitórias**, **Empates**.")
        ex_times = pd.DataFrame({
            "Data": ["12/01/2025","12/01/2025","19/01/2025","19/01/2025"],
            "Temporada": ["2025","2025","2025","2025"],
            "Time": ["Time 1","Time 2","Time 1","Time 2"],
            "Vitórias": [2,1,1,0],
            "Empates":  [0,1,2,1],
        })
        cA1, cA2 = st.columns(2)
        with cA1:
            st.download_button("⬇️ Modelo CSV (Times por Data)", data=ex_times.to_csv(index=False).encode("utf-8"),
                               file_name="modelo_times_por_data.csv", mime="text/csv", key="tpl_times_csv")
        with cA2:
            st.download_button("⬇️ Modelo Excel (Times por Data)", data=to_xlsx_bytes(ex_times, "Times"),
                               file_name="modelo_times_por_data.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="tpl_times_xlsx")
        up_times = st.file_uploader("Upload CSV/XLSX — Times por Data", type=["csv","xlsx","xls"], key="up_times_per_date")
        if up_times is not None:
            try:
                dfimp = pd.read_excel(up_times) if up_times.name.lower().endswith((".xlsx",".xls")) else pd.read_csv(up_times)
                res = import_times_table(dfimp)
                if isinstance(res, dict) and res.get("error"):
                    st.error(res["error"])
                else:
                    st.success("Times importados com sucesso. (Vitórias/Empates por data aplicados)")
            except Exception as e:
                st.error(f"Erro ao importar Times por Data: {e}")

        st.markdown("---")
        st.markdown("#### 2. Vínculo Jogador ↔ Data ↔ Time")
        st.caption("Colunas esperadas (qualquer ordem): **Data**, **Nome** (ou **Jogador**), **Time** (ex.: Time 1, Time 2).")
        ex_links = pd.DataFrame({
            "Data": ["12/01/2025","12/01/2025","12/01/2025","19/01/2025"],
            "Nome": ["Fulano","Beltrano","Ciclano","Fulano"],
            "Time": ["Time 1","Time 1","Time 2","Time 2"],
        })
        cB1, cB2 = st.columns(2)
        with cB1:
            st.download_button("⬇️ Modelo CSV (Vínculos Jogador-Data-Time)", data=ex_links.to_csv(index=False).encode("utf-8"),
                               file_name="modelo_vinculos.csv", mime="text/csv", key="tpl_links_csv")
        with cB2:
            st.download_button("⬇️ Modelo Excel (Vínculos Jogador-Data-Time)", data=to_xlsx_bytes(ex_links, "Vinculos"),
                               file_name="modelo_vinculos.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="tpl_links_xlsx")
        up_links = st.file_uploader("Upload CSV/XLSX — Vínculos Jogador-Data-Time", type=["csv","xlsx","xls"], key="up_links")
        if up_links is not None:
            try:
                dfimp = pd.read_excel(up_links) if up_links.name.lower().endswith((".xlsx",".xls")) else pd.read_csv(up_links)
                res = import_player_links(dfimp)
                st.success(f"Vínculos importados. Linhas processadas: {res.get('rows',0)} · Jogadores não encontrados: {res.get('missing_players',0)}")
            except Exception as e:
                st.error(f"Erro ao importar vínculos: {e}")

        st.markdown("---")
        st.markdown("#### 3. Cartões por Data")
        st.caption("Colunas esperadas (qualquer ordem): **Data**, **Jogador** (ou **Nome**), **CA**, **CV**.")
        ex_cards = pd.DataFrame({
            "Data": ["12/01/2025","12/01/2025","19/01/2025"],
            "Jogador": ["Fulano","Beltrano","Fulano"],
            "CA": [1,0,0],
            "CV": [0,1,0],
        })
        cC1, cC2 = st.columns(2)
        with cC1:
            st.download_button("⬇️ Modelo CSV (Cartões)", data=ex_cards.to_csv(index=False).encode("utf-8"),
                               file_name="modelo_cartoes.csv", mime="text/csv", key="tpl_cards_csv")
        with cC2:
            st.download_button("⬇️ Modelo Excel (Cartões)", data=to_xlsx_bytes(ex_cards, "Cartoes"),
                               file_name="modelo_cartoes.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="tpl_cards_xlsx")
        up_cards = st.file_uploader("Upload CSV/XLSX — Cartões por Data", type=["csv","xlsx","xls"], key="up_cards")
        if up_cards is not None:
            try:
                dfimp = pd.read_excel(up_cards) if up_cards.name.lower().endswith((".xlsx",".xls")) else pd.read_csv(up_cards)
                res = import_cards_table(dfimp)
                st.success(f"Cartões importados. Gravados: {res.get('gravados',0)} · Ignorados: {res.get('ignorados',0)}")
            except Exception as e:
                st.error(f"Erro ao importar cartões: {e}")

        st.markdown("---")
        if st.button("🔁 Recalcular tudo após importações", key="btn_recalc_after_import"):
            recalc_all_rounds(close_all=False, regen_notes=True)
            st.success("Recalculo concluído.")

    # ===================================
    # 2) INCLUSÃO MANUAL DE LANÇAMENTOS
    # ===================================
    with st.expander("🧾 Inclusão manual de lançamentos (por data)", expanded=False):
        c1, c2, c3 = st.columns(3)
        d_manual = c1.date_input("Data", value=date.today(), key="man_date")
        season_manual = c2.text_input("Temporada (opcional)", value=str(date.today().year), key="man_season")
        team_manual = c3.selectbox("Time (opcional)", ["(sem time)","Time 1","Time 2","Time 3","Time 4"], key="man_team")

        # jogador
        pl = df_query("SELECT id, COALESCE(nickname,name) AS nome FROM players WHERE active=1 ORDER BY nome")
        pid_sel = st.selectbox(
            "Jogador", options=pl["id"].tolist() if not pl.empty else [],
            format_func=lambda i: pl.loc[pl["id"]==i, "nome"].iloc[0] if not pl.empty else str(i),
            key="man_player"
        )

        c4, c5, c6 = st.columns(3)
        pres = c4.checkbox("Presença", value=True, key="man_presence")
        ca_i  = c5.number_input("CA", min_value=0, step=1, value=0, key="man_ca")
        cv_i  = c6.number_input("CV", min_value=0, step=1, value=0, key="man_cv")

        c7, c8, c9 = st.columns(3)
        w_i = c7.number_input("Vitórias (individual – goleiro)", min_value=0, step=1, value=0, key="man_w")
        d_i = c8.number_input("Empates (individual – goleiro)",  min_value=0, step=1, value=0, key="man_d")
        apply_individual = c9.checkbox("Fixar como individual (override)", value=False, key="man_ind")

        if st.button("➕ Incluir lançamento", key="btn_add_manual"):
            if not pid_sel:
                st.warning("Selecione um jogador.")
            else:
                d_iso = d_manual.isoformat()
                rid, _ = get_or_create_round_by_date(d_iso, season=normalize_season(season_manual), four_gk_default=False)
                tid = None
                if team_manual and team_manual != "(sem time)":
                    tid = get_or_create_team_round(rid, team_manual)

                # presença + cartões
                try:
                    exec_sql(
                        "INSERT INTO player_round(round_id, player_id, presence, team_round_id, yellow_cards, red_cards) "
                        "VALUES(:r,:p,:pr,:t,:ca,:cv)",
                        {"r": rid, "p": int(pid_sel), "pr": 1 if pres else 0, "t": tid, "ca": int(ca_i), "cv": int(cv_i)}
                    )
                except Exception:
                    exec_sql(
                        "UPDATE player_round SET presence=:pr, team_round_id=:t, yellow_cards=:ca, red_cards=:cv "
                        "WHERE round_id=:r AND player_id=:p",
                        {"r": rid, "p": int(pid_sel), "pr": 1 if pres else 0, "t": tid, "ca": int(ca_i), "cv": int(cv_i)}
                    )

                # se informar vitórias/empates, grava como individual (útil p/ goleiro)
                if (w_i or d_i) or apply_individual:
                    save_gk_individual(rid, int(pid_sel), int(w_i), int(d_i), team_round_id=tid)

                recalc_round(rid)
                generate_round_notes_sequence()
                st.success("Lançamento incluído/atualizado!")

    st.divider()
    st.caption("Edição manual das principais tabelas; salve e recalcule quando necessário.")

    with st.expander("📄 Jogadores (visualização)"):
        st.dataframe(df_query("SELECT id, name, nickname, position, role, is_goalkeeper, plan, active FROM players ORDER BY name"),
                     use_container_width=True, hide_index=True)
    with st.expander("📄 Rodadas (visualização)"):
        st.dataframe(df_query("SELECT id, date, season, notes, closed, four_goalkeepers FROM rounds ORDER BY date DESC, id DESC"),
                     use_container_width=True, hide_index=True)

    rounds_admin = df_query("SELECT id, date, season FROM rounds ORDER BY date DESC, id DESC")
    if rounds_admin.empty:
        st.info("Cadastre ao menos uma rodada.")
    else:
        rounds_admin["id"] = pd.to_numeric(rounds_admin["id"], errors="coerce").fillna(0).astype(int)
        id2date = {int(r.id): str(r.date) for r in rounds_admin.itertuples(index=False)}

        rid_a = st.selectbox(
            "Rodada para editar",
            options=rounds_admin["id"].tolist(),
            format_func=lambda rid: f"{id2date.get(int(rid), '-') }  (#{int(rid)})",
            key="rid_admin"
        )

        with st.expander("🗑 Excluir rodada", expanded=False):
            info = df_query("SELECT date, season, notes FROM rounds WHERE id=:r", {"r": rid_a})
            prc = df_query("SELECT COUNT(*) AS n FROM player_round WHERE round_id=:r", {"r": rid_a}).iloc[0]["n"]
            trc = df_query("SELECT COUNT(*) AS n FROM teams_round  WHERE round_id=:r", {"r": rid_a}).iloc[0]["n"]
            st.warning(
                f"Esta ação apagará **definitivamente** a rodada #{rid_a} "
                f"({info.iloc[0]['date'] if not info.empty else '-'}), "
                f"{int(trc)} times e {int(prc)} lançamentos de jogadores."
            )
            confirm = st.text_input("Digite **EXCLUIR** para confirmar", value="", key=f"del_confirm_{rid_a}")
            if st.button("Excluir rodada", key=f"btn_del_{rid_a}"):
                if confirm.strip().upper() == "EXCLUIR":
                    exec_sql("DELETE FROM player_round WHERE round_id=:r", {"r": rid_a})
                    exec_sql("DELETE FROM teams_round  WHERE round_id=:r", {"r": rid_a})
                    exec_sql("DELETE FROM rounds       WHERE id=:r",       {"r": rid_a})
                    generate_round_notes_sequence()
                    st.success("Rodada excluída com sucesso.")
                    st.rerun()
                else:
                    st.error("Confirmação inválida. Digite EXCLUIR para prosseguir.")

        c1,c2 = st.columns(2)
        with c1.expander("🏷 Times da rodada (pontos) — editar/excluir"):
            df_t = df_query("SELECT id, name AS Time, wins AS Vitórias, draws AS Empates, points AS Pontos FROM teams_round WHERE round_id=:r ORDER BY name", {"r": rid_a})
            edited = st.data_editor(df_t, use_container_width=True, num_rows="dynamic", key=f"ed_tr_{rid_a}")
            if st.button("Salvar times", key=f"save_tr_{rid_a}"):
                for _, r in edited.iterrows():
                    exec_sql("UPDATE teams_round SET name=:n, wins=:w, draws=:d, points=:p WHERE id=:id",
                             {"n": r["Time"], "w": int(r["Vitórias"]), "d": int(r["Empates"]), "p": int(r["Pontos"]), "id": int(r["id"])})
                recalc_round(rid_a); st.success("Times salvos e rodada recalculada.")
            del_tr = st.multiselect("Times para excluir",
                                    options=edited["id"].tolist(),
                                    format_func=lambda i: edited.loc[edited["id"]==i, "Time"].iloc[0],
                                    key=f"deltr_{rid_a}")
            if st.button("Excluir times selecionados", key=f"btn_deltr_{rid_a}"):
                for tid in del_tr:
                    exec_sql("DELETE FROM player_round WHERE team_round_id=:t AND round_id=:r",
                             {"t": int(tid), "r": rid_a})
                    exec_sql("DELETE FROM teams_round WHERE id=:id", {"id": int(tid)})
                recalc_round(rid_a)
                st.success("Times excluídos e rodada recalculada.")

        with c2.expander("🧤 Goleiros — pontuação individual (override) — editar/excluir"):
            df_g = df_query("""
                SELECT pr.id, COALESCE(p.nickname,p.name) AS Goleiro,
                       pr.team_round_id AS TeamId, pr.wins AS Vitórias, pr.draws AS Empates, pr.points AS Pontos,
                       COALESCE(pr.individual_override,0) AS Override
                  FROM player_round pr
                  JOIN players p ON p.id=pr.player_id
                 WHERE pr.round_id=:r AND (p.role='GOLEIRO' OR p.is_goalkeeper=1)
                 ORDER BY Goleiro
            """, {"r": rid_a})
            st.caption("Edite vitórias/empates/pontos. Override=1 mantém o valor individual no recálculo.")
            edited_g = st.data_editor(df_g, use_container_width=True, num_rows="dynamic", key=f"ed_gk_{rid_a}")
            if st.button("Salvar goleiros", key=f"save_gk_{rid_a}"):
                for _, r in edited_g.iterrows():
                    exec_sql("""UPDATE player_round
                                   SET wins=:w, draws=:d, points=:p, individual_override=:o
                                 WHERE id=:id""",
                             {"w": int(r["Vitórias"]), "d": int(r["Empates"]), "p": int(r["Pontos"]), "o": int(r["Override"]), "id": int(r["id"])})
                recalc_round(rid_a); st.success("Goleiros salvos e rodada recalculada.")
            del_g = st.multiselect("Lançamentos de goleiros para excluir",
                                   options=edited_g["id"].tolist() if not df_g.empty else [],
                                   format_func=lambda i: edited_g.loc[edited_g["id"]==i, "Goleiro"].iloc[0] if not df_g.empty else str(i),
                                   key=f"delgk_{rid_a}")
            if st.button("Excluir lançamentos de goleiros", key=f"btn_delgk_{rid_a}"):
                for gid in del_g:
                    exec_sql("DELETE FROM player_round WHERE id=:id", {"id": int(gid)})
                recalc_round(rid_a)
                st.success("Lançamentos de goleiros excluídos.")

        with st.expander("🟨🟥 Cartões — editar/excluir"):
            df_c = df_query("""
                SELECT pr.id, COALESCE(p.nickname,p.name) AS Jogador,
                       COALESCE(pr.yellow_cards,0) AS CA, COALESCE(pr.red_cards,0) AS CV
                  FROM player_round pr
                  JOIN players p ON p.id=pr.player_id
                 WHERE pr.round_id=:r ORDER BY Jogador
            """, {"r": rid_a})
            edited_c = st.data_editor(df_c, use_container_width=True, num_rows="dynamic", key=f"ed_cards_{rid_a}")
            if st.button("Salvar cartões", key=f"save_cards_{rid_a}"):
                for _, r in edited_c.iterrows():
                    exec_sql("UPDATE player_round SET yellow_cards=:a, red_cards=:v WHERE id=:id",
                             {"a": int(r["CA"]), "v": int(r["CV"]), "id": int(r["id"])})
                st.success("Cartões salvos.")
            del_c = st.multiselect("Lançamentos de cartões para excluir",
                                   options=edited_c["id"].tolist() if not df_c.empty else [],
                                   format_func=lambda i: edited_c.loc[edited_c["id"]==i, "Jogador"].iloc[0] if not df_c.empty else str(i),
                                   key=f"delcards_{rid_a}")
            if st.button("Excluir cartões selecionados", key=f"btn_delcards_{rid_a}"):
                for cid in del_c:
                    exec_sql("DELETE FROM player_round WHERE id=:id", {"id": int(cid)})
                st.success("Cartões excluídos.")

    c1,c2 = st.columns(2)
    if c1.button("🔁 Recalcular tudo e fechar rodadas", key="admin_update_all"):
        recalc_all_rounds(close_all=True, regen_notes=True)
        st.success("Recalculado e rodadas fechadas.")
    if c2.button("🔢 Regenerar notas (1º Rodada, 2º...)", key="admin_notes"):
        generate_round_notes_sequence()
        st.success("Notas regeneradas.")

# ---- Caixa ----
with tabs[6]:
    st.subheader("💰 Controle de Caixa")

    # Helper: última data de rodada da temporada
    def _last_round_date_for_season(season:str):
        df = df_query("SELECT MAX(date) AS d FROM rounds WHERE COALESCE(season,'')=:s", {"s": season})
        if df.empty or pd.isna(df.iloc[0]["d"]):
            return None
        try:
            return datetime.fromisoformat(str(df.iloc[0]["d"])).date()
        except Exception:
            try:
                return pd.to_datetime(str(df.iloc[0]["d"])).date()
            except Exception:
                return None

    seasons = _season_list()
    sel_season = st.selectbox("Temporada", options=seasons, index=len(seasons)-1, key="cash_season")
    sel_season = normalize_season(sel_season) or str(date.today().year)
    st.caption(f"Caixa da Temporada **{sel_season}**")

    last_round_dt = _last_round_date_for_season(sel_season)

    c1, _ = st.columns([2,1])
    with c1:
        opening_val = st.number_input("Saldo Anterior (R$)", min_value=0.0, step=1.0, value=float(_get_opening(sel_season)), key="cash_opening")
        if st.button("Salvar saldo anterior", key="btn_save_opening"):
            _set_opening(sel_season, opening_val)
            st.success("Saldo anterior salvo.")

    st.divider()

    st.markdown("### 🧾 Mensalistas (marque 1x por mês)")
    mtx = _flags_to_matrix(sel_season).copy()
    if mtx.empty:
        st.info("Nenhum mensalista ativo cadastrado (Plano = Mensalista).")
    else:
        ui = mtx.rename(columns={"player_id":"ID"}).set_index("ID").reset_index()
        ids_list = ui["ID"].dropna().astype(int).tolist()
        in_sql, in_params = _expand_in(ids_list, "pid")
        names = df_query(
            f"SELECT id, COALESCE(nickname,name) AS Nome FROM players WHERE id IN ({in_sql})",
            in_params
        ) if ids_list else pd.DataFrame(columns=["id","Nome"])
        id2name = {int(r["id"]): str(r["Nome"]) for _, r in names.iterrows()} if not names.empty else {}
        ui.insert(1, "Nome", ui["ID"].map(id2name))

        ed = st.data_editor(
            ui,
            use_container_width=True,
            num_rows="dynamic",
            key=f"ed_cash_mtx_{sel_season}",
            column_config={"Nome": st.column_config.TextColumn(disabled=True)},
            height=420)

        if st.button("Salvar mensalistas", key=f"save_cash_mtx_{sel_season}"):
            back = ed.rename(columns={"ID":"player_id"})
            for m in range(1,13):
                col = _month_name_pt(m)
                if col in back.columns:
                    back[col] = back[col].fillna(False).astype(bool)
            _matrix_to_flags(sel_season, back)
            st.success("Mensalistas do caixa salvos.")

    st.divider()

    # ========== LANÇAMENTOS MANUAIS (FÁCIL) + GRADE (c/ limite de data) ==========
    st.markdown("### 🧮 Lançamentos manuais (Entradas/Saídas)")

    # Formulário simples vinculado à rodada (data vem da rodada)
    _rds = df_query(
        "SELECT id, date, COALESCE(notes,'') AS notes "
        "FROM rounds WHERE COALESCE(season,'')=:s ORDER BY date DESC",
        {"s": sel_season}
    )
    if _rds.empty:
        st.info("Não há rodadas nesta temporada. Crie uma rodada para habilitar o lançamento rápido.")
    else:
        def _fmt_round_opt(i:int):
            row = _rds[_rds["id"]==i].iloc[0]
            n = (str(row["notes"]).strip() or f"#{int(i)}")
            return f"{n} — {row['date']}"

        colf1, colf2 = st.columns([2, 1])
        with colf1:
            sel_round_id = st.selectbox(
                "Rodada",
                options=_rds["id"].astype(int).tolist(),
                format_func=_fmt_round_opt,
                key=f"cash_fast_round_{sel_season}"
            )
        with colf2:
            fast_type = st.selectbox("Tipo", ["Entrada", "Saída"], key=f"cash_fast_type_{sel_season}")

        colf3, colf4 = st.columns([3, 1])
        with colf3:
            fast_desc = st.text_input("Descrição", key=f"cash_fast_desc_{sel_season}", placeholder="Ex.: Doação, Água, Material, etc.")
        with colf4:
            fast_val = st.number_input("Valor (R$)", min_value=0.0, step=1.0, value=0.0, key=f"cash_fast_val_{sel_season}")

        if st.button("➕ Adicionar lançamento", key=f"cash_fast_add_{sel_season}"):
            if not sel_round_id:
                st.warning("Selecione a rodada.")
            elif (fast_val or 0.0) <= 0.0:
                st.warning("Informe um valor maior que zero.")
            else:
                rrow = _rds[_rds["id"]==int(sel_round_id)].iloc[0]
                rdate_iso = str(rrow["date"])
                if last_round_dt and datetime.fromisoformat(rdate_iso).date() > last_round_dt:
                    st.error("Data do lançamento excede a última rodada da temporada.")
                else:
                    exec_sql(
                        "INSERT INTO cash_extra(date, season, type, description, value) "
                        "VALUES(:d,:s,:t,:ds,:v)",
                        {"d": rdate_iso, "s": sel_season, "t": fast_type, "ds": fast_desc.strip(), "v": float(fast_val)}
                    )
                    st.success("Lançamento adicionado com sucesso.")

    # Grade (limitando por data)
    if last_round_dt:
        df_extra = df_query("""
            SELECT id,
                   date AS Data,
                   type AS Tipo,
                   description AS Descrição,
                   value AS Valor
              FROM cash_extra
             WHERE season=:s
               AND date <= :lim
             ORDER BY date
        """, {"s": sel_season, "lim": last_round_dt.isoformat()})
    else:
        df_extra = df_query("""
            SELECT id,
                   date AS Data,
                   type AS Tipo,
                   description AS Descrição,
                   value AS Valor
              FROM cash_extra
             WHERE season=:s
             ORDER BY date
        """, {"s": sel_season})

    st.caption("Edite abaixo se necessário (observa o limite da última rodada para salvar).")
    ed_extra = st.data_editor(
        df_extra,
        use_container_width=True,
        num_rows="dynamic",
        key=f"ed_cash_extra_{sel_season}"
    )

    c1, c2 = st.columns(2)
    with c1:
        if st.button("💾 Salvar lançamentos", key=f"save_cash_extra_{sel_season}"):
            for _, r in ed_extra.iterrows():
                rid = r.get("id")
                raw_d = r.get("Data")
                try:
                    d_iso = parse_br_date(raw_d) or (pd.to_datetime(raw_d).date().isoformat() if raw_d else None)
                except Exception:
                    d_iso = None

                if last_round_dt and d_iso:
                    try:
                        _d = datetime.fromisoformat(d_iso).date()
                    except Exception:
                        _d = pd.to_datetime(d_iso).date()
                    if _d > last_round_dt:
                        st.error(f"Data '{raw_d}' excede a última rodada ({last_round_dt.isoformat()}). Ajuste antes de salvar.")
                        st.stop()

                tp   = str(r.get("Tipo") or "Entrada").strip() or "Entrada"
                desc = str(r.get("Descrição") or "").strip()
                val  = float(pd.to_numeric(r.get("Valor"), errors="coerce") or 0.0)

                if pd.isna(rid) or rid is None:
                    exec_sql(
                        "INSERT INTO cash_extra(date, season, type, description, value) "
                        "VALUES(:d,:s,:t,:ds,:v)",
                        {"d": d_iso, "s": sel_season, "t": tp, "ds": desc, "v": val}
                    )
                else:
                    exec_sql(
                        "UPDATE cash_extra SET date=:d, type=:t, description=:ds, value=:v "
                        "WHERE id=:id",
                        {"d": d_iso, "t": tp, "ds": desc, "v": val, "id": int(rid)}
                    )
            st.success("Lançamentos salvos.")
    with c2:
        to_del = st.multiselect(
            "Selecionar lançamentos para excluir",
            options=ed_extra["id"].dropna().astype(int).tolist() if not ed_extra.empty else [],
            key=f"del_cash_extra_list_{sel_season}"
        )
        if st.button("🗑 Excluir selecionados", key=f"del_cash_extra_{sel_season}"):
            for i in to_del:
                exec_sql("DELETE FROM cash_extra WHERE id=:id", {"id": int(i)})
            st.success("Lançamentos excluídos.")

    st.divider()

    # =================== RESUMO POR MÊS (até última rodada) ===================
    st.markdown("### 📅 Resumo por mês (até a última rodada)")
    mm = st.selectbox("Mês", options=list(range(1,13)), format_func=lambda m: f"{_month_name_pt(m)}/{sel_season}", key=f"cash_month_{sel_season}")

    # Limites do mês
    m_start, m_end = _month_bounds(sel_season, mm)
    m_start_d = datetime.fromisoformat(m_start).date()
    m_end_d   = datetime.fromisoformat(m_end).date()
    # Se houver última rodada, cortar o fim do período
    if last_round_dt:
        if last_round_dt < m_start_d:
            # não há dados válidos neste mês ainda
            eff_start, eff_end = None, None
        else:
            eff_start = m_start_d
            eff_end   = min(m_end_d, last_round_dt)
    else:
        eff_start, eff_end = m_start_d, m_end_d

    # Função helper local para somatórios filtrando data
    def _sum_cash_extra(season:str, start_d:date, end_d:date):
        if not start_d or not end_d:
            return 0.0, 0.0
        df = df_query("""
            SELECT COALESCE(type,'') AS type, COALESCE(value,0) AS value
              FROM cash_extra
             WHERE season=:s AND date BETWEEN :a AND :b
        """, {"s": season, "a": start_d.isoformat(), "b": end_d.isoformat()})
        entradas = float(df[df["type"]=="Entrada"]["value"].sum()) if not df.empty else 0.0
        saidas   = float(df[df["type"]=="Saída"]["value"].sum())   if not df.empty else 0.0
        return entradas, saidas

    # Cartões no mês (até o corte)
    def _cards_sum(season:str, start_d:date, end_d:date):
        if not start_d or not end_d:
            return (0,0)
        df = df_query("""
          SELECT COALESCE(SUM(pr.yellow_cards),0) AS ca,
                 COALESCE(SUM(pr.red_cards),0)    AS cv
            FROM player_round pr
            JOIN rounds r ON r.id=pr.round_id
           WHERE r.date BETWEEN :a AND :b
        """, {"a": start_d.isoformat(), "b": end_d.isoformat()})
        if df.empty: return (0,0)
        return int(df.iloc[0]["ca"] or 0), int(df.iloc[0]["cv"] or 0)

    # Avulsos (presenças) no mês até o corte
    def _avulsos_month(season:str, start_d:date, end_d:date):
        if not start_d or not end_d:
            return 0
        df = df_query("""
          SELECT COUNT(*) AS n
            FROM player_round pr
            JOIN rounds r  ON r.id=pr.round_id
            JOIN players p ON p.id=pr.player_id
           WHERE pr.presence=1
             AND COALESCE(p.plan,'Mensalista')='Avulso'
             AND r.date BETWEEN :a AND :b
        """, {"a": start_d.isoformat(), "b": end_d.isoformat()})
        return int(df.iloc[0]["n"] or 0) if not df.empty else 0

    # Quantas rodadas no mês até o corte (para juiz)
    def _rounds_in_month(start_d:date, end_d:date):
        if not start_d or not end_d:
            return 0
        df = df_query("SELECT COUNT(*) AS n FROM rounds WHERE date BETWEEN :a AND :b", {"a": start_d.isoformat(), "b": end_d.isoformat()})
        return int(df.iloc[0]["n"] or 0) if not df.empty else 0

    monthly_fee = float(get_setting("monthly_fee","0") or 0)
    single_fee  = float(get_setting("single_fee","0") or 0)
    rent        = float(get_setting("rent_court","0") or 0)
    ref_fee     = float(get_setting("referee_fee","0") or 0)
    has_ref     = (get_setting("has_referee","0")=="1")
    yc_fee      = float(get_setting("yellow_card_fee","0") or 0)
    rc_fee      = float(get_setting("red_card_fee","0") or 0)

    # Mensalistas pagos no mês (sem corte por dia — é um flag mensal)
    m_mensalistas = _mensalistas_paid_count(sel_season, mm)

    # Entradas / Saídas extras até o corte
    extra_in, extra_out = _sum_cash_extra(sel_season, eff_start, eff_end)
    # Avulsos e rodadas até o corte
    m_avulsos = _avulsos_month(sel_season, eff_start, eff_end)
    rounds_m  = _rounds_in_month(eff_start, eff_end)
    # Cartões até o corte
    ca, cv = _cards_sum(sel_season, eff_start, eff_end)
    cards_income = (ca * yc_fee) + (cv * rc_fee)

    entradas = {
        "Mensalidade": m_mensalistas * monthly_fee,
        "Avulsos":     m_avulsos * single_fee,
        "Cartões (Entrada)": cards_income,
        "Extras (Entrada)": extra_in,
    }
    saidas = {
        "Aluguel da quadra": rent if eff_start else 0.0,  # aluguel é mensal; mantém se o mês está “liberado”
        "Juiz": (ref_fee * rounds_m) if has_ref else 0.0,
        "Extras (Saída)": extra_out,
    }
    tin = sum(entradas.values())
    tout = sum(saidas.values())
    saldo_mes = tin - tout

    # Tabela do mês
    linhas = []
    label_mes = f"{_month_name_pt(mm)}/{sel_season}"
    for desc_i, v in entradas.items():
        linhas.append([label_mes, "Entrada", desc_i, float(v)])
    for desc_i, v in saidas.items():
        linhas.append([label_mes, "Saída", desc_i, float(v)])
    df_res = pd.DataFrame(linhas, columns=["Mês/Ano","Tipo","Descrição","Valor"])
    st.dataframe(df_res, use_container_width=True, hide_index=True)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Entradas", f"R$ {tin:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

    c2_val = tout
    c2.metric("Saídas",   f"R$ {c2_val:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

    c3.metric("Saldo do mês (até corte)", f"R$ {saldo_mes:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

    # Saldo acumulado da temporada até o mês selecionado (respeitando corte)
    def _season_running_balance_with_cut(season:str, up_to_month:int, cut_dt:date):
        opening = _get_opening(season)
        acc = 0.0
        for m in range(1, up_to_month+1):
            ms, me = _month_bounds(season, m)
            ms_d = datetime.fromisoformat(ms).date()
            me_d = datetime.fromisoformat(me).date()
            if cut_dt and cut_dt < ms_d:
                break  # meses posteriores ao corte
            eff_ms = ms_d
            eff_me = min(me_d, cut_dt) if cut_dt else me_d

            # recomputa “saldo do mês” como acima (sem repetir mensalistas pagos para meses > up_to_month)
            m_mensal = _mensalistas_paid_count(season, m)
            ex_in, ex_out = _sum_cash_extra(season, eff_ms, eff_me)
            avu = _avulsos_month(season, eff_ms, eff_me)
            rnd = _rounds_in_month(eff_ms, eff_me)
            ca_i, cv_i = _cards_sum(season, eff_ms, eff_me)
            cards_i = (ca_i * yc_fee) + (cv_i * rc_fee)

            ent = (m_mensal * monthly_fee) + (avu * single_fee) + cards_i + ex_in
            sai = (rent if eff_ms else 0.0) + ((ref_fee * rnd) if has_ref else 0.0) + ex_out
            acc += (ent - sai)
        return opening + acc

    s_temp = _season_running_balance_with_cut(sel_season, mm, last_round_dt)
    c4.metric("Saldo da temporada", f"R$ {s_temp:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

    st.divider()

    # =================== RESUMO POR ANO (até última rodada) ===================
    st.markdown("### 📊 Resumo do ano (até a última rodada)")

    def _year_summary(season:str, cut_dt:date):
        rows = []
        total_in = total_out = 0.0
        for m in range(1,13):
            ms, me = _month_bounds(season, m)
            ms_d = datetime.fromisoformat(ms).date()
            me_d = datetime.fromisoformat(me).date()
            if cut_dt and cut_dt < ms_d:
                break
            eff_ms = ms_d
            eff_me = min(me_d, cut_dt) if cut_dt else me_d

            # Se não há nenhum dia válido no mês, pula
            if cut_dt and cut_dt < ms_d:
                continue

            m_mensal = _mensalistas_paid_count(season, m)
            ex_in, ex_out = _sum_cash_extra(season, eff_ms, eff_me)
            avu = _avulsos_month(season, eff_ms, eff_me)
            rnd = _rounds_in_month(eff_ms, eff_me)
            ca_i, cv_i = _cards_sum(season, eff_ms, eff_me)
            cards_i = (ca_i * yc_fee) + (cv_i * rc_fee)

            ent = (m_mensal * monthly_fee) + (avu * single_fee) + cards_i + ex_in
            sai = (rent if eff_ms else 0.0) + ((ref_fee * rnd) if has_ref else 0.0) + ex_out
            saldo = ent - sai
            total_in += ent
            total_out += sai

            rows.append([_month_name_pt(m), ent, sai, saldo])

        dfy = pd.DataFrame(rows, columns=["Mês","Entradas","Saídas","Saldo"])
        # Linha total
        if not dfy.empty:
            dfy.loc[len(dfy)] = ["Total", float(total_in), float(total_out), float(total_in-total_out)]
        return dfy

    df_year = _year_summary(sel_season, last_round_dt)
    if df_year.empty:
        st.info("Sem dados para o ano selecionado (até a última rodada).")
    else:
        # Evitar erro de Arrow com a linha 'Total' (mantemos tudo como texto formatado)
        df_year_fmt = df_year.copy()
        for col in ["Entradas","Saídas","Saldo"]:
            df_year_fmt[col] = df_year_fmt[col].apply(lambda v: f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
        st.dataframe(df_year_fmt, use_container_width=True, hide_index=True, key=f"year_summary_{sel_season}")
