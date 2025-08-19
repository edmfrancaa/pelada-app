# db.py
import os
from sqlalchemy import create_engine, text

def _get_database_url():
    # 1) tenta via variável de ambiente
    url = os.environ.get("DATABASE_URL", "").strip()
    if url:
        return url
    # 2) tenta via secrets do Streamlit Cloud
    try:
        import streamlit as st  # só existe em runtime do app
        url = st.secrets.get("DATABASE_URL", "").strip()
    except Exception:
        url = ""
    return url

def init_db():
    url = _get_database_url()

    if url:
        # Ex.: postgresql+psycopg2://user:pass@host:5432/dbname
        engine = create_engine(url, pool_pre_ping=True)
    else:
        # SQLite local (bom p/ demo; pode zerar em reinícios no Cloud)
        engine = create_engine(
            "sqlite:///data.db",
            connect_args={"check_same_thread": False}
        )

    # Tabelas base mínimas (idempotente)
    with engine.begin() as con:
        con.execute(text("""
        CREATE TABLE IF NOT EXISTS settings (
          key TEXT PRIMARY KEY,
          value TEXT
        );"""))

        con.execute(text("""
        CREATE TABLE IF NOT EXISTS players (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT UNIQUE NOT NULL,
          nickname TEXT,
          position TEXT,
          role TEXT,
          is_goalkeeper INTEGER DEFAULT 0,
          plan TEXT DEFAULT 'Mensalista',
          active INTEGER DEFAULT 1
        );"""))

        con.execute(text("""
        CREATE TABLE IF NOT EXISTS rounds (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          date TEXT,
          season TEXT,
          notes TEXT,
          closed INTEGER DEFAULT 0,
          four_goalkeepers INTEGER DEFAULT 0
        );"""))

        con.execute(text("""
        CREATE TABLE IF NOT EXISTS teams_round (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          round_id INTEGER NOT NULL,
          name TEXT,
          wins INTEGER DEFAULT 0,
          draws INTEGER DEFAULT 0,
          points INTEGER DEFAULT 0
        );"""))

        con.execute(text("""
        CREATE TABLE IF NOT EXISTS player_round (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          round_id INTEGER NOT NULL,
          player_id INTEGER NOT NULL,
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
        );"""))

        # As tabelas do "Caixa" você já cria no app.py via ensure_cash_tables()

    return engine
