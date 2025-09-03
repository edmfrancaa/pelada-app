# db_users.py
import os
import hashlib
import binascii
import secrets
import unicodedata
import string
import pandas as pd
from sqlalchemy import create_engine, text

# Diretório e banco de autenticação (fora da pasta do app para evitar conflitos de permissão)
_AUTH_DIR = os.path.join(os.path.expanduser("~"), ".pelada")
os.makedirs(_AUTH_DIR, exist_ok=True)
_AUTH_PATH = os.path.join(_AUTH_DIR, "auth.sqlite")
_auth_engine = create_engine(f"sqlite:///{_AUTH_PATH}", future=True)

def _ensure_auth_tables():
    with _auth_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              username TEXT UNIQUE NOT NULL,
              password_hash TEXT NOT NULL,
              created_at TEXT DEFAULT (datetime('now'))
            )
        """))
_ensure_auth_tables()

# ---------- helpers ----------
def _username_canonical(u: str) -> str:
    """normaliza usuário para unicidade (lower + strip)"""
    return (u or "").strip().casefold()

def _user_slug(u: str) -> str:
    """gera slug seguro p/ nome do arquivo do banco do usuário"""
    u = (u or "").strip()
    nfkd = unicodedata.normalize("NFKD", u)
    no_acc = "".join([c for c in nfkd if not unicodedata.combining(c)])
    allowed = string.ascii_letters + string.digits + "._"
    slug = []
    for ch in no_acc:
        if ch in allowed:
            slug.append(ch)
        elif ch.isspace() or ch in "-/\\@":
            slug.append("_")
    s = "".join(slug).strip("._")
    return s.casefold() or "user"

def _hash_password(password: str) -> str:
    """PBKDF2-SHA256 com salt (mais forte que sha256 simples)"""
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return binascii.hexlify(salt).decode() + "$" + binascii.hexlify(dk).decode()

def _verify_password(password: str, stored: str) -> bool:
    try:
        salt_hex, hash_hex = stored.split("$", 1)
        salt = binascii.unhexlify(salt_hex)
        expected = binascii.unhexlify(hash_hex)
        test = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
        return secrets.compare_digest(test, expected)
    except Exception:
        return False

# ---------- classe pública usada pelo app.py ----------
class AuthManager:
    """
    Compatível com o app.py:
      - get_user(username) -> dict | None (com 'password_hash')
      - create_user(username, password) -> (ok: bool, err: str|None)
      - verify_password(password, stored_hash) -> bool
      - get_or_create_user_engine(username, ensure_base_cb=None) -> engine
    """

    def get_user(self, username: str):
        u = _username_canonical(username)
        with _auth_engine.begin() as conn:
            df = pd.read_sql(
                text("""SELECT id, username, password_hash
                        FROM users
                        WHERE lower(trim(username)) = :u"""),
                conn,
                params={"u": u},
            )
        return None if df.empty else df.iloc[0].to_dict()

    def create_user(self, username: str, password: str):
        """
        Cria usuário novo.
        Retorna (True, None) em sucesso, (False, "mensagem") em erro.
        """
        try:
            u_disp = (username or "").strip()
            if not u_disp or not password:
                return False, "Usuário e senha são obrigatórios."
            u_canon = _username_canonical(u_disp)

            # já existe?
            if self.get_user(u_disp):
                return False, "Usuário já existe."

            with _auth_engine.begin() as conn:
                conn.execute(
                    text("INSERT INTO users(username, password_hash) VALUES(:u,:p)"),
                    {"u": u_canon, "p": _hash_password(password)},
                )
            return True, None
        except Exception as e:
            return False, f"Falha ao gravar no auth.sqlite: {e}"

    def verify_password(self, password: str, stored_hash: str) -> bool:
        return _verify_password(password, stored_hash)

    def get_or_create_user_engine(self, username: str, ensure_base_cb=None):
        """
        Retorna o engine do banco dedicado do usuário em ./data/<slug>.sqlite.
        Se não existir, cria. Se ensure_base_cb for passado, chama para criar as tabelas base.
        """
        os.makedirs("data", exist_ok=True)
        slug = _user_slug(username)
        db_path = os.path.join("data", f"{slug}.sqlite")
        eng = create_engine(f"sqlite:///{db_path}", future=True)

        if ensure_base_cb:
            try:
                ensure_base_cb(eng)  # cria tabelas mínimas
            except Exception:
                # se der erro ao criar base, ainda retornamos o engine para o app lidar
                pass

        return eng
