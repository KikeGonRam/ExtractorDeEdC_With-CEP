# db_log.py
import os, time, hashlib, logging, traceback
import mysql.connector
from contextlib import contextmanager
from typing import Optional, Any, Dict, Tuple

# ─────────────────────────────
# Logging (nivel por env LOG_LEVEL=DEBUG para máximo detalle)
# ─────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("db_log")
if not logger.handlers:
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
else:
    logger.setLevel(LOG_LEVEL)

def _mask(v: Any) -> str:
    if v is None: return "None"
    s = str(v)
    return s[:2] + "…" if len(s) > 4 else "***"

def _cfg() -> Dict[str, Any]:
    cfg = dict(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        database=os.getenv("DB_NAME", "extractor_estados"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASS", ""),
        autocommit=True,
        connection_timeout=int(os.getenv("DB_CONN_TIMEOUT", "10")),
    )
    # Traza de configuración (sin exponer password)
    logger.debug(
        "DB cfg: host=%s port=%s db=%s user=%s autocommit=%s timeout=%s",
        cfg["host"], cfg["port"], cfg["database"], cfg["user"], cfg["autocommit"], cfg["connection_timeout"]
    )
    return cfg

def _connect_with_retries(retries: int = 3, delay_s: float = 1.5):
    cfg = _cfg()
    last_exc = None
    for attempt in range(1, retries + 1):
        t0 = time.perf_counter()
        try:
            logger.info("Conectando a MySQL (intento %d/%d)…", attempt, retries)
            cn = mysql.connector.connect(**cfg)
            # sanity check
            try:
                cn.ping(reconnect=True, attempts=1, delay=0)
            except Exception:
                logger.warning("Ping falló tras conectar; continúo igual.")
            dt = (time.perf_counter() - t0) * 1000
            logger.info("Conexión OK en %.1f ms", dt)
            return cn
        except Exception as e:
            dt = (time.perf_counter() - t0) * 1000
            last_exc = e
            code = getattr(e, "errno", "?")
            msg = getattr(e, "msg", str(e))
            logger.error("Conexión fallida (%.0f ms). errno=%s msg=%s", dt, code, msg)
            logger.debug("Trace:\n%s", traceback.format_exc())
            if attempt < retries:
                time.sleep(delay_s)
    # si agotó reintentos, vuelve a lanzar
    raise last_exc

@contextmanager
def _conn():
    cn = _connect_with_retries()
    try:
        yield cn
    finally:
        try:
            cn.close()
            logger.debug("Conexión MySQL cerrada.")
        except Exception:
            logger.debug("No se pudo cerrar la conexión (ya cerrada?):\n%s", traceback.format_exc())

def sha256_of_bytes(data: bytes) -> str:
    h = hashlib.sha256(); h.update(data); return h.hexdigest()

def _exec(cur, sql: str, params: Tuple[Any, ...]) -> None:
    """Ejecuta con trazado y medición de tiempo."""
    logger.debug("SQL: %s", sql.strip().replace("\n", " "))
    # Evita loguear blobs largos
    safe_params = tuple((p if (isinstance(p, (int, float)) or (isinstance(p, str) and len(p) <= 256))
                         else f"<{type(p).__name__} len={len(p) if hasattr(p,'__len__') else '?'}>")
                        for p in params)
    logger.debug("Params: %s", safe_params)
    t0 = time.perf_counter()
    cur.execute(sql, params)
    dt = (time.perf_counter() - t0) * 1000
    logger.debug("OK (%.1f ms), rowcount=%s", dt, getattr(cur, "rowcount", "?"))

# ─────────────────────────────
# API pública
# ─────────────────────────────
def log_start(
    empresa: str,
    banco: str,                     # 'banorte' | 'bbva' | 'santander' | 'inbursa'
    archivo_nombre: str,
    archivo_bytes: Optional[bytes], # None = no calcular hash/tamaño
    resultado: str,                 # 'xlsx' | 'zip'
    ip: Optional[str] = None,
) -> int:
    logger.info("log_start: banco=%s, archivo=%s, resultado=%s, empresa=%s, ip=%s",
                banco, archivo_nombre, resultado, empresa, ip)
    tam = len(archivo_bytes) if archivo_bytes is not None else None
    sha = sha256_of_bytes(archivo_bytes) if archivo_bytes is not None else None
    if tam is not None:
        logger.debug("Archivo: tam=%s bytes sha=%s", tam, sha[:10] + "…" if sha else None)

    with _conn() as cn:
        try:
            cur = cn.cursor()
            _exec(cur, """
                INSERT INTO solicitudes
                  (archivo_nombre, archivo_tamano, archivo_sha256,
                   banco, empresa, resultado, estado, ip_cliente)
                VALUES (%s,%s,%s,%s,%s,%s,'processing',%s)
            """, (archivo_nombre, tam, sha, banco, empresa, resultado, ip))
            req_id = cur.lastrowid
            logger.info("Solicitud insertada id=%s", req_id)
            return int(req_id)
        except Exception as e:
            code = getattr(e, "errno", "?")
            msg = getattr(e, "msg", str(e))
            logger.error("Fallo en INSERT. errno=%s msg=%s", code, msg)
            logger.debug("Trace:\n%s", traceback.format_exc())
            raise

def log_finish(req_id: int, ok: bool, duracion_ms: Optional[int] = None, error: Optional[str] = None):
    logger.info("log_finish: id=%s estado=%s duracion_ms=%s", req_id, "ok" if ok else "fail", duracion_ms)
    with _conn() as cn:
        try:
            cur = cn.cursor()
            _exec(cur, """
                UPDATE solicitudes
                   SET estado=%s, duracion_ms=%s, error=%s
                 WHERE id=%s
            """, ('ok' if ok else 'fail', duracion_ms, (error or None), req_id))
            logger.debug("UPDATE aplicado para id=%s", req_id)
        except Exception as e:
            code = getattr(e, "errno", "?")
            msg = getattr(e, "msg", str(e))
            logger.error("Fallo en UPDATE. errno=%s msg=%s", code, msg)
            logger.debug("Trace:\n%s", traceback.format_exc())
            raise

# ─────────────────────────────
# Diagnóstico rápido desde consola:
#   python db_log.py
# ─────────────────────────────
if __name__ == "__main__":
    logger.info("=== Diagnóstico conexión MySQL ===")
    try:
        with _conn() as cn:
            cur = cn.cursor()
            t0 = time.perf_counter()
            cur.execute("SELECT 1")
            dt = (time.perf_counter() - t0) * 1000
            logger.info("SELECT 1 OK (%.1f ms), result=%s", dt, cur.fetchone())
    except Exception as e:
        logger.error("Diagnóstico falló: %s", e)
        logger.debug("Trace:\n%s", traceback.format_exc())
