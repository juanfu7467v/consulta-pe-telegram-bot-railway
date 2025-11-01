import os
import re
import asyncio
import threading
import traceback
import time
import json
import base64
import requests # Necesario para la API de GitHub
from collections import deque
from datetime import datetime, timezone, timedelta
from urllib.parse import unquote
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
from telethon.tl.types import PeerUser
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto
from telethon.errors.rpcerrorlist import UserBlockedError

# --- Configuración ---

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
PUBLIC_URL = os.getenv("PUBLIC_URL", "https://consulta-pe-bot.up.railway.app").rstrip("/")
SESSION_STRING = os.getenv("SESSION_STRING", None)
PORT = int(os.getenv("PORT", 8080))

# --- Configuración de GitHub para Persistencia ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO") # Formato: usuario/repositorio
SESSION_FILE_PATH = "storage/telethon_session.txt" # Ruta para guardar la sesión
CACHE_DIR = "storage/cache/" # Directorio para guardar los archivos de caché

if not GITHUB_TOKEN or not GITHUB_REPO:
    print("⚠️ WARNING: GITHUB_TOKEN y/o GITHUB_REPO no están configurados. La persistencia en GitHub y el caché no funcionarán.")

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# El chat ID/nombre del bot al que enviar los comandos (BOT PRINCIPAL)
LEDERDATA_BOT_ID = "@LEDERDATA_OFC_BOT" 
# El chat ID/nombre del bot de respaldo (NUEVO BOT)
LEDERDATA_BACKUP_BOT_ID = "@lederdata_publico_bot"
ALL_BOT_IDS = [LEDERDATA_BOT_ID, LEDERDATA_BACKUP_BOT_ID]

TIMEOUT_FAILOVER = 25 
TIMEOUT_TOTAL = 40 

# --- Utilidades de GitHub ---

def _get_github_headers(token: str):
    """Retorna los headers de autenticación para la API de GitHub."""
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
    }

def _get_file_sha(filepath: str, repo_slug: str, token: str):
    """Obtiene el SHA de un archivo existente en GitHub."""
    if not token or not repo_slug: return None
    url = f"https://api.github.com/repos/{repo_slug}/contents/{filepath}"
    headers = _get_github_headers(token)
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json().get("sha")
        elif response.status_code == 404:
            return None # Archivo no existe
        else:
            print(f"Error al obtener SHA de {filepath} (Status: {response.status_code}): {response.text}")
            return None
    except Exception as e:
        print(f"Excepción al obtener SHA: {e}")
        return None

def _write_file_to_github(filepath: str, content: str, commit_message: str, repo_slug: str, token: str):
    """Escribe o actualiza un archivo en GitHub."""
    if not token or not repo_slug: 
        print("Error: GITHUB_TOKEN o GITHUB_REPO no configurados para escribir en GitHub.")
        return False
        
    url = f"https://api.github.com/repos/{repo_slug}/contents/{filepath}"
    sha = _get_file_sha(filepath, repo_slug, token)
    
    # El contenido debe ser Base64
    content_b64 = base64.b64encode(content.encode('utf-8')).decode('utf-8')
    
    payload = {
        "message": commit_message,
        "content": content_b64,
        "branch": "main" # Asumiendo la rama principal
    }
    if sha:
        payload["sha"] = sha # Necesario para actualizar
        
    headers = _get_github_headers(token)
    
    try:
        response = requests.put(url, headers=headers, data=json.dumps(payload), timeout=10)
        if response.status_code in [200, 201]:
            print(f"✅ Archivo '{filepath}' guardado/actualizado en GitHub.")
            return True
        else:
            print(f"❌ Error al guardar '{filepath}' en GitHub (Status: {response.status_code}): {response.text}")
            return False
    except Exception as e:
        print(f"Excepción al escribir en GitHub: {e}")
        return False

def _read_file_from_github(filepath: str, repo_slug: str, token: str):
    """Lee un archivo desde GitHub y retorna su contenido."""
    if not token or not repo_slug: return None
    url = f"https://api.github.com/repos/{repo_slug}/contents/{filepath}"
    headers = _get_github_headers(token)
    
    try:
        # Usamos el header 'Accept: application/vnd.github.v3.raw' para obtener el contenido RAW directamente
        raw_headers = headers.copy()
        raw_headers["Accept"] = "application/vnd.github.v3.raw"
        
        response = requests.get(url, headers=raw_headers, timeout=10)
        if response.status_code == 200:
            return response.text # El contenido RAW
        elif response.status_code == 404:
            return None
        else:
            print(f"Error al leer {filepath} desde GitHub (Status: {response.status_code}): {response.text}")
            return None
    except Exception as e:
        print(f"Excepción al leer desde GitHub: {e}")
        return None

# --- Lógica de Persistencia de Sesión ---

def load_session_from_github():
    """Intenta cargar la cadena de sesión de Telegram desde GitHub."""
    global SESSION_STRING
    if not GITHUB_TOKEN or not GITHUB_REPO:
        print("Skipping load: GITHUB_TOKEN/GITHUB_REPO not set.")
        return
        
    print(f"Attempting to load session from GitHub: {SESSION_FILE_PATH}")
    content = _read_file_from_github(SESSION_FILE_PATH, GITHUB_REPO, GITHUB_TOKEN)
    
    if content and content.strip():
        SESSION_STRING = content.strip()
        print("🔑 Session string loaded successfully from GitHub.")
    else:
        print("Session file not found or empty in GitHub.")

def save_session_to_github(session_str: str):
    """Guarda la cadena de sesión de Telegram en GitHub."""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        print("Skipping save: GITHUB_TOKEN/GITHUB_REPO not set.")
        return
        
    print(f"Attempting to save session to GitHub: {SESSION_FILE_PATH}")
    _write_file_to_github(
        SESSION_FILE_PATH, 
        session_str, 
        "Actualizar cadena de sesión de Telethon", 
        GITHUB_REPO, 
        GITHUB_TOKEN
    )

# --- Lógica de Caché de API (Lectura y Escritura) ---

def _get_cache_filepath(command: str) -> str:
    """Genera la ruta del archivo de caché basado en el comando."""
    # Normalizar el comando para un nombre de archivo seguro
    filename = re.sub(r'[^a-zA-Z0-9_.-]', '_', command).lower()
    # Limitar la longitud del nombre de archivo
    if len(filename) > 200:
        filename = filename[:200]
    return f"{CACHE_DIR}{filename}.json"

def get_cached_result(command: str):
    """Busca un resultado en el caché de GitHub."""
    if not GITHUB_TOKEN or not GITHUB_REPO: return None
    
    filepath = _get_cache_filepath(command)
    cache_path_in_repo = filepath.lstrip(CACHE_DIR).replace("//", "/") # Quitar el prefijo local
    
    # 1. Leer desde GitHub
    content = _read_file_from_github(cache_path_in_repo, GITHUB_REPO, GITHUB_TOKEN)
    
    if content:
        try:
            # 2. Deserializar el JSON
            result = json.loads(content)
            # Marcar como caché para la respuesta
            result["_from_cache"] = True 
            print(f"💡 Resultado de CACHÉ para comando: {command}")
            return result
        except json.JSONDecodeError:
            print(f"Error: No se pudo decodificar el JSON del caché para {command}.")
            return None
    return None

def save_result_to_cache(command: str, result: dict):
    """Guarda un resultado exitoso en el caché de GitHub."""
    if not GITHUB_TOKEN or not GITHUB_REPO: 
        print("Skipping cache save: GITHUB_TOKEN/GITHUB_REPO not set.")
        return
        
    filepath = _get_cache_filepath(command)
    cache_path_in_repo = filepath.lstrip(CACHE_DIR).replace("//", "/")
    
    # 1. Serializar el resultado a JSON
    content = json.dumps(result, indent=4)
    
    # 2. Escribir a GitHub
    _write_file_to_github(
        cache_path_in_repo, 
        content, 
        f"Cache: Resultado automático para comando {command}", 
        GITHUB_REPO, 
        GITHUB_TOKEN
    )

# --- Manejo de Fallos por Bot (Implementación de tu lógica) ---

# Diccionario para rastrear los fallos por timeout/bloqueo: {bot_id: datetime_of_failure}
bot_fail_tracker = {}
BOT_FAIL_TIMEOUT_HOURS = 6 

def is_bot_blocked(bot_id: str) -> bool:
    """Verifica si el bot está temporalmente bloqueado por fallos previos."""
    last_fail_time = bot_fail_tracker.get(bot_id)
    if not last_fail_time:
        return False

    # Usamos la hora actual para la verificación
    now = datetime.now()
    six_hours_ago = now - timedelta(hours=BOT_FAIL_TIMEOUT_HOURS)

    if last_fail_time > six_hours_ago:
        time_left = last_fail_time + timedelta(hours=BOT_FAIL_TIMEOUT_HOURS) - now
        print(f"🚫 Bot {bot_id} bloqueado. Restan: {time_left}")
        return True
    
    print(f"✅ Bot {bot_id} ha cumplido su tiempo de bloqueo. Desbloqueado.")
    bot_fail_tracker.pop(bot_id, None)
    return False

def record_bot_failure(bot_id: str):
    """Registra la hora actual como la última hora de fallo del bot."""
    print(f"🚨 Bot {bot_id} ha fallado y será BLOQUEADO por {BOT_FAIL_TIMEOUT_HOURS} horas.")
    bot_fail_tracker[bot_id] = datetime.now()

# --- Aplicación Flask ---

app = Flask(__name__)
CORS(app)

# --- Bucle Asíncrono para Telethon ---

loop = asyncio.new_event_loop()
threading.Thread(
    target=lambda: (asyncio.set_event_loop(loop), loop.run_forever()), daemon=True
).start()

def run_coro(coro):
    """Ejecuta una corrutina en el bucle principal y espera el resultado."""
    return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=TIMEOUT_TOTAL + 5) 

# --- Cargar la Sesión de GitHub ANTES de inicializar el cliente ---

# Cargar la sesión desde GitHub si no está en las variables de entorno
if not SESSION_STRING:
    load_session_from_github()
    
# --- Configuración del Cliente Telegram ---

if SESSION_STRING and SESSION_STRING.strip():
    session = StringSession(SESSION_STRING)
    print("🔑 Usando SESSION_STRING (cargado de ENV o GitHub)")
else:
    # Si la sesión falla en cargar de ENV/GitHub, usamos un nombre de archivo dummy.
    # El archivo NO se usará para la persistencia, sino el StringSession.
    session = "consulta_pe_session" 
    print("📂 Usando sesión 'consulta_pe_session' (Temporal, se necesita login)")

client = TelegramClient(session, API_ID, API_HASH, loop=loop)

# Mensajes en memoria (usaremos esto como caché de respuestas)
messages = deque(maxlen=2000)
_messages_lock = threading.Lock()

# Diccionario para esperar respuestas específicas: 
response_waiters = {} 

# Login pendiente
pending_phone = {"phone": None, "sent_at": None}

# --- Lógica de Limpieza y Extracción de Datos (Se mantiene igual) ---

def clean_and_extract(raw_text: str):
    """Limpia el texto de cabeceras/pies y extrae campos clave. REEMPLAZA MARCA LEDER BOT."""
    if not raw_text:
        return {"text": "", "fields": {}}

    text = raw_text
    
    # 1. Reemplazar la marca LEDER_BOT por CONSULTA PE
    text = re.sub(r"^\[\#LEDER\_BOT\]", "[CONSULTA PE]", text, flags=re.IGNORECASE | re.DOTALL)
    
    # 2. Eliminar cabecera (patrón más robusto)
    header_pattern = r"^\[.*?\]\s*→\s*.*?\[.*?\](\r?\n){1,2}"
    text = re.sub(header_pattern, "", text, flags=re.IGNORECASE | re.DOTALL)
    
    # 3. Eliminar pie (patrón más robusto para créditos/paginación/warnings al final)
    footer_pattern = r"((\r?\n){1,2}\[|Página\s*\d+\/\d+.*|(\r?\n){1,2}Por favor, usa el formato correcto.*|↞ Anterior|Siguiente ↠.*|Credits\s*:.+|Wanted for\s*:.+)"
    text = re.sub(footer_pattern, "", text, flags=re.IGNORECASE | re.DOTALL)
    
    # 4. Limpiar separador (si queda)
    text = re.sub(r"\-{3,}", "", text, flags=re.IGNORECASE | re.DOTALL)

    # 5. Limpiar espacios
    text = text.strip()

    # 6. Extraer datos clave
    fields = {}
    dni_match = re.search(r"DNI\s*:\s*(\d{8})", text, re.IGNORECASE)
    if dni_match: fields["dni"] = dni_match.group(1)
    
    photo_type_match = re.search(r"Foto\s*:\s*(rostro|huella|firma|adverso|reverso).*", text, re.IGNORECASE)
    if photo_type_match: fields["photo_type"] = photo_type_match.group(1).lower()

    return {"text": text, "fields": fields}

# --- Handler de nuevos mensajes (Se mantiene igual) ---

async def _on_new_message(event):
    """Intercepta mensajes y resuelve las esperas de API si aplica."""
    try:
        sender_is_bot = False
        
        if not hasattr(_on_new_message, 'bot_ids'):
            _on_new_message.bot_ids = {}
            for bot_name in ALL_BOT_IDS:
                try:
                    entity = await client.get_entity(bot_name)
                    _on_new_message.bot_ids[bot_name] = entity.id
                except Exception as e:
                    print(f"Error al obtener entidad para {bot_name}: {e}")

        if event.sender_id in _on_new_message.bot_ids.values():
            sender_is_bot = True
        
        if not sender_is_bot:
            return 
            
        raw_text = event.raw_text or ""
        cleaned = clean_and_extract(raw_text)
        
        msg_urls = []

        if getattr(event, "message", None) and getattr(event.message, "media", None):
            media_list = []
            if isinstance(event.message.media, (MessageMediaDocument, MessageMediaPhoto)):
                media_list.append(event.message.media)
            elif hasattr(event.message.media, 'webpage') and event.message.media.webpage and hasattr(event.message.media.webpage, 'photo'):
                 pass
            
            if media_list:
                try:
                    timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
                    
                    for i, media in enumerate(media_list):
                        file_ext = '.file'
                        if hasattr(media, 'document') and hasattr(media.document, 'attributes'):
                            file_ext = os.path.splitext(getattr(media.document, 'file_name', 'file'))[1]
                        elif isinstance(media, MessageMediaPhoto) or (hasattr(media, 'photo') and media.photo):
                            file_ext = '.jpg'
                            
                        dni_part = f"_{cleaned['fields'].get('dni')}" if cleaned["fields"].get("dni") else ""
                        type_part = f"_{cleaned['fields'].get('photo_type')}" if cleaned['fields'].get('photo_type') else ""
                        unique_filename = f"{timestamp_str}_{event.message.id}{dni_part}{type_part}_{i}{file_ext}"
                        
                        saved_path = await client.download_media(event.message, file=os.path.join(DOWNLOAD_DIR, unique_filename))
                        filename = os.path.basename(saved_path)
                        
                        url_obj = {
                            "url": f"{PUBLIC_URL}/files/{filename}", 
                            "type": cleaned['fields'].get('photo_type', 'file'),
                            "text_context": raw_text.split('\n')[0].strip()
                        }
                        msg_urls.append(url_obj)
                        
                except Exception as e:
                    print(f"Error al descargar media: {e}")
        
        msg_obj = {
            "chat_id": getattr(event, "chat_id", None),
            "from_id": event.sender_id,
            "date": event.message.date.isoformat() if getattr(event, "message", None) else datetime.utcnow().isoformat(),
            "message": cleaned["text"],
            "fields": cleaned["fields"],
            "urls": msg_urls 
        }

        resolved = False
        with _messages_lock:
            keys_to_check = list(response_waiters.keys())
            for command_id in keys_to_check:
                waiter_data = response_waiters.get(command_id)
                if not waiter_data: continue

                command_dni = waiter_data.get("dni")
                message_dni = cleaned["fields"].get("dni")
                
                dni_match = command_dni and command_dni == message_dni
                no_dni_command = not command_dni 
                
                sender_bot_name = next((name for name, id_ in _on_new_message.bot_ids.items() if id_ == event.sender_id), None)
                sent_to_match = sender_bot_name and sender_bot_name == waiter_data.get("sent_to_bot")

                if sent_to_match and (dni_match or no_dni_command):
                    
                    waiter_data["messages"].append(msg_obj)
                    waiter_data["has_response"] = True
                    
                    if "Por favor, usa el formato correcto" in msg_obj["message"]:
                        loop.call_soon_threadsafe(waiter_data["future"].set_result, msg_obj)
                        waiter_data["timer"].cancel()
                        response_waiters.pop(command_id, None)
                        resolved = True
                        break

        if not resolved:
            with _messages_lock:
                messages.appendleft(msg_obj)

    except Exception:
        traceback.print_exc() 

client.add_event_handler(_on_new_message, events.NewMessage(incoming=True))

# --- Función Central para Llamadas API (Comandos) ---

async def _call_api_command(command: str, timeout: int = TIMEOUT_TOTAL):
    """Envía un comando al bot y espera la respuesta(s), con lógica de respaldo y bloqueo por fallo."""
    if not await client.is_user_authorized():
        raise Exception("Cliente no autorizado. Por favor, inicie sesión.")

    # 1. Intentar obtener de la caché de GitHub
    cached_result = get_cached_result(command)
    if cached_result:
        return cached_result
    
    # 2. Si no hay caché, proceder con la consulta a Telegram
    
    command_id = time.time()
    
    dni_match = re.search(r"/\w+\s+(\d{8})", command)
    dni = dni_match.group(1) if dni_match else None
    
    bots_to_try = [LEDERDATA_BOT_ID, LEDERDATA_BACKUP_BOT_ID]
    
    for attempt, current_bot_id in enumerate(bots_to_try, 1):
        
        if is_bot_blocked(current_bot_id) and attempt == 1:
            print(f"🚫 Bot {current_bot_id} está BLOQUEADO temporalmente. Saltando al bot de respaldo.")
            continue 
        elif is_bot_blocked(current_bot_id) and attempt == 2:
            print(f"🚫 Bot de Respaldo {current_bot_id} también está BLOQUEADO. No hay bots disponibles.")
            break 

        future = loop.create_future()
        waiter_data = {
            "future": future,
            "messages": [], 
            "dni": dni,
            "command": command,
            "timer": None, 
            "sent_to_bot": current_bot_id,
            "has_response": False 
        }
        
        current_timeout = TIMEOUT_FAILOVER if attempt == 1 else TIMEOUT_TOTAL
        
        def _on_timeout(bot_id_on_timeout=current_bot_id, command_id_on_timeout=command_id):
            with _messages_lock:
                waiter_data = response_waiters.pop(command_id_on_timeout, None)
                if waiter_data and not waiter_data["future"].done():
                    
                    if waiter_data["messages"]:
                        print(f"✅ Timeout alcanzado para acumulación en {bot_id_on_timeout}. Devolviendo {len(waiter_data['messages'])} mensaje(s).")
                        loop.call_soon_threadsafe(
                            waiter_data["future"].set_result, 
                            waiter_data["messages"]
                        )
                    else:
                        if not waiter_data["has_response"]:
                            record_bot_failure(bot_id_on_timeout)
                        
                        loop.call_soon_threadsafe(
                            waiter_data["future"].set_result, 
                            {"status": "error_timeout", "message": f"Tiempo de espera de respuesta agotado ({current_timeout}s). No se recibió NINGÚN mensaje para el comando: {command}.", "bot": bot_id_on_timeout, "fail_recorded": not waiter_data["has_response"]}
                        )

        waiter_data["timer"] = loop.call_later(current_timeout, _on_timeout)

        with _messages_lock:
            response_waiters[command_id] = waiter_data

        print(f"📡 Enviando comando (Intento {attempt}) a {current_bot_id} [Timeout: {current_timeout}s]: {command}")
        
        try:
            await client.send_message(current_bot_id, command)
            
            result = await future
            
            if isinstance(result, dict) and result.get("status") == "error_timeout" and attempt == 1:
                print(f"⌛ Timeout de NO RESPUESTA de {LEDERDATA_BOT_ID}. Intentando con {LEDERDATA_BACKUP_BOT_ID}.")
                continue 
            elif isinstance(result, dict) and result.get("status") == "error_timeout" and attempt == 2:
                return result 
            
            if isinstance(result, dict) and "Por favor, usa el formato correcto" in result.get("message", ""):
                 return {"status": "error_bot_format", "message": result.get("message"), "bot_used": current_bot_id}

            list_of_messages = result if isinstance(result, list) else [] 
            
            if isinstance(list_of_messages, list) and len(list_of_messages) > 0:
                
                final_result = list_of_messages[0].copy() 
                final_result["full_messages"] = [msg["message"] for msg in list_of_messages] 
                consolidated_urls = {} 
                
                type_map = {
                    "rostro": "ROSTRO", 
                    "huella": "HUELLA", 
                    "firma": "FIRMA", 
                    "adverso": "ADVERSO", 
                    "reverso": "REVERSO"
                }
                
                for msg in list_of_messages:
                    for url_obj in msg.get("urls", []):
                        key = type_map.get(url_obj["type"].lower())
                        
                        if key:
                            if key not in consolidated_urls:
                                consolidated_urls[key] = url_obj["url"]
                        else:
                            base_key = "FILE"
                            i = 1
                            if base_key in consolidated_urls:
                                while f"{base_key}_{i}" in consolidated_urls:
                                    i += 1
                                consolidated_urls[f"{base_key}_{i}"] = url_obj["url"]
                            else:
                                consolidated_urls[base_key] = url_obj["url"]

                    if not final_result["fields"].get("dni") and msg["fields"].get("dni"):
                        final_result["fields"] = msg["fields"]
                        
                final_result["urls"] = consolidated_urls 
                final_result["message"] = "\n---\n".join(final_result["full_messages"])
                final_result.pop("full_messages")
                
                final_result.pop("chat_id", None)
                final_result.pop("from_id", None)
                final_result.pop("date", None)
                
                final_json = {
                    "message": final_result["message"],
                    "fields": final_result["fields"],
                    "urls": final_result["urls"],
                }
                
                if final_json["fields"].get("dni"):
                    final_json["dni"] = final_json["fields"]["dni"]
                    final_json["fields"].pop("dni")
                
                final_json["status"] = "ok"
                final_json["bot_used"] = current_bot_id
                
                # 3. Guardar el resultado exitoso en la caché de GitHub
                save_result_to_cache(command, final_json)
                
                return final_json
                
            else: 
                return {"status": "error", "message": f"Respuesta vacía o inesperada del bot {current_bot_id}.", "bot_used": current_bot_id}
            
        except UserBlockedError as e:
            error_msg = f"Error de Telethon/conexión/fallo: You blocked this user (caused by SendMessageRequest)"
            print(f"❌ Error de BLOQUEO en {current_bot_id}: {error_msg}. Registrando fallo y pasando al siguiente bot.")
            
            record_bot_failure(current_bot_id)
            
            with _messages_lock:
                 if command_id in response_waiters:
                    waiter_data = response_waiters.pop(command_id, None)
                    if waiter_data and waiter_data["timer"]:
                        waiter_data["timer"].cancel()
                        
            if attempt == 1:
                continue
            else:
                return {"status": "error", "message": error_msg, "bot_used": current_bot_id}
            
        except Exception as e:
            error_msg = f"Error de Telethon/conexión/fallo: {str(e)}"
            if attempt == 1:
                print(f"❌ Error en {LEDERDATA_BOT_ID}: {error_msg}. Intentando con {LEDERDATA_BACKUP_BOT_ID}.")
                record_bot_failure(LEDERDATA_BOT_ID)
                
                with _messages_lock:
                     if command_id in response_waiters:
                        waiter_data = response_waiters.pop(command_id, None)
                        if waiter_data and waiter_data["timer"]:
                            waiter_data["timer"].cancel()
                            
                continue
            else:
                return {"status": "error", "message": error_msg, "bot_used": current_bot_id}
        finally:
            with _messages_lock:
                if command_id in response_waiters:
                    waiter_data = response_waiters.pop(command_id, None)
                    if waiter_data and waiter_data["timer"]:
                        waiter_data["timer"].cancel()

    final_bot = LEDERDATA_BOT_ID
    if is_bot_blocked(LEDERDATA_BACKUP_BOT_ID) or not is_bot_blocked(LEDERDATA_BOT_ID):
         final_bot = LEDERDATA_BOT_ID
    elif is_bot_blocked(LEDERDATA_BOT_ID) and not is_bot_blocked(LEDERDATA_BACKUP_BOT_ID):
         final_bot = LEDERDATA_BACKUP_BOT_ID
    
    return {"status": "error", "message": f"Falló la consulta después de 2 intentos. Ambos bots están bloqueados o agotaron el tiempo de espera.", "bot_used": final_bot}


# --- Rutina de reconexión / ping ---

async def _ensure_connected():
    """Mantiene la conexión y autorización activa, y guarda la sesión si es nueva."""
    while True:
        try:
            if not client.is_connected():
                print("🔌 Intentando reconectar Telethon...")
                await client.connect()
            
            is_auth = await client.is_user_authorized()
            
            if client.is_connected() and not is_auth:
                 print("⚠️ Telethon conectado, pero no autorizado. Reintentando auth...")
                 try:
                    await client.start()
                    # Si el start tiene éxito (ej. con archivo de sesión), is_auth se actualiza
                    if await client.is_user_authorized():
                        is_auth = True
                        # Si la sesión es ahora un StringSession y no estaba guardada, la guardamos
                        if isinstance(client.session, StringSession):
                            new_string = client.session.save()
                            if new_string != SESSION_STRING: # Guardar solo si es una sesión nueva/actualizada
                                save_session_to_github(new_string)

                 except Exception:
                     pass

            if is_auth:
                await client.get_entity(LEDERDATA_BOT_ID) 
                await client.get_entity(LEDERDATA_BACKUP_BOT_ID) 
                await client.get_dialogs(limit=1) 
                print("✅ Reconexión y verificación de bots exitosa.")
            else:
                 print("🔴 Cliente no autorizado. Requerido /login.")


        except Exception:
            traceback.print_exc()
        await asyncio.sleep(300) # Dormir 5 minutos

asyncio.run_coroutine_threadsafe(_ensure_connected(), loop)

# --- Rutas HTTP Base (Login/Status/General) ---

@app.route("/")
def root():
    return jsonify({
        "status": "ok",
        "message": "Gateway API para LEDER DATA Bot activo. Consulta /status para la sesión.",
    })

@app.route("/status")
def status():
    try:
        is_auth = run_coro(client.is_user_authorized())
    except Exception:
        is_auth = False

    current_session = None
    try:
        if is_auth and isinstance(client.session, StringSession):
            current_session = client.session.save()
    except Exception:
        pass
    
    bot_status = {}
    for bot_id in ALL_BOT_IDS:
        is_blocked = is_bot_blocked(bot_id)
        bot_status[bot_id] = {
            "blocked": is_blocked,
            "last_fail": bot_fail_tracker.get(bot_id).isoformat() if bot_fail_tracker.get(bot_id) else None
        }

    return jsonify({
        "authorized": bool(is_auth),
        "pending_phone": pending_phone["phone"],
        "session_loaded": bool(SESSION_STRING),
        "session_string_saved_to_github": bool(current_session), # Indica si se generó una string
        "bot_status": bot_status,
    })

@app.route("/login")
def login():
    phone = request.args.get("phone")
    if not phone: return jsonify({"error": "Falta parámetro phone"}), 400

    async def _send_code():
        await client.connect()
        if await client.is_user_authorized(): return {"status": "already_authorized"}
        try:
            await client.send_code_request(phone)
            pending_phone["phone"] = phone
            pending_phone["sent_at"] = datetime.utcnow().isoformat()
            return {"status": "code_sent", "phone": phone}
        except Exception as e: return {"status": "error", "error": str(e)}

    result = run_coro(_send_code())
    return jsonify(result)

@app.route("/code")
def code():
    code = request.args.get("code")
    if not code: return jsonify({"error": "Falta parámetro code"}), 400
    if not pending_phone["phone"]: return jsonify({"error": "No hay login pendiente"}), 400

    phone = pending_phone["phone"]
    async def _sign_in():
        try:
            await client.sign_in(phone, code)
            await client.start()
            pending_phone["phone"] = None
            pending_phone["sent_at"] = None
            
            # OBTENER y GUARDAR la nueva StringSession en GitHub
            new_string = client.session.save()
            save_session_to_github(new_string) 
            
            return {"status": "authenticated", "session_string": new_string, "saved_to_github": True}
        except errors.SessionPasswordNeededError: return {"status": "error", "error": "2FA requerido"}
        except Exception as e: return {"status": "error", "error": str(e)}

    result = run_coro(_sign_in())
    return jsonify(result)

# ... (Las rutas /send, /get y /files se mantienen iguales) ...

@app.route("/send")
def send_msg():
    chat_id = request.args.get("chat_id")
    msg = request.args.get("msg")
    if not chat_id or not msg:
        return jsonify({"error": "Faltan parámetros"}), 400

    async def _send(): 
        target = int(chat_id) if chat_id.isdigit() else chat_id
        entity = await client.get_entity(target)
        await client.send_message(entity, msg)
        return {"status": "sent", "to": chat_id, "msg": msg}
    try:
        result = run_coro(_send())
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500 

@app.route("/get")
def get_msgs():
    with _messages_lock:
        data = list(messages)
        return jsonify({
            "message": "found data" if data else "no data",
            "result": {"quantity": len(data), "coincidences": data},
        })

@app.route("/files/<path:filename>")
def files(filename):
    """
    Ruta para descargar archivos. Se añade as_attachment=True para forzar la descarga 
    en lugar de visualizar el archivo, lo que es ideal para appcreator24.
    """
    return send_from_directory(DOWNLOAD_DIR, filename, as_attachment=True)


# ----------------------------------------------------------------------
# --- Rutas HTTP de API (Comandos LEDER DATA) ----------------------------
# (Se mantienen los comandos y la lógica de parseo de parámetros)
# ----------------------------------------------------------------------

@app.route("/dni", methods=["GET"])
@app.route("/dnif", methods=["GET"]) 
@app.route("/dnidb", methods=["GET"])
@app.route("/dnifdb", methods=["GET"])
@app.route("/c4", methods=["GET"])
@app.route("/dnivaz", methods=["GET"]) 
@app.route("/dnivam", methods=["GET"])
@app.route("/dnivel", methods=["GET"])
@app.route("/dniveln", methods=["GET"])
@app.route("/fa", methods=["GET"])
@app.route("/fadb", methods=["GET"])
@app.route("/fb", methods=["GET"])
@app.route("/fbdb", methods=["GET"])
@app.route("/cnv", methods=["GET"])
@app.route("/cdef", methods=["GET"])
@app.route("/antpen", methods=["GET"])
@app.route("/antpol", methods=["GET"])
@app.route("/antjud", methods=["GET"])
@app.route("/actancc", methods=["GET"])
@app.route("/actamcc", methods=["GET"])
@app.route("/actadcc", methods=["GET"])
@app.route("/osiptel", methods=["GET"])
@app.route("/claro", methods=["GET"])
@app.route("/entel", methods=["GET"])
@app.route("/pro", methods=["GET"]) 
@app.route("/sen", methods=["GET"]) 
@app.route("/sbs", methods=["GET"]) 
@app.route("/tra", methods=["GET"]) 
@app.route("/tremp", methods=["GET"]) 
@app.route("/sue", methods=["GET"]) 
@app.route("/cla", methods=["GET"]) 
@app.route("/sune", methods=["GET"]) 
@app.route("/cun", methods=["GET"]) 
@app.route("/colp", methods=["GET"]) 
@app.route("/mine", methods=["GET"]) 
@app.route("/pasaporte", methods=["GET"]) 
@app.route("/seeker", methods=["GET"]) 
@app.route("/afp", methods=["GET"]) 
@app.route("/bdir", methods=["GET"]) 
@app.route("/meta", methods=["GET"]) 
@app.route("/fis", methods=["GET"]) 
@app.route("/fisdet", methods=["GET"]) 
@app.route("/det", methods=["GET"]) 
@app.route("/rqh", methods=["GET"]) 
@app.route("/antpenv", methods=["GET"]) 
@app.route("/dend", methods=["GET"]) 
@app.route("/dence", methods=["GET"]) 
@app.route("/denpas", methods=["GET"]) 
@app.route("/denci", methods=["GET"]) 
@app.route("/denp", methods=["GET"]) 
@app.route("/denar", methods=["GET"]) 
@app.route("/dencl", methods=["GET"]) 
@app.route("/agv", methods=["GET"]) 
@app.route("/agvp", methods=["GET"]) 
@app.route("/cedula", methods=["GET"]) 
def api_dni_based_command():
    
    command_name = request.path.lstrip('/') 
    
    dni_required_commands = [
        "dni", "dnif", "dnidb", "dnifdb", "c4", "dnivaz", "dnivam", "dnivel", "dniveln", 
        "fa", "fadb", "fb", "fbdb", "cnv", "cdef", "antpen", "antpol", "antjud", 
        "actancc", "actamcc", "actadcc", "tra", "sue", "cla", "sune", "cun", "colp", 
        "mine", "afp", "antpenv", "dend", "meta", "fis", "det", "rqh", "agv", "agvp"
    ]
    
    query_required_commands = [
        "tel", "telp", "cor", "nmv", "tremp", 
        "fisdet",
        "dence", "denpas", "denci", "denp", "denar", "dencl", 
        "cedula", 
    ]
    
    optional_commands = ["osiptel", "claro", "entel", "pro", "sen", "sbs", "pasaporte", "seeker", "bdir"]
    
    param = ""

    if command_name in dni_required_commands:
        param = request.args.get("dni")
        if not param or not param.isdigit() or len(param) != 8:
            return jsonify({"status": "error", "message": f"Parámetro 'dni' es requerido y debe ser un número de 8 dígitos para /{command_name}."}), 400
    
    elif command_name in query_required_commands:
        
        param_value = None
        
        if command_name == "fisdet":
            param_value = request.args.get("caso") or request.args.get("distritojudicial") or request.args.get("query")
            if not param_value:
                dni_val = request.args.get("dni")
                det_val = request.args.get("detalle")
                if dni_val and det_val:
                    param_value = f"{dni_val}|{det_val}"
                elif dni_val:
                    param_value = dni_val
        
        elif command_name == "dence":
            param_value = request.args.get("carnet_extranjeria")
        elif command_name == "denpas":
            param_value = request.args.get("pasaporte")
        elif command_name == "denci":
            param_value = request.args.get("cedula_identidad")
        elif command_name == "denp":
            param_value = request.args.get("placa")
        elif command_name == "denar":
            param_value = request.args.get("serie_armamento")
        elif command_name == "dencl":
            param_value = request.args.get("clave_denuncia")
            
        elif command_name == "cedula":
            param_value = request.args.get("cedula")
        
        param = param_value or request.args.get("dni") or request.args.get("query")
             
        if not param:
            return jsonify({"status": "error", "message": f"Parámetro de consulta es requerido para /{command_name}."}), 400
    
    elif command_name in optional_commands:
        param_dni = request.args.get("dni")
        param_query = request.args.get("query")
        param_pasaporte = request.args.get("pasaporte") if command_name == "pasaporte" else None
        
        param = param_dni or param_query or param_pasaporte or ""
        
    else:
        param = request.args.get("dni") or request.args.get("query") or ""

        
    command = f"/{command_name} {param}".strip()
    
    try:
        result = run_coro(_call_api_command(command, timeout=TIMEOUT_FAILOVER))
        
        if result.get("status", "").startswith("error"):
            is_timeout_or_connection_error = "timeout" in result.get("message", "").lower() or "telethon" in result.get("message", "").lower() or result.get("status") == "error_timeout"
            status_code = 500 if is_timeout_or_connection_error else 400
            result.pop("bot_used", None)
            return jsonify(result), status_code
            
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error interno: {str(e)}"}), 500

@app.route("/dni_nombres", methods=["GET"])
def api_dni_nombres():
    
    nombres = unquote(request.args.get("nombres", "")).strip()
    ape_paterno = unquote(request.args.get("apepaterno", "")).strip()
    ape_materno = unquote(request.args.get("apematerno", "")).strip()

    if not ape_paterno or not ape_materno:
        return jsonify({"status": "error", "message": "Faltan parámetros: 'apepaterno' y 'apematerno' son obligatorios."}), 400

    formatted_nombres = nombres.replace(" ", ",")
    formatted_apepaterno = ape_paterno.replace(" ", "+")
    formatted_apematerno = ape_materno.replace(" ", "+")

    command = f"/nm {formatted_nombres}|{formatted_apepaterno}|{formatted_apematerno}"
    
    try:
        result = run_coro(_call_api_command(command, timeout=TIMEOUT_FAILOVER))
        if result.get("status", "").startswith("error"):
            is_timeout_or_connection_error = "timeout" in result.get("message", "").lower() or "telethon" in result.get("message", "").lower() or result.get("status") == "error_timeout"
            result.pop("bot_used", None)
            return jsonify(result), 500 if is_timeout_or_connection_error else 400
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error interno: {str(e)}"}), 500

@app.route("/venezolanos_nombres", methods=["GET"])
def api_venezolanos_nombres():
    
    query = unquote(request.args.get("query", "")).strip()
    
    if not query:
        return jsonify({"status": "error", "message": "Parámetro 'query' (nombres_apellidos) es requerido para /venezolanos_nombres."}), 400

    command = f"/nmv {query}"
    
    try:
        result = run_coro(_call_api_command(command, timeout=TIMEOUT_FAILOVER))
        if result.get("status", "").startswith("error"):
            is_timeout_or_connection_error = "timeout" in result.get("message", "").lower() or "telethon" in result.get("message", "").lower() or result.get("status") == "error_timeout"
            result.pop("bot_used", None)
            return jsonify(result), 500 if is_timeout_or_connection_error else 400
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error interno: {str(e)}"}), 500
        
# ----------------------------------------------------------------------
# --- Inicio de la Aplicación ------------------------------------------
# ----------------------------------------------------------------------

if __name__ == "__main__":
    try:
        run_coro(client.connect())
        if not run_coro(client.is_user_authorized()):
             run_coro(client.start())
             
        if run_coro(client.is_user_authorized()):
             # Si la sesión se cargó/inició correctamente, obtenemos la string y la guardamos en GitHub
             current_session_string = client.session.save()
             if current_session_string != SESSION_STRING:
                 save_session_to_github(current_session_string)

        run_coro(client.get_entity(LEDERDATA_BOT_ID)) 
        run_coro(client.get_entity(LEDERDATA_BACKUP_BOT_ID)) 
    except Exception:
        pass
    print(f"🚀 App corriendo en http://0.0.0.0:{PORT}")
    app.run(host="0.0.0.0", port=PORT, threaded=True)
