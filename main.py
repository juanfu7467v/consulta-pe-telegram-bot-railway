import os
import re
import asyncio
import threading
import traceback
import time
import requests
import json
import base64
from collections import deque
from datetime import datetime, timezone, timedelta
from urllib.parse import unquote, quote
# IMPORTAR CONCURRENT.FUTURES
from concurrent.futures import TimeoutError as FutureTimeoutError 
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
from telethon.tl.types import PeerUser
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto
from telethon.errors.rpcerrorlist import UserBlockedError

# --- Configuración y Variables de Entorno ---

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
PUBLIC_URL = os.getenv("PUBLIC_URL", "https://consulta-pe-bot.up.railway.app").rstrip("/")
# CLAVE: SESSION_STRING se carga directamente de la variable de entorno al inicio.
# Se añade una variable para el ID único del bot en la base de datos externa
BOT_API_ID = os.getenv("BOT_API_ID", "mi_bot_principal")
SESSION_STRING = os.getenv("SESSION_STRING", None)
PORT = int(os.getenv("PORT", 8080))

# --- VARIABLES DE ENTORNO PARA GITHUB (ELIMINADAS) ---

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

LEDERDATA_BOT_ID = "@LEDERDATA_OFC_BOT" 
LEDERDATA_BACKUP_BOT_ID = "@lederdata_publico_bot"
ALL_BOT_IDS = [LEDERDATA_BOT_ID, LEDERDATA_BACKUP_BOT_ID]

# MODIFICACIÓN CLAVE DE TIMEOUTS
TIMEOUT_FAILOVER = 15 
TIMEOUT_TOTAL = 50 
SYNC_WAIT_TIMEOUT = 55 # Tiempo máximo de espera para el worker de Gunicorn

# --- LÓGICA DE PERSISTENCIA (MODIFICADA) ---

# URL base de la API externa
API_BASE_URL = "https://base-datos-consulta-pe.fly.dev"

def _load_session_from_api(bot_id: str) -> str | None:
    """Intenta recuperar la session_string de la API externa."""
    try:
        url = f"{API_BASE_URL}/historial/session_string"
        print(f"📡 Intentando recuperar sesión desde {url}...")
        
        # Se asume que la API GET es rápida, por lo que usamos un timeout bajo de 10s
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            # La ruta devuelve un arreglo, buscamos el objeto con el 'id' correcto
            for item in data:
                if item.get("id") == bot_id and item.get("session_string"):
                    print(f"✅ Sesión recuperada de la API para ID: {bot_id}.")
                    return item["session_string"]
            print(f"⚠️ No se encontró una sesión válida en la API para ID: {bot_id}.")
            return None
        else:
            print(f"❌ Error al consultar la API (HTTP {response.status_code}).")
            return None
    except requests.RequestException as e:
        print(f"❌ Error de conexión al intentar cargar sesión: {e}")
        return None

def _save_session_to_api(bot_id: str, session_str: str) -> bool:
    """Guarda la session_string en la API externa mediante POST."""
    try:
        url = f"{API_BASE_URL}/guardar-post/session_string"
        headers = {"Content-Type": "application/json"}
        payload = {
            "id": bot_id,
            "session_string": session_str
        }
        
        print(f"📡 Enviando sesión a {url}...")
        # Se asume que la API POST es rápida, por lo que usamos un timeout bajo de 10s
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        
        if response.status_code in [200, 201]:
            print("✅ Session_string guardada/actualizada con éxito en la API externa.")
            return True
        else:
            print(f"❌ Error al guardar sesión en la API (HTTP {response.status_code}, respuesta: {response.text})")
            return False
    except requests.RequestException as e:
        print(f"❌ Error de conexión al intentar guardar sesión: {e}")
        return False

# --- Carga de Sesión ANTES de la Inicialización del Cliente ---

# 1. Intentar cargar desde la variable de entorno (prioridad si existe y es válida)
if SESSION_STRING and SESSION_STRING.strip() and SESSION_STRING != "consulta_pe_bot":
    session_to_use = SESSION_STRING
    print("🔑 Usando SESSION_STRING cargada de variable de entorno.")
# 2. Intentar cargar desde la API externa
else:
    api_session = _load_session_from_api(BOT_API_ID)
    if api_session:
        session_to_use = api_session
        print("💡 Usando SESSION_STRING recuperada de la API externa.")
        SESSION_STRING = api_session # Actualiza la variable de entorno en memoria para /status
    else:
        # Usará un archivo de sesión local que se perderá al reiniciar el contenedor, 
        # forzando un nuevo login si las otras dos opciones fallan.
        session_to_use = "consulta_pe_session" 
        print("📂 No hay SESSION_STRING persistente. Usando sesión de archivo local (requiere login inicial).")

session = StringSession(session_to_use)
# --- Fin Carga de Sesión ---


# --- Manejo de Fallos por Bot (Sin cambios) ---
bot_fail_tracker = {}
BOT_FAIL_TIMEOUT_HOURS = 6 

def is_bot_blocked(bot_id: str) -> bool:
    """Verifica si el bot está temporalmente bloqueado por fallos previos."""
    last_fail_time = bot_fail_tracker.get(bot_id)
    if not last_fail_time:
        return False

    now = datetime.now()
    six_hours_ago = now - timedelta(hours=BOT_FAIL_TIMEOUT_HOURS)

    if last_fail_time > six_hours_ago:
        return True
    
    print(f"✅ Bot {bot_id} ha cumplido su tiempo de bloqueo. Desbloqueado.")
    bot_fail_tracker.pop(bot_id, None)
    return False

def record_bot_failure(bot_id: str):
    """Registra la hora actual como la última hora de fallo del bot."""
    print(f"🚨 Bot {bot_id} ha fallado y será BLOQUEADO por {BOT_FAIL_TIMEOUT_HOURS} horas.")
    bot_fail_tracker[bot_id] = datetime.now()

# --- Aplicación Flask (Sin cambios) ---
app = Flask(__name__)
CORS(app)

# --- Bucle Asíncrono para Telethon (Sin cambios) ---
loop = asyncio.new_event_loop()
threading.Thread(
    target=lambda: (asyncio.set_event_loop(loop), loop.run_forever()), daemon=True
).start()

def run_coro(coro):
    """Ejecuta una corrutina en el bucle principal y espera el resultado SÍNCRONAMENTE."""
    return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=SYNC_WAIT_TIMEOUT) 

# --- Configuración del Cliente Telegram (Sin cambios) ---
client = TelegramClient(session, API_ID, API_HASH, loop=loop)

# Mensajes en memoria
messages = deque(maxlen=2000)
_messages_lock = threading.Lock()
response_waiters = {} 
pending_phone = {"phone": None, "sent_at": None}

# --- Lógica de Limpieza y Extracción de Datos (Sin cambios) ---
def clean_and_extract(raw_text: str):
    
    if not raw_text:
        return {"text": "", "fields": {}}

    text = raw_text
    
    # 1. Reemplazar la marca LEDER_BOT por CONSULTA PE
    text = re.sub(r"^\[\#LEDER\_BOT\]", "[CONSULTA PE]", text, flags=re.IGNORECASE | re.DOTALL)
    
    # 2. Eliminar cabecera (patrón más robusto)
    header_pattern = r"^\[.*?\]\s*→\s*.*?\[.*?\](\r?\n){1,2}"
    text = re.sub(header_pattern, "", text, flags=re.IGNORECASE | re.DOTALL)
    
    # 3. ELIMINAR EXPLICITAMENTE MARCA LEDERDATA Y CRÉDITOS
    # Patrón para eliminar pie (créditos, paginación, warnings al final, y marcas específicas)
    footer_pattern = r"((\r?\n){1,2}\[|Página\s*\d+\/\d+.*|(\r?\n){1,2}Por favor, usa el formato correcto.*|↞ Anterior|Siguiente ↠.*|Credits\s*:.+|Wanted for\s*:.+|\s*@lederdata.*|(\r?\n){1,2}\s*Marca\s*@lederdata.*|(\r?\n){1,2}\s*Créditos\s*:\s*\d+)"
    text = re.sub(footer_pattern, "", text, flags=re.IGNORECASE | re.DOTALL)
    
    # 4. Limpiar separador (si queda)
    text = re.sub(r"\-{3,}", "", text, flags=re.IGNORECASE | re.DOTALL)

    # 5. Limpiar espacios
    text = text.strip()

    # 6. Extraer datos clave
    fields = {}
    dni_match = re.search(r"DNI\s*:\s*(\d{8})", text, re.IGNORECASE)
    if dni_match: fields["dni"] = dni_match.group(1)
    
    ruc_match = re.search(r"RUC\s*:\s*(\d{11})", text, re.IGNORECASE)
    if ruc_match: fields["ruc"] = ruc_match.group(1)

    photo_type_match = re.search(r"Foto\s*:\s*(rostro|huella|firma|adverso|reverso).*", text, re.IGNORECASE)
    if photo_type_match: fields["photo_type"] = photo_type_match.group(1).lower()
    
    # 7. MANEJO DE MENSAJES DE NO ENCONTRADO
    not_found_pattern = r"\[⚠️\]\s*(no se encontro información|no se han encontrado resultados|no se encontró una|no hay resultados|no tenemos datos|no se encontraron registros)"
    if re.search(not_found_pattern, text, re.IGNORECASE | re.DOTALL):
         fields["not_found"] = True

    return {"text": text, "fields": fields}

# --- Handler de nuevos mensajes (Sin cambios) ---
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

        # 2. Manejar archivos (media)
        if getattr(event, "message", None) and getattr(event.message, "media", None):
            media_list = []
            
            if isinstance(event.message.media, (MessageMediaDocument, MessageMediaPhoto)):
                media_list.append(event.message.media)
            
            if media_list:
                try:
                    timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
                    
                    for i, media in enumerate(media_list):
                        file_ext = '.file'
                        is_photo = False
                        
                        if hasattr(media, 'document') and hasattr(media.document, 'attributes'):
                            file_ext = os.path.splitext(getattr(media.document, 'file_name', 'file'))[1]
                        elif isinstance(media, MessageMediaPhoto) or (hasattr(media, 'photo') and media.photo):
                            file_ext = '.jpg'
                            is_photo = True
                            
                        dni_part = f"_{cleaned['fields'].get('dni')}" if cleaned["fields"].get("dni") else ""
                        type_part = f"_{cleaned['fields'].get('photo_type')}" if cleaned['fields'].get('photo_type') else ""
                        unique_filename = f"{timestamp_str}_{event.message.id}{dni_part}{type_part}_{i}{file_ext}"
                        
                        saved_path = await client.download_media(event.message, file=os.path.join(DOWNLOAD_DIR, unique_filename))
                        filename = os.path.basename(saved_path)
                        
                        url_obj = {
                            "url": f"{PUBLIC_URL}/files/{filename}", 
                            "type": cleaned['fields'].get('photo_type', 'image' if is_photo else 'document'),
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

        # 3. Intentar resolver la espera de la API
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
                    
                    # Si recibimos un error de formato o de "no encontrado", resolvemos inmediatamente
                    is_immediate_error = "Por favor, usa el formato correcto" in msg_obj["message"] or msg_obj["fields"].get("not_found", False)
                    
                    if is_immediate_error:
                        if waiter_data["timer"]:
                             waiter_data["timer"].cancel()
                        loop.call_soon_threadsafe(waiter_data["future"].set_result, msg_obj)
                        response_waiters.pop(command_id, None)
                        resolved = True
                        break

        # 4. Agregar a la cola de historial si no se usó para una respuesta específica
        if not resolved:
            with _messages_lock:
                messages.appendleft(msg_obj)

    except Exception:
        traceback.print_exc() 

client.add_event_handler(_on_new_message, events.NewMessage(incoming=True))


# ----------------------------------------------------------------------
# --- FUNCIONES DE GITHUB (ELIMINADAS) ---------------------------------
# ----------------------------------------------------------------------
def _extract_data_for_save(command: str, result: dict) -> tuple[str, dict] | tuple[None, None]:
    # Función dummy, ya que no se guardará en GitHub
    return (None, None)

async def _guardar_datos_github(tipo: str, datos: dict):
    # Función dummy, ya que no se guardará en GitHub
    pass
        
# ----------------------------------------------------------------------
# --- FUNCIÓN CENTRAL MODIFICADA (Eliminada llamada a GitHub) ---
# ----------------------------------------------------------------------

async def _call_api_command(command: str, timeout: int = TIMEOUT_TOTAL):
    """Envía un comando al bot y espera la respuesta(s), con lógica de respaldo y bloqueo por fallo."""
    if not await client.is_user_authorized():
        raise Exception("Cliente no autorizado. Por favor, inicie sesión.")

    command_id = time.time()
    
    # Extraer DNI para hacer match con la respuesta
    dni_match = re.search(r"/\w+\s+(\d{8})", command)
    dni = dni_match.group(1) if dni_match else None
    
    bots_to_try = [LEDERDATA_BOT_ID, LEDERDATA_BACKUP_BOT_ID]
    
    max_timeout = TIMEOUT_TOTAL # 50s
    final_error = None 
    
    for attempt, current_bot_id in enumerate(bots_to_try, 1):
        
        if is_bot_blocked(current_bot_id):
            print(f"🚫 Bot {current_bot_id} está BLOQUEADO temporalmente. Saltando.")
            if attempt == len(bots_to_try): 
                final_error = {"status": "error", "message": f"Ambos bots están bloqueados. Último bot bloqueado: {current_bot_id}.", "bot_used": current_bot_id}
            continue

        future = loop.create_future()
        current_timeout = TIMEOUT_FAILOVER if attempt == 1 else max_timeout
        
        waiter_data = {
            "future": future,
            "messages": [], 
            "dni": dni,
            "command": command,
            "timer": None, 
            "sent_to_bot": current_bot_id,
            "has_response": False 
        }
        
        def _on_timeout(bot_id_on_timeout=current_bot_id, command_id_on_timeout=command_id):
            """Función de callback del timer para manejar el timeout de acumulación."""
            with _messages_lock:
                waiter_data = response_waiters.pop(command_id_on_timeout, None)
                if waiter_data and not waiter_data["future"].done():
                    
                    if waiter_data["messages"]:
                        # Devolver mensajes acumulados si hay alguno (cumple acumulación)
                        print(f"✅ Timeout de {current_timeout}s alcanzado para acumulación en {bot_id_on_timeout}. Devolviendo {len(waiter_data['messages'])} mensaje(s).")
                        loop.call_soon_threadsafe(
                            waiter_data["future"].set_result, 
                            waiter_data["messages"]
                        )
                    else:
                        # Si no hay respuesta, es un timeout (esto activa el failover si es el primer bot)
                        loop.call_soon_threadsafe(
                            waiter_data["future"].set_result, 
                            {"status": "error_timeout", "message": f"Tiempo de espera de respuesta agotado ({current_timeout}s). No se recibió NINGÚN mensaje.", "bot": bot_id_on_timeout, "fail_recorded": False}
                        )

        waiter_data["timer"] = loop.call_later(current_timeout, _on_timeout)

        with _messages_lock:
            response_waiters[command_id] = waiter_data

        print(f"📡 Enviando comando (Intento {attempt}) a {current_bot_id} [Timeout: {current_timeout}s]: {command}")
        
        try:
            await client.send_message(current_bot_id, command)
            result = await future
            
            # --- Lógica de Failover/Retorno de Resultado ---
            
            # 1. Timeout de NO RESPUESTA (15s)
            if isinstance(result, dict) and result.get("status") == "error_timeout" and attempt == 1:
                print(f"⌛ Timeout de NO RESPUESTA de {LEDERDATA_BOT_ID} (15s). Intentando con {LEDERDATA_BACKUP_BOT_ID}.")
                final_error = result
                continue
                
            # 2. Si el segundo bot falla por TIMEOUT (50s), retornamos el error.
            elif isinstance(result, dict) and result.get("status") == "error_timeout" and attempt == 2:
                final_error = result
                break 
            
            # 3. Manejo de error de formato/No encontrado (resuelve inmediatamente en _on_new_message)
            if isinstance(result, dict):
                 if "Por favor, usa el formato correcto" in result.get("message", ""):
                      return {"status": "error_bot_format", "message": "Formato de consulta incorrecto. " + result.get("message"), "bot_used": current_bot_id}
                 # *** NUEVO: MANEJO DE NO ENCONTRADO ***
                 if result["fields"].get("not_found", False):
                      return {"status": "error_not_found", "message": "No se encontraron resultados para dicha consulta. Intenta con otro dato.", "bot_used": current_bot_id}
                      
            # 4. Procesar respuesta exitosa (lista de mensajes acumulados o mensaje simple)
            list_of_messages = result if isinstance(result, list) else [] 
            
            if isinstance(list_of_messages, list) and len(list_of_messages) > 0:
                
                final_result = list_of_messages[0].copy() 
                final_result["full_messages"] = [msg["message"] for msg in list_of_messages] 
                
                consolidated_urls = {} 
                type_map = {"rostro": "ROSTRO", "huella": "HUELLA", "firma": "FIRMA", "adverso": "ADVERSO", "reverso": "REVERSO"}
                
                for msg in list_of_messages:
                    for url_obj in msg.get("urls", []):
                        key_type = url_obj["type"].lower()
                        key = type_map.get(key_type)
                        
                        if key:
                            if key not in consolidated_urls:
                                consolidated_urls[key] = url_obj["url"]
                        else:
                            base_key = url_obj["type"].upper()
                            i = 1
                            key_name = base_key
                            if key_name in consolidated_urls:
                                while f"{base_key}_{i}" in consolidated_urls: i += 1
                                key_name = f"{base_key}_{i}"
                            consolidated_urls[key_name] = url_obj["url"]

                    if not final_result["fields"].get("dni") and msg["fields"].get("dni"):
                        final_result["fields"] = msg["fields"]
                    if not final_result["fields"].get("ruc") and msg["fields"].get("ruc"):
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
                
                # Mover DNI/RUC al nivel superior si existen en 'fields'
                dni_val_final = final_json["fields"].get("dni")
                ruc_val_final = final_json["fields"].get("ruc")

                if dni_val_final:
                    final_json["dni"] = dni_val_final
                    final_json["fields"].pop("dni")
                if ruc_val_final:
                    final_json["ruc"] = ruc_val_final
                    final_json["fields"].pop("ruc")
                
                final_json["status"] = "ok"
                # *** ELIMINADA: LÓGICA DE GUARDADO EN GITHUB ***
                
                return final_json
                
            else: 
                final_error = {"status": "error", "message": f"Respuesta vacía o inesperada del bot {current_bot_id}.", "bot_used": current_bot_id}
                if attempt == 1:
                    print(f"❌ Respuesta vacía de {LEDERDATA_BOT_ID}. Intentando con {LEDERDATA_BACKUP_BOT_ID}.")
                    continue
                else:
                    break
            
        # --- Manejo de Errores de Conexión/Bloqueo ---
        
        except UserBlockedError as e:
            error_msg = f"Error de Telethon/conexión/fallo: You blocked this user (caused by SendMessageRequest)"
            print(f"❌ Error de BLOQUEO en {current_bot_id}: {error_msg}. Registrando fallo y pasando al siguiente bot.")
            
            record_bot_failure(current_bot_id)
            final_error = {"status": "error", "message": error_msg, "bot_used": current_bot_id}
            
            with _messages_lock:
                 if command_id in response_waiters:
                    waiter_data = response_waiters.pop(command_id, None)
                    if waiter_data and waiter_data["timer"]:
                        waiter_data["timer"].cancel()
                        
            if attempt == 1:
                continue
            else:
                break
            
        except Exception as e:
            error_msg = f"Error de Telethon/conexión/fallo: {str(e)}"
            final_error = {"status": "error", "message": error_msg, "bot_used": current_bot_id}
            
            is_serious_error = not ("Timeout" in str(e) or "Timed out" in str(e))
            if is_serious_error:
                 print(f"❌ Error grave de Telethon en {current_bot_id}: {error_msg}. Registrando fallo.")
                 record_bot_failure(current_bot_id)
            else:
                 print(f"❌ Error de Timeout de Telethon en {current_bot_id}: {error_msg}.")
                 
            with _messages_lock:
                 if command_id in response_waiters:
                    waiter_data = response_waiters.pop(command_id, None)
                    if waiter_data and waiter_data["timer"]:
                        waiter_data["timer"].cancel()

            if attempt == 1:
                continue
            else:
                break
                
        finally:
            with _messages_lock:
                if command_id in response_waiters:
                    waiter_data = response_waiters.pop(command_id, None)
                    if waiter_data and waiter_data["timer"]:
                        waiter_data["timer"].cancel()

    if final_error:
        # Si ambos fallaron, eliminamos el 'bot_used' del error final antes de devolver
        final_error.pop("bot_used", None)
        return final_error
        
    return {"status": "error", "message": "Fallo desconocido. Ambos bots están bloqueados o agotaron el tiempo de espera."}


# --- Rutinas y Rutas HTTP ---

async def _ensure_connected():
    """Mantiene la conexión y autorización activa. Tarea de fondo 24/7."""
    while True:
        try:
            if not client.is_connected():
                print("🔌 Intentando reconectar Telethon...")
                await client.connect()
            
            if client.is_connected() and not await client.is_user_authorized():
                 print("⚠️ Telethon conectado, pero no autorizado. Requerido /login para obtener SESSION_STRING.")
                 try:
                    # Intenta un start/re-start
                    # Esto solo funcionará si la sesión existe localmente O si la SESSION_STRING de entorno/API es válida
                    await client.start()
                    if await client.is_user_authorized():
                        print("✅ Sesión restaurada con éxito.")
                 except Exception:
                     pass

            if await client.is_user_authorized():
                # Pruebas ligeras para mantener la conexión viva y verificar permisos
                await client.get_entity(LEDERDATA_BOT_ID) 
                await client.get_entity(LEDERDATA_BACKUP_BOT_ID) 
                await client.get_dialogs(limit=1) 
                print("✅ Cliente autorizado y verificación de bots exitosa.")
            else:
                 print("🔴 Cliente no autorizado.")


        except Exception:
            pass
        await asyncio.sleep(300) # Revisa cada 5 minutos

asyncio.run_coroutine_threadsafe(_ensure_connected(), loop)

# --- Rutas HTTP ---

@app.route("/")
def root():
    return jsonify({
        "status": "ok",
        "message": "Gateway API para LEDER DATA Bot activo. Consulta /status para la sesión.",
    })

# --- NUEVA RUTA: Guardar la sesión mediante POST en la API externa ---
@app.route("/guardar-post/session_string", methods=["POST"])
def save_session_string():
    """
    Ruta para guardar la session_string en la base de datos de persistencia.
    Esperamos JSON: {"id": "mi_bot_principal", "session_string": "..."}
    """
    try:
        data = request.get_json()
        if not data or "id" not in data or "session_string" not in data:
            return jsonify({"status": "error", "message": "Faltan 'id' o 'session_string' en el cuerpo JSON."}), 400
        
        bot_id = data["id"]
        session_str = data["session_string"]
        
        # Ejecutar la función de guardado (que ya contiene la lógica de requests.post)
        if _save_session_to_api(bot_id, session_str):
             return jsonify({
                 "status": "ok", 
                 "message": f"Session_string recibida y enviada a la API externa para el ID: {bot_id}.",
                 "persisted_id": bot_id
             }), 200
        else:
             return jsonify({
                 "status": "error", 
                 "message": "Fallo al guardar la session_string en la API externa. Revisa los logs."
             }), 500
             
    except Exception as e:
        print(f"Error en /guardar-post/session_string: {e}")
        return jsonify({"status": "error", "message": f"Error interno: {str(e)}"}), 500

# --- FIN NUEVA RUTA ---


@app.route("/status")
def status():
    global SESSION_STRING # Asegurar que accedemos a la variable global
    
    # Intento de conexión ligera
    try:
        run_coro(client.connect()) 
    except FutureTimeoutError:
         pass 
    except Exception:
         pass

    # Verificación de autorización
    is_auth = False
    try:
        is_auth = run_coro(client.is_user_authorized())
    except FutureTimeoutError:
         is_auth = False
    except Exception:
        is_auth = False

    # Obtener la sesión actual para el estado
    current_session = None
    try:
        if is_auth and client.is_connected():
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
        # Se muestra True si SESSION_STRING tiene un valor cargado (de ENV o API)
        "session_loaded_from_env_or_api": True if SESSION_STRING else False, 
        "current_session_string": current_session, # Muestra la string actual si está conectado
        "bot_status": bot_status,
        "github_save_enabled": False,
        "api_persistence_enabled": True
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

    try:
        result = run_coro(_send_code())
        return jsonify(result)
    except FutureTimeoutError:
        return jsonify({"status": "error", "error": f"Timeout en la conexión o proceso de login (espera max {SYNC_WAIT_TIMEOUT}s). Revise el estado de la conexión."}), 500
    except Exception as e:
        return jsonify({"status": "error", "error": f"Error interno en /login: {str(e)}"}), 500

@app.route("/code")
def code():
    global SESSION_STRING # Usamos la variable global
    code = request.args.get("code")
    if not code: return jsonify({"error": "Falta parámetro code"}), 400
    if not pending_phone["phone"]: return jsonify({"error": "No hay login pendiente"}), 400

    phone = pending_phone["phone"]
    async def _sign_in():
        try:
            await client.connect() 
            await client.sign_in(phone, code)
            await client.start() 
            pending_phone["phone"] = None
            pending_phone["sent_at"] = None
            
            # **CLAVE: Guarda la nueva sesión generada e IMPRIME el String**
            new_string = client.session.save()
            SESSION_STRING = new_string # Actualiza la variable global en memoria
            
            print("=============================================================================================")
            print("✅ AUTENTICACIÓN EXITOSA. COPIE ESTA SESIÓN y configúrela en la variable de entorno SESSION_STRING:")
            print("=============================================================================================")
            print(new_string)
            print("=============================================================================================")
            
            # CLAVE DE PERSISTENCIA: LLAMADA POST AL SERVIDOR EXTERNO
            persistence_ok = _save_session_to_api(BOT_API_ID, new_string)
            
            note_message = "You must copy this session_string and set it as the SESSION_STRING environment variable for 24/7 persistence."
            if persistence_ok:
                 note_message += " **ADICIONALMENTE: Persistencia en API externa exitosa.**"
            else:
                 note_message += " **ADVERTENCIA: Fallo al guardar en la API externa. Verifique logs y el estado de la API.**"
            
            return {"status": "authenticated", "session_string": new_string, "NOTE": note_message}
            
        except errors.SessionPasswordNeededError: return {"status": "error", "error": "2FA requerido"}
        except Exception as e: return {"status": "error", "error": str(e)}

    try:
        result = run_coro(_sign_in())
        return jsonify(result)
    except FutureTimeoutError:
        return jsonify({"status": "error", "error": f"Timeout en la conexión o proceso de código (espera max {SYNC_WAIT_TIMEOUT}s). Revise el estado de la conexión."}), 500
    except Exception as e:
        return jsonify({"status": "error", "error": f"Error interno en /code: {str(e)}"}), 500

@app.route("/send")
def send_msg():
    chat_id = request.args.get("chat_id")
    msg = request.args.get("msg")
    if not chat_id or not msg:
        return jsonify({"error": "Faltan parámetros"}), 400

    async def _send(): 
        await client.connect() 
        target = int(chat_id) if chat_id.isdigit() else chat_id
        entity = await client.get_entity(target)
        await client.send_message(entity, msg)
        return {"status": "sent", "to": chat_id, "msg": msg}
    try:
        result = run_coro(_send())
        return jsonify(result)
    except FutureTimeoutError:
         return jsonify({"status": "error", "error": f"Timeout al enviar mensaje (espera max {SYNC_WAIT_TIMEOUT}s). Revise el estado de la conexión."}), 500
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
    return send_from_directory(DOWNLOAD_DIR, filename, as_attachment=True)

# ----------------------------------------------------------------------
# --- Rutas HTTP de API (Comandos LEDER DATA) - Sin cambios funcionales ---
# ----------------------------------------------------------------------

@app.route("/sunat", methods=["GET"])
@app.route("/sun", methods=["GET"]) 
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
    command_name_path = request.path.lstrip('/') 
    command_name = "sun" if command_name_path in ["sunat", "sun"] else command_name_path
    
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

    # SUN (Comando especial que acepta DNI o RUC)
    if command_name == "sun":
        param = request.args.get("dni_o_ruc") or request.args.get("query")
        if not param or not param.isdigit() or len(param) not in [8, 11]:
            return jsonify({"status": "error", "message": f"Parámetro 'dni_o_ruc' o 'query' es requerido y debe ser un DNI (8 dígitos) o RUC (11 dígitos) para /{command_name_path}."}), 400
    
    elif command_name in dni_required_commands:
        param = request.args.get("dni")
        if not param or not param.isdigit() or len(param) != 8:
            return jsonify({"status": "error", "message": f"Parámetro 'dni' es requerido y debe ser un número de 8 dígitos para /{command_name_path}."}), 400
    
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
        
        elif command_name == "dence": param_value = request.args.get("carnet_extranjeria")
        elif command_name == "denpas": param_value = request.args.get("pasaporte")
        elif command_name == "denci": param_value = request.args.get("cedula_identidad")
        elif command_name == "denp": param_value = request.args.get("placa")
        elif command_name == "denar": param_value = request.args.get("serie_armamento")
        elif command_name == "dencl": param_value = request.args.get("clave_denuncia")
        elif command_name == "cedula": param_value = request.args.get("cedula")
        
        param = param_value or request.args.get("dni") or request.args.get("query")
             
        if not param:
            return jsonify({"status": "error", "message": f"Parámetro de consulta es requerido para /{command_name_path}."}), 400
    
    elif command_name in optional_commands:
        param_dni = request.args.get("dni")
        param_query = request.args.get("query")
        param_pasaporte = request.args.get("pasaporte") if command_name == "pasaporte" else None
        
        param = param_dni or param_query or param_pasaporte or ""
        
    else:
        param = request.args.get("dni") or request.args.get("query") or ""

        
    command = f"/{command_name} {param}".strip()
    
    try:
        result = run_coro(_call_api_command(command, timeout=TIMEOUT_TOTAL)) 
        
        if result.get("status", "").startswith("error"):
            # Lógica de códigos de estado HTTP mejorada
            status_code = 500
            if result.get("status") == "error_bot_format":
                 status_code = 400
            elif result.get("status") == "error_not_found":
                 status_code = 404 # 404 para "No encontrado"
            elif "timeout" in result.get("message", "").lower() or result.get("status") == "error_timeout":
                 status_code = 504 # 504 para Gateway Timeout
            
            return jsonify(result), status_code
            
        return jsonify(result)
    
    except FutureTimeoutError:
         return jsonify({"status": "error", "message": f"Error interno: Timeout síncrono excedido (max {SYNC_WAIT_TIMEOUT}s). La API de Telegram está saturada o no responde a tiempo para la espera síncrona."}), 504
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
        result = run_coro(_call_api_command(command, timeout=TIMEOUT_TOTAL))
        if result.get("status", "").startswith("error"):
            status_code = 500
            if result.get("status") == "error_bot_format":
                 status_code = 400
            elif result.get("status") == "error_not_found":
                 status_code = 404
            elif "timeout" in result.get("message", "").lower() or result.get("status") == "error_timeout":
                 status_code = 504
            return jsonify(result), status_code
            
        return jsonify(result)
        
    except FutureTimeoutError:
         return jsonify({"status": "error", "message": f"Error interno: Timeout síncrono excedido (max {SYNC_WAIT_TIMEOUT}s). La API de Telegram está saturada o no responde a tiempo para la espera síncrona."}), 504
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error interno: {str(e)}"}), 500

@app.route("/venezolanos_nombres", methods=["GET"])
def api_venezolanos_nombres():
    query = unquote(request.args.get("query", "")).strip()
    
    if not query:
        return jsonify({"status": "error", "message": "Parámetro 'query' (nombres_apellidos) es requerido para /venezolanos_nombres."}), 400

    command = f"/nmv {query}"
    
    try:
        result = run_coro(_call_api_command(command, timeout=TIMEOUT_TOTAL))
        if result.get("status", "").startswith("error"):
            status_code = 500
            if result.get("status") == "error_bot_format":
                 status_code = 400
            elif result.get("status") == "error_not_found":
                 status_code = 404
            elif "timeout" in result.get("message", "").lower() or result.get("status") == "error_timeout":
                 status_code = 504
            return jsonify(result), status_code
            
        return jsonify(result)
        
    except FutureTimeoutError:
         return jsonify({"status": "error", "message": f"Error interno: Timeout síncrono excedido (max {SYNC_WAIT_TIMEOUT}s). La API de Telegram está saturada o no responde a tiempo para la espera síncrona."}), 504
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error interno: {str(e)}"}), 500
        
# ----------------------------------------------------------------------
# --- FIN DEL CÓDIGO ---
# ----------------------------------------------------------------------
