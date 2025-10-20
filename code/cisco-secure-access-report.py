#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cisco Secure Access
Author: Vagner Silva
Credits: Victor Azevedo
"""

import requests
import time
import calendar
from datetime import datetime, timedelta
import csv
import json
import re

# =============================
# CONSTANTES / ENDPOINTS
# =============================
API_BASE = "https://api.sse.cisco.com"
AUTH_URL = f"{API_BASE}/auth/v2/token"
# ACTIVITY_ENDPOINT will be determined dynamically based on event_type_filter
CATEGORIES_ENDPOINT = f"{API_BASE}/reports/v2/categories"

# =============================
# UTILITÁRIOS DE TEMPO
# =============================
def dt_to_epoch_millis(dt: datetime) -> int:
    """
    Converte datetime NAIVE (interpretado como hora local) para epoch em milissegundos.
    Mantém o comportamento que funcionava antes (equivalente a time.mktime).
    """
    return int(time.mktime(dt.timetuple()) * 1000)

def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def elapsed(start_ts: float) -> str:
    return str(timedelta(seconds=int(time.time() - start_ts)))

# =============================
# RATE LIMITER (MÁX 18000/HORA para Reporting API)
# =============================
class RateLimiter:
    def __init__(self, max_requests=18000, per_seconds=3600):
        self.max_requests = max_requests
        self.per_seconds = per_seconds
        self.window_start = time.time()
        self.count = 0

    def check(self):
        now = time.time()
        if now - self.window_start >= self.per_seconds:
            self.window_start = now
            self.count = 0

        if self.count >= self.max_requests:
            wait = int(self.per_seconds - (now - self.window_start))
            if wait < 0:
                wait = 0
            print(f"\n⏸️ Rate limit atingido ({self.max_requests}/hora). Aguardando {wait}s...")
            time.sleep(wait)
            self.window_start = time.time()
            self.count = 0

        self.count += 1

# =============================
# AUTENTICAÇÃO
# =============================
def get_token(client_id: str, client_secret: str) -> str:
    resp = requests.post(
        AUTH_URL,
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def prompt_credentials_with_test() -> tuple[str, str, str]:
    """
    Pide CLIENT_ID/CLIENT_SECRET hasta conseguir un token válido.
    Retorna (client_id, client_secret, token).
    """
    while True:
        client_id = input("🔑 CLIENT_ID: ").strip()
        client_secret = input("🔑 CLIENT_SECRET: ").strip()
        try:
            token = get_token(client_id, client_secret)
            print("✅ Autenticación OK.\n")
            return client_id, client_secret, token
        except requests.HTTPError as e:
            msg = ""
            try:
                msg = e.response.text[:200]
            except Exception:
                pass
            print(f"❌ Falha na autenticação ({e}). Detalhe: {msg}")
            print("Tente novamente.\n")
        except Exception as e:
            print(f"❌ Erro inesperado ao obter token: {e}")
            print("Tente novamente.\n")

# =============================
# PROMPT INTERACTIVO DE DATA
# =============================
def interactive_prompt_dates() -> tuple[int, int, list[int], int]:
    anos = [2025, 2024, 2023, 2022, 2021]
    print("Selecione o ano:")
    for i, a in enumerate(anos, 1):
        print(f"{i}. {a}")
    while True:
        try:
            ano_idx = int(input("Ano (número): "))
            if 1 <= ano_idx <= len(anos):
                ano = anos[ano_idx - 1]
                break
        except ValueError:
            pass
        print("Entrada inválida. Tente novamente.")
    meses = ["janeiro","fevereiro","março","abril","maio","junho",
             "julho","agosto","setembro","outubro","novembro","dezembro"]
    print("\nSelecione o mês:")
    for i, m in enumerate(meses, 1):
        print(f"{i}. {m}")
    while True:
        try:
            mes = int(input("Mês (número): "))
            if 1 <= mes <= 12:
                break
        except ValueError:
            pass
        print("Entrada inválida. Tente novamente.")
    max_dia = calendar.monthrange(ano, mes)[1]
    print("\nSelecione o dia:")
    print("0. Todos os dias do mês")
    while True:
        try:
            dia = int(input("Dia (número ou 0): "))
            if 0 <= dia <= max_dia:
                break
        except ValueError:
            pass
        print("Entrada inválida. Tente novamente.")
    dias_to_process = list(range(1, max_dia + 1)) if dia == 0 else [dia]
    return ano, mes, dias_to_process, dia

# =============================
# OBTENER TODAS LAS CATEGORIAS
# =============================
def get_all_available_categories(token: str) -> list[dict]:
    """
    Obtiene TODAS las categorías de la API.
    """
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(CATEGORIES_ENDPOINT, headers=headers, timeout=30)
        resp.raise_for_status()
        
        all_categories_response = resp.json()
        all_categories = all_categories_response.get('data', [])
        
        if isinstance(all_categories, list):
            print("\n--- All Available Categories from API (ID, Type, Label) ---")
            sorted_categories = sorted(all_categories, key=lambda x: (x.get('type', ''), x.get('label', '')))
            for cat in sorted_categories:
                if isinstance(cat, dict):
                    print(f"  ID: {cat.get('id')}, Type: '{cat.get('type')}', Label: '{cat.get('label')}'")
            print("-----------------------------------------------------------")
            return all_categories
        else:
            print(f"❌ La respuesta de {CATEGORIES_ENDPOINT} no contiene una lista en la clave 'data'. Tipo de 'data': {type(all_categories)}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al obtener categorías desde la API: {e}")
        return []

# =============================
# DOWNLOAD DE JANELA COM PAGINAÇÃO
# =============================
def fetch_activity_window(
    token: str,
    client_id: str,
    client_secret: str,
    from_ts: int,
    to_ts: int,
    limit: int = 1000,
    offset_ceiling: int | None = None,
    verbose: bool = False,
    rate_limiter: RateLimiter | None = None,
    filters: dict | None = None,
    activity_endpoint_url: str = f"{API_BASE}/reports/v2/activity" # Dynamic endpoint URL
) -> tuple[list[dict], bool, str]:
    """
    Busca eventos entre from_ts y to_ts (epoch ms), paginando por offset.
    Retorna (eventos, need_minute_fallback, token_atualizado).
    Activa fallback minuto-a-minuto si:
      - HTTP 400/404; o
      - offset >= offset_ceiling (Ex.: 10000).
    Hace retry de red con backoff y renueva token en 403 (hasta 5x).
    """
    offset = 0
    events: list[dict] = []
    need_minute_fallback = False

    consecutive_403 = 0
    max_403_attempts = 5
    max_retries_conn = 5

    while True:
        if offset_ceiling is not None and offset >= offset_ceiling:
            need_minute_fallback = True
            if verbose:
                print(f"   ⚠️ Offset {offset} >= ceiling {offset_ceiling}. Ativando fallback minuto-a-minuto.")
            break

        if rate_limiter:
            rate_limiter.check()

        params = {
            "from": str(from_ts),
            "to": str(to_ts),
            "limit": limit,
            "offset": offset
        }
        if filters:
            params.update(filters)
        
        headers = {"Authorization": f"Bearer {token}"}

        resp = None
        for attempt in range(max_retries_conn):
            try:
                resp = requests.get(activity_endpoint_url, headers=headers, params=params, timeout=60)
                break
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ReadTimeout) as e:
                wait = (2 ** attempt)
                print(f"   ⚠️ Erro de conexão ({e}). Tentando novamente em {wait}s...")
                time.sleep(wait)
        if resp is None:
            print("   🚨 Falhas de conexão repetidas. Abortando este intervalo.")
            break

        if resp.status_code == 200:
            try:
                payload = resp.json()
            except Exception as e:
                print(f"   ⚠️ Erro ao decodificar JSON: {e}. Cuerpo (200 chars): {resp.text[:200]}")
                break

            batch = payload.get("data", [])
            if not batch:
                break

            events.extend(batch)
            offset += len(batch)

            if verbose:
                print(f"      🔹 {len(batch)} eventos (offset agora {offset})")

            consecutive_403 = 0
            if len(batch) < limit:
                break

            continue

        if resp.status_code == 403:
            consecutive_403 += 1
            print(f"   ⚠️ HTTP 403 detectado ({consecutive_403}/{max_403_attempts}). Renovando token e tentando de novo...")
            try:
                token = get_token(client_id, client_secret)
            except Exception as e:
                print(f"   ❌ Erro ao renovar token: {e}")
                time.sleep(5)

            if consecutive_403 >= max_403_attempts:
                print("   🚨 403 persistente após varias renovaciones. Parando este intervalo.")
                break
            continue

        if resp.status_code in (400, 404):
            need_minute_fallback = True
            print(f"   ⚠️ HTTP {resp.status_code} — ativando fallback minuto-a-minuto para essa hora.")
            break

        print(f"   ⚠️ HTTP {resp.status_code} retornado. Mensagem: {resp.text[:200]}")
        break

    return events, need_minute_fallback, token

# =============================
# BUSCA DA HORA COM FALLBACK DE MINUTO
# =============================
def fetch_hour_with_minute_fallback(
    token: str,
    client_id: str,
    client_secret: str,
    hour_start_dt: datetime,
    limit: int = 1000,
    offset_ceiling: int = 10000,
    verbose: bool = True,
    rate_limiter: RateLimiter | None = None,
    filters: dict | None = None,
    activity_endpoint_url: str = f"{API_BASE}/reports/v2/activity" # Dynamic endpoint URL
) -> tuple[list[dict], str]:
    """
    Tenta buscar la hora entera. Si bate 400/404 o offset_ceiling,
    cae para minuto-a-minuto (60 llamadas).
    """
    hour_end_dt = hour_start_dt + timedelta(hours=1) - timedelta(milliseconds=1)
    from_ts = dt_to_epoch_millis(hour_start_dt)
    to_ts = dt_to_epoch_millis(hour_end_dt)

    if verbose:
        print(f"\n⏳ Hourly: {fmt_dt(hour_start_dt)} to {fmt_dt(hour_end_dt)}")

    hour_events_from_api, need_minute_fallback, token = fetch_activity_window(
        token, client_id, client_secret, from_ts, to_ts,
        limit=limit, offset_ceiling=offset_ceiling, verbose=verbose,
        rate_limiter=rate_limiter, filters=filters, activity_endpoint_url=activity_endpoint_url
    )

    if not need_minute_fallback:
        if verbose:
            print(f"   ✅ Hour OK: {len(hour_events_from_api)} eventos")
        return hour_events_from_api, token

    collected: list[dict] = []
    if verbose:
        print("   ↪️ Iniciando fallback minuto-a-minuto (60 minutos).")
    for m in range(60):
        minute_start = hour_start_dt + timedelta(minutes=m)
        minute_end = minute_start + timedelta(minutes=1) - timedelta(milliseconds=1)
        m_from = dt_to_epoch_millis(minute_start)
        m_to = dt_to_epoch_millis(minute_end)

        if verbose:
            print(f"      ➤ Minute: {fmt_dt(minute_start)} to {fmt_dt(minute_end)} ... ", end="")

        minute_events, _, token = fetch_activity_window(
            token, client_id, client_secret, m_from, m_to,
            limit=limit, offset_ceiling=None, verbose=False,
            rate_limiter=rate_limiter, filters=filters, activity_endpoint_url=activity_endpoint_url
        )
        collected.extend(minute_events)

        if verbose:
            print(f"   {len(minute_events)} events")

    if verbose:
        print(f"   ✅ Fallback minute total: {len(collected)} eventos na hora {hour_start_dt.strftime('%Y-%m-%d %H:00')}")
    return collected, token

# =============================
# CSV
# =============================
def _parse_event_datetime(ev: dict) -> datetime | None:
    """
    Tenta montar um datetime do evento:
      - se houver 'timestamp' ISO8601 string, usa;
      - senão se houver 'timestamp' epoch em milissegundos, usa;
      - senão combina 'date' (YYYY-MM-DD) + 'time' (HH:MM:SS), se existirem.
    """
    ts_val = ev.get("timestamp")
    if isinstance(ts_val, str) and ts_val:
        try:
            return datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
        except ValueError:
            pass # Fallback to other parsing
    elif isinstance(ts_val, (int, float)): # Handle epoch milliseconds
        try:
            return datetime.fromtimestamp(ts_val / 1000) # Convert milliseconds to seconds
        except ValueError:
            pass # Fallback to other parsing

    d = ev.get("date")
    t = ev.get("time")
    if isinstance(d, str) and isinstance(t, str):
        try:
            return datetime.fromisoformat(f"{d}T{t}")
        except ValueError:
            pass
    return None

def save_to_csv_custom_format(events: list[dict], writer: csv.DictWriter):
    """
    Salva eventos usando un writer CSV ya abierto, en el formato personalizado.
    """
    for ev in events:
        dt = _parse_event_datetime(ev)
        
        # Policy Identity: Prioritize rule.label, then policy.name, then policyName, then identity.policyIdentity
        policy_identity = ''
        if ev.get('rule', {}).get('label'):
            policy_identity = ev['rule']['label']
        elif ev.get('policy', {}).get('name'):
            policy_identity = ev['policy']['name']
        elif ev.get('policyName'):
            policy_identity = ev['policyName']
        else:
            for id_data in ev.get('identities', []):
                if isinstance(id_data, dict) and id_data.get('policyIdentity'):
                    policy_identity = id_data['policyIdentity']
                    break

        # Identities and Identity Types
        identities_data = ev.get('identities', [])
        identity_labels = []
        identity_types = []
        for id_data in identities_data:
            if isinstance(id_data, dict):
                label = id_data.get('label')
                if isinstance(label, str):
                    identity_labels.append(label)
                
                # Extract type from nested dictionary if present, or directly if string
                id_type_obj = id_data.get('type')
                if isinstance(id_type_obj, dict):
                    id_type_label = id_type_obj.get('label')
                    if isinstance(id_type_label, str):
                        identity_types.append(id_type_label)
                elif isinstance(id_type_obj, str):
                     identity_types.append(id_type_obj)
        
        # Categories
        categories_data = ev.get('categories', [])
        category_labels = [cat_data.get('label', '') for cat_data in categories_data if isinstance(cat_data, dict) and cat_data.get('label')]

        writer.writerow({
            "Date": dt.strftime("%Y-%m-%d") if dt else "",
            "Time": dt.strftime("%H:%M:%S") if dt else "",
            "Policy Identity": policy_identity,
            "Identity Type": identity_types[0] if identity_types else "", # Assuming primary identity type
            "Identities": "; ".join(identity_labels),
            "Identity Types": "; ".join(identity_types),
            "Record Type": ev.get('recordType', ev.get('type', '')), # Use 'type' if 'recordType' is missing
            "Internal Ip Address": ev.get('internalip', ''),
            "External Ip Address": ev.get('externalip', ''),
            "Action": ev.get('verdict', ''),
            "Destination": ev.get('domain', ev.get('dest', ev.get('url', ''))), # Prioritize domain, then dest, then url
            "Categories": "; ".join(category_labels),
            "Full Event JSON": json.dumps(ev, ensure_ascii=False) # New column
        })

def save_raw_events_to_csv(events: list[dict], writer: csv.DictWriter):
    """
    Salva eventos en formato CSV con el JSON completo del evento.
    """
    for ev in events:
        dt = _parse_event_datetime(ev)
        writer.writerow({
            "timestamp": dt.isoformat() if dt else "",
            "full_event_json": json.dumps(ev, ensure_ascii=False) # Save full event as JSON string
        })

def sanitize_filename(text: str) -> str:
    """Sanitizes a string to be used as a filename segment."""
    text = text.replace(" ", "_")
    text = re.sub(r'[^\w.-]', '', text) # Remove non-alphanumeric (except . and -)
    return text.lower()

# =============================
# MAIN
# =============================
def main():
    # 1) Credenciais com teste
    client_id, client_secret, token = prompt_credentials_with_test()

    # 2) Prompt de datas (ano/mês/dia[s])
    ano, mes, dias_to_process, selected_day_for_filename = interactive_prompt_dates()

    # --- Event Type Selection ---
    # Based on the documentation: https://developer.cisco.com/docs/cloud-security/secure-access-api-reference-reporting-overview/#request-query-parameters
    valid_event_types = ["dns", "proxy", "firewall", "ip", "ztna", "remote-access", "intrusion"]
    event_type_filter = ''
    print("\n--- Select Event Type ---")
    for i, etype in enumerate(valid_event_types, 1):
        print(f"{i}. {etype}")
    
    while True:
        try:
            type_choice_idx = input(f"Enter the number for the event type (1-{len(valid_event_types)}): ").strip()
            type_choice_idx = int(type_choice_idx)
            if 1 <= type_choice_idx <= len(valid_event_types):
                event_type_filter = valid_event_types[type_choice_idx - 1]
                break
        except ValueError:
            pass
        print("Invalid input. Please enter a valid number.")

    activity_endpoint_url = f"{API_BASE}/reports/v2/activity/{event_type_filter}"
    print(f"\n✅ Using dedicated activity endpoint: '{activity_endpoint_url}'")

    # 3) Category Selection
    all_available_categories = get_all_available_categories(token) # This call will now print ALL categories
    
    category_selection_choice = ''
    if all_available_categories:
        print("\n--- Category Filtering Options ---")
        print("1. Use the predefined list of categories (each in its own CSV)")
        print("2. Select a single category interactively from the full list (single CSV)")
        print("3. Select multiple categories by ID (each in its own CSV)") # Updated text
        print("0. No category filtering (single CSV)")
        
        while True:
            try:
                category_selection_choice = input("Enter your choice (0, 1, 2, or 3): ").strip()
                if category_selection_choice in ['0', '1', '2', '3']:
                    break
            except ValueError:
                pass
            print("Invalid input. Please enter 0, 1, 2, or 3.")
    else:
        print("⚠️ Could not load any categories from the API. No category filtering will be applied.")
        category_selection_choice = '0' # Force no category filtering if categories can't be loaded

    # --- CSV Format Selection (Applies to all generated CSVs) ---
    print("\n--- CSV Output Format Options ---")
    print("1. Custom formatted CSV (Date;Time;Policy Identity;...)")
    print("2. All Data (Raw JSON event in a column)")
    
    csv_format_choice = ''
    while True:
        try:
            csv_format_choice = input("Enter your choice (1 or 2): ").strip()
            if csv_format_choice in ['1', '2']:
                break
        except ValueError:
            pass
        print("Invalid input. Please enter 1 or 2.")

    if csv_format_choice == '1':
        csv_fieldnames = [
            "Date", "Time", "Policy Identity", "Identity Type", "Identities", 
            "Identity Types", "Record Type", "Internal Ip Address", 
            "External Ip Address", "Action", "Destination", "Categories", "Full Event JSON"
        ]
        save_events_function = save_to_csv_custom_format
        csv_format_suffix = "custom"
    else: # choice == '2'
        csv_fieldnames = ["timestamp", "full_event_json"]
        save_events_function = save_raw_events_to_csv
        csv_format_suffix = "raw_json"

    # --- Definir tus filtros de EXCLUSIÓN aquí (filtros del lado del cliente) ---
    # Lista de nombres de usuario a excluir.
    excluded_identity_names = {
        "user_a",
        "user_b",
        "service_account_1"
    }
    # Si no quieres excluir ninguna identidad, déjalo como un set vacío:
    # excluded_identity_names = set()
    # -----------------------------------------------------------------------------

    # --- Prepare for data fetching based on category selection ---
    categories_to_process_list = [] # List of (api_filter_dict, filename_segment) tuples
    
    if category_selection_choice == '1': # Predefined list, each in its own CSV
        requested_category_names_raw = [
            "Adult",
            "Advertisements",
            "Online Storage and Backup",
            "Illegal Downloads",
            "File Transfer Services",
            "DoH and DoT ou Personal VPN", 
            "Streaming Video",
            "Infrastructure and Content Delivery Networks"
        ]
        
        requested_category_names = []
        for name in requested_category_names_raw:
            if name == "DoH and DoT ou Personal VPN":
                requested_category_names.extend(["Encrypted DNS", "Personal VPN"]) # Corrected for API labels
            else:
                requested_category_names.append(name)

        category_label_to_info = {re.sub(r'[^a-z0-9]', '', cat['label'].lower()): cat for cat in all_available_categories}
        
        print("\n--- Attempting to match your predefined categories ---")
        for req_name in requested_category_names:
            cleaned_req_name = re.sub(r'[^a-z0-9]', '', req_name.lower())
            cat_info = category_label_to_info.get(cleaned_req_name)
            if cat_info:
                categories_to_process_list.append(
                    ({"categories": str(cat_info['id'])}, sanitize_filename(cat_info['label']))
                )
                print(f"✅ Matched '{req_name}' (Type: {cat_info['type']}) to ID: {cat_info['id']}")
            else:
                print(f"⚠️ Could not find category ID for '{req_name}' (cleaned: '{cleaned_req_name}'). Please check the 'All Available Categories from API' list above for exact names.")
        print("----------------------------------------------------")
        
        if not categories_to_process_list:
            print("⚠️ No predefined categories were matched. No category-specific CSVs will be generated for this option.")
            return # Exit main if no categories to process for option 1

    elif category_selection_choice == '2': # Single interactive category
        print("\n--- Select a single category by ID ---")
        
        available_ids = {cat['id'] for cat in all_available_categories if 'id' in cat}
        selected_id = None
        while True:
            try:
                selected_id_str = input("Enter the ID of the category you want to filter by: ").strip()
                selected_id = int(selected_id_str)
                if selected_id in available_ids:
                    selected_cat_info = next((cat for cat in all_available_categories if cat['id'] == selected_id), None)
                    categories_to_process_list.append(
                        ({"categories": str(selected_id)}, sanitize_filename(selected_cat_info['label']))
                    )
                    print(f"✅ Selected category: '{selected_cat_info['label']}' (ID: {selected_id}, Type: {selected_cat_info['type']})")
                    break
                else:
                    print(f"Invalid ID: {selected_id}. Please enter a valid category ID from the list above.")
            except ValueError:
                print("Invalid input. Please enter a number.")
        print("----------------------------------------------------")

    elif category_selection_choice == '3': # Multiple interactive categories, each in its own CSV
        print("\n--- Select multiple categories by ID (comma-separated, e.g., 161,27,9) ---")
        print("Please refer to the 'All Available Categories from API' list above for IDs.")
        
        available_ids_set = {cat['id'] for cat in all_available_categories if 'id' in cat}
        selected_ids_for_multi = []
        
        while True:
            input_ids_str = input("Enter category IDs (comma-separated): ").strip()
            if not input_ids_str:
                print("No IDs entered. Please try again.")
                continue
            
            raw_ids = [s.strip() for s in input_ids_str.split(',') if s.strip()]
            
            valid_ids_in_input = []
            invalid_inputs = []
            for id_str in raw_ids:
                try:
                    num_id = int(id_str)
                    if num_id in available_ids_set:
                        valid_ids_in_input.append(num_id)
                    else:
                        invalid_inputs.append(id_str)
                except ValueError:
                    invalid_inputs.append(id_str)
            
            if invalid_inputs:
                print(f"Invalid or unknown IDs entered: {', '.join(invalid_inputs)}. Please re-enter all IDs correctly.")
            elif not valid_ids_in_input:
                print("No valid category IDs selected. Please try again.")
            else:
                selected_ids_for_multi = list(set(valid_ids_in_input)) # Remove duplicates
                selected_ids_for_multi.sort() # For consistent output
                print(f"✅ Selected categories with IDs: {selected_ids_for_multi}")

                # For each selected ID, create a separate entry in categories_to_process_list
                for cat_id in selected_ids_for_multi:
                    selected_cat_info = next((cat for cat in all_available_categories if cat['id'] == cat_id), None)
                    if selected_cat_info:
                        categories_to_process_list.append(
                            ({"categories": str(cat_id)}, sanitize_filename(selected_cat_info['label']))
                        )
                break
        print("----------------------------------------------------")

    else: # category_selection_choice == '0' or no categories loaded
        print("⚠️ No specific categories selected for filtering. Fetching all events without category filter.")
        categories_to_process_list.append(
            ({}, "no_cat_filter") # Empty filters, generic filename suffix
        )
    
    # --- Main Data Fetching and CSV Writing Loop ---
    if not categories_to_process_list:
        print("❌ No categories were selected for processing. Exiting.")
        return

    for current_api_filters, current_category_filename_segment in categories_to_process_list:
        csv_file = f"activity_{ano}_{mes:02d}_{selected_day_for_filename:02d}_{current_category_filename_segment}_{event_type_filter}_{csv_format_suffix}.csv"
        
        print(f"\n🚀 Starting data collection for category '{current_category_filename_segment}' into '{csv_file}'")

        file_exists = False
        try:
            with open(csv_file, "r", encoding="utf-8"):
                file_exists = True
        except FileNotFoundError:
            pass

        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            csv_writer = csv.DictWriter(f, fieldnames=csv_fieldnames, delimiter=';')
            if not file_exists:
                csv_writer.writeheader()

            rate_limiter = RateLimiter(max_requests=18000, per_seconds=3600)

            start_time = time.time()
            total_events_for_this_category = 0
            for idx, current_day in enumerate(dias_to_process):
                print(f"\n📅 Dia: {current_day} ({idx+1}/{len(dias_to_process)})")
                for hour in range(24):
                    hour_start = datetime(ano, mes, current_day, hour, 0, 0)
                    print(f"⏱️ Tempo decorrido: {elapsed(start_time)}")

                    events_hour, token = fetch_hour_with_minute_fallback(
                        token, client_id, client_secret, hour_start,
                        limit=1000,
                        offset_ceiling=10000,
                        verbose=True,
                        rate_limiter=rate_limiter,
                        filters=current_api_filters, # Use category-specific filters
                        activity_endpoint_url=activity_endpoint_url
                    )

                    # --- Aplicar filtro de exclusión del lado del cliente ---
                    if excluded_identity_names:
                        filtered_events_hour = []
                        for event in events_hour:
                            should_exclude_event = False
                            event_identities = event.get('identities', [])

                            for identity_data in event_identities:
                                if isinstance(identity_data, dict): # Ensure it's a dict
                                    identity_label = identity_data.get('label')
                                    if identity_label and identity_label in excluded_identity_names:
                                        should_exclude_event = True
                                        break

                            if not should_exclude_event:
                                filtered_events_hour.append(event)
                        events_hour = filtered_events_hour
                        print(f"      🔹 Después de excluir identidades: {len(events_hour)} eventos restantes.")
                    # --------------------------------------------------------

                    print(f"   ✅ Hour OK: {len(events_hour)} eventos")
                    save_events_function(events_hour, csv_writer) # Use the chosen function
                    total_events_for_this_category += len(events_hour)
            
            print(f"\n🏁 Concluído! {total_events_for_this_category} eventos salvos em {csv_file}")
            print(f"⏱️ Tempo total para esta categoria: {elapsed(start_time)}")

    print(f"\n✅ All requested data collection processes completed.")
    print(f"⏱️ Tempo total da execução do script: {elapsed(start_time)}")

if __name__ == "__main__":
    main()
