import os
import shutil
import xmltodict
from lxml import etree
import logging
from datetime import datetime, timezone, timedelta
import json
import httpx
import mimetypes
import asyncio
import urllib.parse

"""
Refactor notes:
- Replaced cookie-based login with OAuth token-based auth (Bearer).
- Added CONFIG global derived from read_config().
- Implemented ensure_token / load_token / is_token_expired / get_token from your snippet.
- Async HTTPX clients now send Authorization headers (and refresh if 401).
- Removed cookie file usage and login() calls.
- Kept existing upload logic (chunk + index) but with token auth.
- Extended read_config to optionally include token settings.
"""

# ---------------------------
# Config helpers
# ---------------------------

def read_config(path):
    with open(path) as f:
        config = json.load(f)

    data = {
        'folder_path': config["info"].get("folder_path", ""),
        'backup_path': config["info"].get("backup_path", ""),
        'error_path': config["info"].get("error_path", ""),
        'temp_solution': config["debug"].get("temp_solution", False),
        'fiddler': config["debug"].get("fiddler", 0),
        'cert_file_fiddler': config["debug"].get("cert_file_fiddler", ""),
        'chunk_size': config["debug"].get("chunk_size", 1024*1024),
        'company_url': config["restapi"].get("company_url", ""),
        'file_cabinet_guid': config["restapi"].get("file_cabinet_guid", ""),
        'username': config["restapi"].get("username", ""),
        'password': config["restapi"].get("password", ""),
        'cert_file': config["restapi"].get("cert_file", ""),
        'organization': config["restapi"].get("organization", ""),
        'log_level': config["logs"].get("log_level", ""),
        # Optional token-related settings
        'temp_path': config.get("paths", {}).get("temp_path", r"C:\\DTW\\temp"),
        'token_file': config.get("auth", {}).get("token_file", "auth_token.json"),
        'token_endpoint': config.get("auth", {}).get("token_endpoint", ""),
    }

    # If token_endpoint is not provided, default to DocuWare token endpoint based on company_url
    if not data['token_endpoint'] and data['company_url']:
        data['token_endpoint'] = f"https://{data['company_url']}/docuware/platform/Account/Token"

    return data


def set_log_level(level_name):
    return {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }.get(level_name.upper(), logging.DEBUG)

# ---------------------------
# MIME + JSON helpers
# ---------------------------

def get_mime_type(file_path):
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type if mime_type else 'application/octet-stream'


def escape_json_string(value):
    if isinstance(value, str):
        value = value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\b", "\\b")
        value = value.replace("\f", "\\f").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    return value


def unescape_json_string(value):
    if isinstance(value, str):
        value = value.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t")
        value = value.replace("\\f", "\f").replace("\\b", "\b").replace("\\\"", "\"").replace("\\\\", "\\")
    return value


def remove_timezone_offset(date_string):
    if not date_string or not str(date_string).strip():
        return None
    try:
        dt = datetime.fromisoformat(date_string)
        return dt.date().strftime('%Y-%m-%d')
    except ValueError as e:
        logging.error(f"Error parsing date: {e}")
        return None


def find_a_upload_file(searchFolder, fileToSearch):
    if fileToSearch is None or not fileToSearch:
        return '', 0
    first_filename = None
    count = 0

    base_name, extension = os.path.splitext(fileToSearch)
    if len(base_name) > 36:
        base_name = base_name[:-36]

    for filename in os.listdir(searchFolder):
        if filename.startswith(base_name) and filename.endswith(extension):
            if first_filename is None:
                first_filename = filename
            count += 1

    return first_filename, count

# ---------------------------
# TOKEN-BASED AUTH (from your snippet)
# ---------------------------
CONFIG = {}


def ensure_token():
    token_info = load_token()
    if not token_info or is_token_expired(token_info):
        return get_token()
    return token_info


def load_token():
    logging.debug("Loading token from file...")
    token_file = CONFIG.get("token_file", "auth_token.json")
    token_path = os.path.join(CONFIG.get("temp_path", r"C:\\DTW\\temp"), token_file)
    if os.path.exists(token_path):
        with open(token_path, "r") as f:
            logging.debug("Token file loaded successfully.")
            return json.load(f)
    return None


def is_token_expired(token_info):
    expires_at = datetime.fromisoformat(token_info["expires_at"])
    return datetime.now() >= expires_at


def get_token():
    logging.info("Starting token retrieval...")
    token_url = CONFIG["token_endpoint"]
    data = {
        "grant_type": "password",
        "client_id": "docuware.platform.net.client",
        "username": CONFIG["username"],
        "password": CONFIG["password"],
        "scope": "docuware.platform"
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }

    try:
        cert_file = CONFIG.get("cert_file")
        verify_option = cert_file if cert_file else True
        # Honor fiddler proxy if enabled
        proxies = {
            "http://": "http://localhost:8888",
            "https://": "http://localhost:8888",
        } if CONFIG.get("fiddler") else None

        response = httpx.post(token_url, data=data, headers=headers, verify=verify_option, proxies=proxies, timeout=30)
        logging.debug(f"Token endpoint responded with status {response.status_code}")
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in", 3600)
            token_info = {
                "access_token": access_token,
                "expires_at": (datetime.now() + timedelta(seconds=expires_in)).isoformat()
            }
            token_file = CONFIG.get("token_file", "auth_token.json")
            os.makedirs(CONFIG.get("temp_path", r"C:\\DTW\\temp"), exist_ok=True)
            with open(os.path.join(CONFIG.get("temp_path", r"C:\\DTW\\temp"), token_file), "w") as f:
                json.dump(token_info, f)

            logging.info("Token successfully obtained and saved.")
            return token_info
        else:
            logging.error(f"Failed to obtain token: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"Error during token retrieval: {str(e)}")
        return None

# ---------------------------
# Upload helpers (use Bearer token)
# ---------------------------

async def upload_big_file(document_data, new_file_path, chunk_size, client, data, url, base_url, access_token):
    return_data = {}
    file_size = os.path.getsize(new_file_path)
    chunk_url = url

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/octet-stream",
        "Content-Length": str(chunk_size),
        "Content-Disposition": f'inline; filename="{urllib.parse.quote(data["FileName"])}"; modificationdate="{data["Created"]}"',
        "X-File-ModifiedDate": data["Created"],
        "X-File-Name": urllib.parse.quote(data["FileName"]),
        "X-File-Size": str(file_size)
    }

    with open(new_file_path, 'rb') as file:
        chunk = file.read(chunk_size)
        offset = 0

        while chunk:
            body = chunk
            headers["Content-Length"] = str(len(body))

            response = await client.post(chunk_url, content=body, headers=headers)

            if response.status_code == 401:
                # refresh token once
                token_info = ensure_token()
                if not token_info:
                    return {"data": data, "status_code": 401, "text": "Unauthorized and token refresh failed"}
                access_token = token_info["access_token"]
                headers["Authorization"] = f"Bearer {access_token}"
                response = await client.post(chunk_url, content=body, headers=headers)

            if response.status_code == 200:
                try:
                    xml_response = xmltodict.parse(response.text)
                    path = xml_response['Document']['FileChunk']['s:Links']['s:Link']['@href']
                    chunk_url = base_url + path
                    offset += len(chunk)
                    chunk = file.read(chunk_size)
                except Exception:
                    return_data = {
                        "data": data,
                        "status_code": response.status_code,
                        "text": response.text
                    }
                    try:
                        nsmap = {
                            'd': 'http://dev.docuware.com/schema/public/services/platform',
                            's': 'http://dev.docuware.com/schema/public/services'
                        }
                        xml_data = etree.fromstring(response.content)
                        doc_id = xml_data.xpath("//d:Field[@FieldName='DWDOCID']/d:Int/text()", namespaces=nsmap)
                        indexing_url = url+f'/{doc_id[0]}/Fields'
                        idx_headers = {
                            "Authorization": f"Bearer {access_token}",
                            "Accept": "application/json",
                            "Content-Type": "application/json"
                        }
                        indexing = await client.put(indexing_url, json=document_data, headers=idx_headers)
                        return_data = {
                            "data": data,
                            "status_code": indexing.status_code,
                            "text": indexing.text
                        }
                    except Exception as e:
                        return_data = {
                            "data": data,
                            "status_code": response.status_code,
                            "text": response.text
                        }
                    return return_data
            else:
                return_data = {
                    "data": data,
                    "status_code": response.status_code,
                    "text": response.text
                }
                return return_data

        return_data = {
            "data": data,
            "status_code": "Error",
            "text": "Chunking completed without the last chunk"
        }
    return return_data


async def upload_small_file(document_data, new_file_path, client, data, url, access_token):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "multipart/form-data",
        "X-File-ModifiedDate": data.get("Created", "")
    }
    mime_type = get_mime_type(new_file_path)
    with open(new_file_path, 'rb') as new_file:
        files = {
            'document': ('', json.dumps(document_data), 'application/json'),
            'file[]': (data["FileName"], new_file, mime_type)
        }
        response = await client.post(url, headers=headers, files=files)
        if response.status_code == 200:
            return {
                "data": data,
                "status_code": response.status_code,
                "text": response.text
            }
        else:
            return {
                "data": data,
                "status_code": response.status_code,
                "text": "Failed to upload file"
            }


async def upload_with_restapi(base_url, data, xml_path, client, url, chunk_size, access_token):
    new_file_path = os.path.join(xml_path, data["FileName"]) 

    document_data = {"Field": []}
    if data["DokumentTyp"]: document_data["Field"].append({"FieldName": "UNTERBELEGART", "Item": data["DokumentTyp"].upper(), "ItemElementName": "String"})
    if data["Belegdatum"]: document_data["Field"].append({"FieldName": "BELEGDATUM", "Item": data["Belegdatum"], "ItemElementName": "Date"})
    if data["Projektnummer"]:
        document_data["Field"].append({"FieldName": "KST_KTR_BEZEICHNUNG", "Item": data["Projektnummer"], "ItemElementName": "String"})
        document_data["Field"].append({"FieldName": "KST", "Item": int(data["Projektnummer"]), "ItemElementName": "Decimal"})
    if data["LiefName"]:
        document_data["Field"].append({"FieldName": "KU__LIEF_NAME", "Item": data["LiefName"], "ItemElementName": "String"})
    elif data["KundeName"]:
        document_data["Field"].append({"FieldName": "KU__LIEF_NAME", "Item": data["KundeName"], "ItemElementName": "String"})
    if data["LiefNr"]:
        document_data["Field"].append({"FieldName": "KU__LIEF_NR_", "Item": int(data["LiefNr"]), "ItemElementName": "Decimal"})
    elif data["KundeNr"]:
        document_data["Field"].append({"FieldName": "KU__LIEF_NR_", "Item": int(data["KundeNr"]), "ItemElementName": "Decimal"})
    if data["FileName"]: document_data["Field"].append({"FieldName": "DATEINAME", "Item": data["FileName"], "ItemElementName": "String"})
    if data["Bemerkung"]: document_data["Field"].append({"FieldName": "KOMMISION_KURZBESCHREIBUNG", "Item": data["Bemerkung"], "ItemElementName": "String"})
    if data["Mandant"]: document_data["Field"].append({"FieldName": "MANDANT", "Item": data["Mandant"], "ItemElementName": "String"})
    if data["Belegnummer"]: document_data["Field"].append({"FieldName": "BELEGNUMMER", "Item": data["Belegnummer"], "ItemElementName": "String"})
    document_data["Field"].append({"FieldName": "VERSANDART", "Item": "AUSGANGSPOST-PDS", "ItemElementName": "String"})
    document_data["Field"].append({"FieldName": "KFM__STATUS1", "Item": ".", "ItemElementName": "String"})
    document_data["Field"].append({"FieldName": "KFM__STATUS2", "Item": ".", "ItemElementName": "String"})
    document_data["Field"].append({"FieldName": "KFM__STATUS3", "Item": ".", "ItemElementName": "String"})
    document_data["Field"].append({"FieldName": "KFM__STATUS4", "Item": ".", "ItemElementName": "String"})
    document_data["Field"].append({"FieldName": "KFM__STATUS5", "Item": ".", "ItemElementName": "String"})
    document_data["Field"].append({"FieldName": "KFM__STATUS6", "Item": ".", "ItemElementName": "String"})
    document_data["Field"].append({"FieldName": "KFM__STATUS7", "Item": ".", "ItemElementName": "String"})
    document_data["Field"].append({"FieldName": "KFM__STATUS8", "Item": ".", "ItemElementName": "String"})
    document_data["Field"].append({"FieldName": "VERSIONSSTATUS", "Item": ".", "ItemElementName": "String"})
    document_data["Field"].append({"FieldName": "TECHN__STATUS", "Item": ".", "ItemElementName": "String"})
    if data["Created"]: document_data["Field"].append({"FieldName": "CREATED", "Item": data["Created"], "ItemElementName": "DateTime"})
    if data["Betrag"]: document_data["Field"].append({"FieldName": "BETRAG", "Item": float(data["Betrag"]), "ItemElementName": "Decimal"})

    logging.debug("Data to push to the server for file %s: \n%s", data["OriginalFileName"],json.dumps(document_data))

    if os.path.isfile(new_file_path):
        return await upload_big_file(document_data, new_file_path, chunk_size, client, data, url, base_url, access_token)
    else:
        return {
            "data": data,
            "status_code": "File Not Found",
            "text": "File not found at path: " + new_file_path
        }


# ---------------------------
# XML parsing (unchanged)
# ---------------------------

async def get_data_from_xml(xml_path, file):
    file_path = os.path.join(xml_path, file)

    data = {
        "OriginalFileName": file, "DokumentID": "", "Belegnummer": "", "FileName": "", "Mandant": "", "DokumentTyp": "", "Bemerkung": "", "Betrag": "", "Created": "", "KundeNr": "", "KundeName": "", "LiefNr": "", "LiefName": "", "Belegdatum": "", "Projektnummer": ""}

    with open(file_path, 'r', encoding='utf-8') as xml_file:
        logging.debug("Reading XML File: %s", file)
        xml_content = xml_file.read()
        xml_dict = xmltodict.parse(xml_content, force_list=('Vorgang',))

        data["DokumentID"] = xml_dict["Dokument"].get("DokumentID", data["DokumentID"]) 
        data["Belegnummer"] = xml_dict["Dokument"].get("Belegnummer", data["Belegnummer"]) 
        data["FileName"] = xml_dict["Dokument"].get("Filename", data["FileName"]) 
        ErfassungspartionID = xml_dict["Dokument"].get("Erfassungspartition_dbid", "")
        data["Mandant"] = "Wegra" if ErfassungspartionID == "1801" else "EAW"
        data["DokumentTyp"] = xml_dict["Dokument"].get("Dokumenttyp", data["DokumentTyp"]) 
        data["Bemerkung"] = xml_dict["Dokument"].get("Bemerkung", data["Bemerkung"]) 
        data["Betrag"] = xml_dict["Dokument"].get("Netto", data["Betrag"]) 
        data["Created"] = xml_dict["Dokument"].get("Created", data["Created"]) 
        searchVorgang = "not Found"

        try:
            for i, doc in enumerate(xml_dict["Dokument"]["Vorgang"]):
                try:
                    data["KundeNr"] = xml_dict["Dokument"]["Vorgang"][i]["Geschaeftspartner"].get("Kundennummer", xml_dict["Dokument"]["Vorgang"][i]["Geschaeftspartner"].get("Lieferantennummer", data["KundeNr"]))
                    data["KundeName"] = xml_dict["Dokument"]["Vorgang"][i]["Geschaeftspartner"].get("Name", data["KundeName"]) 
                except:
                    pass
                data["Belegdatum"] = xml_dict["Dokument"]["Vorgang"][i].get("Belegdatum", data["Belegdatum"]) 
                data["Belegdatum"] = remove_timezone_offset(data["Belegdatum"]) 
                data["Projektnummer"] = xml_dict["Dokument"]["Vorgang"][i].get("Projektnummer", data["Projektnummer"]) 

                match data["DokumentTyp"]:
                    case "Eingangsrechnung":
                        searchVorgang = "EingangsRechnungImpl"
                        if doc["Vorgangstyp"] == searchVorgang:
                            data["LiefNr"] = xml_dict["Dokument"]["Vorgang"][i]["Geschaeftspartner"].get("Lieferantennummer", data["LiefNr"]) 
                            data["LiefName"] = xml_dict["Dokument"]["Vorgang"][i]["Geschaeftspartner"].get("Name", data["LiefName"]) 
                            data["Belegdatum"] = xml_dict["Dokument"]["Vorgang"][i].get("Belegdatum", data["Belegdatum"]) 
                            data["Belegdatum"] = remove_timezone_offset(data["Belegdatum"]) 
                            data["Projektnummer"] = xml_dict["Dokument"]["Vorgang"][i].get("Projektnummer", "")  
        except:
            pass
        data["status"] = "Success"
        logging.debug("FILE: %s - Logging data object as JSON: %s", data['OriginalFileName'],json.dumps(data))
        logging.info("reading through XML file completed: %s",file)
        return data

# ---------------------------
# MAIN (token-based)
# ---------------------------

async def main():
    global CONFIG

    config_data = read_config(os.path.join(r"C:\\DTW\\xml2dwctrl", "config.json"))
    CONFIG = {
        "company_url": config_data["company_url"],
        "file_cabinet_guid": config_data["file_cabinet_guid"],
        "username": config_data["username"],
        "password": config_data["password"],
        "cert_file": config_data["cert_file_fiddler"] if config_data["fiddler"] else config_data["cert_file"],
        "token_endpoint": config_data["token_endpoint"],
        "token_file": config_data["token_file"],
        "temp_path": config_data["temp_path"],
        "fiddler": config_data["fiddler"],
    }

    folder_path = config_data["folder_path"]
    backup_path = config_data["backup_path"]
    error_path = config_data["error_path"]
    temp_solution = config_data["temp_solution"]
    chunk_size = config_data["chunk_size"]
    company_url = config_data["company_url"]
    file_cabinet_guid = config_data["file_cabinet_guid"]
    cert_file = CONFIG["cert_file"]
    log_level = config_data["log_level"]
    log_file_path = r"C:\\DTW\\xml2dwctrl\\LOGS"
    base_url = f"https://{company_url}/"

    # Logging setup
    if not os.path.exists(log_file_path):
        os.makedirs(log_file_path)
    log_file_dest = os.path.join(log_file_path, 'log.txt')
    logging.basicConfig(filename=log_file_dest, level=set_log_level(log_level), format='%(asctime)s - %(levelname)s - %(message)s')

    # Ensure token exists
    token_info = ensure_token()
    if not token_info:
        logging.critical("Token retrieval failed. Exiting.")
        return
    access_token = token_info["access_token"]

    # Ensure backup/error folders
    os.makedirs(backup_path, exist_ok=True)
    os.makedirs(error_path, exist_ok=True)

    current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
    subbackup_path = os.path.join(backup_path, current_datetime)
    suberror_path = os.path.join(error_path, current_datetime)

    current_date = datetime.now().strftime("%Y%m%d")

    if temp_solution:
        folder_path = os.path.join(folder_path, current_date)

    # Gather XML parse tasks
    tasks = []
    for f in os.listdir(folder_path):
        if f.endswith(".xml"):
            tasks.append(get_data_from_xml(folder_path, f))

    results = await asyncio.gather(*tasks)

    uploaded_files = []

    client_config = {
        'verify': cert_file if cert_file else True,
        'proxies': {
            "http://": "http://localhost:8888",
            "https://": "http://localhost:8888",
        } if config_data["fiddler"] else None
    }

    async with httpx.AsyncClient(**client_config) as client:
        url = f"https://{company_url}/docuware/platform/FileCabinets/{file_cabinet_guid}/Documents"
        logging.debug("Verbinden mit: %s", url)

        batch_size = 20
        for i in range(0, len(results), batch_size):
            batch = results[i:i + batch_size]
            batch_tasks = [upload_with_restapi(base_url, item, folder_path, client, url, chunk_size, access_token) for item in batch]
            batch_results = await asyncio.gather(*batch_tasks)
            uploaded_files.extend(batch_results)

    # Handle results + file moves
    for data in uploaded_files:
        if data["data"]["status"] == "Success":
            logging.info("[ERFOLG] - DATEI: %s - 'get_data_from_xml()' - Status: %s - Nachricht: %s", data["data"]["OriginalFileName"], data["data"]["status"], data["data"].get("error", "Erfolg"))
        else:
            logging.error("[FEHLER] - DATEI: %s - 'get_data_from_xml()' - Status: %s - Nachricht: %s", data["data"]["OriginalFileName"], data["data"]["status"], data["data"].get("error", "Fehler"))

        if data["status_code"] == 200:
            logging.info("[ERFOLG] - DATEI: %s - 'upload_with_restapi()' - Status: %s - Nachricht: %s", data["data"]["OriginalFileName"], data["status_code"], data["text"])
        else:
            logging.error("[FEHLER] - DATEI: %s - 'upload_with_restapi()' - Status: %s - Nachricht: %s", data["data"]["OriginalFileName"], data["status_code"], data["text"])

        if not temp_solution:
            try:
                xml_src = os.path.join(folder_path, data["data"]["OriginalFileName"]) 
                bin_src = os.path.join(folder_path, data["data"]["FileName"]) 
                if data["data"]["status"] == "Success" and data["status_code"] == 200:
                    if os.path.isfile(xml_src) and os.path.isfile(bin_src):
                        os.makedirs(subbackup_path, exist_ok=True)
                        shutil.move(xml_src, os.path.join(subbackup_path, data["data"]["OriginalFileName"]))
                        shutil.move(bin_src, os.path.join(subbackup_path, data["data"]["FileName"]))
                elif data["data"]["status"] == "Failed" or data["status_code"] != 200:
                    if os.path.isfile(xml_src) and os.path.isfile(bin_src):
                        os.makedirs(suberror_path, exist_ok=True)
                        shutil.move(xml_src, os.path.join(suberror_path, data["data"]["OriginalFileName"]))
                        shutil.move(bin_src, os.path.join(suberror_path, data["data"]["FileName"]))
            except Exception as e:
                logging.error("DATEI: %s - Fehler: %s", data["data"]["OriginalFileName"], str(e))


if __name__ == "__main__":
    asyncio.run(main())
