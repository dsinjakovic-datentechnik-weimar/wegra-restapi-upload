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

# Read configuration file
def read_config(path):
    # Öffnet die angegebene Konfigurationsdatei und lädt deren Inhalt als JSON.
    with open(path) as f:
        config = json.load(f)
    
    # Initialisierung eines leeren Dictionaries zur Speicherung der Konfigurationsdaten.
    data = {}

    # Extrahieren und Zuweisen der relevanten Konfigurationsdaten aus dem geladenen JSON.
    # Es werden Standardwerte gesetzt, falls bestimmte Schlüssel nicht vorhanden sind.
    data = {
        'folder_path': config["info"].get("folder_path", ""),  # Pfad zum Ordner, der verarbeitet werden soll
        'backup_path': config["info"].get("backup_path", ""),  # Pfad zum Backup-Ordner
        'error_path': config["info"].get("error_path", ""),  # Pfad zum Fehler-Ordner
        'temp_solution': config["debug"].get("temp_solution", False),  # Temporäre Lösung (true/false)
        'fiddler': config["debug"].get("fiddler", 0),  # Fiddler-Proxy-Einstellung (true/false)
        'cert_file_fiddler': config["debug"].get("cert_file_fiddler", ""),  # Zertifikatsdatei für Fiddler
        'chunk_size': config["debug"].get("chunk_size", 1024*1024),  # Standardgröße für Daten-Chunks (1 MB)
        'company_url': config["restapi"].get("company_url", ""),  # Basis-URL der REST-API
        'file_cabinet_guid': config["restapi"].get("file_cabinet_guid", ""),  # GUID des Datei-Schranks
        'username': config["restapi"].get("username", ""),  # Benutzername für die REST-API
        'password': config["restapi"].get("password", ""),  # Passwort für die REST-API
        'cert_file': config["restapi"].get("cert_file", ""),  # Zertifikatsdatei für die REST-API
        'organization': config["restapi"].get("organization", ""),  # Name der Organisation
        'log_level': config["logs"].get("log_level", "")  # Protokollierungsstufe (Log-Level)
    }

    # Rückgabe der gesammelten Konfigurationsdaten als Dictionary.
    return data


def set_log_level(level_name):
    return {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }.get(level_name.upper(), logging.DEBUG)  # Default to DEBUG if not found

# Determine MIME type
def get_mime_type(file_path):
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type if mime_type else 'application/octet-stream'

# Escape and unescape JSON strings
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

# Remove timezone offset from date string
def remove_timezone_offset(date_string):
    if not date_string.strip():
        return None
    try:
        dt = datetime.fromisoformat(date_string)
        return dt.date().strftime('%Y-%m-%d')
    except ValueError as e:
        logging.error(f"Error parsing date: {e}")
        return None

# Find an upload file
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

# login function
def login():
    # grab data from the config file and store it in a variable. config_data is a dictianory
    config_data = read_config(os.path.join(r"C:\DTW\xml2dwctrl", "config.json"))
    # split the dictianiry into variables for easier use
    company_url = config_data["company_url"]
    username = config_data["username"]
    password = config_data["password"]
    # Schaue mal ob fiddler an ist, wenn ja wir brauchen fiddler cert, sonst brachen wir normales "cert_file"
    cert_file = config_data["cert_file_fiddler"] if config_data["fiddler"] else config_data["cert_file"]
    organization = config_data["organization"]
    # url erstellen. Hier brauchen wir richtiges "company_url". 
    url = f"https://{company_url}/docuware/platform/Account/Logon"
    '''
    Dieser Header gibt den Medientyp der Ressource an, die an den Server gesendet wird. 

    "Content-Type" gibt den Medientyp des Anfragetexts an
    "application/x-www-form-urlencoded" ist ein MIME-Typ, der verwendet wird, wenn Formulardaten in HTTP-Anfragen übermittelt werden

    "Accept gibt" den Medientyp, die für die Antwort akzeptabel sind 
    "application/json" ist ein MIME-Typ, der angibt, dass der Client von Server Daten in JSON-Format erwatet.

    '''
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    
    # Data die wir an Server schicken
    }
    data = {
        "LicenseType": "",
        "Password": password,
        "RedirectToMyselfInCaseOfError": "false",
        "RememberMe": "false",
        "UserName": username,
        "HostID": "DTW_CLIENT_PDS_IMPORT"
    }
    # Überprüfen, ob die Organisation in "config_data" existiert, und wenn ja, fügen Sie sie in "data" ein.
    if organization:
        data["Organization"] = organization
    # Cookies abholen
    cookies_path = r'C:\DTW\temp\login.cookies'
    client_config = {}
    # client_config anpassen, wenn cert_file vorhanden ist. Wenn cert_file, 'verify':cert_file, sonst ohne verify. 
    if cert_file:
        client_config = {
            'verify':cert_file,
            'proxies': {
                "http://": "http://localhost:8888",
                "https://": "http://localhost:8888",
            } if config_data["fiddler"] else None
        }
    else: 
        client_config = {
            'proxies': {
                "http://": "http://localhost:8888",
                "https://": "http://localhost:8888",
            } if config_data["fiddler"] else None
        }
    # Request mit dem httpx-Modul abrufen und Cookies in eine Cookie-Datei schreiben.
    with httpx.Client(**client_config) as client:
        response = client.post(url, headers=headers, data=data)
        with open(cookies_path, "w") as file:
            for cookie in client.cookies.jar:
                file.write(f"{cookie.name}={cookie.value}\n")
    # Response zurückgeben. Hier sehen wir, ob alles in Ordnung gelaufen ist (Code: 200) oder ob irgendwelche Fehler passiert sind (z.B. Code: 404).
    return response.json()

# Asynchronous Funktion Daten aus XML Datei auszulesen
async def get_data_from_xml(xml_path, file):
    file_path = os.path.join(xml_path, file)
    
    # Initialize the data dictionary 
    data = {
        "OriginalFileName": file, "DokumentID": "", "Belegnummer": "", "FileName": "", "Mandant": "", "DokumentTyp": "", "Bemerkung": "", "Betrag": "", "Created": "", "KundeNr": "", "KundeName": "", "LiefNr": "", "LiefName": "", "Belegdatum": "", "Projektnummer": ""}
    
    # XML Datei ablesen und data dictianary ausfüllen (Daten bearbeitung)
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

        # versuch Dokument/Vorgang in XML Datei finden und Kunden Nr oder Lieferant Nr und Kunden Name oder Lieferant Name auszulesen.
        try:
            for i, doc in enumerate(xml_dict["Dokument"]["Vorgang"]):
                try: #data[KundenNr] ist Kundennummer, sonst ist es Lieferantnummer, sons ist es vorhandenes KundenNr (Default ""); gleich für Kundenname
                    data["KundeNr"] = xml_dict["Dokument"]["Vorgang"][i]["Geschaeftspartner"].get("Kundennummer", xml_dict["Dokument"]["Vorgang"][i]["Geschaeftspartner"].get("Lieferantennummer", data["KundeNr"]))
                    data["KundeName"] = xml_dict["Dokument"]["Vorgang"][i]["Geschaeftspartner"].get("Name", data["KundeName"])
                except:
                    pass
                # andere Daten die in Vorgang gefunden sind
                data["Belegdatum"] = xml_dict["Dokument"]["Vorgang"][i].get("Belegdatum", data["Belegdatum"])
                data["Belegdatum"] = remove_timezone_offset(data["Belegdatum"])
                data["Projektnummer"] = xml_dict["Dokument"]["Vorgang"][i].get("Projektnummer", data["Projektnummer"])
                
                match data["DokumentTyp"]:
                    case "Eingangsrechnung": # Wenn Dokumenttyp == Eingangsrechnung
                        searchVorgang = "EingangsRechnungImpl"
                        if doc["Vorgangstyp"] == searchVorgang: # mach nur wenn Vorgang/Vorgangstyp == "EingagnsRechnungImpl"
                            # Hier brauchen wir LiefNr und LiefName, LiefNr = Vorgang/Lieferantennummer, sonst vorhandenes LiefNr (Default ""); gleich für LiefName
                            data["LiefNr"] = xml_dict["Dokument"]["Vorgang"][i]["Geschaeftspartner"].get("Lieferantennummer", data["LiefNr"])
                            data["LiefName"] = xml_dict["Dokument"]["Vorgang"][i]["Geschaeftspartner"].get("Name", data["LiefName"])
                            # andere Daten die in Vorgang gefunden sind
                            data["Belegdatum"] = xml_dict["Dokument"]["Vorgang"][i].get("Belegdatum", data["Belegdatum"])
                            data["Belegdatum"] = remove_timezone_offset(data["Belegdatum"])
                            data["Projektnummer"] = xml_dict["Dokument"]["Vorgang"][i].get("Projektnummer", "")  
        except:
            pass
        data["status"] = "Success"
        # Funktion loggen 
        logging.debug("FILE: %s - Logging data object as JSON: %s", data['OriginalFileName'],json.dumps(data))
        logging.info("reading through XML file completed: %s",file)
        return data

async def upload_big_file(document_data, new_file_path, chunk_size, client, data, url, base_url):
    return_data = {}
    file_size = os.path.getsize(new_file_path)
    chunk_url = url

    headers = {
        "Content-Type": "application/octet-stream", # application/ocet-stream is used to indicate we are sending binary to the server. In our usecase we are using it to upload (a chunk of) a file to the server
        "Content-Length": str(chunk_size),  # the size of uploaded document. In case of chunk uploading document like we are doing here, we have to make sure our content-length is the length (size) of our chunk
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
            
            if response.status_code == 200:
                try: # if it works its still part of the chunk
                    # Parse response to get the URL for the next chunk upload
                    xml_response = xmltodict.parse(response.text)
                    path = xml_response['Document']['FileChunk']['s:Links']['s:Link']['@href']
                    chunk_url = base_url + path
                    offset += len(chunk)
                    chunk = file.read(chunk_size)
                except: # if it doesnt work. Chunking is done and the file was correctly uploaded. Finish the run
                    return_data = {
                        "data": data,
                        "status_code": response.status_code,
                        "text": response.text
                    }
                    try: # last chunk try and index the file
                        nsmap = {
                            'd': 'http://dev.docuware.com/schema/public/services/platform',  # default namespace
                            's': 'http://dev.docuware.com/schema/public/services'  # 's' prefixed namespace
                        }
                        xml_data = etree.fromstring(response.content)
                        doc_id = xml_data.xpath("//d:Field[@FieldName='DWDOCID']/d:Int/text()", namespaces=nsmap)
                        indexing_url = url+f'/{doc_id[0]}/Fields'
                        headers = {
                            "Accept": "application/json",
                            "Content-Type": "application/json"
                        }
                        indexing = await client.put(indexing_url, json=document_data, headers=headers)
                        return_data = {
                            "data": data,
                            "status_code": indexing.status_code,
                            "text": indexing.text
                        }
                    except Exception as e: # we should probably delete the file from the archive here and set it in Failed folder. Currently only set it in Failed and leave the unindexed file in Archive
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

async def upload_small_file(document_data, new_file_path, client, data, url):
    headers = {
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
            return_data = {
                "data": data,
                "status_code": response.status_code,
                "text": response.text
            }
        else:
            return_data = {
                "data": data,
                "status_code": response.status_code,
                "text": "Failed to upload file"
            }

    return return_data

# Asynchronous uploading
async def upload_with_restapi(base_url, data, xml_path, client, url, chunk_size):
    return_data = {}

    new_file_path = os.path.join(xml_path, data["FileName"])
  
    document_data = {"Field": []} # no chunk update (upload_small_file()) needs the Field to be Fields. Note*** same change happens bellow. Since we are only using the big file upload I haven't automated this. But since I'm leaving the upload_small_file for documentation purposes I'm commenting this as well
    if data["DokumentTyp"]: document_data["Field"].append({"FieldName": "UNTERBELEGART", "Item": data["DokumentTyp"].upper(), "ItemElementName": "String"})
    if data["Belegdatum"]: document_data["Field"].append({"FieldName": "BELEGDATUM", "Item": data["Belegdatum"], "ItemElementName": "Date"})
    if data["Projektnummer"]: 
        document_data["Field"].append({"FieldName": "KST_KTR_BEZEICHNUNG", "Item": data["Projektnummer"], "ItemElementName": "String"})
        document_data["Field"].append({"FieldName": "KST", "Item": int(data["Projektnummer"]), "ItemElementName": "Decimal"})
    if data["LiefName"]: 
        document_data["Field"].append({"FieldName": "KU__LIEF_NAME", "Item": data["LiefName"], "ItemElementName": "String"})
    elif data["KundeName"]:
        document_data["Field"].append({"FieldName": "KU__LIEF_NAME", "Item": data["KundeName"], "ItemElementName": "String"})
    if data["LiefNr"]: document_data["Field"].append({"FieldName": "KU__LIEF_NR_", "Item": int(data["LiefNr"]), "ItemElementName": "Decimal"})
    elif data["KundeNr"]: document_data["Field"].append({"FieldName": "KU__LIEF_NR_", "Item": int(data["KundeNr"]), "ItemElementName": "Decimal"})
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
        return_data = await upload_big_file(document_data, new_file_path, chunk_size, client, data, url, base_url)
    else:
        return_data = {
            "data": data,
            "status_code": "File Not Found",
            "text": "File not found at path: " + new_file_path
        }

    return return_data

import os
import asyncio
import httpx
import logging
from datetime import datetime
import shutil

async def main():
    # Konfigurationsdaten aus der JSON-Datei lesen
    config_data = read_config(os.path.join(r"C:\DTW\xml2dwctrl", "config.json"))
    folder_path = config_data["folder_path"]
    backup_path = config_data["backup_path"]
    error_path = config_data["error_path"]
    temp_solution = config_data["temp_solution"]
    chunk_size = config_data["chunk_size"]
    company_url = config_data["company_url"]
    file_cabinet_guid = config_data["file_cabinet_guid"]
    cert_file = config_data["cert_file_fiddler"] if config_data["fiddler"] else config_data["cert_file"]
    log_level = config_data["log_level"]
    log_file_path = r"C:\DTW\xml2dwctrl\LOGS"
    base_url = f"https://{company_url}/"

    # Erstellen des Verzeichnisses für Log-Dateien, falls nicht vorhanden
    if not os.path.exists(log_file_path):
        os.makedirs(log_file_path)

    log_file_dest = os.path.join(log_file_path, 'log.txt')
    log_lvl = set_log_level(log_level)
    logging.basicConfig(filename=log_file_dest, level=log_lvl, format='%(asctime)s - %(levelname)s - %(message)s')

    # Anmeldung durchführen
    login()

    # Sicherstellen, dass die Verzeichnisse für Backup und Fehler vorhanden sind
    if not os.path.exists(backup_path):
        os.makedirs(backup_path)
    if not os.path.exists(error_path):
        os.makedirs(error_path)

    # Aktuelles Datum und Uhrzeit für Verzeichnisnamen
    current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
    subbackup_path = os.path.join(backup_path, current_datetime)
    suberror_path = os.path.join(error_path, current_datetime)

    current_date = datetime.now().strftime("%Y%m%d")

    # Falls die temporäre Lösung aktiviert ist, den Pfad anpassen.
    # temp_solution wird verwendet, solange das alte System und Topfact noch aktiv sind.
    # Danach sollte temp_solution auf 0 gesetzt werden, und folder_path sollte auf einen Ordner geändert werden, 
    # in dem PDS aktiv die Dateien speichert. 
    # Derzeit greift Topfact die Dateien ab und speichert sie im Backup, weshalb wir nur auf diese Dateien zugreifen 
    # und sie in das neue System überführen möchten.
    # Sobald temp_solution auf 0 gesetzt ist, wird unser Programm die Dateien im Backup speichern.

    if temp_solution:
        folder_path = os.path.join(folder_path, current_date)

    # Sammeln von Aufgaben für das Verarbeiten von XML-Dateien
    tasks = []
    for f in os.listdir(folder_path):
        if f.endswith(".xml"):
            tasks.append(get_data_from_xml(folder_path, f))

    # Ausführen der Aufgaben
    results = await asyncio.gather(*tasks)

    uploaded_files = []
    cookies = {}
    cookies_path = r'C:\DTW\temp\login.cookies'

    # Cookies für die RestAPI-Verbindung lesen
    with open(cookies_path, "r") as f:
        logging.debug("Lese Cookies (RestAPI-Verbindung)")
        for line in f:
            name, value = line.strip().split('=', 1)
            cookies[name] = value

    client_config = {
        'verify': cert_file if cert_file else True,
        'proxies': {
            "http://": "http://localhost:8888",
            "https://": "http://localhost:8888",
        } if config_data["fiddler"] else None, 
        'cookies': cookies
    }

    # Asynchrone HTTPX-Client-Sitzung erstellen
    async with httpx.AsyncClient(**client_config) as client:
        url = f"https://{company_url}/docuware/platform/FileCabinets/{file_cabinet_guid}/Documents"
        logging.debug("Verbinden mit: %s", url)

        batch_size = 20

        # Hochladen der Daten in Stapeln (Batches)
        # Der Batching-Prozess dient dazu, die Daten in kleineren Gruppen zu verarbeiten, anstatt alle auf einmal hochzuladen.
        # Dies hilft, die System- und Netzwerkressourcen zu optimieren und Überlastungen zu vermeiden.
        for i in range(0, len(results), batch_size):
            # Erstellen eines Batches von Ergebnissen mit der Größe 'batch_size'.
            # 'range(0, len(results), batch_size)' durchläuft die Ergebnisse in Schritten der definierten Batch-Größe.
            batch = results[i:i + batch_size]

            # Erstellen einer Liste von Aufgaben (Tasks) für jedes Element im aktuellen Batch.
            # 'upload_with_restapi' wird für jedes Element im Batch aufgerufen.
            batch_tasks = [upload_with_restapi(base_url, item, folder_path, client, url, chunk_size) for item in batch]

            # Asynchrone Ausführung aller Aufgaben im aktuellen Batch.
            # 'await asyncio.gather(*batch_tasks)' sorgt dafür, dass das Programm wartet, bis alle Upload-Aufgaben abgeschlossen sind,
            # bevor es mit dem nächsten Batch fortfährt. Dadurch wird eine effiziente und parallele Verarbeitung ermöglicht.
            batch_results = await asyncio.gather(*batch_tasks)

            # Die Ergebnisse des aktuellen Batches werden zur Liste 'uploaded_files' hinzugefügt.
            uploaded_files.extend(batch_results)


    # Überprüfen und Verarbeiten der hochgeladenen Dateien
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
            if data["data"]["status"] == "Success" and data["status_code"] == 200:
                try:
                    if os.path.isfile(os.path.join(folder_path, data["data"]["OriginalFileName"])) and os.path.isfile(os.path.join(folder_path, data["data"]["FileName"])):
                        # Verschieben der XML-Datei in den Backup-Ordner
                        if not os.path.exists(subbackup_path):
                            os.makedirs(subbackup_path)
                        shutil.move(os.path.join(folder_path, data["data"]["OriginalFileName"]), os.path.join(subbackup_path, data["data"]["OriginalFileName"]))
                        # Verschieben der PDF/anderen Datei in den Backup-Ordner
                        shutil.move(os.path.join(folder_path, data["data"]["FileName"]), os.path.join(subbackup_path, data["data"]["FileName"]))
                except Exception as e:
                    logging.error("DATEI: %s - Fehler: %s", data["data"]["OriginalFileName"], str(e))
            elif data["data"]["status"] == "Failed" or data["status_code"] != 200:
                try:
                    if os.path.isfile(os.path.join(folder_path, data["data"]["OriginalFileName"])) and os.path.isfile(os.path.join(folder_path, data["data"]["FileName"])):
                        # Verschieben der XML-Datei in den Fehler-Ordner
                        if not os.path.exists(suberror_path):
                            os.makedirs(suberror_path)
                        shutil.move(os.path.join(folder_path, data["data"]["OriginalFileName"]), os.path.join(suberror_path, data["data"]["OriginalFileName"]))
                        # Verschieben der PDF/anderen Datei in den Fehler-Ordner
                        shutil.move(os.path.join(folder_path, data["data"]["FileName"]), os.path.join(suberror_path, data["data"]["FileName"]))
                except Exception as e:
                    logging.error("DATEI: %s - Fehler: %s", data["data"]["OriginalFileName"], str(e))




if __name__ == "__main__":
    asyncio.run(main())
