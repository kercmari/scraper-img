import time
import os
import json
import logging
import re
import random
from urllib.parse import unquote
from datetime import datetime
from typing import Any, List, Dict
from multiprocessing import Pool, Manager
import requests
from colorama import Fore
from fake_useragent import UserAgent
from .upload_cloudinary import UploadImage, Cloudinary
from model import APIConfig  # Asegúrate de que la ruta es correcta
from .utils import _write_files, _update_path_url_json, format_duration  # Asegúrate de que utils.py está en PYTHONPATH
import html  # Importamos el módulo html para deshacer entidades HTML

# Configurar el nivel de logging
logging.basicConfig(level=logging.INFO)

class ImageScraper:
    def __init__(self,
                 data: List[Dict[str, Any]],
                 account: Cloudinary = None,
                 results_simplified: str = None,
                 results_obtained: str = None,
                 pathFiles: str = None,
                 table: str = None,
                 tipo_imagen: str = None,
                 setp_next: bool = False,
                 apis: List[APIConfig] = None,
                 num_processes: int = 1,
                 proxies: List[str] = None,
                 ssl_verify: bool = False,
                 custom_ssl_cert: str = None) -> None:

        self.account = account
        self.data = data
        self.results_simplified = f'{results_simplified}_organized.json'
        self.results_obtained = f'{results_obtained}_results.json'
        self.pathFiles = f'JSON/{pathFiles}'
        self.folder = f'{self.account.name_folder}/{pathFiles}/{table}'
        self.setp_next = setp_next
        self.tipo_imagen = tipo_imagen
        self._data = None
        self.apis = [api for api in apis if api.enabled] if apis else []
        self.num_processes = num_processes
        self.proxies = None if not proxies else proxies
        self.ssl_verify = ssl_verify
        self.custom_ssl_cert = custom_ssl_cert

        if not os.path.exists(self.pathFiles):
            os.makedirs(self.pathFiles, exist_ok=True)

        if self.account and isinstance(self.account, Cloudinary):
            self.cloud = UploadImage(
                cloud_name=account.cloud_name,
                api_key=account.api_key,
                api_secret=account.api_secret
            )

        if not self.setp_next:
            logging.info(f"{Fore.GREEN}Inicio scraping {Fore.RESET}")

    @property
    def data_update(self) -> List[Dict]:
        if self._data is None:
            self._data = [dict(t) for t in {tuple(d.items()) for d in self.data}]
        return self._data

    def get_image_from_apis(self, *search_by_property, name_property_url: Dict[str, Any], search_specific: str = ''):
        data_simplified = self.data_update
        data_obtained = self.data
        start_time = datetime.now()

        if self.setp_next and isinstance(self.account, Cloudinary):
            with open(f'{self.pathFiles}/{self.results_obtained}', 'r') as f:
                data_simplified = json.load(f)
            with open(f'{self.pathFiles}/{self.results_simplified}', 'r') as f:
                data_obtained = json.load(f)
            
            logging.info(f"{Fore.GREEN}Subir archivos a Cloudinary{Fore.RESET}")
            self.cloud.update_path_url(self.pathFiles,
                                    data=data_simplified,
                                    data_return=data_obtained,
                                    name_property_url=name_property_url,
                                    results_simplified=self.results_simplified,
                                    results_obtained=self.results_obtained,
                                    folder=self.folder)
        else:
            chunk_size = max(len(data_simplified) // self.num_processes, 1)
            data_chunks = [data_simplified[i:i + chunk_size] for i in range(0, len(data_simplified), chunk_size)]
            manager = Manager()
            return_dict = manager.dict()
            lock = manager.Lock()
            args = [
                (
                    chunk,
                    search_by_property,
                    name_property_url,
                    search_specific,
                    self.apis,
                    i,
                    return_dict,
                    lock,
                    self.proxies,
                    self.ssl_verify,
                    self.custom_ssl_cert
                )
                for i, chunk in enumerate(data_chunks)
            ]

            with Pool(processes=self.num_processes) as pool:
                pool.starmap(process_chunk, args)

            data_simplified = []
            for i in range(len(data_chunks)):
                data_simplified.extend(return_dict[i])

            for original, updated in zip(data_obtained, data_simplified):
                for key in name_property_url.keys():
                    original[key] = updated.get(key)

            _write_files(self.pathFiles, self.results_obtained, data_obtained)
            _write_files(self.pathFiles, self.results_simplified, data_simplified)
            
            end_time = datetime.now()
            scraping_duration = end_time - start_time
            logging.info(f"Los datos se guardaron en el directorio: {self.pathFiles}")
            logging.info(f"{Fore.GREEN}Fin de scraping{Fore.RESET}")

            if self.account and isinstance(self.account, Cloudinary):
                logging.info(f"{Fore.GREEN}Subir archivos a Cloudinary{Fore.RESET}")
                self.cloud.update_path_url(self.pathFiles,
                                           data=data_simplified,
                                           data_return=data_obtained,
                                           name_property_url=name_property_url,
                                           results_simplified=self.results_simplified,
                                           results_obtained=self.results_obtained,
                                           folder=self.folder,
                                           scraping_duration=scraping_duration)

def process_chunk(data_chunk, search_by_property, name_property_url, search_specific, apis, process_number, return_dict, lock, proxies, ssl_verify, custom_ssl_cert):
    session = requests.Session()
    total_items = len(data_chunk)
    rate_limiters = {api.name: RateLimiter(api.name) for api in apis}
    ua = UserAgent()
    base_url_ignore_list = {}  # Diccionario local para ignorar URLs base
    used_user_agents = set()   # Conjunto local de User Agents usados en este proceso

    def get_random_proxy():
        if not proxies:
            logging.warning("No proxies available.")
            return None
        proxy = random.choice(proxies)
        logging.info(f"Using proxy: {proxy}")
        return {
            'http': proxy,
            'https': proxy
        }

    def get_new_headers_and_proxy():
        new_user_agent = ua.random
        while new_user_agent in used_user_agents:
            new_user_agent = ua.random
        used_user_agents.add(new_user_agent)

        proxy = get_random_proxy()
        logging.info(f"Proxy aleatorio: {proxy}")
        
        headers = {'User-Agent': new_user_agent}
        return headers, proxy

    for i, item in enumerate(data_chunk):
        title = ' '.join(str(item[name]) for name in search_by_property)
        search_query = f'{title} {search_specific}'.strip()
        logging.info(f"Proceso {process_number}: {Fore.YELLOW}{i+1}/{total_items}{Fore.RESET}. Buscando imagen de: {search_query.upper()}")

        image_url = None
        for api in apis:
            api_name = api.name
            api_key = api.api_key

            base_url_list = getattr(api, 'base_url_list', [])
            rate_limiter = rate_limiters.get(api_name)
            if not rate_limiter:
                rate_limiter = RateLimiter(api_name)
                rate_limiters[api_name] = rate_limiter

            if rate_limiter.can_request():
                try:
                    if api_name == 'custom_service':
                        headers, proxy = get_new_headers_and_proxy()
                        logging.info(f"Proceso {process_number}: Usando header: {headers} y proxy: {proxy}")
                        image_url = search_custom_service(session, search_query, base_url_list, headers, proxy, base_url_ignore_list, ssl_verify, custom_ssl_cert)
                    rate_limiter.increment_request()
                except Exception as e:
                    logging.error(f"Proceso {process_number}: Error al buscar en {api_name}: {e}")
                    continue

                if image_url:
                    logging.info(f"Proceso {process_number}: Imagen encontrada usando {api_name}")
                    break
                else:
                    logging.info(f"Proceso {process_number}: No se encontró imagen usando {api_name}.")

        if image_url:
            for key in name_property_url.keys():
                item[key] = image_url
                logging.info(f"Proceso {process_number}: Imagen almacenada en la propiedad: {key}")
        else:
            logging.warning(f"Proceso {process_number}: No se encontró imagen para '{search_query}'.")

        time.sleep(0.1)  # Añadir un pequeño retraso después de procesar cada ítem

    with lock:
        return_dict[process_number] = data_chunk  

def search_custom_service(session, query, base_url_list, headers, proxy, base_url_ignore_list, ssl_verify, custom_ssl_cert):
    query_encoded = query.replace(' ', '+')
    num_base_urls = len(base_url_list)
    attempts = 0  # Contador de intentos para evitar loops infinitos
    base_url_index = 0  # Índice local para manejar las URLs base

    while attempts < num_base_urls:
        logging.debug(f"Attempt {attempts}/{num_base_urls}, Base URL Index: {base_url_index}")

        # Asegurarse de que el índice está dentro del rango
        if base_url_index >= num_base_urls:
            base_url_index = 0

        # Obtener la URL base actual
        base_url_entry = base_url_list[base_url_index]
        base_url = base_url_entry['base']
        query_param_name = base_url_entry.get('query_param_name', 'q')
        decode_url = base_url_entry.get('decode', False)

        # Verificar si la URL base está en la lista de ignorados
        retry_time = base_url_ignore_list.get(base_url)
        if retry_time:
            if time.time() < retry_time:
                logging.info(f"Base URL {base_url} está siendo ignorada hasta {datetime.fromtimestamp(retry_time)}")
                # Pasar a la siguiente URL base
                base_url_index += 1
                attempts += 1
                continue
            else:
                # El tiempo de espera ha pasado, eliminar de la lista de ignorados
                del base_url_ignore_list[base_url]

        # Imprimir la base URL que se está utilizando
        logging.info(f"Proceso está utilizando la base URL: {base_url}")

        # Construir la URL de la petición
        request_url = f"{base_url}?{query_param_name}={query_encoded}"

        # Generar y mostrar el comando curl
        curl_command = f"curl -X GET '{request_url}'"
        for header_name, header_value in headers.items():
            curl_command += f" -H '{header_name}: {header_value}'"
        logging.info(f"Generated CURL command: {curl_command}")

        try:
            req = requests.Request('GET', request_url, headers=headers)
            prepared = session.prepare_request(req)

            # Enviar la solicitud con un timeout de 5 segundos
            response = session.send(
                prepared,
                proxies=proxy,
                timeout=5,
                verify=ssl_verify if custom_ssl_cert is None else custom_ssl_cert
            )
            response.raise_for_status()
            html_content = response.text

            # Llamada a la función extract_image_url para extraer la URL de la imagen
            image_url = extract_image_url(html_content, decode_url)

            if image_url:
                logging.info(f"Image found: {image_url}")
                return image_url
            else:
                logging.warning(f"No valid image URLs found in {base_url}. Trying the next base_url.")
                # Pasar a la siguiente URL base
                base_url_index += 1
                attempts += 1
                continue

        except requests.exceptions.Timeout:
            logging.warning(f"Request to {base_url} timed out after 5 seconds. Trying the next base_url.")
            # Pasar a la siguiente URL base
            base_url_index += 1
            attempts += 1
            continue
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logging.warning(f"Received a 429 Too Many Requests error at {base_url}. Ignoring this base URL for 15 minutes.")
                base_url_ignore_list[base_url] = time.time() + 900  # Ignorar por 15 minutos
                # Pasar a la siguiente URL base
                base_url_index += 1
                attempts += 1
                continue
            else:
                logging.error(f"HTTP error in custom_service for {base_url}: {e}. Trying the next base_url.")
                # Pasar a la siguiente URL base
                base_url_index += 1
                attempts += 1
                continue
        except Exception as e:
            logging.error(f"Error in custom_service: {e}. Trying the next base_url.")
            # Pasar a la siguiente URL base
            base_url_index += 1
            attempts += 1
            continue

        time.sleep(0.1)  # Añadir un pequeño retraso para reducir el uso de CPU

    # Si no se encontró ninguna imagen válida después de probar todas las URLs base disponibles
    logging.error("No valid image URLs found after trying all available base URLs.")
    return None

def extract_image_url(html_content, decode_url):
    """Extrae la URL de la imagen del contenido HTML."""
    # Deshacer entidades HTML
    html_content = html.unescape(html_content)
    if decode_url:
        # Aquí puedes incluir el código existente si es necesario
        pass  # Asumiendo que el código existente está correcto
    else:
        # Buscar todas las URLs que terminan con extensiones de imagen
        image_pattern = re.compile(
            r'https?://[^\s\'"]+?\.(?:jpg|jpeg|gif|bmp|webp)',
            re.IGNORECASE
        )
        image_matches = image_pattern.findall(html_content)

        if image_matches:
            valid_image_urls = []
            for image_url in image_matches:
                # Verificar que la URL no contiene otro 'http' interno
                if image_url.count('http') > 1:
                    continue  # Ignorar URLs con 'http' anidados

                # Verificar que la URL es válida y termina con una extensión de imagen
                if image_url.startswith('http') and re.search(
                    r'\.(?:jpg|jpeg|png|gif|bmp|webp)(\?[^\s\'"]*)?$',
                    image_url,
                    re.IGNORECASE
                ):
                    valid_image_urls.append(image_url)
            if valid_image_urls:
                return valid_image_urls[0]
        return None

class RateLimiter:
    def __init__(self, api_name):
        self.api_name = api_name
        self.limit = self.get_rate_limit(api_name)
        self.requests_made = 0

    def get_rate_limit(self, api_name):
        limits = {
            'pexels': 200,
            'pixabay': 5000,
            'unsplash': 50,
            'flickr': 3600,
            'custom_service': 1200
        }
        return limits.get(api_name, 1000)

    def can_request(self):
        return self.requests_made < self.limit

    def increment_request(self):
        self.requests_made += 1
