import time
import os
import json
import logging
import random
from urllib.parse import unquote, urlparse
from datetime import datetime
from typing import Any, List, Dict
import requests
from colorama import Fore, Style
from fake_useragent import UserAgent
from .upload_cloudinary import UploadImage, Cloudinary
from model import APIConfig
from .utils import _write_files, _update_path_url_json, format_duration
import html
import gc
import faulthandler
from bs4 import BeautifulSoup
import concurrent.futures
import multiprocessing
import re
import threading
import time

faulthandler.enable()

# Configuración general de logging a nivel INFO para suprimir DEBUG
logging.basicConfig(level=logging.INFO)

# Suprimir mensajes DEBUG de PIL.Image
logging.getLogger("PIL.Image").setLevel(logging.WARNING)

# Suprimir mensajes DEBUG de urllib3
logging.getLogger("urllib3").setLevel(logging.WARNING)
class RateLimiter:
    def __init__(self, api_name):
        self.api_name = api_name
        self.limit = self.get_rate_limit(api_name)
        self.requests_made = 0
        self.lock = threading.Lock()

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
        with self.lock:
            return self.requests_made < self.limit

    def increment_request(self):
        with self.lock:
            self.requests_made += 1

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
                 num_processes: int = 4,
                 proxies: List[str] = None,
                 ssl_verify: bool = False,
                 custom_ssl_cert: str = None) -> None:

        self.account = account
        self.data = data
        self.results_simplified = f'{results_simplified}_organized.json'
        self.results_obtained = f'{results_obtained}_results.json'
        self.pathFiles = f'JSON/{pathFiles}'
        self.folder = f'{self.account.name_folder}/{pathFiles}/{table}' if self.account else ''
        self.setp_next = setp_next
        self.tipo_imagen = tipo_imagen
        self._data = None
        self.apis = [api for api in apis if api.enabled] if apis else []
        self.num_processes = min(num_processes, os.cpu_count() or 1)
        self.proxies = proxies if proxies else []
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
        scraping_start_time = time.perf_counter()  # Tiempo de inicio
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
            manager = multiprocessing.Manager()
            total_processed = manager.Value('i', 0)
            total_items = len(data_simplified)
            results = []
            with concurrent.futures.ProcessPoolExecutor(max_workers=self.num_processes) as executor:
                futures = []
                for idx, item in enumerate(data_simplified):
                    args = (
                        item,
                        search_by_property,
                        name_property_url,
                        search_specific,
                        self.apis,
                        idx,
                        self.proxies,
                        self.ssl_verify,
                        self.custom_ssl_cert,
                        total_items,
                        total_processed
                    )
                    future = executor.submit(process_item, *args)
                    futures.append((future, idx, item))

                for future, idx, item in futures:
                    try:
                        result_item = future.result(timeout=15)
                        results.append(result_item)
                        logging.info(f"Tarea {idx} completada exitosamente.")
                    except concurrent.futures.TimeoutError:
                        logging.warning(f"Tarea {idx} excedió el tiempo límite y fue terminada.")
                        results.append(item)
                    except Exception as e:
                        logging.error(f"Error en la tarea {idx}: {e}", exc_info=True)
                        results.append(item)

            data_simplified = results

            for original, updated in zip(data_obtained, data_simplified):
                for key in name_property_url.keys():
                    original[key] = updated.get(key, '')

            _write_files(self.pathFiles, self.results_obtained, data_obtained)
            _write_files(self.pathFiles, self.results_simplified, data_simplified)
            
            end_time = datetime.now()
            scraping_end_time = time.perf_counter()  # Tiempo de finalización
            total_scraping_time = scraping_end_time - scraping_start_time
            logging.info(f"Duración total del scraping de imágenes: {total_scraping_time:.2f} segundos.")

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

def process_item(item, search_by_property, name_property_url, search_specific, apis, idx, proxies, ssl_verify, custom_ssl_cert, total_items, total_processed):
    session = requests.Session()
    try:
        rate_limiters = {api.name: RateLimiter(api.name) for api in apis}
        ua = UserAgent()
        base_url_ignore_list = {}
        used_user_agents = set()

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

        title = ' '.join(str(item.get(name, '')) for name in search_by_property)
        search_query = f'{title} {search_specific}'.strip()

        total_processed.value += 1
        current_total = total_processed.value

        logging.info(f"Proceso {os.getpid()}: {Fore.YELLOW}{current_total}/{total_items}{Fore.RESET}. Buscando imagen de: {search_query.upper()}")
        logging.debug(f"Proceso {os.getpid()}: Tarea {idx}: search_query: {search_query}")

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
                        logging.debug(f"Proceso {os.getpid()}: Tarea {idx}: Usando header: {headers} y proxy: {proxy}")
                        image_url = search_custom_service(session, search_query, base_url_list, headers, proxy, base_url_ignore_list, ssl_verify, custom_ssl_cert)
                    elif api_name == 'pexels':
                        image_url = search_pexels(session, search_query, api_key)
                    elif api_name == 'pixabay':
                        image_url = search_pixabay(session, search_query, api_key)
                    elif api_name == 'unsplash':
                        image_url = search_unsplash(session, search_query, api_key)
                    elif api_name == 'flickr':
                        image_url = search_flickr(session, search_query, api_key)
                    rate_limiter.increment_request()
                except Exception as e:
                    logging.error(f"Proceso {os.getpid()}: Tarea {idx}: Error al buscar en {api_name}: {e}", exc_info=True)
                    continue

                if image_url:
                    logging.info(f"Proceso {os.getpid()}: Tarea {idx}: Imagen encontrada usando {api_name}")
                    break
                else:
                    logging.info(f"Proceso {os.getpid()}: Tarea {idx}: No se encontró imagen usando {api_name}.")
            else:
                logging.info(f"Proceso {os.getpid()}: Tarea {idx}: Límite de solicitudes alcanzado para {api_name}, intentando con el siguiente API.")

        if image_url:
            for key in name_property_url.keys():
                item[key] = image_url
                logging.info(f"Proceso {os.getpid()}: Tarea {idx}: Imagen almacenada en la propiedad: {key}")
        else:
            for key in name_property_url.keys():
                item[key] = ''
                logging.warning(f"Proceso {os.getpid()}: Tarea {idx}: No se encontró imagen para '{search_query}'. Asignando cadena vacía a '{key}'.")

        time.sleep(0.1)
        return item
    finally:
        session.close()
        gc.collect()

def search_custom_service(session, query, base_url_list, headers, proxy, base_url_ignore_list, ssl_verify, custom_ssl_cert):
    logging.debug(f"Iniciando search_custom_service con query: {query}")
    query_encoded = query.replace(' ', '+')
    num_base_urls = len(base_url_list)
    attempts = 0
    base_url_index = 0

    while attempts < num_base_urls:
        logging.debug(f"Attempt {attempts + 1}/{num_base_urls}, Base URL Index: {base_url_index}")

        if base_url_index >= num_base_urls:
            base_url_index = 0

        base_url_entry = base_url_list[base_url_index]
        base_url = base_url_entry['base']
        query_param_name = base_url_entry.get('query_param_name', 'q')
        decode_url = base_url_entry.get('decode', False)

        retry_time = base_url_ignore_list.get(base_url)
        if retry_time:
            if time.time() < retry_time:
                logging.info(f"Base URL {base_url} está siendo ignorada hasta {datetime.fromtimestamp(retry_time)}")
                base_url_index += 1
                attempts += 1
                continue
            else:
                del base_url_ignore_list[base_url]

        logging.info(f"Proceso {os.getpid()}: Usando la base URL: {base_url}")

        request_url = f"{base_url}?{query_param_name}={query_encoded}"

        curl_command = f"curl -X GET '{request_url}'"
        for header_name, header_value in headers.items():
            curl_command += f" -H '{header_name}: {header_value}'"
        logging.debug(f"Generated CURL command: {curl_command}")

        try:
            req = requests.Request('GET', request_url, headers=headers)
            prepared = session.prepare_request(req)

            response = session.send(
                prepared,
                proxies=proxy,
                timeout=5,
                verify=ssl_verify if custom_ssl_cert is None else custom_ssl_cert
            )
            response.raise_for_status()
            html_content = response.text

            max_html_size = 1 * 1024 * 1024
            if len(html_content) > max_html_size:
                logging.warning(f"Contenido HTML demasiado grande ({len(html_content)} bytes). Limitando a {max_html_size} bytes.")
                html_content = html_content[:max_html_size]

            image_url = extract_image_url(html_content, decode_url)

            if image_url:
                logging.info(f"{Fore.YELLOW} Image found: {image_url}{Fore.RESET}")
                logging.debug(f"Finalizando search_custom_service con resultado: {image_url}")
                return image_url
            else:
                logging.warning(f"No valid image URLs found in {base_url}. Trying the next base_url.")
                base_url_index += 1
                attempts += 1
                continue

        except requests.exceptions.Timeout:
            logging.warning(f"Request to {base_url} timed out after 5 seconds. Trying the next base_url.")
            base_url_index += 1
            attempts += 1
            continue
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logging.warning(f"Received a 429 Too Many Requests error at {base_url}. Ignoring this base URL for 15 minutes.")
                base_url_ignore_list[base_url] = time.time() + 900
                base_url_index += 1
                attempts += 1
                continue
            else:
                logging.error(f"HTTP error in custom_service for {base_url}: {e}")
                base_url_index += 1
                attempts += 1
                continue
        except Exception as e:
            logging.error(f"Error in custom_service: {e}", exc_info=True)
            base_url_index += 1
            attempts += 1
            continue

        time.sleep(0.1)

    logging.error("No valid image URLs found after trying all available base URLs.")
    return None

def extract_image_url(html_content, decode_url):
    logging.debug(f"Tamaño del contenido HTML: {len(html_content)} bytes")
    html_content = html.unescape(html_content)
    
    try:
        if decode_url:
            json_content = "{" + html_content + "}"
            json_content = json_content.replace("'", '"')
            json_content = json_content.encode('utf-8').decode('unicode_escape')
            try:
                data = json.loads(json_content)
                logging.debug("Contenido decodificado como JSON.")
                image_url = data.get('iurl')
                if image_url:
                    image_url = image_url.replace('\\/', '/')
                    logging.debug(f"URL de la imagen extraída: {image_url}")
                    return image_url
                else:
                    logging.warning("No se encontró la clave 'iurl' en el JSON.")
                    return None
            except json.JSONDecodeError as e:
                logging.error(f"Error al decodificar el JSON: {e}")
                image_url = find_image_url_in_decoded_content(html_content)
                return image_url
        else:
            soup = BeautifulSoup(html_content, 'lxml')
            img_tags = soup.find_all('img')
            valid_image_urls = []
            for img in img_tags:
                src = img.get('src')
                if src:
                    if src.startswith('http') and any(src.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.bmp', '.webp']):
                        valid_image_urls.append(src)
            if valid_image_urls:
                logging.debug(f"URL de la imagen extraída: {valid_image_urls[0]}")
                return valid_image_urls[0]
    except Exception as e:
        logging.error(f"Error al extraer la URL de la imagen: {e}", exc_info=True)
        return None
    return None

def find_image_url_in_decoded_content(content):
    content = content.replace('\\/', '/')
    content = bytes(content, "utf-8").decode("unicode_escape")
    image_pattern = re.compile(
        r'https?://[^\s\'"]+?\.(?:jpg|jpeg|bmp|webp)',
        re.IGNORECASE
    )
    image_matches = image_pattern.findall(content)
    if image_matches:
        logging.debug(f"URL de la imagen encontrada en contenido decodificado: {image_matches[0]}")
        return image_matches[0]
    else:
        logging.warning("No se encontró una URL de imagen en el contenido decodificado.")
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
