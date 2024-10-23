import time
import os
import json
import logging
import re
from datetime import datetime
from typing import Any, List, Dict
from multiprocessing import Pool, Manager

import requests
from colorama import Fore

from .upload_cloudinary import UploadImage, Cloudinary
from model import APIConfig  # Asegúrate de que la ruta es correcta
from .utils import _write_files, _update_path_url_json, format_duration  # Asegúrate de que utils.py está en PYTHONPATH

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
                num_processes: int = 1) -> None:
        
        self.account = account
        self.data = data
        self.results_simplified = f'{results_simplified}_organized.json'
        self.results_obtained = f'{results_obtained}_results.json'
        self.pathFiles = f'JSON/{pathFiles}'
        self.folder = f'{self.account.name_folder}/{pathFiles}/{table}'
        self.setp_next = setp_next
        self.tipo_imagen = tipo_imagen
        self._data = None
        # Filtramos las APIs habilitadas
        self.apis = [api for api in apis if api.enabled] if apis else []
        self.num_processes = num_processes
        
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
            # Cargar datos existentes
            with open(f'{self.pathFiles}/{self.results_obtained}', 'r') as f:
                data_simplified = json.load(f)
            with open(f'{self.pathFiles}/{self.results_simplified}', 'r') as f:
                data_obtained = json.load(f)
            
            logging.info(f"{Fore.GREEN}Subir archivos a Cloudinary{Fore.RESET}")
            self.cloud.update_path_url(self.pathFiles,
                                    data=data_simplified,
                                    data_return=data_obtained,
                                    name_property_url=name_property_url,
                                    results_simplified = self.results_simplified,
                                    results_obtained = self.results_obtained,
                                    folder=self.folder)
        else:
            # Dividir los datos para procesamiento paralelo
            chunk_size = max(len(data_simplified) // self.num_processes, 1)
            data_chunks = [data_simplified[i:i + chunk_size] for i in range(0, len(data_simplified), chunk_size)]

            # Usar Manager para compartir datos entre procesos
            manager = Manager()
            return_dict = manager.dict()
            lock = manager.Lock()

            # Preparar argumentos para cada proceso
            args = [(chunk, search_by_property, name_property_url, search_specific, self.apis, i, return_dict, lock) for i, chunk in enumerate(data_chunks)]

            with Pool(processes=self.num_processes) as pool:
                pool.starmap(self.process_chunk, args)

            # Combinar los resultados
            data_simplified = []
            for i in range(len(data_chunks)):
                data_simplified.extend(return_dict[i])

            # Actualizar los datos originales con las URLs de las imágenes
            for original, updated in zip(data_obtained, data_simplified):
                for key in name_property_url.keys():
                    original[key] = updated.get(key)

            # Guardar los datos
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

    def process_chunk(self, data_chunk, search_by_property, name_property_url, search_specific, apis, process_number, return_dict, lock):
        session = requests.Session()
        total_items = len(data_chunk)
        rate_limiters = {api.name: RateLimiter(api.name) for api in apis}

        for i, item in enumerate(data_chunk):
            title = ' '.join(str(item[name]) for name in search_by_property)
            search_query = f'{title} {search_specific}'.strip()
            logging.info(f"Proceso {process_number}: {Fore.YELLOW}{i+1}/{total_items}{Fore.RESET}. Buscando imagen de: {search_query.upper()}")

            image_url = None
            for api in apis:
                api_name = api.name
                api_key = api.api_key
                base_url = api.base_url
                rate_limiter = rate_limiters.get(api_name)
                if not rate_limiter:
                    # Si no hay rate limiter para esta API, usar RateLimiter default
                    rate_limiter = RateLimiter(api_name)
                    rate_limiters[api_name] = rate_limiter

                if rate_limiter.can_request():
                    try:
                        if api_name == 'pexels':
                            image_url = self.search_pexels(session, search_query, api_key)
                        elif api_name == 'pixabay':
                            image_url = self.search_pixabay(session, search_query, api_key)
                        elif api_name == 'unsplash':
                            image_url = self.search_unsplash(session, search_query, api_key)
                        elif api_name == 'flickr':
                            image_url = self.search_flickr(session, search_query, api_key)
                        elif api_name == 'custom_service':
                            query_param_name = getattr(api, 'query_param_name', 'q')  # Nombre del parámetro de búsqueda
                            headers = getattr(api, 'headers', {'User-Agent': 'Mozilla/5.0'})  # Headers adicionales
                            image_url = self.search_custom_service(session, search_query, base_url, query_param_name, headers)
                        rate_limiter.increment_request()
                    except Exception as e:
                        logging.error(f"Proceso {process_number}: Error al buscar en {api_name}: {e}")
                        continue  # Continuar con la siguiente API

                    if image_url:
                        logging.info(f"Proceso {process_number}: Imagen encontrada usando {api_name}")
                        break
                    else:
                        logging.info(f"Proceso {process_number}: No se encontró imagen usando {api_name}, intentando con el siguiente API.")
                else:
                    logging.info(f"Proceso {process_number}: Límite de solicitudes alcanzado para {api_name}, intentando con el siguiente API.")

            if image_url:
                for key in name_property_url.keys():
                    item[key] = image_url
                    logging.info(f"Proceso {process_number}: Imagen almacenada en la propiedad: {key}")
            else:
                logging.warning(f"Proceso {process_number}: No se encontró imagen para '{search_query}' en las APIs proporcionadas.")

        # Actualizar el diccionario compartido
        with lock:
            return_dict[process_number] = data_chunk

    def search_pexels(self, session, query, api_key):
        headers = {
            'Authorization': api_key
        }
        params = {
            'query': query,
            'per_page': 1
        }
        response = session.get('https://api.pexels.com/v1/search', headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        photos = data.get('photos', [])
        if photos:
            image_url = photos[0]['src']['original']
            return image_url
        else:
            return None

    def search_pixabay(self, session, query, api_key):
        params = {
            'key': api_key,
            'q': query,
            'image_type': 'photo',
            'per_page': 3
        }
        response = session.get('https://pixabay.com/api/', params=params)
        response.raise_for_status()
        data = response.json()
        hits = data.get('hits', [])
        if hits:
            image_url = hits[0]['largeImageURL']
            return image_url
        else:
            return None

    def search_unsplash(self, session, query, api_key):
        headers = {
            'Authorization': f'Client-ID {api_key}'
        }
        params = {
            'query': query,
            'per_page': 1
        }
        response = session.get('https://api.unsplash.com/search/photos', headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        results = data.get('results', [])
        if results:
            image_url = results[0]['urls']['regular']
            return image_url
        else:
            return None

    def search_flickr(self, session, query, api_key):
        params = {
            'method': 'flickr.photos.search',
            'api_key': api_key,
            'text': query,
            'per_page': 1,
            'format': 'json',
            'nojsoncallback': 1
        }
        response = session.get('https://api.flickr.com/services/rest/', params=params)
        response.raise_for_status()
        data = response.json()
        photos = data.get('photos', {}).get('photo', [])
        if photos:
            photo = photos[0]
            farm_id = photo['farm']
            server_id = photo['server']
            photo_id = photo['id']
            secret = photo['secret']
            image_url = f'https://farm{farm_id}.staticflickr.com/{server_id}/{photo_id}_{secret}.jpg'
            return image_url
        else:
            return None

    def search_custom_service(self, session, query, base_url, query_param_name, headers):
        # Reemplazar espacios por '+'
        query_encoded = query.replace(' ', '+')

        # Construir la URL manualmente
        request_url = f"{base_url}?{query_param_name}={query_encoded}"

        # Construir el comando curl
        curl_command = f"curl -X GET '{request_url}'"

        # Agregar headers al comando curl
        for header_name, header_value in headers.items():
            curl_command += f" -H '{header_name}: {header_value}'"

        # Imprimir el comando curl en el log
        logging.info(f"Comando CURL: {curl_command}")

        try:
            # Realizar la solicitud directamente con la URL construida
            response = session.get(request_url, headers=headers)
            response.raise_for_status()
            html_content = response.text

            # Guardar la respuesta en un archivo .txt con timestamp Unix
            # timestamp = int(datetime.now().timestamp())
            # filename = f"response_{timestamp}.txt"
            # with open(filename, 'w', encoding='utf-8') as file:
            #     file.write(html_content)
            # logging.info(f"Respuesta guardada en el archivo: {filename}")

            # Extraer el bloque de función que comienza con 'function R(a){'
            function_pattern = r'function\s+R\(\s*a\s*\)\s*{.*?}'
            function_match = re.search(function_pattern, html_content, re.DOTALL)
            if not function_match:
                logging.warning("No se encontró la función 'function R(a){' en la respuesta HTML.")
                return None

            # Obtener el contenido después de la función 'function R(a){'
            content_after_function = html_content[function_match.end():]
         
            # Buscar la primera URL de imagen en el contenido después de la función
            image_pattern = r'(https?://[^\s\'"]+\.(?:jpg|jpeg|gif|bmp|webp))'
            image_match = re.search(image_pattern, content_after_function, re.IGNORECASE)
            if image_match:
                image_url = image_match.group(0)
                logging.info(f"Imagen encontrada exitosamente: {image_url}")
                time.sleep(1) # Esperar 1 s antes de buscar la URL de la imagen
                return image_url  # Devolver la primera URL de imagen encontrada
            else:
                logging.warning("No se encontraron URLs de imágenes después de la función 'function R(a){'.")
                return None

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logging.warning("Se recibió un error 429 Too Many Requests. Esperando antes de reintentar...")
                retry_after = int(e.response.headers.get("Retry-After", 60))  # Tiempo de espera en segundos
                time.sleep(retry_after)
                # Reintentar la solicitud después de esperar
                return self.search_custom_service(session, query, base_url, query_param_name, headers)
            else:
                logging.error(f"Error al buscar en custom_service: {e}")
                return None
        except Exception as e:
            logging.error(f"Error al buscar en custom_service: {e}")
            return None

class RateLimiter:
    def __init__(self, api_name):
        self.api_name = api_name
        self.limit = self.get_rate_limit(api_name)
        self.requests_made = 0

    def get_rate_limit(self, api_name):
        limits = {
            'pexels': 200,    # 200 solicitudes por hora
            'pixabay': 5000,  # 5000 solicitudes por día
            'unsplash': 50,   # 50 solicitudes por hora
            'flickr': 3600,   # 3600 solicitudes por hora (estimado)
            'custom_service': 1200  # Ajustar según la política del servicio
        }
        return limits.get(api_name, 1000)

    def can_request(self):
        return self.requests_made < self.limit

    def increment_request(self):
        self.requests_made += 1
