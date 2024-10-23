import time
import io
import logging
from typing import Any, List, Dict
from datetime import datetime
from dataclasses import dataclass
from urllib.parse import urlparse
import cloudscraper
import requests
import cloudinary
from cloudinary import uploader
from colorama import Fore
from PIL import Image 
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed

from .utils import _write_files, _update_path_url_json, format_duration

@dataclass
class Cloudinary:
    cloud_name: str
    api_key: str
    api_secret: str
    name_folder: str
    cloudy_thread: int = 4  # Número de hilos para subir a Cloudinary

def is_valid_url(url: str) -> bool:
    """Valida si una URL es válida."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False

def _compress_image(url: str) -> bytes:
    """Descarga y comprime una imagen desde una URL utilizando cloudscraper."""
    if not is_valid_url(url):
        logging.error(f"La URL proporcionada es inválida: '{url}'")
        return None
    try:
        scraper = cloudscraper.create_scraper()  # Crea un scraper que maneja Cloudflare
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Referer': 'https://www.google.com/',  # Opcional, dependiendo de los requisitos del servidor
        }
        logging.info(f"Iniciando descarga de imagen desde: {url}")
        start_time = time.perf_counter()
        
        response = scraper.get(url, headers=headers, stream=True, verify=False, timeout=8)
        response.raise_for_status()
        
        end_time = time.perf_counter()
        duration = end_time - start_time
        logging.info(f"Descarga completada en {duration:.2f} segundos.")
        
        image_io = io.BytesIO()
        image = Image.open(io.BytesIO(response.content))
        image.save(image_io, format='WEBP', quality=70)  # Comprimir la imagen a WEBP con calidad 70
        return image_io.getvalue()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            logging.error(f"Acceso denegado (403) al intentar descargar la imagen desde '{url}'.")
        else:
            logging.error(f"Error HTTP al procesar la imagen desde la URL '{url}': {e}")
        return None
    except Exception as e:
        logging.error(f"Error al procesar la imagen desde la URL '{url}': {e}", exc_info=True)
        return None

class UploadImage:
    def __init__(self, cloud_name: str, api_key: str, api_secret: str, cloudy_thread: int = 4) -> None:
        cloudinary.config(
            cloud_name=cloud_name,
            api_key=api_key,
            api_secret=api_secret
        )
        self.cloudy_thread = cloudy_thread
        self.executor = ThreadPoolExecutor(max_workers=self.cloudy_thread)
    
    def upload_to_cloudinary(self, file: bytes, folder: str) -> str:
        """Sube un archivo a Cloudinary y devuelve la URL segura."""
        try:
            upload_result = uploader.upload(file, format="webp", folder=folder)
            return upload_result["secure_url"]
        except Exception as e:
            logging.error(f"Error al subir la imagen a Cloudinary: {e}", exc_info=True)
            return None

    def _upload_image_task(self, i, key, url, data_obtained, pathFiles, results_obtained, cloudinary_folder_proyect):
        """Función que será ejecutada en paralelo para subir la imagen y actualizar data_obtained."""
        logging.info(f"URL: {url}")
        compressed_image = _compress_image(url)
        if compressed_image:
            cloudinary_url = self.upload_to_cloudinary(compressed_image, cloudinary_folder_proyect)
            if cloudinary_url:
                data_obtained[i][key] = cloudinary_url
                _update_path_url_json(f'{pathFiles}/{results_obtained}', key, i, cloudinary_url)
                logging.info(f"{Fore.YELLOW}{i+1}/{len(data_obtained)}. Imagen cargada exitosamente: {cloudinary_url} {Fore.RESET}")
            else:
                logging.error(f"No se pudo subir la imagen a Cloudinary para la URL '{url}'.")
        else:
            logging.error(f"No se pudo comprimir la imagen desde la URL '{url}'.")

    def update_path_url(self,
                        pathFiles: str, 
                        folder: str,
                        data: Any, 
                        data_return: Any, 
                        name_property_url: dict, 
                        results_simplified: str, 
                        results_obtained: str, 
                        scraping_duration: Any = None) -> None:        
            
        url_prefix = "https://res.cloudinary.com/"
        cloudinary_folder_proyect = f"{folder}_{int(datetime.now().timestamp())}"
        data_obtained = data
        data_existent = data_return
        start_time_upload = datetime.now()
        
        # Crear una lista de tareas para subir imágenes
        tasks = []
        for i in range(len(data_obtained)):  
            for key, value in name_property_url.items():          
                if isinstance(value, str):
                    url = data_obtained[i].get(key)
                    if not url:
                        logging.warning(f"El elemento {i} no tiene una URL para la clave '{key}'.")
                        continue

                    if is_valid_url(url) and not url.startswith(url_prefix):
                        tasks.append((i, key, url))
                elif isinstance(value, list):
                    urls = data_obtained[i].get(key, [])
                    if not isinstance(urls, list):
                        logging.warning(f"El valor de '{key}' no es una lista de URLs en el índice {i}.")
                        continue
                    for j, url in enumerate(urls):
                        if not url:
                            logging.warning(f"El elemento {i}, índice {j} no tiene una URL para la clave '{key}'.")
                            continue
                        if is_valid_url(url) and not url.startswith(url_prefix):
                            tasks.append((i, key, url))

        logging.info(f"{Fore.GREEN}Iniciando la subida de {len(tasks)} imágenes a Cloudinary.{Fore.RESET}")
        upload_start_time = time.perf_counter()

        # Enviar las tareas al ThreadPoolExecutor
        futures = []
        for task in tasks:
            i, key, url = task
            future = self.executor.submit(
                self._upload_image_task,
                i, key, url, data_obtained, pathFiles, results_obtained, cloudinary_folder_proyect
            )
            futures.append(future)

        # Esperar a que todas las tareas terminen
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error en la tarea de subida: {e}", exc_info=True)

        # Actualizar data_existent con los nuevos enlaces de Cloudinary
        for item_a, item_b in zip(data_existent, data_obtained):
            for key in name_property_url.keys():
                item_a[key] = item_b.get(key, '')

        # Guardar los archivos actualizados
        _write_files(pathFiles, results_obtained, data_obtained)
        _write_files(pathFiles, results_simplified, data_existent)
        logging.info("Los datos se guardaron en el directorio: %s", pathFiles)
        logging.info(f"{Fore.GREEN}Fin de subida de archivos a Cloudinary{Fore.RESET}")

        upload_end_time = time.perf_counter()
        total_upload_time = upload_end_time - upload_start_time
        logging.info(f"Duración total de la subida a Cloudinary: {total_upload_time:.2f} segundos.")

        end_time_upload = datetime.now()
        upload_duration = end_time_upload - start_time_upload

        print()
        if scraping_duration:
            logging.info(f"Duración del scraping: {format_duration(scraping_duration)}")
        logging.info(f"Duración de la subida a Cloudinary: {format_duration(upload_duration)}")

    def close(self):
        """Cierra el ThreadPoolExecutor."""
        self.executor.shutdown(wait=True)
