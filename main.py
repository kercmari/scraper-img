import os
import logging
import json
from datetime import datetime
from dotenv import load_dotenv

from model import Config, APIConfig
from src import ImageScraper, Cloudinary
from src.utils import format_duration

load_dotenv()
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

def main():
    # Cargar variables de entorno
    cloud_name = os.getenv('CLOUD_NAME')
    api_key = os.getenv('API_KEY')
    api_secret = os.getenv('API_SECRET')
    name_folder = os.getenv('FOLDER')
    proxies = json.loads(os.getenv('PROXIES', '[]'))  # Suponiendo que proxies están en formato JSON
    ssl_verify = json.loads(os.getenv('SSL_VERIFY', 'false').lower())
    custom_ssl_cert = os.getenv('CUSTOM_SSL_CERT')

    # Verificar que las variables de entorno se cargaron correctamente
    if not all([cloud_name, api_key, api_secret, name_folder]):
        raise ValueError("Faltan variables de entorno necesarias para Cloudinary.")
    
    # Configuración de Cloudinary
    cloud = Cloudinary(
        cloud_name=cloud_name,
        api_key=api_key,
        api_secret=api_secret,
        name_folder=name_folder
    )

    # Crear directorio JSON si no existe
    if not os.path.exists('JSON'):
        os.mkdir('JSON')

    # Verificar existencia de config.json
    if not os.path.isfile('config.json'):
        raise FileNotFoundError("No se encontró el archivo config.json")
    
    with open('config.json', 'r') as f:
        config_data = json.load(f)
    
    # Analizar configuraciones de APIs
    apis_config = [APIConfig(**api) for api in config_data.get('apis', [])]
    config_data['apis'] = apis_config

    try:
        model_config = Config(**config_data)
    except TypeError as e:
        raise e

    # Cargar datos de extracción
    with open(f'{model_config.archivo_extraccion}.json', 'r') as f:
        data = json.load(f)
    
    start_time = datetime.now()
    scraper = ImageScraper(
        data=data, 
        account=cloud, 
        results_obtained=model_config.nombre_archivo, 
        results_simplified=model_config.nombre_archivo, 
        pathFiles=model_config.identificador_proyecto, 
        table=model_config.nombre_carpeta_cloudinary,
        tipo_imagen="",
        apis=model_config.apis,
        num_processes=model_config.num_processes,
        proxies=proxies,
        ssl_verify=ssl_verify,
        custom_ssl_cert=custom_ssl_cert
    )

    scraper.get_image_from_apis(
        *model_config.campos_buscar,
        name_property_url=model_config.imagenes,
        search_specific=model_config.extra_busqueda
    )
    
    end_time = datetime.now()
    scraping_duration = end_time - start_time

    logging.info(f"Duración total: {format_duration(scraping_duration)}")

if __name__ == '__main__':
    main()
