from typing import Any, List, Dict
from dataclasses import dataclass

@dataclass
class APIConfig:
    name: str
    api_key: str = None
    base_url_list: str = None
    query_param_name: str = 'search'
    headers_list: Dict[str, str] = None
    enabled: bool = True

@dataclass
class Config:
    archivo_extraccion: str
    identificador_proyecto: str
    nombre_archivo: str
    nombre_carpeta_cloudinary: str
    campos_buscar: List[str]
    imagenes: Dict[str, Any]
    extra_busqueda: str
    apis: List[APIConfig]
    num_processes: int
