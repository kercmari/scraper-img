# scraping-img
Scraping de Imágenes. Realiza una búsqueda y obtiene la url de la imagen para subirla a Cloudinary.

## Descarga las dependencias
```sh
pip install -r requirements.txt
```

## Requisitos
- Navegador Chrome.
- Controlador `chromedriver`.
- Cuenta de Cloudinary.

Una vez que tengas tu cuenta. Complete los campos necesarios para utilizar. En el archivo: `.env`

```sh
CLOUD_NAME = ""
API_KEY = ""
API_SECRET = ""
FOLDER = ""
```

## Uso
Configure el archivo `config.json` especificando los directorios donde se guardarán los datos localmente, el archivo JSON utilizado para exportar los datos. Y los diferentes campos a rellenar.

```json
{
    "archivo_extraccion": "", // Nombre del archivo donde se hara la extraccion de los datos
    "identificador_proyecto": "", // Identificador del proyecto
    "nombre_archivo": "", // Nombre del archivo que se almacenara los datos
    "nombre_carpeta_cloudinary": "", // Nombre de la carpeta que se almacenara los archivos en cloudinary
    "campos_buscar": [  
        
        // Busqueda con el uso de las propiedades del item

    ],
    "imagenes": {
        "property_1": "",
        "property_2": []
    }, // Especificar el nombre de la propiedad que se almacena la url de image
    "extra_busqueda": "" // Extra para ser especificos en la busqueda
}
```

Ejecute el programa:
```sh
python main.py
```
## Desarrolladores:
 - [Khristopher Santiago](https://github.com/khrsantiago)
 - [Francisco Griman](https://github.com/fcoagz)
