from fastapi import FastAPI,Query, HTTPException, status , Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.encoders import jsonable_encoder


from functools import wraps
from pydantic import BaseModel
from typing import  List

import requests
import traceback
import logging

import json
import os
from datetime import datetime,timedelta
import time

###Se configura un sistema de log basico

fecha_log = datetime.now().strftime("%Y_%m_%d")	
logger = logging.getLogger('log_api')
logger.setLevel(logging.DEBUG)
path = os.getcwd()
carpeta = os.path.dirname(os.path.realpath(__file__)) + '/Logs/'
if not os.path.isdir (carpeta):
    os.mkdir(carpeta)
file_path = carpeta  + fecha_log + '.log'
# fh = logging.FileHandler(file_path)
fh = logging.FileHandler(file_path, mode='w')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)
formatter = logging.Formatter(
'%(asctime)s - %(levelname)s: %(message)s', "%H:%M:%S")
fh.setFormatter(formatter)
logger.addHandler(fh)
#Fin de configuracion de log

class PChoiva(BaseModel):
    manha:int
    tarde:int
    noite:int
    
class Ceo(BaseModel):
    manha:str
    tarde:str
    noite:str

class Prediccion(BaseModel):
    fecha:str
    tMax:float
    tMin:float
    pchoiva:PChoiva
    ceo:Ceo
    
#creamos un decorador para comprobar que el archivo de log es el del dia actual
#y que no haya ficheros muy antiguos
def comprobar_fichero_log(func):
    
    @wraps(func)
    async def wrapper(*args, **kwargs):
        
        if not os.path.isdir (carpeta):
             os.mkdir(carpeta)
        
        fecha =  datetime.now().strftime("%Y_%m_%d")

        if not (os.path.exists( carpeta  + fecha + '.log')):
            fh = logging.FileHandler(carpeta  + fecha + '.log', mode='w')
            fh.setLevel(logging.DEBUG)
            logger.addHandler(fh)
            formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s: %(message)s', "%H:%M:%S")
            fh.setFormatter(formatter)
            logger.addHandler(fh)

        # Borrar logs anteriores a 15 dias
        for filename in os.listdir(carpeta):
            if os.path.getmtime(os.path.join(carpeta, filename)) < time.time() - 14 * 86400:
                if os.path.isfile(os.path.join(carpeta, filename)):
                    if filename.endswith(".log"):
                        os.remove(os.path.join(carpeta, filename))

        return await func(*args, **kwargs)
    return wrapper

#Cargamos ciertos datos extraidos del pdf de documentación del api de meteogalicia
#Una descripcion del estado del cielo en base al valor numerico que proporcinan
#Una lista de aquellos ids de territorio que son validos
with open("variables.json") as f:
    datos_json = json.load(f)
    territorios_validos = datos_json["territorios_validos"]
    map_cielo = datos_json["map_cielo"]
    map_cielo = {int(key):value for key, value in map_cielo.items()}

#Creamos una excepcion para cuando se intente acceder a un endpoint no declarado poder gestionarlo
async def not_found(request, exc):
    return JSONResponse(content={'detail': "Endpoint no encontrado, revise la ruta"}, status_code=exc.status_code)

exception_handlers = {
    404: not_found
}

#Algunos metadatos para la parte de documentacion
tags_metadata = [
  
    {
        "name": "Meteo",
        "description": "Endpoints relativos al api de Meteogalicia",
        "externalDocs": {
            "description": "Documentacion api meteogalicia",
            "url": "https://www.meteogalicia.gal/web/RSS/rssIndex.action?request_locale=es",
        },
    },
]
app = FastAPI(
    exception_handlers=exception_handlers,
    title="API Prueba Meteogalicia",
    version="1.0",
    openapi_tags=tags_metadata,
    swagger_ui_parameters={"defaultModelsExpandDepth": -1}
)

def busca_valor_en_prediccion(prediccion:dict,clave:str):
    """
    Retorna el valor buscado dentro de una prediccion
    En caso de encontrarlo retornara -9999 como valor no disponible 
    """

    if clave == "pchoiva":
        #Comprobamos que la prediccion incluya la variable de probabilidad de lluvia  
        #y en caso afirmativa que contanga las 3 posibilidades

        if "pchoiva" not in prediccion:
            return { "manha":"-9999","noite":"-9999","tarde":"-9999" }

        if "manha" not in prediccion["pchoiva"]:
            prediccion["pchoiva"]["manha"] = "-9999"

        if "tarde" not in prediccion["pchoiva"]:
            prediccion["pchoiva"]["tarde"] = "-9999"

        if "noite" not in prediccion["pchoiva"]:
            prediccion["pchoiva"]["noite"] = "-9999"

        #Adaptamos el valor que recibimos de la prediccion de meteogalicia, a la clase PChoiva
        prediccion["pchoiva"] = PChoiva(  
            manha= prediccion["pchoiva"]["manha"],
            tarde= prediccion["pchoiva"]["tarde"],
            noite= prediccion["pchoiva"]["noite"]
        )

    if clave == "ceo":

        if "ceo" not in prediccion:
            return { "manha":"-No disponible","noite":"-No disponible","tarde":"-No disponible" }
        
        if "manha" not in prediccion["ceo"]:
            prediccion["ceo"]["manha"] = -9999

        if "tarde" not in prediccion["ceo"]:
            prediccion["ceo"]["tarde"] = -9999

        if "noite" not in prediccion["ceo"]:
            prediccion["ceo"]["noite"] = -9999

        #Buscamos el valor descriptivo correspondiente al valor numerico que entrega metegalicia
        prediccion["ceo"]["manha"] = map_cielo[prediccion["ceo"]["manha"]] if prediccion["ceo"]["manha"] in map_cielo else "No disponible"
        prediccion["ceo"]["tarde"] = map_cielo[prediccion["ceo"]["tarde"]] if prediccion["ceo"]["tarde"] in map_cielo else "No disponible"
        prediccion["ceo"]["noite"] = map_cielo[prediccion["ceo"]["noite"]] if prediccion["ceo"]["noite"] in map_cielo else "No disponible"
        
        #Adaptamos el valor que recibimos de la prediccion de meteogalicia, a la clase Ceo
        prediccion["ceo"] = Ceo(  
            manha= prediccion["ceo"]["manha"],
            tarde= prediccion["ceo"]["tarde"],
            noite= prediccion["ceo"]["noite"]
        )
    if clave in prediccion:
        return prediccion[clave]

    return "-9999"

def busca_prediccion_list(predicciones:list,fecha_a_buscar:str):
    """
    Retorna la prediccion buscada dentro de la lista que recibe como parametro
    en caso de encontrarla retornara None
    """

    for prediccion in predicciones:
        if 'dataPredicion' in prediccion and prediccion['dataPredicion']==fecha_a_buscar:
            return prediccion
    
    return None


@app.get("/api/observacion",include_in_schema=False)
@app.get("/api/observacion/{id_municipio}",response_model=List[Prediccion],tags=["Meteo"])
@comprobar_fichero_log
async def get_prediccion_meteo(id_municipio = Query(None, description = 'Municipio a consultar')):
    """
    Permite hacer una consulta al api de meteogalicia y extraer datos para el día en curso y los dos siguientes \n
    Recibe como parametro obligatorio el id del territorio sobre el cual buscar informacion\n
    por ejemplo:\n
    15036: Ferrol       15078: Santiago de Compostela       27035: Negueira de Muñiz\n
    15030: A Coruña     32024: Celanova                     36057:Vigo

    """
    datos_a_devolver = []
    if not id_municipio:
        logger.info("Peticion sin territorio")
        raise HTTPException(
            status_code = 412,
            detail = "El territorio es obligatorio"
        )
        
    if not id_municipio.isnumeric():
        logger.info(f"Peticion de un territorio no numerico {id_municipio}")
        raise HTTPException(
            status_code = 422,
            detail = "El id del municipio no es valido",
            headers = {"WWW-Authenticate": "Bearer"},
        )

    if str(id_municipio) not in territorios_validos:
         logger.info(f"Peticion de un territorio invalido {id_municipio}")
         raise HTTPException(
            status_code = 412,
            detail = "El territorio no esta disponible"
        )

    try:

        rest_api_url = f"https://servizos.meteogalicia.gal/mgrss/predicion/jsonPredConcellos.action?idConc={id_municipio}"
        r = requests.get(rest_api_url)
        datos = r.json()['predConcello']['listaPredDiaConcello']
        for x in range(3):
            dia_a_buscar = (datetime.today() + timedelta(days=x)).date()
            prediccion = busca_prediccion_list(datos, dia_a_buscar.strftime('%Y-%m-%dT00:00:00'))
            if not prediccion:
                logging.info(f"La prediccion para el dia {dia_a_buscar} no esta disponible")
                #en caso de no exister esa prediccion creamos un objeto vacio para que la funcion de busca_valor_en_prediccion devuelva no disponible
                prediccion = {} 

            dato = Prediccion(
                fecha = str(dia_a_buscar),
                tMax = busca_valor_en_prediccion(prediccion,"tMax"),
                tMin = busca_valor_en_prediccion(prediccion,"tMin"),
                pchoiva = busca_valor_en_prediccion(prediccion,"pchoiva"),
                ceo = busca_valor_en_prediccion(prediccion,"ceo")
            )
            datos_a_devolver.append(dato)
       
    except Exception:
         logger.warning("Error en /api/observacion %s", traceback.format_exc())
    
    logger.info(f"Peticion completada para id territorio {id_municipio}")            
   
    return jsonable_encoder(datos_a_devolver)