import requests
import json
from connect_db import *


class ZonaEleitoral:

    def __init__(self):        
        pass

    def get_zonas(self):
        url = "https://resultados.tse.jus.br/oficial/ele2024/619/config/mun-e000619-cm.json"
        response = requests.get(url).content
        byte = response.decode()
        data = json.loads(byte)
        estados_list = []
        count = 0

        for estado in data['abr']:
            zonas_dict = {}
            municipios_list = []
            count+=1

            zonas_dict['id'] = count 
            zonas_dict['estado'] = estado['ds']
            
            for municipios in estado['mu']:
                lista_mun = {}
                lista_mun['nome_mun'] = municipios['nm']
                lista_mun['nr_zona'] = municipios['z']
                municipios_list.append(lista_mun)

            zonas_dict['municipios'] = municipios_list
            estados_list.append(zonas_dict)
                     
        return estados_list

    def salva_zonas(self, zonas):
        mongo = MongoDB('zonas-eleitorais')
        bulk = []
        for zona in zonas:
            bulk.append(UpdateOne(
                filter={"id": zona['id']},
                update={"$set": zona},
                upsert=True
            ))

        mongo.bulk_batch('zonas_eleitorais', bulk)


if __name__ == "__main__":
    zona_eleitoral = ZonaEleitoral()
    zonas_dict = zona_eleitoral.get_zonas()
    salva_zonas_dict = zona_eleitoral.salva_zonas(zonas_dict)