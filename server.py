import pika
# importing os module for environment variables
import os
# importing necessary functions from dotenv library
from dotenv import load_dotenv, dotenv_values 
from minio import Minio
import pandas as pd
from pymongo import MongoClient
import json
from bson import ObjectId

# loading variables from .env file
load_dotenv()

# Configuração do cliente MinIO
minioClient = Minio(
    'localhost:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)

# Configuração do cliente MongoDB
mongoClient = MongoClient(os.getenv("STRING_CONNECTION"))
db = mongoClient['dbDataset']
collectionDetailsDatasets = db['detailsdatasets']
collectionDatasets = db['datasets']

def process_file_and_update_db(idDataset, file_path):
    # Ler o arquivo com Pandas
    #df = pd.read_excel(file_path)

    # Carregar um arquivo Excel
    xlsx = pd.ExcelFile(file_path)

    record = {}
    details = []
    # Para cada folha, ler em um DataFrame e processar
    for sheet_name in xlsx.sheet_names:
        df = pd.read_excel(xlsx, sheet_name=sheet_name)
        # Transformar DataFrame em um formato apropriado
        details.append({
            'sheet_name': sheet_name,
            'data': df.to_dict(orient='records')
        })
    
    # Realizar operações com Pandas
    record["details"] = details    
    record['dataset_id'] = idDataset

    # Define o filtro para encontrar o documento que você quer atualizar
    filtro = {'_id': ObjectId(idDataset)}

    # Define as alterações que você quer fazer
    novo_valor = {'$set': {'status': 'PROCESSED'}}

    print("filtro", filtro)
    print("novo_valor", novo_valor)
    print("record", [record])

    collectionDatasets.update_one(filtro, novo_valor)
    collectionDetailsDatasets.insert_many([record])
    print("Dados salvos no MongoDB com sucesso!")
 
def receive_message(ch, method, properties, body):
    print(f"Received {body.decode()}")
    dataset = json.loads(body.decode())

    # Coletar o nome do arquivo do corpo da mensagem
    file_name = dataset["filePath"]
    idDataset = dataset["idDataset"]

    # Baixar o arquivo do MinIO
    data = minioClient.get_object('datasets', file_name)
    file_path = f'./tmp/{file_name}'
    with open(file_path, 'wb') as file_data:
        for d in data.stream(32*1024):
            file_data.write(d)
    
    # Processar o arquivo e atualizar o banco de dados
    process_file_and_update_db(idDataset, file_path)

#Config connection
URL = os.getenv("AMQP_URL")
queue="datasets"
params = pika.URLParameters(URL)
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue = queue, durable = True)
channel.basic_consume(queue = queue, on_message_callback = receive_message, auto_ack = True)

print('Waiting for messages. To exit press CTRL+C')
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()





