import os
import requests
import zipfile
import boto3
import json
from io import BytesIO

def get_kaggle_credentials(secret_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name='sa-east-1')  

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        
        secret = get_secret_value_response['SecretString']
        
        credentials = json.loads(secret)
        
        return credentials['KAGGLE_USERNAME'], credentials['KAGGLE_KEY']

    except Exception as e:
        print(f"Erro ao buscar as credenciais do Secrets Manager: {str(e)}")
        raise e

def lambda_handler(event, context):
    s3_bucket = 'landzone-vendas'
    secret_name = 'kaggle_credentials'  
    
    KAGGLE_USERNAME, KAGGLE_KEY = get_kaggle_credentials(secret_name)

    dataset_url = f"https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce"
    
    headers = {
        'Authorization': f'Basic {KAGGLE_USERNAME}:{KAGGLE_KEY}'
    }
    
    response = requests.get(dataset_url, headers=headers, stream=True)
    
    if response.status_code == 200:
        zip_data = BytesIO(response.content)
        
        with zipfile.ZipFile(zip_data, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.endswith('.csv'):
                    file_name = os.path.basename(file_info.filename)
                    
                    file_data = zip_ref.read(file_info.filename)
                    
                    s3_client = boto3.client('s3')
                    s3_client.put_object(Bucket=s3_bucket, Key=file_name, Body=file_data)
                    
                    print(f'Arquivo {file_name} carregado no S3 com sucesso!')
    
    else:
        print(f"Falha no download do dataset. Código de status: {response.status_code}")
    
    return {
        'statusCode': 200,
        'body': 'Dataset baixado e carregado no S3!'
    }
