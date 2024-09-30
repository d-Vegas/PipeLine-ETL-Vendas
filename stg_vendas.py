import awswrangler as wr
import pandas as pd
import unicodedata
import re

def remover_acentos(texto):
    texto_normalizado = unicodedata.normalize('NFKD', texto)
    texto_ascii = re.sub(r'[^\w\s]', '', texto_normalizado)
    return texto_ascii

def lambda_handler(event, context):
    source_bucket = 'landzone-vendas'
    destination_bucket = 'stg-vendas'

    for record in event['Records']:
        file_name = record['s3']['object']['key']
        source_path = f"s3://{source_bucket}/{file_name}"
        destination_path = f"s3://{destination_bucket}/{file_name.replace('.csv', '.parquet')}"

        print(f"Processando o arquivo: {file_name}")

        try:
            
            df = wr.s3.read_csv(source_path)
           
            
            numeric_columns = df.select_dtypes(include=['number']).columns

            
            df[numeric_columns] = df[numeric_columns].apply(
                lambda x: x.fillna(x.median()) if 'payment' in x.name else x.fillna(0)
            )

            
            object_columns = df.select_dtypes(include=['object']).columns
            df[object_columns] = df[object_columns].fillna('desconhecido')
            
            
            df.columns = df.columns.str.strip()

            
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].apply(lambda x: remover_acentos(str(x).lower()))

           
            df.drop_duplicates(inplace=True)

            
            wr.s3.to_parquet(
                df=df,
                path=destination_path,
                dataset=True,
                mode='overwrite'
            )

            print(f"Arquivo {file_name.replace('.csv', '.parquet')} processado e salvo no bucket {destination_bucket}.")

        except Exception as e:
            print(f"Erro ao processar o arquivo {file_name}: {e}")
