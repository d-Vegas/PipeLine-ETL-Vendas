import awswrangler as wr
import pandas as pd
import unicodedata
import re

def remover_acentos(texto):
    texto_normalizado = unicodedata.normalize('NFKD', texto)
    texto_ascii = re.sub(r'[^\w\s_]', '', texto_normalizado)
    return texto_ascii

def lambda_handler(event, context):
    source_bucket = 'landzone-vendas'
    destination_bucket = 'stg-vendas'

    # Leitura dos arquivos CSV de cada tabela para DataFrames
    try:
        df_customers = wr.s3.read_csv(f"s3://{source_bucket}/olist_customers_dataset.csv")
        df_orders = wr.s3.read_csv(f"s3://{source_bucket}/olist_orders_dataset.csv")
        df_order_items = wr.s3.read_csv(f"s3://{source_bucket}/olist_order_items_dataset.csv")
        df_order_reviews = wr.s3.read_csv(f"s3://{source_bucket}/olist_order_reviews_dataset.csv")
        df_order_payments = wr.s3.read_csv(f"s3://{source_bucket}/olist_order_payments_dataset.csv")
        df_products = wr.s3.read_csv(f"s3://{source_bucket}/olist_products_dataset.csv")
        df_sellers = wr.s3.read_csv(f"s3://{source_bucket}/olist_sellers_dataset.csv")
        df_geolocation = wr.s3.read_csv(f"s3://{source_bucket}/olist_geolocation_dataset.csv")

        # Remoção de valores 'not defined'
        dataframes = [df_customers, df_orders, df_order_items, df_order_reviews, df_order_payments, df_products, df_sellers, df_geolocation]
        for df in dataframes:
            df.replace('not defined', pd.NA, inplace=True)

        # Preenchimento de valores nulos
        for df in dataframes:
            # Preencher valores faltantes para colunas numéricas
            numeric_columns = df.select_dtypes(include=['number']).columns
            df[numeric_columns] = df[numeric_columns].apply(lambda x: x.fillna(x.median()) if 'payment' in x.name else x.fillna(0))

            # Preencher valores faltantes para colunas do tipo 'object'
            object_columns = df.select_dtypes(include=['object']).columns
            df[object_columns] = df[object_columns].fillna('desconhecido')

            # Remover espaços das colunas e normalizar texto
            df.columns = df.columns.str.strip()
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].apply(lambda x: remover_acentos(str(x).lower()))

            # Remover duplicatas
            df.drop_duplicates(inplace=True)

        # Criação das tabelas de dimensão e fato
        # Dimensão Clientes
        dim_customers = df_customers[['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state']]

        # Dimensão Produtos
        dim_products = df_products[['product_id', 'product_category_name', 'product_name_lenght', 'product_description_lenght',
                                    'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']]

        # Dimensão Vendedores
        dim_sellers = df_sellers[['seller_id', 'seller_zip_code_prefix', 'seller_city']]

        # Dimensão Geolocalização
        dim_geolocation = df_geolocation[['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng', 'geolocation_city', 'geolocation_state']]

        # Dimensão Pedidos
        dim_orders = df_orders[['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'order_approved_at',
                                'order_delivered_carrier_date', 'order_delivered_customer_date']]

        # Dimensão Pagamentos
        dim_payments = df_order_payments[['order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value']]

        # Dimensão Revisões
        dim_reviews = df_order_reviews[['review_id', 'order_id', 'review_score', 'review_comment_title', 'review_comment_message',
                                        'review_creation_date', 'review_answer_timestamp']]

        # Tabela Fato (juntando informações relevantes)
        fact_order_details = df_order_items.merge(dim_orders, on='order_id', how='left') \
                                           .merge(dim_products, on='product_id', how='left') \
                                           .merge(dim_sellers, on='seller_id', how='left') \
                                           .merge(dim_payments, on='order_id', how='left') \
                                           .merge(dim_reviews, on='order_id', how='left')

        # Salvando tabelas de dimensão no bucket Staging como Parquet
        dimensions = {
            'dim_customers': dim_customers,
            'dim_products': dim_products,
            'dim_sellers': dim_sellers,
            'dim_geolocation': dim_geolocation,
            'dim_orders': dim_orders,
            'dim_payments': dim_payments,
            'dim_reviews': dim_reviews
        }

        for dim_name, dim_df in dimensions.items():
            wr.s3.to_parquet(
                df=dim_df,
                path=f"s3://{destination_bucket}/{dim_name}.parquet",
                dataset=True,
                mode='overwrite'
            )
            print(f"Tabela de dimensão {dim_name} salva no bucket {destination_bucket}.")

        # Salvando tabela fato no bucket Staging como Parquet
        wr.s3.to_parquet(
            df=fact_order_details,
            path=f"s3://{destination_bucket}/fact_order_details.parquet",
            dataset=True,
            mode='overwrite'
        )
        print(f"Tabela Fato fact_order_details salva no bucket {destination_bucket}.")

    except Exception as e:
        print(f"Erro ao processar os arquivos: {e}")
