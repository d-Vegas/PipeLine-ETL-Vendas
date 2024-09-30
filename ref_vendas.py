import awswrangler as wr
import pandas as pd

def lambda_handler(event, context):
    source_bucket = 'stg-vendas'
    destination_bucket = 'ref-vendas'

    try:
        # Definindo a lista de datasets e os caminhos do S3
        datasets = {
            "fact_order_details": f's3://{source_bucket}/fact_order_details.parquet/',
            "dim_customers": f's3://{source_bucket}/dim_customers.parquet/',
            "dim_products": f's3://{source_bucket}/dim_products.parquet/',
        }

        # Carregando a tabela fato e as dimensões necessárias
        df_fact_order_details = wr.s3.read_parquet(datasets["fact_order_details"])
        df_dim_products = wr.s3.read_parquet(datasets["dim_products"])
        df_dim_customers = wr.s3.read_parquet(datasets["dim_customers"])

        # Removendo valores nulos específicos, conforme necessário
        df_fact_order_details.fillna({'payment_value': 0}, inplace=True)

        # Cálculos derivados
        df_fact_order_details['delivery_time_days'] = (
            pd.to_datetime(df_fact_order_details['order_delivered_customer_date'], errors='coerce') -
            pd.to_datetime(df_fact_order_details['order_purchase_timestamp'], errors='coerce')
        ).dt.days

        # Adicionar valor total do pedido para cada item de pedido
        df_fact_order_details['total_order_value'] = df_fact_order_details['price']

        # Análise de Desempenho de Produtos
        product_performance = df_fact_order_details.groupby('product_id').agg(
            product_sales_volume=('order_item_id', 'count'),
            product_sales_value=('total_order_value', 'sum'),
            product_return_rate=('order_status', lambda x: (x == 'canceled').sum() / x.count()),
            average_review_score=('review_score', 'mean')
        ).reset_index()

        # Adicionando informações da dimensão de produtos
        product_performance = product_performance.merge(df_dim_products[['product_id', 'product_category_name']],
                                                        on='product_id', how='left')

        # Salvando a análise de desempenho de produtos no S3
        wr.s3.to_parquet(
            df=product_performance,
            path=f's3://{destination_bucket}/product_performance.parquet',
            dataset=True,
            mode='overwrite'
        )

        # Análise Geográfica (utilizando 'customer_city' e 'customer_state')
        regional_sales = df_fact_order_details.merge(df_dim_customers[['customer_id', 'customer_city', 'customer_state']],
                                                     on='customer_id', how='left').groupby(['customer_state', 'customer_city', 'product_id']).agg(
            regional_sales_volume=('order_item_id', 'count'),
            regional_sales_value=('total_order_value', 'sum')
        ).reset_index()

        # Salvando a análise geográfica no S3
        wr.s3.to_parquet(
            df=regional_sales,
            path=f's3://{destination_bucket}/regional_sales.parquet',
            dataset=True,
            mode='overwrite'
        )

        # Análise de Padrões de Pagamento
        payment_analysis = df_fact_order_details.groupby('payment_type').agg(
            payment_frequency=('payment_type', 'count'),
            average_payment_value=('payment_value', 'mean')
        ).reset_index()

        # Salvando a análise de padrões de pagamento no S3
        wr.s3.to_parquet(
            df=payment_analysis,
            path=f's3://{destination_bucket}/payment_analysis.parquet',
            dataset=True,
            mode='overwrite'
        )

        # Análise de Potenciais Fraudes
        average_payment_value = df_fact_order_details['payment_value'].mean()
        fraud_analysis = df_fact_order_details.groupby('product_category_name').agg(
            total_order_value=('total_order_value', 'sum'),
            is_suspicious_order=('payment_value', lambda x: (x > average_payment_value * 3).any())
        ).reset_index()

        # Salvando a análise de potenciais fraudes no S3
        wr.s3.to_parquet(
            df=fraud_analysis,
            path=f's3://{destination_bucket}/fraud_analysis.parquet',
            dataset=True,
            mode='overwrite'
        )

        # Análise de Tempo de Entrega
        delivery_analysis = df_fact_order_details.merge(df_dim_customers[['customer_id', 'customer_city', 'customer_state']],
                                                        on='customer_id', how='left').groupby(['customer_state', 'customer_city', 'product_id']).agg(
            average_delivery_time=('delivery_time_days', 'mean'),
            delivery_delay_rate=('delivery_time_days', lambda x: (x > 5).sum() / x.count() if x.count() > 0 else 0)
        ).reset_index()

        # Salvando a análise de tempo de entrega no S3
        wr.s3.to_parquet(
            df=delivery_analysis,
            path=f's3://{destination_bucket}/delivery_analysis.parquet',
            dataset=True,
            mode='overwrite'
        )

        print("Todos os Parquets foram criados e salvos no bucket REF.")

    except Exception as e:
        print(f"Erro ao processar a camada REF: {e}")
