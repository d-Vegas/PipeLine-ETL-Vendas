import awswrangler as wr
import pandas as pd

def lambda_handler(event, context):
    source_bucket = 'stg-vendas'
    destination_bucket = 'ref-vendas'

    try:
        # Leitura dos datasets do S3
        orders = wr.s3.read_parquet(path=f's3://{source_bucket}/olist_orders_dataset.parquet/')
        customers = wr.s3.read_parquet(path=f's3://{source_bucket}/olist_customers_dataset.parquet/')
        items = wr.s3.read_parquet(path=f's3://{source_bucket}/olist_order_items_dataset.parquet/')
        payments = wr.s3.read_parquet(path=f's3://{source_bucket}/olist_order_payments_dataset.parquet/')
        reviews = wr.s3.read_parquet(path=f's3://{source_bucket}/olist_order_reviews_dataset.parquet/')
        products = wr.s3.read_parquet(path=f's3://{source_bucket}/olist_products_dataset.parquet/')
        sellers = wr.s3.read_parquet(path=f's3://{source_bucket}/olist_sellers_dataset.parquet/')
        geolocation = wr.s3.read_parquet(path=f's3://{source_bucket}/olist_geolocation_dataset.parquet/')

        # Criando a tabela fato (`fact_orders`) a partir dos merges necessários
        fact_orders = orders.merge(customers, on='customer_id', how='left') \
            .merge(items, on='order_id', how='left') \
            .merge(products, on='product_id', how='left') \
            .merge(sellers, on='seller_id', how='left') \
            .merge(payments, on='order_id', how='left') \
            .merge(reviews, on='order_id', how='left') \
            .merge(geolocation, left_on='customer_zip_code_prefix', right_on='geolocation_zip_code_prefix', how='left')

        # Conversão de datas
        fact_orders['order_purchase_timestamp'] = pd.to_datetime(fact_orders['order_purchase_timestamp'], errors='coerce')
        fact_orders['order_delivered_customer_date'] = pd.to_datetime(fact_orders['order_delivered_customer_date'], errors='coerce')
        fact_orders['order_approved_at'] = pd.to_datetime(fact_orders['order_approved_at'], errors='coerce')

        # Calculando colunas derivadas para a tabela fato
        fact_orders['delivery_time_days'] = (fact_orders['order_delivered_customer_date'] - fact_orders['order_purchase_timestamp']).dt.days
        fact_orders['total_order_value'] = fact_orders['price'] * fact_orders['order_item_id']

        # Tratando valores nulos na coluna 'payment_value'
        fact_orders['payment_value'].fillna(0, inplace=True)

        # Salvando a tabela fato
        wr.s3.to_parquet(
            df=fact_orders,
            path=f's3://{destination_bucket}/fact_orders.parquet',
            dataset=True,
            mode='overwrite'
        )

        # Análise de Desempenho de Produtos
        product_performance = fact_orders.groupby('product_id').agg(
            product_sales_volume=('order_item_id', 'count'),
            product_sales_value=('total_order_value', 'sum'),
            product_return_rate=('order_status', lambda x: (x == 'returned').sum() / x.count()),
            average_review_score=('review_score', 'mean')
        ).reset_index()

        wr.s3.to_parquet(
            df=product_performance,
            path=f's3://{destination_bucket}/product_performance.parquet',
            dataset=True,
            mode='overwrite'
        )

        # Análise de Margem de Lucro (se houver dados de custo)
        if 'cost' in fact_orders.columns:
            fact_orders['product_margin'] = (fact_orders['price'] - fact_orders['cost']) / fact_orders['price']
            product_margin = fact_orders[['product_id', 'product_margin']].drop_duplicates()

            wr.s3.to_parquet(
                df=product_margin,
                path=f's3://{destination_bucket}/product_margin.parquet',
                dataset=True,
                mode='overwrite'
            )

        # Análise de Categorias de Produtos
        category_performance = fact_orders.groupby('product_category_name').agg(
            category_sales_volume=('order_item_id', 'count'),
            category_conversion_rate=('order_status', lambda x: (x == 'delivered').sum() / x.count())
        ).reset_index()

        wr.s3.to_parquet(
            df=category_performance,
            path=f's3://{destination_bucket}/category_performance.parquet',
            dataset=True,
            mode='overwrite'
        )

        # Análise Geográfica
        regional_sales = fact_orders.groupby(['customer_city', 'product_id']).agg(
            regional_sales_volume=('order_item_id', 'count'),
            regional_sales_value=('total_order_value', 'sum')
        ).reset_index()

        wr.s3.to_parquet(
            df=regional_sales,
            path=f's3://{destination_bucket}/regional_sales.parquet',
            dataset=True,
            mode='overwrite'
        )

        # Análise de Padrões de Pagamento
        payment_analysis = fact_orders.groupby('payment_type').agg(
            payment_frequency=('payment_type', 'count'),
            average_payment_value=('payment_value', 'mean')
        ).reset_index()

        wr.s3.to_parquet(
            df=payment_analysis,
            path=f's3://{destination_bucket}/payment_analysis.parquet',
            dataset=True,
            mode='overwrite'
        )

        # Análise de Potenciais Fraudes
        fraud_analysis = fact_orders.groupby('product_category_name').agg(
            total_order_value=('total_order_value', 'sum'),
            is_suspicious_order=('payment_value', lambda x: (x > fact_orders['payment_value'].mean() * 3).any())  # Exemplo de transações suspeitas
        ).reset_index()

        wr.s3.to_parquet(
            df=fraud_analysis,
            path=f's3://{destination_bucket}/fraud_analysis.parquet',
            dataset=True,
            mode='overwrite'
        )

        # Análise de Tempo de Entrega
        delivery_analysis = fact_orders.groupby(['customer_city', 'product_id']).agg(
            average_delivery_time=('delivery_time_days', 'mean'),
            delivery_delay_rate=('delivery_time_days', lambda x: (x > 5).sum() / x.count() if x.count() > 0 else 0)  # Evitar divisão por zero
        ).reset_index()

        wr.s3.to_parquet(
            df=delivery_analysis,
            path=f's3://{destination_bucket}/delivery_analysis.parquet',
            dataset=True,
            mode='overwrite'
        )

        print("Todos os Parquets foram criados e salvos no bucket REF.")

    except Exception as e:
        print(f"Erro ao processar a camada REF: {e}")
