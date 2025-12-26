import sqlite3
import pandas as pd
import logging

from ingestion_db import ingest_db

logging.basicConfig(
    filename='logs/get_sales_summary.log',
    level=logging.DEBUG, 
    format='%(asctime)s %(levelname)s:%(message)s',
    filemode='a'
)

def create_sales_summary(conn):
  '''This func will merge the diff tables to get sales summary and add new columns in the resultant data'''
  sales_summary = pd.read_sql_query("""
  WITH agg_orders AS (
      SELECT 
          "Customer ID",
          "Product ID",
          SUM(Sales) AS total_sales,
          SUM(Profit) AS total_profit,
          COUNT("Order ID") AS total_orders
      FROM orders
      GROUP BY "Customer ID", "Product ID"
  )
  SELECT 
      ao."Customer ID",
      c."Customer Name",
      ao."Product ID",
      p."Product Name",
      p."Category",
      p."Sub-Category",
      ao.total_sales,
      ao.total_profit,
      ao.total_orders
  FROM agg_orders ao
  JOIN customers c ON ao."Customer ID" = c."Customer ID"
  JOIN products p ON ao."Product ID" = p."Product ID"
  ORDER BY ao.total_sales DESC;
  """, conn)

  return sales_summary

def clean_data(df):
    '''This will clean the data and create new columns for analysis'''
    df.fillna(0, inplace=True)

    threshold = df['total_sales'].quantile(0.9)
    df['high_value_customer'] = df['total_sales'] > threshold
    df['profit_margin'] = df['total_profit'] / df['total_sales'].replace(0,1)
    df['avg_order_value'] = df['total_sales'] / df['total_orders'].replace(0,1)
    df['product_rank_per_customer'] = df.groupby('Customer ID')['total_sales'].rank(method='dense', ascending=False)
    df['sales_to_profit_ratio'] = df['total_sales'] / df['total_profit'].replace(0,1)

    # Category-level aggregation
    category_agg = df.groupby(['Category', 'Sub-Category']).agg({'total_sales':'sum', 'total_profit':'sum'}).reset_index()
    category_agg['category_sales_pct'] = category_agg['total_sales'] / category_agg['total_sales'].sum()
    category_agg['category_profit_ratio'] = category_agg['total_profit'] / category_agg['total_sales']

    df = df.merge(category_agg[['Category','Sub-Category','category_sales_pct','category_profit_ratio']],
                  on=['Category','Sub-Category'], how='left')

    return df

if __name__ == '__main__':
    conn = sqlite3.connect('/content/sales.db')

    logging.info('Creating Summary Table....')
    sales_summary_df = create_sales_summary(conn)
    logging.info(sales_summary_df.head())

    logging.info('Cleaning Data....')
    sales_summary_clean = clean_data(sales_summary_df)
    logging.info(sales_summary_clean.head())

    logging.info('Ingesting Data....')
    ingest_db(sales_summary_clean, 'sales_summary', conn)
    logging.info('Completed')