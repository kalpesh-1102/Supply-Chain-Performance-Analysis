# Python script with comprehensive logging and ingestion capabilities
import pandas as pd 
import logging
import os 
import numpy as np 
from sqlalchemy import create_engine

# Ensure log directory exists
os.makedirs("logs", exist_ok=True)

# Logging configuration
logging.basicConfig(
    filename="logs/get_vendor_summary.logs",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(connection):
    '''This function will merge different tables to get the overall vendor summary and add new columns in the resultant data'''
    vendor_sales_summary = pd.read_sql_query("""
        WITH FreightSummary AS (
            SELECT
                VendorNumber, 
                SUM(Freight) AS FreightCost 
            FROM vendor_invoice
            GROUP BY VendorNumber
        ), 
        PurchaseSummary AS (
            SELECT
                p.VendorNumber,
                p.VendorName,
                p.Brand, 
                p.Description, 
                p.PurchasePrice, 
                pp.Volume, 
                pp.Price AS ActualPrice,
                SUM(p.Quantity) AS TotalPurchaseQuantity,
                SUM(p.Dollars) AS TotalPurchaseDollars
            FROM purchases p
            JOIN purchase_prices pp
                ON p.Brand = pp.Brand
            WHERE p.PurchasePrice > 0
            GROUP BY 
                p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
        ),
        SalesSummary AS (
            SELECT 
                VendorNo, 
                Brand, 
                SUM(SalesDollars) AS TotalSalesDollars,
                SUM(SalesPrice) AS TotalSalesPrice,
                SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(ExciseTax) AS TotalExciseTax
            FROM sales
            GROUP BY VendorNo, Brand
        )
        SELECT 
            ps.VendorNumber,
            ps.VendorName, 
            ps.Brand,
            ps.Description, 
            ps.PurchasePrice, 
            ps.ActualPrice,
            ps.Volume, 
            ps.TotalPurchaseQuantity, 
            ps.TotalPurchaseDollars,
            ss.TotalSalesQuantity,
            ss.TotalSalesDollars,
            ss.TotalSalesPrice,
            ss.TotalExciseTax,
            fs.FreightCost
        FROM PurchaseSummary ps 
        LEFT JOIN SalesSummary ss
            ON ps.VendorNumber = ss.VendorNo
            AND ps.Brand = ss.Brand
        LEFT JOIN FreightSummary fs
            ON ps.VendorNumber = fs.VendorNumber
        ORDER BY ps.TotalPurchaseDollars DESC
        """, connection)

    return vendor_sales_summary

def clean_data(df):
    '''This function will clean the data and create new analytical columns'''
    # changing datatype to float
    df['Volume'] = df['Volume'].astype('float')
    
    # filling missing values with 0
    df.fillna(0, inplace=True)
    
    # removing spaces from categorical columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    
    # creating new columns for better analysis
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalesToPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']
    
    # Handle division by zero / infinity
    df['ProfitMargin'] = df['ProfitMargin'].replace([np.inf, -np.inf], 0)
    df['StockTurnover'] = df['StockTurnover'].replace([np.inf, -np.inf], 0)
    df['SalesToPurchaseRatio'] = df['SalesToPurchaseRatio'].replace([np.inf, -np.inf], 0)
    
    return df

if __name__ == '__main__':
    try:
        connection = create_engine("mysql+pymysql://root:password@localhost:3306/database")

        logging.info('Creating Vendor Summary Table......')
        summary_df = create_vendor_summary(connection)
        logging.info(summary_df.head().to_string())

        logging.info('Cleaning Data......')
        clean_df = clean_data(summary_df)
        logging.info(clean_df.head().to_string())

        logging.info('Ingesting data into MySQL......')
        clean_df.to_sql('vendor_sales_summary', connection, if_exists='replace', index=False)
        logging.info('Completed Successfully')

    except Exception as e:
        logging.exception(f"Error occurred: {e}")
