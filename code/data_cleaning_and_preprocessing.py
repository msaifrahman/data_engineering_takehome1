from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import re


def clean_data(df: DataFrame) -> DataFrame:
    
    # Remove duplicates
    df = df.dropDuplicates()
    
    numeric_cols = []
    
    # Trim string columns
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, trim(col(col_name)))
    
    return df


def preprocess_columns(df: DataFrame) -> DataFrame:
    
    """
    Cleans column names by:
    - Replacing spaces and special characters with underscore
    - Removing leading/trailing underscores
    - Avoiding duplicate column names
    """
    
    new_columns = []
    seen = set()
    
    for col in df.columns:
        # Replace special characters and spaces with underscore
        new_col = re.sub(r'[ ,;{}()\n\t=/-]+', '_', col)
        
        # Remove leading/trailing underscores
        new_col = new_col.strip('_')
        
        # Handle duplicate column names after cleaning
        original_new_col = new_col
        counter = 1
        while new_col in seen:
            new_col = f"{original_new_col}_{counter}"
            counter += 1
        
        seen.add(new_col)
        new_columns.append(new_col)
    
    # Apply new column names
    df_clean = df.toDF(*new_columns)
    
    return df_clean
