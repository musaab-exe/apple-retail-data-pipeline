from pyspark.sql.functions import trim,regexp_replace, col,lower
from pyspark.sql.functions import coalesce, expr
from pyspark.sql.functions import try_to_date
from pyspark.errors import DateTimeException
from pyspark.sql import functions as F


#e.g., cast IDs to string, dates to timestamp
def cast_columns(df, col_type_dict):	
    transformations = {}

    for c, d in col_type_dict.items():
        if d == 'date':
            transformations[c] = F.coalesce(
                F.try_to_date(F.col(c), 'yyyy-MM-dd'),
                F.try_to_date(F.col(c), 'dd-MM-yyyy')
            )
        elif d == 'string':
            transformations[c] = F.lower(F.trim(F.col(c).cast('string')))
        elif d == 'int':
            transformations[c] = F.expr(f'try_cast(try_cast({c} as double) as int)')
        else:
            transformations[c] = F.col(c).try_cast(d)

    return df.withColumns(transformations)

#Drop duplicates after validation
def deduplicate(df, subset_cols=None):
    if subset_cols:
        return df.dropDuplicates(subset=subset_cols)
    return df.distinct()


#Fill nulls or handle flagged rows
def fill_missing(df, col_value_dict):	
    return df.fillna(col_value_dict)

#E.g., quantity * price in sales fact
def add_total_price(df, q, p):	
    return df.withColumn('total_spent',q*p)

#For standardization
def rename_columns_lower(df):	
    return df.withColumnsRenamed({col: col.lower() for col in df.columns})

# Turns values like $ 6.0 or USD 7 to float or int
def alphaNum_to_num(df, column, dtype='float'): 
    schema_dict = dict(df.dtypes)
    
    if column in schema_dict and schema_dict[column] == 'string':
        return df.withColumn(
            column,
            regexp_replace(col(column), '[^0-9\\.]+', '').cast('float').cast(dtype)
        )

    return df
        

