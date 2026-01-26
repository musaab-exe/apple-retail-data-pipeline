from pyspark.sql.functions import broadcast
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql import functions as F


def null_check(df):

    aggs = [
        F.count(F.when(F.col(c).isNull(),1)).alias(c)
        for c in df.columns
    ]

    null_count = df.agg(*aggs).collect()[0].asDict()

    return null_count

def dup_check(df):
    total = df.count()
    distinct = df.distinct().count()

    return total - distinct

def dup_PK_check(df,pk_col):
    total = df.count()
    distinct_id = df.select(pk_col).distinct().count()

    return total - distinct_id

def dup_FK_check(
    child_df,
    child_col,
    parent_df,
    parent_col,
    drop=False,
    fail=False
): 

    invalid_df = child_df.join(
        broadcast(parent_df), 
        child_df[child_col]==parent_df[parent_col], 
        'left_anti'
    )
    invalid_count = invalid_df.count()

    if invalid_count > 0:
        msg = f'{invalid_count} invalid FK values found in [{child_col}]'

        if fail:
            raise ValueError(msg)

        if drop:
            valid_df = child_df.join(
                parent_df,
                child_df[child_col] == parent_df[parent_col],
                'left_semi'
            )
            return valid_df, invalid_count

    return child_df, invalid_count

# def assert_column_type(df, col, expected_type):
#     if col not in df.columns:
#         raise ValueError(f'Column {col}" does not exist')

#     actual_type = df.schema[col].dataType

#     if not isinstance(actual_type, expected_type):
#         raise TypeError(
#             f'Column {col} has type {actual_type}, '
#             f'expected {expected_type}'
#         )

def profile_table(df, table_name, pk_col=None,cfk_cols=[],p_dfs=[],pfk_col=[]):

    nulls = null_check(df)
    dupes = dup_check(df)
    fks = {}
    if cfk_cols and p_dfs and pfk_col:
        for i in range(len(cfk_cols)):
            fks[cfk_cols[i]] = dup_FK_check(df,cfk_cols[i],p_dfs[i],pfk_col[i],drop=False)[1]

    if pk_col:
        pk_dupes = dup_PK_check(df, pk_col)

    return {
        'table': table_name,
        'nulls': nulls,
        'duplicate_rows': dupes,
        'duplicate_pks': pk_dupes if pk_col else None,
        'invalid_fk' : fks if cfk_cols!=[]  else None
    }

def profile_table_reader(profiles):
    for p in profiles:
        null_cols = {k: v for k, v in p['nulls'].items() if v > 0}
        if p['invalid_fk']!=None:
            invalid_fk = {k: v for k, v in p['invalid_fk'].items() if v > 0}
        else:
            invalid_fk = {}
        print(f'\nData Quality Report — {p["table"].upper()}')
        print('-' * 40)

        if p['duplicate_rows'] > 0 or \
        (p['duplicate_pks'] is not None\
         and p['duplicate_pks'] > 0) or \
        null_cols:
            if p['duplicate_rows'] > 0:
                print(f'WARNING: {p["duplicate_rows"]} duplicate rows detected')

            if p['duplicate_pks'] is not None and p['duplicate_pks'] > 0:
                print(f'WARNING: {p["duplicate_pks"]} duplicate primary keys detected')

            # Only show columns with actual nulls/invalid fks
            if null_cols:
                print('WARNING: Null values detected in columns:')
                for col, cnt in null_cols.items():
                    print(f'  - {col}: {cnt}')
            if invalid_fk:
                print('WARNING: Invalid FKs detected:')
                for col, cnt in invalid_fk.items():
                    print(f'  - {col}: {cnt}')
        else:
            print('                ALL CLEAR               ')
        print('-' * 40)
