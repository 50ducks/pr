#transform

import unicodedata

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, to_date, regexp_replace, when, trim
from pyspark.sql.types import StringType

from pk_dict import pk_dict

_date_formats = ["yyyy-MM-dd", "dd-MM-yyyy", "dd.MM.yyyy"]
_invalid_chars_pattern = r'[^а-яА-ЯёЁa-zA-Z0-9\s]'

def to_date_format(_df: DataFrame):
    output_df = _df
    for column in _df.columns:
        if "_DATE" in column:
            output_df = _df.withColumn(column,
                coalesce(
                    *[to_date(col(column), fmt) for fmt in _date_formats]
                )
            )
    return output_df

def deduplication(_df: DataFrame, _table_name: str):
    primary_keys = pk_dict[_table_name]
    if primary_keys != "none":
        output_df = _df.dropDuplicates(primary_keys)
        return output_df
    else:
        return _df

def clean_dataframe(_df: DataFrame):
    string_columns = [field.name for field in _df.schema.fields if field.dataType.simpleString() == 'string']

    for col_name in string_columns:
        _df = _df.withColumn(col_name, regexp_replace(col(col_name), _invalid_chars_pattern, ''))
        _df = _df.withColumn(col_name, trim(col(col_name)))
        _df = _df.withColumn(col_name, when(col(col_name) == '', None).otherwise(col(col_name)))
    output_df = _df
    return output_df