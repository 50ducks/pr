#load

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from config import url, properties, db_schema
from pk_dict import pk_dict
from transform import clean_dataframe

import logs

def load_data (_spark: SparkSession, _df: DataFrame, _table_name: str, _conn):
    
    #table primary keys
    primary_keys = pk_dict[_table_name]

    if primary_keys != "none":
        #loading DataFrame from existing table
        existing_df = _spark.read.jdbc(url=url, table=_table_name, properties=properties)
        existing_df = clean_dataframe(existing_df)
        
        #TempView for .csv DataFrame and existing table Dataframe
        _df.createOrReplaceTempView("new_data")
        existing_df.createOrReplaceTempView("existing_data")
        
        #~~~ new rows writing start
        new_rows = _spark.sql(f"""
            SELECT * FROM new_data
            WHERE CONCAT_WS('_', {', '.join(primary_keys)}) NOT IN (
                SELECT CONCAT_WS('_', {', '.join(primary_keys)}) FROM existing_data
        )
        """)
        
    #log
        logs.set_total_new_rows(logs.get_total_new_rows() + new_rows.count())
        
        new_rows.write.jdbc(url=url, table=_table_name, mode="append", properties=properties)
        #~~~ new rows writing end
    
        #~~~ updated rows writing start
        temp_table_name = "util.rows_to_update"
        
        comparison_clause = " OR ".join([f"new_data.{col} IS DISTINCT FROM existing_data.{col}" for col in _df.columns])
        update_condition = " AND ".join([f"new_data.{pk} = existing_data.{pk}" for pk in primary_keys])
        updated_rows = _spark.sql(f"""
            SELECT new_data.* 
            FROM new_data
            JOIN existing_data 
            ON {update_condition}
            WHERE {comparison_clause}
        """)
        
        updated_rows.createOrReplaceTempView("updated_rows")
        updated_rows.write.jdbc(url=url, table=f"{temp_table_name}", mode="overwrite", properties=properties)
        
        set_clause = ", ".join([f'"{col}" = {temp_table_name}."{col}"' for col in _df.columns])
        update_query = f"""
            UPDATE {_table_name} AS t
            SET {set_clause}
            FROM {temp_table_name}
            WHERE {" AND ".join([f't."{pk}" = {temp_table_name}."{pk}"' for pk in primary_keys])};
            DROP TABLE IF EXISTS {temp_table_name};
        """

        cur = _conn.cursor()
        cur.execute(update_query)
        _conn.commit()
    #log
        logs.set_total_updated_rows(logs.get_total_updated_rows() + new_rows.count())
        
        cur.close()
        #~~~ updated rows writing end
        
    else:
        new_properties = properties
        new_properties["truncate"] = "true"
        _df.write.jdbc(url=url, table=_table_name, mode="overwrite", properties=new_properties)
        