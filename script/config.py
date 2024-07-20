#config

import os

directory_path = os.path.abspath(os.path.join('..', 'src'))

url = "jdbc:postgresql://localhost:5432/project"

db_schema = "ds."

properties = {
    "user": "postgres",
    "password": "Zzikbt26",
    "driver": "org.postgresql.Driver"
}

logs_table = "logs.loading_logs"