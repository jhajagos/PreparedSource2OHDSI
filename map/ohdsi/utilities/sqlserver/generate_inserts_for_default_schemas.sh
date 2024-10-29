
# transfer_sql_inserts_54.sql (mssql; full reload)
python .\write_sql_server_insert_statements.py

# transfer_sql_inserts_54_exclude_concepts.sql (mssql; exclude loading concept tables)
python .\write_sql_server_insert_statements.py -o transfer_sql_inserts_54_exclude_concepts.sql -x

# transfer_psql_inserts_54.sql (psql; full reload)
python .\write_sql_server_insert_statements.py -d psql -s ohdsi -o transfer_psql_inserts_54.sql

# transfer_psql_inserts_54_exclude_concepts.sql (psql; exclude loading concept tables)
python .\write_sql_server_insert_statements.py -d psql -s ohdsi -o transfer_psql_inserts_54_exclude_concepts.sql -x