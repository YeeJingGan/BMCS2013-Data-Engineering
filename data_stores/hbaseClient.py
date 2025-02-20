"""
Author: Thong Pei Ting, Yeap Jie Shen
"""

import happybase
import warnings

# PLEASE ADD DOCUMENTATION FOR EACH AND EVERY FUNCTION

class HBaseClient:
    def __init__(self, host = 'localhost', port = 9090):
        self.connection = happybase.Connection(host = host, port = port)

    def create_table(self, table_name, schema = {'cf1' : dict()}):
        if table_name not in self.connection.tables():
            self.connection.create_table(
                table_name,
                schema
            )
            print(f"Table '{table_name}' created.")
        else:
            warnings.warn(f"Table '{table_name}' already exists.")

    def get_tables(self):
        return self.connection.tables()

    def put_key(self, table_name, row_key, data):
        table = self.connection.table(table_name)
        
        try:
            table.put(row_key, data)
        except Exception as e:
            warnings.warn(f'Insertion of {row_key} with data {data} failed!!')
            warnings.warn(f'Error: {e}')
        
        print(f"Data inserted into row '{row_key}'.")

    def put_keys(self, table_name, rows : list(tuple())):
        table = self.connection.table(table_name)

        try:
            with table.batch() as b:
                for row in rows:
                    b.put(row[0], row[1])
        except Exception as e:
            warnings.warn(f'Most recent insertion of {row[0]} with data {row[1]} failed!!')
            warnings.warn(f'Error: {e}')
        
    def read_key(self, table_name, row_key, columns):
        table = self.connection.table(table_name)
            
        try:
            row = table.row(row_key, columns = columns)
        except Exception as e:
            warnings.warn(f'Retrieving row with key {row_key} failed')
        finally:
            return row
            
        # print(f"Data for row '{row_key}': {row}")

    def read_keys(self, table_name, row_keys, columns):
        table = self.connection.table(table_name)

        try:
            rows = table.rows(row_keys, columns = columns)
        except Exception as e:
            warnings.warn(f'Retrieving row with keys {row_keys} failed')
        finally:
            return rows

    def delete_row(self, table_name, row, columns = None):
        table = self.connection.table(table_name)

        try:
            table.delete(row, columns)
        except Exception as e:
            warnings.warn(f'Failed during deleting row {row}' + f'with columns {columns}' if columns else '')

    def delete_table(self, table_name, disable = False):
        try:
            self.connection.delete_table(name = table_name, disable = disable)
        except Exception as e:
            warnings.warn(f'Failed during deleting table {table_name}')