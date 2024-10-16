import sqlite3
import yaml

from fields import Field

def TableName(name=None):
    def decorator(cls):
        if name is not None:
            cls._table_name = name
        else:
            cls._table_name = cls.__name__
        return cls
    return decorator


class BaseModel:
    def __init_subclass__(cls):
        cls._fields = {key: value for key, value in cls.__dict__.items() if isinstance(value, Field)}
        cls._table_name = cls.__name__
    
    @classmethod
    def schema(cls):
        return {key: value.field_type for key, value in cls.fields.items()}

    @classmethod
    def table_name(cls):
        return cls._table_name
    
    @classmethod
    def get_database_name(cls):
        with open("config.yml", "r") as stream:
            config = yaml.safe_load(stream)
            stream.close()
        return config['database']

    @classmethod
    def execute(cls, query):

        database = cls.get_database_name()

        con = sqlite3.connect(database)
        cursor = con.cursor()
        cursor.execute(query)
        con.commit()
        cursor.close()
        con.close()
    
    @classmethod
    def create_table(cls):
        columns = ", ".join([f"{key} {value.field_type}" for key, value in cls._fields.items()])
        query = f"""CREATE TABLE {cls._table_name} (
            {columns}
        )
        """
        cls.execute(query)


    @classmethod
    def insert(cls, **kwargs):
        if kwargs.keys() == cls._fields.keys():
            query = f"""INSERT INTO {cls._table_name} ({', '.join(kwargs.keys())}) VALUES ({', '.join(['"' + str(x)+'"' for x in kwargs.values()])})"""
            cls.execute(query)

        elif len(kwargs.keys()) > len(cls._fields.keys()):
            extra_fields = set(kwargs.keys()) - set(cls._fields.keys())

            raise ValueError(f"extra fields: {extra_fields}")
        else:
            missing_fields = set(cls._fields.keys()) - set(kwargs.keys())
            raise ValueError(f"missing fields: {missing_fields}")
        
    @classmethod
    def get_all(cls):

        database = cls.get_database_name()

        con = sqlite3.connect(database)
        cursor = con.cursor()
        cursor.execute(f"SELECT * FROM {cls._table_name}")
        records = cursor.fetchall()
        cursor.close()
        con.close()

        return records
    
    @classmethod
    def get_by_column(cls, column, condition):
        
        if column in cls._fields.keys():

            query = f"""
                SELECT * FROM {cls._table_name} WHERE {column} {condition}
"""         
            database = cls.get_database_name()

            con = sqlite3.connect(database)
            cursor = con.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            cursor.close()
            con.close()

            return records
    
        else:
            raise ValueError(f"Column with name '{column}' does not exist")

    @classmethod
    def run_query(cls, query):

        database = cls.get_database_name()

        con = sqlite3.connect(database)
        cursor = con.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        con.commit()
        con.close()
        return records
    
    def __str__(cls):
        return str(cls.schema())
