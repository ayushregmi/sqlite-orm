import yaml
from fields import Field
import sqlite3

def TableName(name=None):
    """
    Decorator to set the table name for a model class.

    Args:
        name (str): Optional name for the table. If not provided, 
                     the class name will be used as the table name.

    Returns:
        function: A decorator that sets the table name on the class.
    """

    def decorator(cls):
        if name is not None:
            cls._table_name = name
        else:
            cls._table_name = cls.__name__
        return cls
    return decorator

def PrimaryKey(field):
    """
    Decorator to mark a field as the primary key.

    Args:
        field (Field): The field to be marked as the primary key.

    Returns:
        Field: The field with the primary key attribute set to True.
    """

    field.primary_key = True
    return field

class BaseModel:
    """
    Base class for defining models.

    This class provides methods for table creation, data manipulation, 
    and querying within a SQLite database.

    Attributes:
        _primary_key (str): The name of the primary key field in the model.
        _fields (dict): A dictionary of field names and their Field instances.
        _table_name (str): The name of the database table.

    Methods:
        schema(): Returns a dictionary representation of the model schema.
        get_database_name(): Retrieves the database name from a configuration file.
        execute(query): Executes a given SQL query on the database.
        create_table(): Creates the table in the database based on the model definition.
        insert(**kwargs): Inserts a new record into the table.
        delete(condition): Deletes records matching a specified condition.
        update(condition, **kwargs): Updates records matching a specified condition.
        get(condition): Retrieves records matching a specified condition.
        get_all(): Fetches all records from the table.
        get_by_column(column, condition): Fetches records filtered by a specific column.
        run_query(query): Executes a raw SQL query and returns the results.
    """

    _primary_key = None

    def __init_subclass__(cls):
        """
        Initializes a subclass of BaseModel by gathering field definitions 
        and determining the primary key.

        This method automatically collects all Field instances defined in 
        the subclass and sets the _table_name and _primary_key attributes.
        """

        cls._fields = {key: value for key, value in cls.__dict__.items() if isinstance(value, Field)}
        cls._table_name = cls.__name__
        cls._primary_key = next((key for key, value in cls._fields.items() if getattr(value, 'primary_key', False)), None)

    @classmethod
    def _to_dict(cls, *args):
        """
        Converts a tuple of values into a dictionary using the model's field names.

        Args:
            *args: A tuple of values corresponding to the model's fields.

        Returns:
            dict: A dictionary mapping field names to their respective values.
        """

        return {k:v for k, v in zip(cls._fields.keys(), args[0])}
    
    @classmethod
    def schema(cls):
        """
        Returns a dictionary representation of the model schema, including 
        the table name, primary key, and field types.

        Returns:
            dict: The schema of the model.
        """

        return {"table name":cls._table_name, "primary key":cls._primary_key,"fields":{key: value.field_type for key, value in cls._fields.items()}}
    
    @classmethod
    def get_database_name(cls):
        """
        Retrieves the database name from a YAML configuration file.

        Returns:
            str: The name of the SQLite database.
        """

        with open("config.yml", "r") as stream:
            config = yaml.safe_load(stream)
            stream.close()
        return config['database']

    @classmethod
    def execute(cls, query):
        """
        Executes a given SQL query on the SQLite database.

        Args:
            query (str): The SQL query to be executed.
        """

        database = cls.get_database_name()

        con = sqlite3.connect(database)
        cursor = con.cursor()

        try:
            cursor.execute(query)
            con.commit()
        finally:
            cursor.close()
            con.close()
    
    @classmethod
    def create_table(cls):
        """
        Creates the table in the SQLite database based on the model's fields 
        and primary key definition.
        """

        columns = ", ".join([f"{key} {value.field_type} {"PRIMARY KEY" if key==cls._primary_key else ""}" for key, value in cls._fields.items()])
        query = f"""CREATE TABLE {cls._table_name} (
            {columns}
        )
        """
        cls.execute(query)


    @classmethod
    def insert(cls, **kwargs):
        """
        Inserts a new record into the database table.

        Args:
            **kwargs: Field names and their values to be inserted.

        Raises:
            ValueError: If the provided fields do not match the model's fields.
        """

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
    def delete(cls, condition):
        """
        Deletes records from the table that match the specified condition.

        Args:
            condition (function): A function that takes a record and returns 
                                  True if the record should be deleted.
        """

        records = cls.get(condition)

        for record in records:
            columns = [f"{column}=='{record[column]}'"for column in cls._fields.keys()]
            columns = " AND ".join(columns)
            query = f"""DELETE FROM {cls._table_name} WHERE {columns}"""
            cls.execute(query)

    @classmethod
    def update(cls, condition, **kwargs):
        """
        Updates records in the table that match the specified condition.

        Args:
            condition (function): A function that takes a record and returns 
                                  True if the record should be updated.
            **kwargs: Field names and their new values to be updated.
        """

        records = cls.get(condition)

        for record in records:
            columns = [f"{column}=='{record[column]}'"for column in cls._fields.keys()]
            columns = " AND ".join(columns)
            set_values = [f"{column}='{kwargs[column] if column in kwargs.keys() else record[column]}'" for column in cls._fields.keys()]
            set_values = ", ".join(set_values)

            query = f"""UPDATE {cls._table_name} SET {set_values} WHERE {columns}"""
            cls.execute(query)
            # print(query)

    @classmethod
    def get(cls, condition):
        """
        Retrieves records that match the specified condition.

        Args:
            condition (function): A function that takes a record and returns 
                                  True if the record should be included.

        Returns:
            list: A list of records matching the condition.
        """

        records = cls.get_all()

        return [record for record in records if condition(record)]
        
    @classmethod
    def get_all(cls):
        """
        Retrieves all records from the table.

        Returns:
            list: A list of all records in the table.
        """

        database = cls.get_database_name()

        con = sqlite3.connect(database)
        cursor = con.cursor()
        try:
            cursor.execute(f"SELECT * FROM {cls._table_name}")
            records = cursor.fetchall()
        finally:
            cursor.close()
            con.close()
            

        return [cls._to_dict(record) for record in records]
    
    @classmethod
    def get_by_column(cls, column, condition):
        """
        Fetches records filtered by a specific column and condition.

        Args:
            column (str): The name of the column to filter by.
            condition (str): The condition to apply in the SQL WHERE clause.

        Returns:
            list: A list of records matching the specified column and condition.

        Raises:
            ValueError: If the specified column does not exist.
        """
        
        if column in cls._fields.keys():

            query = f"""SELECT * FROM {cls._table_name} WHERE {column} {condition}"""         
            database = cls.get_database_name()

            con = sqlite3.connect(database)
            cursor = con.cursor()
            try:
                cursor.execute(query)
                records = cursor.fetchall()
            finally:
                cursor.close()
                con.close()

            return [cls._to_dict(record) for record in records]
    
        else:
            raise ValueError(f"Column with name '{column}' does not exist")

    @classmethod
    def run_query(cls, query):
        """
        Executes a raw SQL query and returns the results.

        Args:
            query (str): The SQL query to execute.

        Returns:
            list: A list of records resulting from the query.
        """

        database = cls.get_database_name()

        con = sqlite3.connect(database)
        cursor = con.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        con.commit()
        con.close()
        return [cls._to_dict(record) for record in records]
    
    def __str__(cls):
        """
        Returns a string representation of the model's schema.

        Returns:
            str: A string representation of the model's schema.
        """

        return str(cls.schema())
