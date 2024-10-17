class Field:
    """
    Base class for defining fields.

    Attributes:
        field_type (str): The type of the field.
    """

    def __init__(self, field_type):
        self.field_type = field_type

class INTEGER(Field):
    """
    Represents an INTEGER field.

    Inherits from the Field class and sets the field type to "INTEGER".
    """

    def __init__(self):
        super(INTEGER, self).__init__("INTEGER")

class FLOAT(Field):
    """
    Represents a FLOAT field.

    Inherits from the Field class and sets the field type to "REAL".
    """

    def __init__(self):
        super(FLOAT, self).__init__("REAL")

class TEXT(Field):
    """
    Represents a TEXT field.

    Inherits from the Field class and sets the field type to "TEXT".
    """

    def __init__(self):
        super(TEXT, self).__init__("TEXT")