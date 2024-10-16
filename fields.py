class Field:
    def __init__(self, field_type):
        self.field_type = field_type

class INTEGER(Field):
    def __init__(self):
        super(INTEGER, self).__init__("INTEGER")

class FLOAT(Field):
    def __init__(self):
        super(FLOAT, self).__init__("REAL")

class TEXT(Field):
    def __init__(self):
        super(TEXT, self).__init__("TEXT")