from mimesis import Person, Address, Datetime
import datetime
import copy
import random

class DataGenerator:

    def __init__(self):
        self.__avro_schema_base = {
                "namespace": "events.test_compatibility",
                "name": "value",
                "type": "record",
                "fields": [
            ]
        }

        self.__person = Person('en')

    def avro_schema1(self):
        """
        Creates schema and populate it with data.

        Return: dict
            {
                "schema": avro schema,
                "value": avro schema with data
            }
        """

        # Define fields
        fields = [
            {
                "name": "identifier",
                "type": "string"
            },
            {
                "name": "first_name",
                "type": "string"
            }
        ]

        # Make a copy of the schema base
        schema = copy.deepcopy(self.__avro_schema_base)
        
        # Add documentation
        schema["doc"] = self.avro_schema1.__name__

        # Add fields to the schema base
        for field in fields:
            schema["fields"].append(field)

        # Generate values
        value = {
            "identifier": self.__person.identifier(),
            "first_name": self.__person.first_name()
        }

        return {"schema": schema, "value": value}

    def avro_schema2(self):
        """
        Creates schema and populate it with data.

        Return: dict
            {
                "schema": avro schema,
                "value": avro schema with data
            }
        """

        # Define fileds
        fields = [
            {
                "name": "identifier",
                "type": "string"
            },
            {
                "name": "first_name",
                "type": "string"
            },
            {
                "name": "date",
                "type": "string",
                "default": "2000-01-01"
            }
        ]

        # Make a copy of the schema base
        schema = copy.deepcopy(self.__avro_schema_base)

        # Add documentation
        schema["doc"] = self.avro_schema2.__name__

        # Add fields to the schema base
        for field in fields:
            schema["fields"].append(field)

        # Generate values
        value = {
            "identifier": self.__person.identifier(),
            "first_name": self.__person.first_name()
        }

        return {"schema": schema, "value": value}

    def avro_schema3(self):
        """
        Creates schema and populate it with data.

        Return: dict
            {
                "schema": avro schema,
                "value": avro schema with data
            }
        """

        # Define fileds
        fields = [
            {
                "name": "identifier",
                "type": "string"
            },
            {
                "name": "first_name",
                "type": "string"
            },
            {
                "name": "date",
                "type": "string"
            }
        ]

        # Make a copy of the schema base
        schema = copy.deepcopy(self.__avro_schema_base)

        # Add documentation
        schema["doc"] = self.avro_schema3.__name__

        # Add fields to the schema base
        for field in fields:
            schema["fields"].append(field)

        # Generate values
        datetime = Datetime()
        
        value = {
            "identifier": self.__person.identifier(),
            "first_name": self.__person.first_name(),
            "date": datetime.formatted_date(fmt="%Y-%m-%d")
        }

        return {"schema": schema, "value": value}

    def avro_schema4(self):
        """
        Creates schema and populate it with data.

        Return: dict
            {
                "schema": avro schema,
                "value": avro schema with data
            }
        """

        # Define fileds
        fields = [
            {
                "name": "identifier",
                "type": "int"
            },
            {
                "name": "first_name",
                "type": "string"
            }
        ]

        # Make a copy of the schema base
        schema = copy.deepcopy(self.__avro_schema_base)

        # Add documentation
        schema["doc"] = self.avro_schema4.__name__

        # Add fields to the schema base
        for field in fields:
            schema["fields"].append(field)

        # Generate values
        value = {
            "identifier": random.randint(1000, 1000000),
            "first_name": self.__person.first_name()
        }

        return {"schema": schema, "value": value}

    def avro_schema5(self):
        """
        Creates schema and populate it with data.

        Return: dict
            {
                "schema": avro schema,
                "value": avro schema with data
            }
        """

        # Define fileds
        fields = [
            {
                "name": "identifier_new",
                "type": "string"
            },
            {
                "name": "first_name",
                "type": "string"
            }
        ]

        # Make a copy of the schema base
        schema = copy.deepcopy(self.__avro_schema_base)

        # Add documentation
        schema["doc"] = self.avro_schema5.__name__

        # Add fields to the schema base
        for field in fields:
            schema["fields"].append(field)

        # Generate values
        value = {
            "identifier": self.__person.identifier(),
            "first_name": self.__person.first_name()
        }

        return {"schema": schema, "value": value}

    def avro_schema6(self):
        """
        Creates schema and populate it with data.

        Return: dict
            {
                "schema": avro schema,
                "value": avro schema with data
            }
        """

        # Define fileds
        fields = [
            {
                "name": "identifier_new",
                "type": "string",
                "aliases": ["identifier"]
            }
        ]

        # Make a copy of the schema base
        schema = copy.deepcopy(self.__avro_schema_base)

        # Add documentation
        schema["doc"] = self.avro_schema6.__name__

        # Add fields to the schema base
        for field in fields:
            schema["fields"].append(field)

        # Generate values
        value = {
            "identifier_new": self.__person.identifier()
        }

        return {"schema": schema, "value": value}

    def avro_schema(self, schema_name):
        """
        Receives a function name and return a function callable.
        
        Input: str with the name of the function.

        Return: function callable.
        """

        func_name = schema_name
        # if the func_name is an attribute of the class and it's callable, run the function
        if hasattr(self, func_name) and callable(getattr(self, func_name)):
            func = getattr(self, func_name)
            return func()
        else:
            print("'{}' is not a recognized attribute.".format(func_name))
