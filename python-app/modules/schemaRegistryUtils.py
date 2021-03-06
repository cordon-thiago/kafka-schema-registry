# References:
## API: https://docs.confluent.io/platform/current/schema-registry/develop/using.html#starting-sr
## To get the subjects: http://localhost:8081/subjects
## To get the schema versions of a subject: http://localhost:8081/subjects/user_register-value/versions
## To get the schema a specific version: http://localhost:8081/subjects/user_register-value/versions/1
## To get the compatibility of a subject: http://localhost:8081/config/user_register-value/?defaultToGlobal=true

import requests
import json

class SchemaRegistryUtils:

    def __init__(self, schema_registry_url):
        self.schema_registry_url = schema_registry_url

    def get_subjects(self):
        get_url = "{schema_registry_url}/subjects".format(schema_registry_url = self.schema_registry_url)
        r = requests.get(get_url)
        try:
            return r.json()
        except:
            print("Error: ", r.text)
            return {"error_message": r.text}

    def get_schema_versions(self, subject):
        get_url = "{schema_registry_url}/subjects/{subject}/versions".format(
            schema_registry_url = self.schema_registry_url,
            subject = subject
        )
        r = requests.get(get_url)
        try:
            return r.json()
        except:
            print("Error: ", r.text)
            return {"error_message": r.text}

    def get_schema(self, subject, version="latest"):
        get_url = "{schema_registry_url}/subjects/{subject}/versions/{version}".format(
            schema_registry_url = self.schema_registry_url, 
            subject = subject,
            version = version
        )
        r = requests.get(get_url)
        try:
            return r.json()
        except:
            print("Error: ", r.text)
            return {"error_message": r.text}

    def get_subject_compatibility(self, subject):
        get_url = "{schema_registry_url}/config/{subject}/?defaultToGlobal=true".format(
            schema_registry_url = self.schema_registry_url, 
            subject = subject
        )
        r = requests.get(get_url)
        try:
            return r.json()
        except:
            print("Error: ", r.text)
            return {"error_message": r.text}

    def set_schema(self, subject, schema):
        post_url = "{schema_registry_url}/subjects/{subject}/versions".format(
            schema_registry_url = self.schema_registry_url, 
            subject = subject
        )
        headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        r = requests.post(post_url, data = json.dumps(schema), headers = headers)
        try:
            return r.json()
        except:
            print("Error: ", r.text)
            return {"error_message": r.text}

    def test_schema_compatibility(self, subject, schema, version="latest"):
        post_url = "{schema_registry_url}/compatibility/subjects/{subject}/versions/{version}".format(
            schema_registry_url = self.schema_registry_url, 
            subject = subject,
            version = version
        )
        headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        r = requests.post(post_url, data = json.dumps(schema), headers = headers)
        try:
            return r.json()
        except:
            print("Error: ", r.text)
            return {"error_message": r.text}

    def set_subject_compatibility(self, subject, compatibility_level):
        post_url = "{schema_registry_url}/config/{subject}".format(
            schema_registry_url = self.schema_registry_url, 
            subject = subject
        )
        headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
        data = {"compatibility": compatibility_level.upper()}
        r = requests.put(post_url, data = json.dumps(data), headers = headers)
        try:
            return r.json()
        except:
            print("Error: ", r.text)
            return {"error_message": r.text}

    def delete_schema_version(self, subject, version="latest"):
        url = "{schema_registry_url}/subjects/{subject}/versions/{version}".format(
            schema_registry_url = self.schema_registry_url, 
            subject = subject,
            version = version
        )
        r = requests.delete(url)
        try:
            return r.json()
        except:
            print("Error: ", r.text)
            return {"error_message": r.text}

    def delete_schema_all(self, subject):
        url = "{schema_registry_url}/subjects/{subject}".format(
                    schema_registry_url = self.schema_registry_url, 
                    subject = subject
                )
        r = requests.delete(url)
        try:
            return r.json()
        except:
            print("Error: ", r.text)
            return {"error_message": r.text}

    def is_schema_compatible(self, schema_base, schema_to_check, subject, compatibility_level, delete_previous_schemas=True):

        is_compatible = False

        if subject in self.get_subjects() and delete_previous_schemas:
            # Delete all subject schemas
            self.delete_schema_all(subject)
            #print("Schema versions deleted from '{subject}' subject: {deleted_schema_versions}".format(subject=subject, deleted_schema_versions=SchemaRegistryUtils.delete_schema_all(schema_registry_url, subject)))
        
        # Register schema base
        schema_base_reg = self.set_schema(subject, schema_base)
        if "error_code" in schema_base_reg.keys() or "error_message" in schema_base_reg.keys():
            print("Error registering the 'schema_base': {}".format(schema_base_reg))

        # Set compatibility level
        self.set_subject_compatibility(subject, compatibility_level)

        # Register schema to ckeck
        schema_check_reg = self.set_schema(subject, schema_to_check)
        if "error_message" in schema_check_reg.keys():
            print("Error registering the 'schema_to_check': {}".format(schema_check_reg))
        elif "error_code" not in schema_check_reg.keys():
            is_compatible = True

        return is_compatible

    def check_schema_compatibility(self, schemas_to_check: dict, subject, compatibility_level):
        
        result = []

        # Generate a list with dict keys (names of the schemas)
        schema_lst = [schema for schema in schemas_to_check.keys()]

        # Assume the first dict item as base
        schema_base = schema_lst[0]

        if "TRANSITIVE" in compatibility_level.upper():

            if subject in self.get_subjects():
                # Delete all subject schemas
                self.delete_schema_all(subject)
            
            # Loop through dict items checking base against the next item
            for i in range(0, len(schema_lst) - 1):
                is_compatible = self.is_schema_compatible(schemas_to_check[schema_base], schemas_to_check[schema_lst[i+1]], subject, compatibility_level, False)
                result.append(
                    {
                        "schema_base": schema_base,
                        "schema_to_check": schema_lst[i+1],
                        "compatibility_level": compatibility_level,
                        "is_compatible": is_compatible
                    }
                )
                
                # If schema is not compatible, maintain the same base to compare with next item in dict
                if not is_compatible:
                    schema_base = schema_base
                else:
                    schema_base = schema_lst[i+1]
        else:
            # Loop through dict items checking base against the next item
            for i in range(0, len(schema_lst) - 1):
                is_compatible = self.is_schema_compatible(schemas_to_check[schema_base], schemas_to_check[schema_lst[i+1]], subject, compatibility_level)
                result.append(
                    {
                        "schema_base": schema_base,
                        "schema_to_check": schema_lst[i+1],
                        "compatibility_level": compatibility_level,
                        "is_compatible": is_compatible
                    }
                )

        return result
