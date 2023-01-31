import boto3
import botocore
import redshift_connector
import os
import random, string


def get_random_password():
    random_source = string.ascii_letters + string.digits
    password = random.choice(string.ascii_lowercase)
    password += random.choice(string.ascii_uppercase)
    password += random.choice(string.digits)
    
    for i in range(22):
        password += random.choice(random_source)

    password_list = list(password)
    password = ''.join(password_list)
    return password


class RedshiftUser:
    def __init__(self,username,conn):
        """username[STRING],
           password[STRING],
           conn[Database Connector]
        """
        self.username = username
        self.conn = conn

    def create(self, password):
        """password[STRING]"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"CREATE USER {self.username} with password '{password}'")
            cursor.close()
        except Exception as e:
            print(e)
            cursor.close()
        
    def remove(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"DROP USER IF EXISTS {self.username}")
            cursor.close()
        except Exception as e:
            print(e)
            cursor.close()
        
    def change_password(self, password):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"ALTER USER {self.username} PASSWORD '{password}'")
            cursor.close()
        except Exception as e:
            print(e)
            cursor.close()

    def change_query_priority(self, priority):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT change_user_priority({self.username}, {priority})")
            cursor.close()
        except Exception as e:
            print(e)
            cursor.close()


class RedshiftGroup:
    def __init__(self,group_name,conn):
        """group_name[STRING],
           conn[Database Connector]
        """
        self.group_name = group_name
        self.conn = conn

    def create(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"CREATE GROUP {self.group_name}")
            cursor.close()
        except Exception as e:
            print(e)
            cursor.close()
        
    
    def add_user(self,user_name):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"ALTER GROUP {self.group_name} ADD USER {user_name}")
            cursor.close()
        except Exception as e:
            print(e)
            cursor.close()

    def add_users(self,list_of_users):
        cursor = self.conn.cursor()
        string_of_users = ""
        for i in list_of_users:
            string_of_users += i + ", "
        try:
            cursor.execute(f"ALTER GROUP {self.group_name} ADD USER {string_of_users}")
            cursor.close()
        except Exception as e:
            print(e)
            cursor.close()

    def remove_user(self,user_name):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"ALTER GROUP {self.group_name} DROP USER {user_name}")
            cursor.close()
        except Exception as e:
            print(e)
            cursor.close()

    def remove_users(self,list_of_users):
        cursor = self.conn.cursor()
        string_of_users = ""
        for i in list_of_users:
            string_of_users += i + ","
        try:
            cursor.execute(f"ALTER GROUP {self.group_name} DROP USER {string_of_users[:-1]}")
            cursor.close()
        except Exception as e:
            print(e)
            cursor.close()

    def grant_schema(self,permission, schema):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"GRANT {permission} ON SCHEMA {schema} TO GROUP {self.group_name}")
            cursor.close()
        except Exception as e:
            print(e)
            cursor.close()
    
    def grant_select(self,permission, schema):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"GRANT {permission} ON ALL TABLES IN SCHEMA {schema} TO GROUP {self.group_name}")
            cursor.close()
        except Exception as e:
            print(e)
            cursor.close()

    def revoke(self,list_of_permissions, list_of_schemas):
        cursor = self.conn.cursor()
        string_of_permissions = ""
        string_of_schemas = ""
        for i in list_of_permissions:
            string_of_permissions += i + ","
        for i in list_of_schemas:
            string_of_schemas += i + ","
        try:
            cursor.execute(f"REVOKE {string_of_permissions[:-1]} ON ALL TABLES IN SCHEMA {string_of_schemas[:-1]} FROM GROUP {self.group_name}")
            cursor.close()
        except Exception as e:
            print(e)
            cursor.close()


class RedshiftSchema:
    def __init__(self,schema_name,conn):
        """schema_name[STRING],
           conn[Database Connector]
        """
        self.schema_name = schema_name
        self.conn = conn

    def create(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
            cursor.close()
        except Exception as e:
            print(e)
            cursor.close()

    def delete(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"DROP SCHEMA IF EXISTS {self.schema_name}")
            cursor.close()
        except Exception as e:
            print(e)
            cursor.close()

        
def lambda_handler(event,context):
    print("Inicio")
    secret_name = "/dev/redshift-creds/values"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    #get secret from secretId
    creds = client.get_secret_value(
            SecretId=secret_name
        )
    #get credentials(username and password)
    user = eval(creds["SecretString"])["cluster_master_username"]
    password = eval(creds["SecretString"])["cluster_master_password"]

    #connect to redshift database
    conn = redshift_connector.connect(
        host = os.environ["REDSHIFT_HOST"],
        database = os.environ["REDSHIFT_DATABASE"],
        user = user,
        password=password
    )
    conn.autocommit = True
    
    # Get users and groups json from ssm
    ssm = boto3.client('ssm')

    response = ssm.get_parameter(
        Name='/dev/redshift-user-groups',
        WithDecryption=True
    )

    resp = eval(response["Parameter"]["Value"])

    created_schemas = []
    created_users = []
    for elements in resp:
        group = elements["grupo"]
        RedshiftGroup(group, conn).create()
        #creating users
        for username in elements["usuarios"]:
            if username in created_users:
                pass
            else:
                password = get_random_password()
                RedshiftUser(username, conn).create(password)
                #Query priority to powerbi users
                if group =="powerbi":
                    priority = "high"
                    RedshiftUser(username, conn).change_query_priority(priority)

            # Registering new users credentials into secrets manager
                secret_user_name = f"/dev/redshift-creds/{username}"
                secret_user_password = password
                try:
                    creds = client.create_secret(
                        Name = secret_user_name,
                        SecretString = secret_user_password
                    )
            # If username already exists, credentials will be updated
                except botocore.exceptions.ClientError as error:
                    if error.response['Error']['Code'] == 'ResourceExistsException':                
                        print("Usuário já existe")
                        
                else:
                    raise error
                created_users.append(username)
            RedshiftGroup(group, conn).add_user(username)

        #Creating group with a list of users

        #Creating the schemas
        for schema in elements["schemas"]:
            if list(schema.keys())[0] in created_schemas:
                pass
            else:
                RedshiftSchema(list(schema.keys())[0], conn).create()
                created_schemas.append(schema)
            if list(schema.values())[0].upper() == 'ALL':
                RedshiftGroup(group, conn).grant_schema(list(schema.values())[0].upper(), list(schema.keys())[0])
                RedshiftGroup(group, conn).grant_select(list(schema.values())[0].upper(), list(schema.keys())[0])
            else:
                RedshiftGroup(group, conn).grant_schema("USAGE", list(schema.keys())[0])
                RedshiftGroup(group, conn).grant_select(list(schema.values())[0].upper(), list(schema.keys())[0])
    print("Sucesso")

    #close connection
    conn.close()    