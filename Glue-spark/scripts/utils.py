
import boto3
import json 

def get_snowflake_cred(secret_name, secrete_region):

    client = boto3.client("secretsmanager", region_name=secrete_region)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    if "SecretString" in get_secret_value_response:

        secret = json.loads(get_secret_value_response["SecretString"])
        snow_user = secret["username"]
        snow_psw = secret["password"]

    return(snow_user, snow_psw)

