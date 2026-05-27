  GNU nano 8.7.1                                                                                   connection_check.py
import boto3
import psycopg2
from dotenv import load_dotenv
import os


load_dotenv("/home/ssm-user/dqm/.env")

client = boto3.client("redshift", region_name="us-east-1")

creds = client.get_cluster_credentials(
    DbUser = os.getenv("REDSHIFT_USER"),
    DbName = os.getenv("REDSHIFT_DATABASE"),
    ClusterIdentifier = os.getenv("REDSHIFT_CLUSTER_ID"),
    AutoCreate=False
)

conn = psycopg2.connect(
    host = os.getenv("REDSHIFT_HOST"),
    port=5439,
    dbname=os.getenv("REDSHIFT_DATABASE"),
    user=creds["DbUser"],
    password=creds["DbPassword"],
    sslmode="require"
)

print("Connected successfully")
conn.close()
