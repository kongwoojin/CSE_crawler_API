import os

import edgedb

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(usecwd=True))

edgedb_host = os.getenv("EDGEDB.HOST")
edgedb_port = os.getenv("EDGEDB.PORT")
edgedb_user = os.getenv("EDGEDB.USER")
edgedb_passwd = os.getenv("EDGEDB.PASSWD")
edgedb_dbname = os.getenv("EDGEDB.DBNAME")


def edgedb_client():
    if edgedb_host == "localhost":
        return edgedb.create_client(
            dsn=f"edgedb://{edgedb_user}:{edgedb_passwd}@{edgedb_host}:{edgedb_port}/{edgedb_dbname}",
            tls_security="insecure"
        )
    else:
        return edgedb.create_client(
            dsn=f"edgedb://{edgedb_user}:{edgedb_passwd}@{edgedb_host}:{edgedb_port}/{edgedb_dbname}",
        )

