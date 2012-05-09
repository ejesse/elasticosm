import pyes


servers = ["http://localhost:9200"]

conn = pyes.ES(servers)

def get_connection():
    global conn
    return conn