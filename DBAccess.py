import MySQLdb


class DBAccess:
    connection = None
    def __init__(self, host, user, passwd, dbname):
        self._host = host
        self._user = user
        self._passwd = passwd
        self._dbname = dbname
    
    def __enter__(self):
        self._connection = MySQLdb.connect(
            host=self._host,
            user=self._user,
            passwd=self._passwd,
            charset="utf8mb4",
            db=self._dbname)
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        self._connection.close()
    
    def getList(self, query):
        data = []
        with self._connection.cursor(MySQLdb.cursors.DictCursor) as cur:
            cur.execute(query)
            for d in cur.fetchall():
                data.append(d)
                
        return data
    
    def getOne(self, query):
        with self._connection.cursor(MySQLdb.cursors.DictCursor) as cur:
            cur.execute(query)
            for d in cur.fetchall():
                return d