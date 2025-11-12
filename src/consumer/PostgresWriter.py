# from pyspark.sql.streaming import ForeachWriter
import psycopg2

class PostgresWriter(object):
    def __init__(self, db_config, insert_fn):
        self.db_config = db_config
        self.insert_fn = insert_fn

    def open(self, partition_id, epoch_id):
        self.conn = psycopg2.connect(**self.db_config)
        self.cur = self.conn.cursor()
        return True

    def process(self, row):
        self.insert_fn(self.cur, row)

    def close(self, error):
        if error:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.cur.close()
        self.conn.close()
