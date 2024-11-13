
import os

import psycopg2

class PostgreSQL:
  def __init__(self) -> None:
    self.host=os.environ["POSTGRESQL_DB_HOST"],
    self.database=os.environ["POSTGRESQL_DB_DATABASE_NAME"],
    self.user=os.environ['POSTGRESQL_DB_USERNAME'],
    self.password=os.environ['POSTGRESQL_DB_PASSWORD']
  
  def connect(self):
    self.conn = psycopg2.connect(
      host=self.host,
      database=self.database,
      user=self.user,
      password=self.password
    )
  
  def create_product(self, name, image_mongodb_id, stock_count, review):
    cur = self.conn.cursor()
    cur.execute('INSERT INTO products (name, image_mongodb_id, stock_count, review)'
                'VALUES (%s, %s, %s, %s)',
                (name,
                image_mongodb_id,
                stock_count,
                review))
    self.conn.commit()
    cur.close()
  
  def close(self):
    if self.conn:
      self.conn.close()