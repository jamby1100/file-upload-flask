
import os

import psycopg2
import psycopg2.extras

class PostgreSQL:
  def __init__(self) -> None:
    self.host=os.environ["POSTGRESQL_DB_HOST"]
    self.database=os.environ["POSTGRESQL_DB_DATABASE_NAME"]
    self.user=os.environ['POSTGRESQL_DB_USERNAME']
    self.password=os.environ['POSTGRESQL_DB_PASSWORD']
  
  def connect(self):
    self.conn = psycopg2.connect(
      host=self.host,
      database=self.database,
      user=self.user,
      password=self.password
    )
  
  def create_product(self, name, image_mongodb_id, price, stock_count, review):
    cur = self.conn.cursor()
    
    # Use the RETURNING clause to get the ID of the newly inserted product
    cur.execute('INSERT INTO products (name, image_mongodb_id, price, stock_count, review) '
                'VALUES (%s, %s, %s, %s, %s) RETURNING id',
                (name, image_mongodb_id, price, stock_count, review))
    
    # Fetch the returned ID
    product_id = cur.fetchone()[0]
    
    # Commit the transaction
    self.conn.commit()
    
    cur.close()

    # Return the ID of the newly created product
    return product_id


  def get_order_details(self, order_id):
    cur = self.conn.cursor()

    # Query to get the order details based on the order_id
    cur.execute('SELECT id, customer_name, total, tax, pretax_amount, created_at '
                'FROM orders WHERE id = %s', (order_id,))
    
    self.conn.commit()
    # Fetch the result
    order_details = cur.fetchone()

    # Close the cursor
    cur.close()

    # If the order was found, return it as a dictionary
    if order_details:
        order = {
            'id': order_details[0],
            'customer_name': order_details[1],
            'total': order_details[2],
            'tax': order_details[3],
            'pretax_amount': order_details[4],
            'created_at': order_details[5]
        }
        return order
    else:
        return None  # Return None if no order with the given order_id is found

  
  def update_product(self, id, qty):
    cur = self.conn.cursor()
    cur.execute('UPDATE products SET stock_count = %s WHERE id = %s',
                (qty, id))
    self.conn.commit()
    cur.close()

  def create_order(self, customer_name, total, tax, pretax_amount, product_id):
    cur = self.conn.cursor()
    cur.execute('INSERT INTO orders (product_id, customer_name, total, tax, pretax_amount) '
                'VALUES (%s, %s, %s, %s, %s) RETURNING id',
                (product_id, customer_name, total, tax, pretax_amount))
    
    # Fetch the ID of the newly inserted record
    order_id = cur.fetchone()[0]
    
    # Commit the transaction
    self.conn.commit()
    cur.close()

    # Return the ID of the newly created order
    return order_id
  
  def create_stock_movement(self, product_id, order_id, quantity):
    cur = self.conn.cursor()
    cur.execute('INSERT INTO stock_movements (product_id, order_id, quantity) '
                'VALUES (%s, %s, %s)',
                (product_id, order_id, quantity))
    self.conn.commit()
    cur.close()
  
  def get_product_by_id(self, product_id):
    cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute('SELECT * FROM products WHERE id = %s', (product_id))
    product = cur.fetchone()
    cur.close()

    if product:
      print(product)
      # Access the values by column name
      return {
        'product_id': product['id'],
        'name': product['name'],
        'image_mongodb_id': product['image_mongodb_id'],
        'price': product['price'],
        'stock_count': product['stock_count'],
        'review': product['review']
      }
    else:
      print('NO PRODUCT')
      return None

  def close(self):
    if self.conn:
      self.conn.close()