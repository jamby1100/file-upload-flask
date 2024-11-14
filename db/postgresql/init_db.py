import os
import psycopg2

conn = psycopg2.connect(
        host=os.environ["POSTGRESQL_DB_HOST"],
        database=os.environ["POSTGRESQL_DB_DATABASE_NAME"],
        user=os.environ['POSTGRESQL_DB_USERNAME'],
        password=os.environ['POSTGRESQL_DB_PASSWORD'])

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute a command: this creates a new table
# https://stackoverflow.com/questions/28558920/postgresql-foreign-key-syntax
cur.execute('DROP TABLE IF EXISTS products CASCADE;')
cur.execute('CREATE TABLE products ('
            'id serial PRIMARY KEY,'
            'name varchar(150) NOT NULL,'
            'image_mongodb_id varchar(150) NOT NULL,'
            'stock_count integer NOT NULL,'
            'constraint stock_nonnegative check (stock_count >= 0),'
            'review text,'
            'price decimal(10, 2) NOT NULL, '  # Adding price column with precision 10 and scale 2 (for 2 decimal places)
            'created_at date DEFAULT CURRENT_TIMESTAMP'
            ');')


cur.execute('DROP TABLE IF EXISTS orders CASCADE;')
cur.execute('CREATE TABLE orders (id serial PRIMARY KEY,'
                                 'customer_name varchar (150) NOT NULL,'
                                 'product_id varchar(150) not null,'
                                 'total decimal NOT NULL,'
                                 'constraint total_nonnegative check (total >= 0),'
                                 'tax decimal NOT NULL,'
                                 'constraint tax_nonnegative check (tax >= 0),'
                                 'pretax_amount decimal NOT NULL,'
                                 'constraint pretax_amount_nonnegative check (pretax_amount >= 0),'
                                 'created_at date DEFAULT CURRENT_TIMESTAMP);'
                                 )

cur.execute('DROP TABLE IF EXISTS stock_movements CASCADE;')
cur.execute('CREATE TABLE stock_movements (id serial PRIMARY KEY,'
                                 'product_id integer REFERENCES products,'
                                 'order_id integer REFERENCES orders,'                    
                                 'quantity integer NOT NULL,'
                                 'constraint quantity_nonnegative check (quantity >= 0),'
                                 'created_at date DEFAULT CURRENT_TIMESTAMP);'
                                 )

# # Insert data into the table

# cur.execute('INSERT INTO books (title, author, pages_num, review)'
#             'VALUES (%s, %s, %s, %s)',
#             ('A Tale of Two Cities',
#              'Charles Dickens',
#              489,
#              'A great classic!')
#             )


# cur.execute('INSERT INTO books (title, author, pages_num, review)'
#             'VALUES (%s, %s, %s, %s)',
#             ('Anna Karenina',
#              'Leo Tolstoy',
#              864,
#              'Another great classic!')
#             )

conn.commit()

cur.close()
conn.close()