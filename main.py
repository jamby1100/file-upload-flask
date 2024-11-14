from decimal import Decimal
import os

from flask import Flask, flash, request, redirect, url_for, render_template, send_from_directory
from werkzeug.utils import secure_filename
from db.mongodb.mongodb import MongoDB
from db.postgresql.postgresql import PostgreSQL
from actions.upload_file import UploadFile
from actions.view_images import ViewImage

UPLOAD_FOLDER = os.getenv("UPLOAD_DIRECTORY")
class MainApp:
    def __init__(self):
        self.app = Flask(__name__)
        self.app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
        self.upload_instance = UploadFile(self.app)
        self.images_instance = ViewImage(self.app)
        self.configurable_routes()

    def configurable_routes(self):
        @self.app.route("/")
        def hello_world():
            return "<p>Hello, World!</p>"
        
        @self.app.route('/uploads/<name>')
        def download_file(name):
            return send_from_directory(self.app.config["UPLOAD_FOLDER"], name)
        
        @self.app.route('/images', methods=['GET'])
        def show_uploaded_images():
            return self.images_instance.list()

        # @self.app.route('/order', methods=['GET', 'POST'])
        # def create_order():
        #     if request.method == 'POST':
        #         pass
        
        @self.app.route("/upload-file", methods=['GET', 'POST'])
        def upload_file_route():
            return self.upload_instance.upload()
        
        @self.app.route('/order/<id>', methods=['GET', 'POST'])
        def create_order(id):
            # For GET request: Fetch product details and display the order form
            if request.method == 'GET':
                psql_instance = PostgreSQL()
                psql_instance.connect()

                product_details = psql_instance.get_product_by_id(id)
                print('product_details', product_details)

                psql_instance.close()
                return render_template('create_order.html', product=product_details, id=id)

            if request.method == 'POST':
                print("AND THE FORM IS")
                print(request.form)

                total_price = request.form.get('total_price')
                tax_amount = request.form.get('tax_amount')
                qty = int(request.form.get("quantity"))

                # Convert the values to Decimal before performing the addition
                total_price_decimal = Decimal(total_price)
                tax_amount_decimal = Decimal(tax_amount)

                psql_instance = PostgreSQL()
                psql_instance.connect()
                order_id = psql_instance.create_order(
                    customer_name=request.form.get("name"),
                    pretax_amount=total_price_decimal,
                    product_id=request.form.get("product_id"),
                    tax=tax_amount_decimal,
                    total = total_price_decimal + tax_amount_decimal
                )

                product_details = psql_instance.get_product_by_id(id)
                print('product_details', product_details)

                qty_after = int(product_details['stock_count']) - qty

                psql_instance.update_product(id, qty=qty_after)
                psql_instance.create_stock_movement(id, order_id, qty)


                psql_instance.close()

                # Render the order received page with the order details
                return redirect(url_for('order_received', order_id=order_id))


        @self.app.route('/order/received/<order_id>', methods=['GET'])
        def order_received(order_id):
            psql_instance = PostgreSQL()
            psql_instance.connect()

            # Fetch the order details
            order_details = psql_instance.get_order_details(order_id)
            psql_instance.close()

            if order_details:
                return render_template('order_received.html', order=order_details)
            else:
                return "Order not found", 404
        
        
    def run(self, **kwargs):
        self.app.run(**kwargs)

# Running the apt apt
if __name__ == "__main__":
    main_app = MainApp()
    main_app.run(debug=True)