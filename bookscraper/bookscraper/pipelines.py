# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class BookscraperPipeline:
    def process_item(self, item, spider):

        adapter = ItemAdapter(item)

        ## Strip all whitespaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'description' and field_name != 'price':
                value = adapter.get(field_name)
                adapter[field_name] = value[0].strip()


        ## Category & Product Type --> switch to lowercase
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()



        ## Price --> convert to float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£', '')
            if value:
                try:
                    adapter[price_key] = float(value)
                except ValueError:
                    # Log the error or raise a DropItem exception if desired
                    spider.logger.error("Failed to convert price field '%s' to float: %s", price_key, value)
                    # Raise DropItem to skip this item
                    raise DropItem(f"Failed to convert price field '{price_key}' to float: {value}")


        ## Availability --> extract number of books in stock
        availability_string = adapter.get('availability')
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter['availability'] = 0
        else:
            availability_array = split_string_array[1].split(' ')
            adapter['availability'] = int(availability_array[0])



        ## Reviews --> convert string to number
        num_reviews_string = adapter.get('num_reviews')
        adapter['num_reviews'] = int(num_reviews_string)


        ## Stars --> convert text to number
        stars_string = adapter.get('stars')
        split_stars_array = stars_string.split(' ')
        stars_text_value = split_stars_array[1].lower()
        if stars_text_value == "zero":
            adapter['stars'] = 0
        elif stars_text_value == "one":
            adapter['stars'] = 1
        elif stars_text_value == "two":
            adapter['stars'] = 2
        elif stars_text_value == "three":
            adapter['stars'] = 3
        elif stars_text_value == "four":
            adapter['stars'] = 4
        elif stars_text_value == "five":
            adapter['stars'] = 5


        return item


import psycopg2

class SaveToPsqlPipeline:
    def open_db_connection(self, spider):
        # Establish a database connection and create a cursor
        self.conn = psycopg2.connect("dbname=books user=postgres password=742486")
        self.cur = self.conn.cursor()
        

    def close_db_connection(self, spider):
        # Close the cursor and database connection
        self.cur.close()
        self.conn.close()
        
    def create_table(self,item,spider):
        try:
            self.cur.execute("""
            CREATE TABLE IF NOT EXISTS books(
            id SERIAL PRIMARY KEY, 
            url VARCHAR(255),
            title TEXT,
            upc VARCHAR(255),
            product_type VARCHAR(255),
            price_excl_tax DECIMAL,
            price_incl_tax DECIMAL,
            tax DECIMAL,
            price DECIMAL,
            availability INTEGER,
            num_reviews INTEGER,
            stars INTEGER,
            category VARCHAR(255),
            description TEXT
        )
        """)

        except Exception as e:
            # If an error occurs, rollback the transaction
            self.conn.rollback()
            # Log the error
            spider.logger.error("Error inserting item into database: %s", e)
            # Drop the item from the pipeline
            raise DropItem("Error inserting item into database")
        
    def insert_items(self, item, spider):
        try:
            ## Define insert statement
            self.cur.execute(""" INSERT INTO books (
                url, 
                title, 
                upc, 
                product_type, 
                price_excl_tax,
                price_incl_tax,
                tax,
                price,
                availability,
                num_reviews,
                stars,
                category,
                description
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )""", (
                item["url"],
                item["title"],
                item["upc"],
                item["product_type"],
                item["price_excl_tax"],
                item["price_incl_tax"],
                item["tax"],
                item["price"],
                item["availability"],
                item["num_reviews"],
                item["stars"],
                item["category"],
                str(item["description"][0])
            ))
            
        except Exception as e:
            # If an error occurs, rollback the transaction
            self.conn.rollback()
            # Log the error
            spider.logger.error("Error inserting item into database: %s", e)
            # Drop the item from the pipeline
            raise DropItem("Error inserting item into database")
        
    def process_item(self, item, spider):
        self.open_db_connection(spider)
        #create table if not exist
        self.create_table(item,spider)
        self.insert_items(item, spider)
        self.conn.commit()
        self.close_db_connection(spider)
        return item
