import psycopg2
import sqlite3
import csv
from sqlite3 import Error
from abc import ABC, abstractmethod

class DataClient(ABC):

    @abstractmethod
    def get_connection(self):
        pass


    @abstractmethod
    def create_table(self, connection):
        pass

    @abstractmethod
    def get_items(self, connection, price_usd_from =100, price_usd_to=10000):
        pass

    @abstractmethod
    def insert(self, connection, link, price, price_usd, description):
        pass

    def run_test(self):
        conn = self.get_connection()
        self.create_table(conn)
        items = self.get_items(conn, 500, 3000)
        for item in items:
            print(item)
        conn.close()


class PostgresClient(DataClient):

    USER="postgres"
    PASSWORD="postgres"
    HOST="127.0.0.1"
    PORT="5432"
    DATABASE="postgres_parse"

    def get_connection(self):
        try:
            connection = psycopg2.connect(
                user=self.USER,
                password=self.PASSWORD,
                host=self.HOST,
                port=self.PORT,
                database=self.DATABASE)
            return connection
        except Error:
            print(Error)

    def create_table(self, connection):
        cursor_object = connection.cursor()
        cursor_object.execute(
            """
            CREATE TABLE IF NOT EXISTS house
                ( id serial PRIMARY KEY,
                  link text,
                  price integer,
                  price_usd integer,
                  description text
                )
            """
        )
        connection.commit()


    def get_items(self, connection, price_usd_from =100, price_usd_to=10000):
        cursor = connection.cursor()
        cursor.execute(f'SELECT * FROM house WHERE price_usd >= {price_usd_from} and price_usd <= {price_usd_to}')
        return cursor.fetchall()


    def insert(self,connection, link, price, price_usd, description):
        cursor = connection.cursor()
        cursor.execute(f" INSERT INTO house (link, price, price_usd, description) VALUES('{link}','{price}', '{price_usd}', '{description}')")

        return connection.commit()


class Sqlite3Client(DataClient):

    DB_NAME = 'Куфар.db'
    def get_connection(self):
        try:
            conn = sqlite3.connect(self.DB_NAME)
            return conn
        except Error:
            print(Error)

    def create_table(self,conn):
        cursor_object = conn.cursor()
        cursor_object.execute(
            """
            CREATE TABLE IF NOT EXISTS house
            (
                id integer PRIMARY KEY autoincrement,
                link text,
                price integer,
                price_usd integer,
                description text
            )
            """
        )
        conn.commit()

    def get_items(self, conn, price_usd_from=100, price_usd_to=10000):
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM house WHERE price_usd >= {price_usd_from} and price_usd <= {price_usd_to}')
        return cursor.fetchall()

    def insert(self, conn, link, price, price_usd, description):
        cursor = conn.cursor()
        cursor.execute(
            f" INSERT INTO house (link, price, price_usd, description) VALUES('{link}','{price}', '{price_usd}', '{description}')")

        return conn.commit()


class CsvClient(DataClient):

    FILE_NAME = 'example_output/house.csv'
    FIELDNAMES = ['link', 'price', 'price_usd', 'description']

    def get_connection(self):
        return None

    def create_table(self, connection=None):

        with open(self.FILE_NAME, mode='w', newline='', encoding='utf-8-sig') as file:

            writer = csv.DictWriter(file, fieldnames=self.FIELDNAMES, delimiter=';', quoting=csv.QUOTE_ALL)
            writer.writeheader()  # Записываем заголовки

    def get_items(self, сonnection=None, price_usd_from=100, price_usd_to=10000):
        results=[]

        with open(self.FILE_NAME, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file, delimiter=';')
            for row in reader:
                try:
                    price = float(row['price_usd'])
                    if price_usd_from <= price <= price_usd_to:
                        results.append(row)
                except (ValueError, KeyError):
                    continue
            return results

    def insert(self, connection, link, price, price_usd, description):

        with open(self.FILE_NAME, mode='a', newline='', encoding='utf-8-sig') as file:
            writer = csv.DictWriter(file, fieldnames=self.FIELDNAMES, delimiter=';', quoting=csv.QUOTE_ALL)
            writer.writerow({
                'link': link,
                'price': price,
                'price_usd': price_usd,
                'description': description
            })





