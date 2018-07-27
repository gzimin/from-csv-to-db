import mysql.connector
import csv
import datetime


class MigrateData:

    def __init__(self, path_to_src_file, table_name, credentials):
        self.path_to_src_file = path_to_src_file
        self.credentials = credentials
        self.table_name = table_name
        self.convert_credentials()
        self.cursor, self.cnx = self.create_connection()
        self.migrate_data_from_file_to_db()


    def convert_credentials(self):
        self.user = self.credentials[0]
        self.password = self.credentials[1]
        self.host = self.credentials[2]
        self.database = self.credentials[3]

    def create_connection(self):
        # Creating connection
        try:
            cnx = mysql.connector.connect(user=self.user,
                                          password=self.password,
                                          host=self.host,
                                          database=self.database)
        except mysql.connector.Error as err:
            print("Something wrong with DB connection, check your credentials")
        cursor = cnx.cursor()
        return cursor, cnx

    # As default we will get all data having string type
    # We need to convert our data type to correct type
    @staticmethod
    def parse_data(data_row):
        for index, element in enumerate(data_row):
            if not element.isdigit():
                data_row[index] = "\"" + data_row[index] + "\""
        return data_row

    def migrate_data_from_file_to_db(self):
        with open(self.path_to_src_file, 'r+') as src_file:

            reader = csv.reader(src_file, delimiter=',')
            # Here we need to get first row with all field names
            first_row = ','.join(next(reader))
            for row in reader:
                values = ','.join(self.parse_data(row))
                add_new_row = "INSERT INTO {} ({}) VALUES ({})".format(self.table_name, first_row, values) + ";"
                self.cursor.execute(add_new_row)
            print(self.cursor.rowcount, "record inserted.")
            self.cnx.commit()
            self.cursor.close()
            self.cnx.close()



path_to_src_file = 'db/db.csv'
table_name = 'testable'
credentials = ['root', 'passw0rd', '127.0.0.1', 'testdb']

Example = MigrateData(path_to_src_file=path_to_src_file, table_name=table_name, credentials=credentials)
