import boto3
import openpyxl
import sqlite3
import pandas as pd

class Pipeline(object):
    def __init__(self):
        self.car_brands = None
        self.yearly_sales = None

    def extract(self):
        """
        This function is to extract the data from an online source,
        which gets us all the data of car brand and specific models
        for this particular year of 2022 monthwise
        """

        car_data = "data/carsales_2022.xlsx"

        self.car_brands = pd.read_excel(car_data)

    def transform(self):
        """
        This function is to transform the extracted dataframe with 
        application of necessary transformations needed 
        """

        #check if '-' and delete them
        

        df = pd.read_excel("data/carsales_2022.xlsx")
        # df['Jan'] = df['Jan'].str.replace(',','')
        # df['Feb'] = df['Feb'].str.replace(',','')
        # df['Mar'] = df['Mar'].str.replace(',','')
        # df['Apr'] = df['Apr'].str.replace(',','')
        # df['May'] = df['May'].str.replace(',','')
        # df['Jun'] = df['Jun'].str.replace(',','')
        # df['Jul'] = df['Jul'].str.replace(',','')
        # df['Aug'] = df['Aug'].str.replace(',','')
        # df['Sep'] = df['Sep'].str.replace(',','')
        # df['Oct'] = df['Oct'].str.replace(',','')
        # df['Nov'] = df['Nov'].str.replace(',','')
        # df['Dec'] = df['Dec'].str.replace(',','')
        # df.loc['yearly_sales'] = df.sum(numeric_only = True, axis = 1)

    def load(self):
        """
        This function is to load the transformed data into a csv format
        file which consists of all the sum of car sales per each car brand
        and an year
        """
        db = DB()
        self.car_brands.to_sql('car_sales', db.conn, if_exists='append',index=False)
        # df.to_csv(file_name, sep='\t')
        

class DB(object):
    def __init__(self,db_file='db.sqlite'):
        self.conn = sqlite3.connect(db_file)
        self.cur = self.conn.cursor()
        self.__init__db()

    def __del__(self):
        self.conn.commit()
        self.conn.close()

    def __init__db(self):
        table1 = f"""CREATE TABLE IF NOT EXISTS car_brands(
            MODEL TEXT,
            JAN INTEGER,
            FEB INTEGER,
            MAR INTEGER,
            APR INTEGER,
            MAY INTEGER,
            JUN INTEGER,
            JUL INTEGER,
            AUG INTEGER,
            SEP INTEGER,
            OCT INTEGER,
            NOV INTEGER,
            DEC INTEGER,
            YEARLY_SALES INTEGER
        );"""

        self.cur.execute(table1)

if __name__ == '__main__':
    pipeline =Pipeline()
    print('Data Pipeline Created')
    print('\t extracting data from source.....')
    pipeline.extract()
    print('\t formatting and transforming data.....')
    pipeline.transform()
    print('\t loading into database.....')
    pipeline.load()

    print('\nDone. See: results in db.sqlite"')

