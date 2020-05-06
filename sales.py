"""
You are provided a few datafiles and you are expected to do the following in a Object Oriented way
- Create a Pipeline to import the Cutomer Data File 
- Create a Pipeline to import the daily transaction data given a date range as input in the commandline
    - I should be able to run your overall program for 1 day, 2 days or 3 days. 
    - I might have additional datasets to run against as well with missing days inbetween
- Enable the programmer to add new customers to the database
- Enable the programmer to add additional sales to the database
- Once the data is imported use Pandas to do the following:
    - Clean and Scrub the data as required
    - Identify the Top 5 customers based on $ spent
    - Identify the Top 3 products being sold
    - Daily trend of sales per products (data and graph)
    - Average sales per day by product (qty) (data and Graph)
    - Average sales per day by $(data and Graph)
    - Average sales per day by customer on $ spent(data and Graph)
- Your program should write the data into a new Sqlite3 database and run from there.

"""
import sys
import datetime 
import sqlite3 as sql
from sqlite3 import Error
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdate
import numpy as np
from jinja2 import Environment, FileSystemLoader




class Pineline:

    @staticmethod
    def loadCustomerData(conn,fileName):
        try:
            customerDf = pd.read_csv(fileName)
            try: 
                for index,row in customerDf.iterrows():
                    #convert each row of data into tuple
                    customerData = (row['ID'], row['name'],row['sex'],row['age'])
                    SalesManager.insertCustomerTable(conn,customerData)
            except Error as e:
                print(e)
        except Error as e:
            print(e)
        
        
    @staticmethod
    def loadSalesData(conn):
        if len(sys.argv) != 3:
            print("Please provide start date and end date in the format yyyy-mm-dd")
            quit()

        startDate = sys.argv[1]
        endDate = sys.argv[2]
        try: 
            myStartDate = datetime.datetime.strptime(startDate, "%Y-%m-%d")
            print(myStartDate)
        except ValueError as e:
            print(e)
            print("Please provide start date in the format yyyy-mm-dd")
            quit()
        try: 
            myEndDate = datetime.datetime.strptime(endDate, "%Y-%m-%d")
            print(myEndDate)
        except ValueError as e:
            print(e)
            print("Please provide end date in the format yyyy-mm-dd")
            quit()
        fileBase = "-SalesData.csv"

        for index in range(myStartDate.day,myEndDate.day +1):
            name = "./"+str(myStartDate.year)+"-"+ myStartDate.strftime("%m")+ "-0"+ str(index) + fileBase
            try:
                dataFile = pd.read_csv(name)
                try: 
                    for index,row in dataFile.iterrows():
                        #convert each row of data into tuple
                        amount = row['Total Amount'][:-1] 
                        salesData = (row['CustomerID'],row['Purchase Date'],row['Purchased Items'],amount)
                        SalesManager.insertSalesTable(conn,salesData)    
                except Error as e:
                    print(e)
            except FileNotFoundError:
                print("The file "+ name+ "is not found")
                pass
            

            
                


class Database:
    
    # connect database
    @staticmethod
    def GetConnection(dbName):
        try: 
            conn = sql.connect(dbName)
            return conn
        except Error as e:
            print(e)  
    # close database
    @staticmethod 
    def CloseConnection(conn):
        try: 
            conn.close()
        except Error as e: 
            print(e)

class SalesManager: 
    # create table for customer data 
    @staticmethod
    def CreateCustomerTable(conn):
        CreateTableSQL =   """
                            CREATE TABLE IF NOT EXISTS Customer (  
                                id integer PRIMARY KEY,
                                name text NOT NULL,
                                sex text, 
                                age integer 
                            );
                            """       
        try:
            cursor = conn.cursor()
            cursor.execute(CreateTableSQL)
        except Error as e :
            print(e) 
      
    
    # create table for Sales data 
    @staticmethod
    def CreateSalesTable(conn):
        CreateTableSQL =   """
                            CREATE TABLE IF NOT EXISTS Sales (
                                id integer PRIMARY KEY,  
                                customerId integer,
                                purchaseDate datetime,
                                purchasedItem text,
                                totalAmount money 

                            );
                            """       
        try:
            cursor = conn.cursor()
            cursor.execute(CreateTableSQL)
        except Error as e :
            print(e) 
    
    @staticmethod
    def insertCustomerTable(conn,customerData):
        sql = """ INSERT INTO Customer (id,name,sex,age) values (?,?,?,?) """
        try:
            cursor = conn.cursor()
            cursor.execute(sql,customerData)
            conn.commit()
            return 
        except Error as e :
            print(e)
        
    @staticmethod
    def insertSalesTable(conn,salesData):
        sql = """ INSERT INTO Sales (customerId,purchaseDate,purchasedItem,totalAmount) values (?,?,?,?) """
        try:
            cursor = conn.cursor()
            cursor.execute(sql,salesData)
            conn.commit()
            return cursor.lastrowid
        except Error as e :
            print(e)

    @staticmethod
    def joinTable(conn):
        sql = """select c.id [customerID], name, sex, age, purchaseDate,purchasedItem,totalAmount from Customer c inner join Sales s on c.id = s.customerId """ 
        try: 
            cursor = conn.cursor()
            cursor.execute(sql)
            rows  = cursor.fetchall()
            # index name
            df = pd.DataFrame(rows,columns=['customer ID','name','sex','age','purchase date','purchased item','total amount'])
            return df
        except Error as e:
            print(e)
            
class DataAnalysis():
    
    @staticmethod
    def DataClean(df):
       
        
        # check how's the datatype, there's no null value
        #print(df.info())
     

        # comment:  There's a customer whose age is only 2. That might because her parents buy laptop for her. 

        # descriptive statistics 
        #print(df.describe())

        # drop the variable we don't need 
        df.drop(['sex','age'],1,inplace = True)

        #covert purchase date to datetime 
        df['purchase date'] = pd.to_datetime(df['purchase date'])

        # group total amount 
        df['amountRange'] = pd.cut(df['total amount'],[0,1000,2000,3000,4000],labels = ['under $1k','$1k -$2k','$2k-$3k','$3k-$4k'])

        return df


    def analysis(df):
        # connect to html templete
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template('myreport.html')


        #Identify the Top 5 customers based on $ spent
        template_vars = {}
        customerAmount = df[['name','total amount']].groupby('name').sum()
        customerAmount = customerAmount.sort_values(['total amount'], ascending=False)
        top5 = customerAmount.head(5)
        print("\n Top 5 customers based on $ spent:\n")
        print(top5)
        # transform the content into html and save in the template_vars dictionary
        template_vars["top5"] = "\n Top 5 customers based on $ spent:\n"
        template_vars["top5table"] = top5.to_html()

        


        #Identify the Top 3 products being sold
        print('\n Top 2 products being sold:\n')
        productCount = pd.DataFrame(df['purchased item'].value_counts())
        print(productCount)
        
        template_vars["top2sold"] = '\n Top 2 products being sold:\n'
        template_vars["top2soldtable"] = productCount.to_html()

        #- Daily trend of sales per products (data and graph)
        df['purchase date'] = df['purchase date'].dt.date
        daily_sales = pd.crosstab(df['purchase date'], df['purchased item'])
        print("\nDaily trend of sales per products\n")
        print(daily_sales)
        plot1 = daily_sales.plot(
             figsize=(12,9)
             )        
        fig = plot1.get_figure()
   
        plt.xlabel("Purchase Date")
        plt.ylabel("Sales")
        plt.title("Daily trend of sales per products")
        plt.show()
        # transform the content into html and save in the template_vars dictionary
        template_vars["prodtren"] = "\nDaily trend of sales per products\n"
        template_vars["prodtrentable"] = daily_sales.to_html()
        
        fig.savefig("output1.png")
        template_vars["prodtrenplot"] = "output1.png"        
        
        

        #- Average sales per day by product (qty) (data and Graph)
        
        average_sales_product = daily_sales.mean()
        print("\nAverage sales per day by product\n")
        
        plot3 = average_sales_product.plot(kind = "bar", figsize = (12,9))
        plt.title("Average sales per day by product")
        plt.xticks(rotation = -45)
        plt.show()
        # transform the content into html and save in the template_vars dictionary
        template_vars["dayavgsale"] = "Average sales per day by product"
        average_sales_product = pd.DataFrame({'Item':average_sales_product.index, 'Average Sales':average_sales_product.values})
        print(average_sales_product)
        template_vars["dayavgsaletable"] = average_sales_product.to_html()
        fig = plot3.get_figure()
        fig.savefig("output3.png")
        template_vars["dayavgsaleplot"] = "output3.png"

        #- Average sales per day by $(data and Graph)
        
        money_sales = pd.crosstab(df['purchase date'],df['amountRange'])
        average_sales_money = money_sales.mean()
        print("\nAverage sales per day by amount\n")
        
        plot4 = average_sales_money.plot(kind = "barh", figsize = (12,9),legend = None)
        plt.title("Average sales per day by amount")
        plt.show()
        # transform the content into html and save in the template_vars dictionary
        template_vars["moneysale"] = "\nAverage sales per day by amount\n"
        average_sales_money = pd.DataFrame({'Amount Range':average_sales_money.index, 'Average Sales':average_sales_money.values})
        print(average_sales_money)
        template_vars["moneysaletable"] = average_sales_money.to_html()
        fig = plot4.get_figure()
        fig.savefig("output4.png")
        template_vars["moneysaleplot"] = "output4.png"

        #- Average sales per day by customer on $ spent(data and Graph)
        days = df["purchase date"].nunique()
        customer_spending = df[['name','total amount']].groupby("name").sum()/days
        customer_spending = customer_spending.sort_values(["total amount"],ascending = True)
        customer_spending.columns = ['Average amount per day']
        print(customer_spending)
        plot5 = customer_spending.plot(kind = "barh", figsize = (12,9),legend = None)
        plt.title("Average sales per day by customer on money spent")
        plt.yticks(wrap = True)
        plt.show()
        # transform the content into html and save in the template_vars dictionary
        template_vars["customer_spending"] = "Average sales per day by customer on money spent"
        template_vars["customer_spendingtable"] = customer_spending.to_html()
        fig = plot5.get_figure()
        fig.savefig("output5.png")
        template_vars["customer_spendingplot"] = "output5.png"
        
        # fit the template_vars dictionary value into the report html template. 
        html_out = template.render(template_vars)
        # save the new html file
        f = open("file.html","w") 
        f.write(html_out)
        f.close()

def main():

    # connect database
    conn = Database.GetConnection("sales.db")

    # create table
    if conn is not None:
        SalesManager.CreateCustomerTable(conn)
        SalesManager.CreateSalesTable(conn)
        with conn:
            # insert table 
            Pineline.loadCustomerData(conn, "CustomerData.csv")
            Pineline.loadSalesData(conn)
            # create a sales dateframe
            df = SalesManager.joinTable(conn)
            #df.to_csv("sales.csv")
            # Clean data
            CleanedData = DataAnalysis.DataClean(df)
            # Pandas functions 
            DataAnalysis.analysis(CleanedData)
            # test insert function
            #SalesManager.insertCustomerTable(conn,(11,"lili",'female',18))
            #SalesManager.insertSalesTable(conn,(11,10/3/2017,'Laptop',1500))
            



  

    else:
        print("Error! Cannot Create Database Connection")


    Database.CloseConnection(conn)


if __name__ == "__main__":
    main()

