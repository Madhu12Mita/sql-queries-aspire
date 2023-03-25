''''
There are 2 tables namely- customer_info and book_info
The aim of this program is to perform appropriate join and get the book name
and author name of each customer
'''

import psycopg2

class Join:

    #establish connection with postgres
    def __init__(self,username,database,password) :
        self.connection = psycopg2.connect(database=database,user=username,password=password)
        self.cursor = self.connection.cursor()

    #create tables
    def create(self,table,columns):
        self.table=table
        if self.check_table(self.table):
            print("Table already exists")
        else:
            print(f"\nCREATING table {self.table}")
        # define the SQL query to create the table
            sql = f"CREATE TABLE {self.table} ({', '.join([f'{column[0]} {column[1]}' for column in columns])})"

        # execute the SQL query to create the table
            self.cursor.execute(sql)

        # commit the changes
            self.connection.commit()

    # insert multiple rows into a table
    def insert(self, values):
        if self.check_table(self.table):
            self.truncate()
            
        print(f"\nINSERTING to {self.table}")
        for vals in values:

            sql = f"INSERT INTO {self.table} VALUES ({', '.join(['%s'] * len(vals))})"
            print(sql, tuple(vals))

            self.cursor.execute(sql, tuple(vals))

            # commit the changes
        self.connection.commit()

    # drop a table if it exists
    def drop(self):
        if self.check_table(self.table):
            self.cursor.execute(f"DROP TABLE {self.table}")
            self.connection.commit()
        else:
            print("Does not exist")

    # print the contents of table
    def display(self):

        print(f"\nSELECT * FROM {self.table}")
        self.cursor.execute(f"SELECT * FROM {self.table}")

        results = self.cursor.fetchall()
        for row in results:
            print(row)

    #removes all rows from a table
    def truncate(self):
        self.cursor.execute(f"truncate table {self.table}")

    # check if table exists or not
    def check_table(self,table_name):
        self.cursor.execute(f"select * from information_schema.tables where table_name=%s", (table_name,))
        bool(self.cursor.rowcount)
        return self.cursor.rowcount
    
    def left_joins(self,table2,result_col,condition):
        sql=f"select {', '.join(['%s'] * len(result_col))} from {self.table} left join {table2} on {condition}"
        print(sql)
        self.cursor.execute(sql)
        result=self.cursor.fetchall()
        for i in result:
            print(i)

obj1=Join("postgres","mkoralla","Ratna@0202")
obj2=Join("postgres","mkoralla","Ratna@0202")
customer_col=[('id','integer primary key'),
              ('name','varchar(20)'),
              ('book_id', 'integer references books_info(id)')]
book_col=[('id','integer primary key'),
          ('book_name','varchar(20)'),
          ('author','varchar(20)')]
obj2.create("books_info",book_col)
obj1.create("customer_info",customer_col)
customer_vals=[
    (1,'madhu',1),
    (2,'janu',2),
    (3,'sai',32)
]

book_vals=[
    (1,'A',"sir"),
    (2,'B',"hi"),
    (3,'C',"po")
]

obj1.insert(customer_vals)
obj2.insert(book_vals)

obj1.left_joins("books_info",['customer_info.name','books_info.book_name','books_info.author'],
                'customer_info.book_id=books_info.id')