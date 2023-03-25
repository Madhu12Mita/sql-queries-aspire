import psycopg2

class DataBase():
    # connection to the Database
    def __init__(self,username,database,password) :
        self.connection = psycopg2.connect(database=database,user=username,password=password)
        self.cursor = self.connection.cursor()

    # check if table exists or not
    def check_table(self,table_name):
        self.cursor.execute(f"select * from information_schema.tables where table_name=%s", (table_name,))
        bool(self.cursor.rowcount)
        return self.cursor.rowcount
    
    # create a table 
    def create(self, table_name, columns):
        self.table=table_name
        self.columns=columns
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
           
            self.cursor.execute(sql, tuple(vals))

            # commit the changes
        self.connection.commit()

    # drop a table if it exists
    def drop(self):
        if self.check_table(self.table):
            self.cursor.execute(f"DROP TABLE {self.table} cascade")
            self.connection.commit()
        else:
            print("Table does not exist")

    # print the contents of table
    def display(self):
        print()
        print(self.table)
        self.cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.table}';")
        columns=self.cursor.fetchall()
        for col in columns:
            for i in col:
                print(i,end=" ")
        print()
        self.cursor.execute(f"SELECT * FROM {self.table}")

        results = self.cursor.fetchall()
        for row in results:
            for i in row:
                print(i,end=" ")
            print()

    #removes all rows from a table
    def truncate(self):
        self.cursor.execute(f"truncate table {self.table} cascade")

# Instantiating the Class object
table1=DataBase("postgres","mkoralla","Ratna@0202")
table2=DataBase("postgres","mkoralla","Ratna@0202")

# CREATE the required tables
col1 = [('id','SERIAL PRIMARY KEY'),
               ('name','VARCHAR(50) NOT NULL')]
#table1.check_table()
table1.create('user_details', col1)

col2= [('id', 'SERIAL PRIMARY KEY'),
    ('first_name', 'VARCHAR(50) NOT NULL'),
    ('last_name', 'VARCHAR(50) NOT NULL'),
    ('email', 'VARCHAR(50) NOT NULL UNIQUE'),
    ('department_id', 'INTEGER NOT NULL REFERENCES user_details(id)')]
table2.create('user_info', col2)


# INSERT values to the created tables
values1 = [(100,"CSE"),
          (300,"EEE"),
          (200,"ECE")]
table1.insert( values1)

values2 = [(1,"Madhu", "Koralla", "madhu@gmail.com", 100),
          (2,"Hello", "hi", "hello@gmial.com", 200),
          (3,"A", "B", "AB@gmail.com", 100)]
table2.insert(values2)

# DISPLAY the created tables
table1.display()
table2.display()