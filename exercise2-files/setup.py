from DbConnector import DbConnector


class Setup:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self):
        query = """ CREATE TABLE User(
                    id varchar(15) PRIMARY KEY
                    has_labels boolean
                    )"""
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        query = """ CREATE TABLE Activity(
                    id integer PRIMARY KEY 
                    user_id varchar(15) 
                    transportation_mode varchar(15) 
                    start_date_time datetime
                    end_date_time datetime
                    FOREIGN KEY (user_id) REFERENCES (User(id))
                    )"""
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_trackpoint_table(self):
        query = """ CREATE TABLE TrackPoint(
                    id integer PRIMARY KEY
                    activity_id integer 
                    lat double 
                    lon double
                    altitude integer 
                    date_days double
                    date_time datetime
                    FOREIGN KEY (activity_id) REFERENCES (Activity(id))
                    )"""
        self.cursor.execute(query)
        self.db_connection.commit()


    def insert_data(self):
        pass



def main():
    program = None
    try:
        # 1. Connect to MySQL server on virtual machine
        program = Setup()

        # 2. Create and define the tables User, Activity and TrackPoint
        program.create_tables()

        # 3. Inserts the data from the Geolife dataset into the database
        program.insert_data()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == "__main__":
    main()