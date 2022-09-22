from DbConnector import DbConnector


class Setup:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
        pass

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