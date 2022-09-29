from DbConnector import DbConnector
import os
import pandas as pd
from tabulate import tabulate


class Queries:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def query_1(self):
        """
        How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
        """

        query_user = (
            """ 
            (SELECT COUNT(*) AS NbOfUsers FROM User)
            """
        )

        query_activity = (
            """ 
            (SELECT COUNT(*) AS NbOfActivities FROM Activity)
            """
        )

        query_trackpoint = (
            """ 
            (SELECT COUNT(*) AS NbOfTrackPoints FROM TrackPoint)
            """
        )

        self.cursor.execute(query_user)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

        self.cursor.execute(query_activity)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

        self.cursor.execute(query_trackpoint)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


    def query_3(self):
        """
        Find the top 20 users with the highest number of activities.
        """
        query =  (
            """
            SELECT user_id, COUNT(*) as Count 
            FROM Activity
            GROUP BY user_id 
            ORDER BY Count DESC 
            LIMIT 20 """
            )

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def query_5(self):
        """
        Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels.
        Do not count the rows where the mode is null.
        """

    def query_7(self):
        """
        Find the total distance (in km) walked in 2008, by user with id=112.
        """
        query = (
            """
            SELECT lat, lon 
            FROM Activity INNER JOIN Trackpoint 
            """
        )

def main():
    program = None
    try:
        program = Queries()
        print("Executing Queries: ")

        print("Query 1: ")
        _ = program.query_1()
        print("Query 3")
        _ = program.query_3()
    except Exception as e:
        print("ERROR: Failed to use database:", e)

    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()