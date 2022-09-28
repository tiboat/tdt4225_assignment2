from DbConnector import DbConnector
import os
import pandas as pd
from tabulate import tabulate


class Queries:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def query_1(
        self, table_name_users, table_name_activities, table_name_trackpoints
    ):
        """
        How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
        """
        query = (
            "SELECT UserCount.NumUsers, ActivitiesCount.NumActivities, TrackpointCount.NumTrackpoints FROM "
            "(SELECT COUNT(*) as NumUsers FROM %s) AS UserCount,"
            "(SELECT COUNT(*) as NumActivities FROM %s) AS ActivitiesCount,"
            "(SELECT COUNT(*) as NumTrackpoints FROM %s) AS TrackpointCount"
        )

        self.cursor.execute(
            query % (table_name_users, table_name_activities,
                     table_name_trackpoints)
        )
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def query_3(self):
        """
        Find the top 20 users with the highest number of activities.
        """
        query =  (
            """
            SELECT user_id, COUNT(*) as Count 
            FROM User 
            GROUP BY user_id 
            ORDER BY Count DESC 
            LIMIT 20 """
            )

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

        _ = program.query_1(
            table_name_users="User",
            table_name_activities="Activity",
            table_name_trackpoints="TrackPoint",
        )
    except Exception as e:
        print("ERROR: Failed to use database:", e)

    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()