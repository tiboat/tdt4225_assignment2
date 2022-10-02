from DbConnector import DbConnector
import os
import pandas as pd
from tabulate import tabulate
from haversine import haversine


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

    def query_4(self):
        """
        Find all users who have taken a taxi.
        """
        query =  (
            "SELECT DISTINCT User.id, transportation_mode FROM User inner join Activity on User.id=Activity.user_id WHERE transportation_mode = 'taxi'"
            )

        self.cursor.execute(
            query
        )
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows


    def query_5(self):
        """
        Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels.
        Do not count the rows where the mode is null.
        """
        query = (
            """
            SELECT transportation_mode, COUNT(*) as Count
            FROM Activity
            WHERE transportation_mode <> 'NULL'
            GROUP BY transportation_mode
            ORDER BY Count DESC 
            """
        )

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


    def query_6a(self):
        """
        Find the year with the most activities
        """
        query =  (
            "SELECT EXTRACT(YEAR FROM start_date_time) AS start_year, COUNT(EXTRACT(YEAR FROM start_date_time)) AS COUNT FROM Activity GROUP BY start_year ORDER BY COUNT DESC LIMIT 1"
            )

        self.cursor.execute(
            query
        )
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def query_7(self):
        """
        Find the total distance (in km) walked in 2008, by user with id=112.
        """
        query = (
            """
            SELECT lat, lon, activity_id 
            FROM Activity INNER JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
            WHERE user_id = 112 AND transportation_mode = 'walk' AND EXTRACT(YEAR FROM date_time) = 2008
            """
        )

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        previous_activity = None
        distance = 0

        for row in rows:
            if previous_activity is None:
                previous_activity = row[2]
                previous_row = row[0:2]
            current_activity_id = row[2]
            current_row = row[0:2]
            if previous_activity == current_activity_id:
                distance += haversine(current_row, previous_row, unit="km")
            else:
                previous_activity = current_activity_id
            previous_row = current_row


        print(distance, 'km')

    def query_11(self):
        """
        Find all users who have registered transportation_mode and their most used transportation_mode.
        """
        query = (
            """
            SELECT User.id, Activity.transportation_mode
            FROM User INNER JOIN Activity ON User.id = Activity.user_id
            WHERE transportation_mode <> 'NULL' 
            ORDER BY User.id
            """
        )

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

        filtered_rows = {i: rows.count(i) for i in rows}.items()

        output_list = []
        previous_item = None
        item_to_add = None
        for item in filtered_rows:
            if previous_item is None:
                previous_item = item
            elif item[0][0] == previous_item[0][0]:
                if item[1] > previous_item[1]:
                    item_to_add = item[0]
                    previous_item = item
                else:
                    item_to_add = previous_item[0]
            else:
                if item_to_add:
                    output_list.append(item_to_add)
                    item_to_add = item[0]
                    previous_item = item
        output_list.append(item_to_add)



        print(tabulate(output_list, headers=self.cursor.column_names))



def main():
    program = None
    try:
        program = Queries()
        print("Executing Queries: ")


        print("Query 1: ")
        _ = program.query_1()
        print("Query 3")
        _ = program.query_3()


        _ = program.query_4()

        print("Query 5")
        _ = program.query_5()

        _ = program.query_6a()

        print("Query 7")
        _ = program.query_7()

        print("Query 11")
        _ = program.query_11()

    except Exception as e:
        print("ERROR: Failed to use database:", e)

    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()