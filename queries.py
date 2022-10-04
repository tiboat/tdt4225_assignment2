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

    def query_2(self):
        """
        Find the average number of activities per user. 140.71
        """
        query = (
            #'SELECT (COUNT(Activity.id)*1.0)/(COUNT(DISTINCT user_id)*1.0) AS AvgActivitiesPerUser FROM Activity'
            """
            SELECT AVG(NofActivities)
            FROM (SELECT COUNT(id) AS NofActivities FROM Activity GROUP BY user_id) as sub
            """
        )

        self.cursor.execute(
            query
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
            """
            SELECT DISTINCT User.id, transportation_mode 
            FROM User inner join Activity on User.id=Activity.user_id 
            WHERE transportation_mode = 'taxi'
            ORDER BY User.id
            """
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

    def query_6a(self):
        """
        Find the year with the most activities. 2008
        """
        query =  (
            """
            SELECT EXTRACT(YEAR FROM start_date_time) AS year, COUNT(id) AS NofActivities 
            FROM Activity 
            GROUP BY year 
            ORDER BY NofActivities DESC LIMIT 1
            """
            )

        self.cursor.execute(
            query
        )
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def query_6b(self):
        """
        Is this also the year with most recorded hours? yes
        """
        query =  (
            """
            SELECT EXTRACT(YEAR FROM start_date_time) AS year, SUM(TIMEDIFF(end_date_time, start_date_time)) AS RecordedHours 
            FROM Activity 
            GROUP BY year 
            ORDER BY RecordedHours DESC LIMIT 1
            """
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
            SELECT lat, lon 
            FROM Activity INNER JOIN Trackpoint 
            """
        )
    def query_8(self):
        """
        Find the top 20 users who have gained the most altitude meters.
        """

        query =  (
            """
            SELECT user_id, (SUM(Dif) * 0.3048) AS AltitudeGainedScaled
            FROM (SELECT tn.activity_id, SUM(tl.altitude-tn.altitude) AS Dif FROM TrackPoint AS tn INNER JOIN TrackPoint AS tl ON tn.id=tl.id-1 WHERE tn.altitude != -777 AND tl.altitude != -777 AND tn.altitude < tl.altitude GROUP BY tn.activity_id) AS sub2, Activity
            WHERE Activity.id = activity_id
            GROUP BY user_id
            ORDER BY AltitudeGainedScaled DESC LIMIT 20
            """
        )

        self.cursor.execute(
            query
        )
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def query_10(self):
        """
        Find the users who have tracked an activity in the Forbidden City of Beijing. lat 39.916, lon 116.397
        """
        query =  (
            """
            SELECT DISTINCT user_id
            FROM Activity INNER JOIN TrackPoint on Activity.id=TrackPoint.activity_id
            WHERE CAST(lat as CHAR) AND CAST(lon as CHAR) AND lat LIKE '39.916%' AND lon LIKE '116.397%'
            """
               )

        self.cursor.execute(
            query
        )
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

def main():
    program = None
    try:
        program = Queries()
        print("Executing Queries: ")


        print("Query 1: ")
        _ = program.query_1()
        print('Query 2: ')
        _ = program.query_2()
        print("Query 3")
        _ = program.query_3()
        print('Query 4: ')
        _ = program.query_4()
        print('Query 6: ')
        _ = program.query_6a()
        _ = program.query_6b()
        print('Query 8: ')
        _ = program.query_8()
        print('Query 10: ')
        _ = program.query_10()
    except Exception as e:
        print("ERROR: Failed to use database:", e)

    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()