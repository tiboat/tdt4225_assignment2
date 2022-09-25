from DbConnector import DbConnector
import os
import pandas as pd

# TODO
#   Documentation

def read_labeled_users():
    """
    Reads the labeled_ids.txt file and returns the list of users mentioned in this file
    (which means these have labeled data)

    Returns: list of users (as strings) which have labeled data
    """
    label_file_path = "./dataset/dataset/labeled_ids.txt"
    # Reading files based on: https://stackoverflow.com/questions/3277503/how-to-read-a-file-line-by-line-into-a-list
    with open(label_file_path) as label_file:
        lines = label_file.readlines()
        return [line.rstrip() for line in lines]


def get_start_and_end_time(trackpoints):
    """

    Args:
        trackpoints:

    Returns:

    """
    start_time = trackpoints["date"].iloc[0] + " " + trackpoints["time"].iloc[0]
    end_time = trackpoints["date"].iloc[-1] + " " + trackpoints["time"].iloc[-1]

    return start_time, end_time


class Setup:

    def __init__(self):
        self.root_data_dir = "./dataset/dataset/Data/"
        self.labeled_users = read_labeled_users()
        self.labels = {}
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_user_table(self):
        query = """ CREATE TABLE IF NOT EXISTS User(
                    id varchar(15) PRIMARY KEY ,
                    has_labels boolean
                    )"""
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        query = """ CREATE TABLE IF NOT EXISTS Activity(
                    id integer PRIMARY KEY, 
                    user_id varchar(15), 
                    transportation_mode varchar(15), 
                    start_date_time datetime,
                    end_date_time datetime,
                    FOREIGN KEY (user_id) REFERENCES (User(id))
                    )"""
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_trackpoint_table(self):
        query = """ CREATE TABLE IF NOT EXISTS TrackPoint(
                    id integer PRIMARY KEY AUTO_INCREMENT,
                    activity_id integer,
                    lat double, 
                    lon double,
                    altitude integer, 
                    date_days double,
                    date_time datetime,
                    FOREIGN KEY (activity_id) REFERENCES (Activity(id))
                    )"""
        self.cursor.execute(query)
        self.db_connection.commit()

    def insert_data_in_tables(self):
        activity_id = 0
        for user in os.listdir(self.root_data_dir):
            # 1. Insert user table data
            # Add true or false value in users dictionary for whether they have labels or not
            self.insert_data_record("User", (user, self.has_label(user)))

            if self.has_label(user):
                # self.insert_user_record(user, )

                activity_dir = os.path.join(self.root_data_dir, user + "/Trajectory/")
                for activity in os.listdir(activity_dir):
                    trackpoints_activity = pd.read_csv(os.path.join(activity_dir, activity),
                                                       names=["lat", "lon", "not_used", "altitude", "date_days", "date",
                                                              "time"],
                                                       skiprows=6)
                    if trackpoints_activity.shape[0] <= 2500:
                        # 2. Insert activity table data
                        start_date_time, end_date_time = get_start_and_end_time(trackpoints_activity)
                        transportation_mode = self.get_transportation_mode(start_date_time, end_date_time, user)
                        self.insert_data_record("Activity", (activity_id, user, transportation_mode, start_date_time,
                                                             end_date_time))

                        # 3. Insert trackpoint table data
                        for _, trackpoint in trackpoints_activity.iterrows():
                            self.insert_data_record("Trackpoint", (activity_id, trackpoint["lat"], trackpoint["lon"],
                                                                   trackpoint["altitude"], trackpoint["date_days"],
                                                                   trackpoint["date"] + " " + trackpoint["time"]))
                        activity_id += 1

    def has_label(self, user):
        """
        Returns whether this user has labeled data

        Args:
            user: string which represents a user

        Returns: true if the given user has labeled data; otherwise false
        """
        return user in self.labeled_users

    def get_transportation_mode(self, start_date_time, end_date_time, user):
        """

        Args:
            start_date_time:
            end_date_time:
            user:

        Returns:

        """
        if self.has_label(user):
            if user not in self.labels.keys():
                self.add_labels_user(user)
            start_and_end_times = list(zip(self.labels[user]["start_date_time"].tolist(),
                                           self.labels[user]["end_date_time"].tolist()))
            # TODO: figure out when to exactly give a transportation mode

            if (start_date_time, end_date_time) in start_and_end_times:
                index = start_and_end_times.index((start_date_time, end_date_time))
                return self.labels[user]["transportation_mode"].iloc(index)

        return "NULL"

    def add_labels_user(self, user):
        """

        Args:
            self:
            user:

        """
        label_file_path = self.root_data_dir + user + "/labels.txt"
        labels_user = pd.read_csv(label_file_path, names=["start_date_time", "end_date_time", "transportation_mode"],
                                  sep="\t", header=None, skiprows=1)
        for _, row in labels_user.iterrows():
            row["start_date_time"] = row["start_date_time"].replace("/", "-")
            row["end_date_time"] = row["end_date_time"].replace("/", "-")

        self.labels[user] = labels_user

    def insert_data_record(self, table_name, values):
        """

        Args:
            table_name:
            values:

        Returns:

        """
        query = f"INSERT INTO {table_name} VALUES {values}"
        self.cursor.execute(query)
        self.db_connection.commit()


def main():
    program = None
    try:
        # 1. Connect to MySQL server on virtual machine
        program = Setup()

        # 2. Create and define the tables User, Activity and TrackPoint
        program.create_user_table()
        program.create_activity_table()
        program.create_trackpoint_table()

        # 3. Inserts the data from the Geolife dataset into the database
        program.insert_data_in_tables()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
           program.connection.close_connection()


if __name__ == "__main__":
    main()