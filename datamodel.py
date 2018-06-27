import uuid
import datetime
import zulu
import base64
from random import randint
from random import random

# PLEASE NOTE: Goal of this app is not to demonstrate Python object model. I want to spend time on actually
# building a tool that can ingest reasonable sample data into Spanner database.
# This class represents a Person record for a spanner table. I have included columns with all data types.
class Person(object):
    # Initialize a class
    def __init__(self):
        right_now = zulu.now()
        dt = zulu.parse(right_now)

        # string field
        self.person_id = str(uuid.uuid4())
        # timestamp
        self.update_timestamp = right_now
        # string fields
        self.firstname = 'fn ' + str(int(dt.timestamp()*100000%100))
        self.lastname = 'ln ' + str(int(dt.timestamp()*100000%99))
        # int fields
        self.sibling_count = randint(0,4)
        self.child_count = randint(0,4)
        # float fields
        self.height = random() * 100
        self.weight = random() * 100
        # date fields
        self.birthdate = datetime.date.today() - datetime.timedelta(days=randint(10000,20000))
        self.account_creation_date = datetime.date.today() - datetime.timedelta(days=randint(1,1000))
        # array field
        self.given_names = [self.firstname,self.lastname]
        # bool field
        self.is_active = randint(0, 10) % 2 == 0
        # byte field
        self.profile_picture = base64.b64encode(self.person_id.encode('utf-8'))

    # return a tuple containing all the fields
    def return_tup(self):
        return (self.person_id, self.update_timestamp, self.firstname, self.lastname, self.sibling_count,
                self.child_count, self.height, self.weight, self.birthdate, self.account_creation_date,
                self.given_names, self.is_active, self.profile_picture
                )

    # Print a record
    def print_record(self):
        print self.return_tup()


# This class represents a Friend record for a spanner table.
class Friend(object):
    # Initialize a class
    def __init__(self, person_id):
        self.person_id = person_id
        # assign a friend id -- PLEASE NOTE: I am assigning new unique. NOT from persons table.
        self.friend_id = str(uuid.uuid4())
        self.status = ["Friends", "Invitation Sent", "Invitation Received", "Blocked"][randint(0, 10) % 4]
        self.connection_date = datetime.date.today()

    # return a tuple containing all the fields
    def return_tup(self):
        return self.person_id, self.friend_id, self.status, self.connection_date


# This class represents Activity record for a spanner table
class Activity(object):
    # Initialize a class
    def __init__(self, person_id):
        self.person_id = person_id
        self.activity_id =  str(uuid.uuid4())
        # type of activity
        # let's assume type = 3 is posts
        self.activity_type = 3

    # return a tuple containing all the fields
    def return_tup(self):
        return self.person_id, self.activity_id, self.activity_type


# This class represents Post activity record for a spanner table
class Post(object):
    def __init__(self, person_id, activity_id):
        self.person_id = person_id
        self.activity_id = activity_id
        self.post_id = str(uuid.uuid4())
        self.post_content = base64.b64encode(self.person_id.encode('utf-8'))
        self.post_timestamp = zulu.now()

    # return a tuple containing all fields
    def return_tup(self):
        return self.person_id, self.activity_id, self.post_id, self.post_content, self.post_timestamp
