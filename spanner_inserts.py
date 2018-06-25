import uuid
import time
import datetime
import zulu
import base64
from random import randint
from random import random
from google.cloud import spanner


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
        self.firstname = 'string  ' + str(int(dt.timestamp()*100000%100))
        self.lastname = 'string  ' + str(int(dt.timestamp()*100000%99))
        #int fields
        self.sibling_count = randint(0,4)
        self.child_count = randint(0,4)
        #float fields
        self.height = random()
        self.weight = random()
        # date fields
        self.birthdate = datetime.date.today()
        self.account_creation_date = datetime.date.today()
        # array field
        self.given_names = [self.string_field1,self.string_field2]
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
        # assign a friend id from person list
        # I am not checking if person and friend ids are same or no
        self.friend_id = persons[randint(0,len(persons))]
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


# Create a batch of records for Spanner batch input. This method also creates a separate file to store
# IDs for newly created records.
def generate_persons():
    persons_batch = []
    batch_size = 1000
    counter = 0
    # file_name = "file_"+str(batch_id)

    # f = open(file_name,'w+')
    try:
        while counter < batch_size:
            new_person = Person()
            # f.write(new_obj.person_id)
            # f.write('\n')
            persons.append(new_person.person_id)
            persons_batch.append(new_person.return_tup())
            counter += 1
    except IOError:
        print 'Issues in writing file'
    finally:
        # f.close()
        print ''
    return persons_batch


# This method generates a list of friends for the person
def generate_friends(friend_count):
    counter = 0
    friends_batch = []

    # for each person, create random number of friends
    for person in persons:
        top = randint(0,friend_count)

        while counter < top:
            # create a new friend object and add it to batch
            new_friend = Friend(person)
            friends_batch.append(new_friend.return_tup())

            # Just append as a comma separated string so that it can be parsed quickly when reading from file
            friends.append(person + "," + new_friend.friend_id)
            counter += 1
    return friends_batch


# This method generates a list of activities performed by person
def generate_activities(activity_count):
    counter = 0
    activities_batch = []

    # for each person, create random number of friends
    for person in persons:
        top = randint(0, activity_count)

        while counter < top:
            # create a new friend object and add it to batch
            new_activity = Activity(person)
            activities_batch.append(new_activity.return_tup())

            # Just append as a comma separated string so that it can be parsed quickly when reading from file
            activities.append(person + "," + new_activity.activity_id)
            counter += 1
    return activities_batch

# This method generates a list of posts for a given post and activity
def generate_posts(post_count):
    counter = 0
    posts_batch = []

    # for each person, create random number of friends
    for person in persons:
        top = randint(0, post_count)

        while counter < top:
            # create a new friend object and add it to batch
            new_post = Post(person)
            posts_batch.append(new_post.return_tup())

            # Just append as a comma separated string so that it can be parsed quickly when reading from file
            activities.append(person + "," + new_post.activity_id + "," + new_post.post_id)
            counter += 1
    return posts_batch


# Inserts sample data into the given database.
# The database and table must already exist and can be created using
# `create_database`.
def insert_data(instance_id, database_id, spanner_client, table_id, columns, values):
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.batch() as batch:
        batch.insert(
            table=table_id,
            columns=('person_id', 'update_timestamp', 'firstname', 'lastname', 'sibling_count', 'child_count',
                     'base_salary', 'bonus', 'birthdate', 'account_creation_date',
                     'given_names', 'is_active', 'profile_picture'),
            values=values)


# following lists keep a list of generated ids for each table. I will dump them in separate files
persons = []
friends = []
activities = []
posts = []


# main method to run application
def main():
    print 'Start'

    counter = 0
    spanner_client = spanner.Client()
    # This loop will run 1000 batches with 1000 entries each resulting in 1 million records
    # I am inserting records into two different instances because I am trying to validate performance afterwards.
    while counter < 1000:
        persons_batch = generate_persons()

        start_time = time.time()

        # Insert into Persons table
        insert_data(instance_id='instance-1',
                    database_id='db1',
                    spanner_client=spanner_client,
                    table_id="Persons",
                    columns=('person_id', 'update_timestamp', 'firstname', 'lastname', 'sibling_count', 'child_count',
                             'base_salary', 'bonus', 'birthdate', 'account_creation_date', 'given_names', 'is_active',
                             'profile_picture'),
                    values=persons_batch)
        print 'Instance 1: Batch' \
              ' ' + str(counter) + ' finished. Elapsed time : ' + str(time.time() - start_time)
        counter += 1

    # generate friends for each person with max friends = 100
    friends_batch = generate_friends(15)
    # Insert into friends table
    insert_data(instance_id='instance-1',
                database_id='db1',
                spanner_client=spanner_client,
                table_id="Friends",
                columns=('person_id', 'friend_id', 'status', 'connection_date'),
                values=friends_batch)

    # generate activities for each person with max activity_type = 10
    activities_batch = generate_activities(10)
    # Insert into Activities table
    insert_data(instance_id='instance-1',
                database_id='db1',
                spanner_client=spanner_client,
                table_id="Activities",
                columns=('person_id', 'activity_id', 'activity_type'),
                values=activities_batch)

    # generate posts for each person and activity
    posts_batch = generate_posts(1)
    # Insert into Posts table
    insert_data(instance_id='instance-1',
                database_id='db1',
                spanner_client=spanner_client,
                table_id="Posts",
                columns=('person_id', 'activity_id', 'post_id', 'post_content', 'post_timestamp'),
                values=posts_batch)

    print 'End'


# Start application
if __name__ == '__main__':
    main()