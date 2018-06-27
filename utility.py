import datamodel
from random import randint

# following lists keep a list of generated ids for each table. I will dump them in separate files
persons = []
friends = []
activities = []
posts = []


# Create a batch of records for Spanner batch input. This method also creates a separate file to store
# IDs for newly created records.
def generate_persons(size):
    persons_batch = []
    counter = 0
    try:
        while counter < size:
            new_person = datamodel.Person()
            persons.append(new_person.person_id)
            persons_batch.append(new_person.return_tup())
            counter += 1
    except IOError:
        print 'Issues in writing file'
    return persons_batch


# This method generates a list of friends for the person
def generate_friends(friend_count):
    counter = 0
    friends_batch = []
    print 'persons record count : ' + str(len(persons))

    # for each person, create random number of friends
    for person in persons:
        top = randint(0,friend_count)
        while counter < top:
            # create a new friend object and add it to batch
            new_friend = datamodel.Friend(person)
            friends_batch.append(new_friend.return_tup())
            # Just append as a comma separated string so that it can be parsed quickly when reading from file
            friends.append(person + "," + new_friend.friend_id)
            counter += 1
        # reset the counter for new person
        counter = 0
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
            new_activity = datamodel.Activity(person)
            activities_batch.append(new_activity.return_tup())
            # Just append as a comma separated string so that it can be parsed quickly when reading from file
            activities.append(person + "," + new_activity.activity_id)
            counter += 1
        # reset the counter for new person
        counter = 0
    return activities_batch


# This method generates a list of posts for a given post and activity
def generate_posts(post_count):
    counter = 0
    posts_batch = []

    # for each person, create random number of friends
    for activity in activities:
        top = randint(0, post_count)
        splits = activity.split(',')
        while counter < top:
            # create a new friend object and add it to batch
            new_post = datamodel.Post(splits[0], splits[1])
            posts_batch.append(new_post.return_tup())

            # Just append as a comma separated string so that it can be parsed quickly when reading from file
            posts.append(activity + "," + new_post.post_id)
            counter += 1
        # reset the counter for new person
        counter = 0
    return posts_batch


# Inserts sample data into the given database.
# The database and table must already exist and can be created using `create_database`.
def insert_data(instance_id, database_id, spanner_client, table_id, columns, values):
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    load_size = len(values)
    # for smaller collections, just insert in one batch
    if load_size <= 1000:
        with database.batch() as batch:
            batch.insert(table=table_id, columns=columns, values=values)
    # for larger collections, split them in micro batches of 1000s
    else:
        start = 0
        stop = 999

        while stop <= load_size - 1:
            print 'start : ' + str(start)
            print 'stop : '+str(stop)
            with database.batch() as batch:
                batch.insert(table=table_id, columns=columns, values=values[start:stop+1])
            start += 1000
            stop += 1000
            if start >= load_size:
                break
            if stop > load_size:
                stop = load_size - 1


# dump Ids in key files for further querying
def create_key_files():
    file_name = "players"
    f = open(file_name, 'w+')
    for person in persons:
        f.write(person)
        f.write('\n')
    f.close()

    file_name = "friends"
    f = open(file_name, 'w+')
    for friend in friends:
        f.write(friend)
        f.write('\n')
    f.close()

    file_name = "activities"
    f = open(file_name, 'w+')
    for activity in activities:
        f.write(activity)
        f.write('\n')
    f.close()

    file_name = "posts"
    f = open(file_name, 'w+')
    for post in posts:
        f.write(post)
        f.write('\n')
    f.close()