import utility
import time
import argparse
from google.cloud import spanner

# main method to run application
def main(instance, database, size):
    print 'Start'
    spanner_client = spanner.Client()
    # This loop will run 1000 batches with 1000 entries each resulting in 1 million records
    # I am inserting records into two different instances because I am trying to validate performance afterwards.
    persons_batch = utility.generate_persons(size)
    print 'persons_batch record count : ' + str(len(persons_batch))

    start_time = time.time()
    # Insert into Persons table
    utility.insert_data(instance_id=instance,
                database_id=database,
                spanner_client=spanner_client,
                table_id="Persons",
                columns=('person_id', 'update_timestamp', 'firstname', 'lastname', 'sibling_count', 'child_count',
                         'height', 'weight', 'birthdate', 'account_creation_date', 'given_names', 'is_active',
                         'profile_picture'),
                values=persons_batch)
    print 'Persons upload finished. Elapsed time : ' + str(time.time() - start_time)

    # generate friends for each person with max friends = 15
    friends_batch = utility.generate_friends(15)
    print 'friends_batch record count : ' + str(len(friends_batch))

    start_time = time.time()
    # Insert into friends table
    utility.insert_data(instance_id=instance,
                database_id=database,
                spanner_client=spanner_client,
                table_id="Friends",
                columns=('person_id', 'friend_id', 'status', 'connection_date'),
                values=friends_batch)
    print 'Friends upload finished. Elapsed time : ' + str(time.time() - start_time)

    # generate activities for each person with max activity_type = 10
    activities_batch = utility.generate_activities(10)
    print 'activities_batch record count : ' + str(len(activities_batch))
    start_time = time.time()
    # Insert into Activities table
    utility.insert_data(instance_id=instance,
                database_id=database,
                spanner_client=spanner_client,
                table_id="Activities",
                columns=('person_id', 'activity_id', 'activity_type'),
                values=activities_batch)
    print 'Activities upload finished. Elapsed time : ' + str(time.time() - start_time)

    # generate posts for each person and activity
    posts_batch = utility.generate_posts(1)
    print 'posts_batch record count : ' + str(len(posts_batch))
    start_time = time.time()
    # Insert into Posts table
    utility.insert_data(instance_id=instance,
                database_id=database,
                spanner_client=spanner_client,
                table_id="Posts",
                columns=('person_id', 'activity_id', 'post_id', 'post_content', 'post_timestamp'),
                values=posts_batch)
    print 'Posts upload finished. Elapsed time : ' + str(time.time() - start_time)

    utility.create_key_files()
    print 'Key files created'

    print 'End'


# Start application
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--instance', help='Spanner instance name', required=True)
    parser.add_argument('-d', '--database', help='Spanner database name', required=True)
    parser.add_argument('-s', '--size', help='Size of sample records', required=True)
    args = parser.parse_args()
    print args
    print parser
    main(args.instance, args.database, int(args.size))