import uuid
import time
import datetime
import zulu
import base64
from random import randint
from random import random
from google.cloud import spanner


# This class represents a sample record for a spanner table. I have included columns with all data types.
class ContentRecord(object):
    # Initialize a class
    def __init__(self):
        right_now = zulu.now()
        dt = zulu.parse(right_now)

        self.pk_field = str(uuid.uuid4())
        self.timestamp_field = right_now
        self.string_field1 = 'string  ' + str(int(dt.timestamp()*100000%100))
        self.string_field2 = 'string  ' + str(int(dt.timestamp()*100000%99))
        self.int_field1 = randint(0,20)
        self.int_field2 = randint(0,10)
        self.float_field1 = random()
        self.float_field2 = random()
        self.date_field1 = datetime.date.today()
        self.date_field2 = datetime.date.today()
        self.array_field = [self.string_field1,self.string_field2]
        self.bool_field = randint(0, 10) % 2 == 0
        self.bytes_field = base64.b64encode(self.pk_field.encode('utf-8'))

    # Return all fields combined in a tuple
    def return_tup(self):
        return (self.pk_field,
                self.timestamp_field,
                self.string_field1,
                self.string_field2,
                self.int_field1,
                self.int_field2,
                self.float_field1,
                self.float_field2,
                self.date_field1,
                self.date_field2,
                self.array_field,
                self.bool_field,
                self.bytes_field
                )

    # Print a record
    def print_record(self):
        print self.return_tup()


# Create a batch of records for Spanner batch input. This method also creates a separate file to store
# IDs for newly created records.
def generate_records_batch(batch_id):
    new_batch = []
    batch_size = 1000
    counter = 0
    file_name = "file_"+str(batch_id)

    f = open(file_name,'w+')
    try:
        while counter < batch_size:
            new_obj = ContentRecord()
            f.write(new_obj.pk_field)
            f.write('\n')
            new_batch.append(new_obj.return_tup())
            counter += 1
    except IOError:
        print 'Issues in writing file'
    finally:
        f.close()
    return new_batch


# Inserts sample data into the given database.
# The database and table must already exist and can be created using
# `create_database`.
def insert_data(instance_id, database_id, spanner_client, batch_id,values):
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.batch() as batch:
        batch.insert(
            table='table1',
            columns=('pk_field',
                     'timestamp_field',
                     'string_field1',
                     'string_field2',
                     'int_field1',
                     'int_field2',
                     'float_field1',
                     'float_field2',
                     'date_field1',
                     'date_field2',
                     'array_field',
                     'bool_field',
                     'bytes_field'),
            values=values)


# main method to run application
def main():
    print 'Start'

    counter = 0
    spanner_client = spanner.Client()
    # This loop will run 1000 batches with 1000 entries each resulting in 1 million records
    # I am inserting records into two different instances because I am trying to validate performance afterwards.
    while counter < 1000:
        values = generate_records_batch(counter)
        start_time = time.time()
        insert_data(instance_id='instance-1', database_id='db1', spanner_client=spanner_client, batch_id=counter, values=values)
        print 'Instance 1: Batch '+str(counter)+' finished. Elapsed time : '+str(time.time()-start_time)
        # reset timer
        start_time = time.time()
        insert_data(instance_id='instance-2', database_id='db2', spanner_client=spanner_client, batch_id=counter, values=values)
        print 'Instance 2: Batch '+str(counter)+' finished. Elapsed time : '+str(time.time()-start_time)
        counter += 1
    print 'End'


# Start application
if __name__ == '__main__':
    main()