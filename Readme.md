This application allows users to generate and insert records into Cloud Spanner.

Following are key characteristics of this application:
<ul>
<li>There're interleaved tables being used. </li>
<li>Parent table Persons has contains all Cloud Spanner data types.</li>
<li>Application takes 3-4 minutes to insert all tables when you select 100k records for parent.</li>
<li>Application assumes you use Default Application Credentials for GCP.</li>
</ul>

Please run following DDL on Cloud Spanner console to create a schema:

CREATE TABLE Persons (
	person_id STRING(200) NOT NULL,
	update_timestamp TIMESTAMP,
	firstname STRING(MAX),
	lastname STRING(MAX),
	sibling_count INT64,
	child_count INT64,
	height FLOAT64,
	weight FLOAT64,
	birthdate DATE,
	account_creation_date DATE,
	given_names ARRAY<STRING(MAX)>,
	is_active BOOL,
	profile_picture BYTES(MAX),
) PRIMARY KEY (person_id);


CREATE TABLE Friends (
person_id STRING(200) NOT NULL,
friend_id  STRING(200) NOT NULL,
status STRING(20),
connection_date  DATE,
) PRIMARY KEY (person_id, friend_id),
INTERLEAVE IN PARENT Persons ON DELETE CASCADE;


CREATE TABLE Activities (
person_id STRING(200)  NOT NULL,
activity_id  STRING(200)  NOT NULL,
activity_type  INT64 NOT NULL,
) PRIMARY KEY (person_id, activity_id),
INTERLEAVE IN PARENT Persons ON DELETE CASCADE;

CREATE TABLE Posts (
person_id STRING(200) NOT NULL,
activity_id  STRING(200) NOT NULL,
post_id  STRING(200) NOT NULL,
post_content  BYTES(MAX),
post_timestamp TIMESTAMP,
) PRIMARY KEY (person_id, activity_id, post_id) ,
INTERLEAVE IN PARENT Activities ON DELETE CASCADE;
 
 

**Clean up:**
gcloud spanner databases ddl update $DATABASE_NAME --instance=$INSTANCE_NAME --ddl='drop table Posts'
gcloud spanner databases ddl update $DATABASE_NAME --instance=$INSTANCE_NAME --ddl='drop table Activities'
gcloud spanner databases ddl update $DATABASE_NAME --instance=$INSTANCE_NAME --ddl='drop table Friends'
gcloud spanner databases ddl update $DATABASE_NAME --instance=$INSTANCE_NAME --ddl='drop table Persons'


