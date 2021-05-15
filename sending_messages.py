import sys
import os
import logging
import psycopg2
import boto3
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

conn = psycopg2.connect(database="postgres", 
			    user="swathihari",
			    password="",
			    host="localhost",
			    port=5432)
			    
			    
   
cur = conn.cursor()

with psycopg2.connect(database="postgres", 
			    user="swathihari",
			    password="",
			    host="localhost",
			    port=5432) as conn:
	with conn.cursor() as cur:
	    with open('data.json', 'a+') as my_data:
	        
	        query_sql_values = """SELECT * FROM landmarks"""
	        
	        (cur.execute(query_sql_values))
	        
	        result = cur.fetchall()
	        
	        my_data = json.dumps(result)
	        
	        print(my_data)
	    
with psycopg2.connect(database="postgres", 
			    user="swathihari",
			    password="",
			    host="localhost",
			    port=5432) as conn:
	with conn.cursor() as cur:
	    with open('closest_landmark_data.json', 'w') as cl_lnmrk_data:
	    
	        
	        query_sql_values = """SELECT
					ST_Distance(ST_GeomFromText('POINT(-87.6348345 41.8786207)', 4326), 
					landmarks.the_geom) AS planar_degrees,
					name,
					architect
					FROM landmarks
					ORDER BY planar_degrees ASC
					LIMIT 5;"""
	        
	        (cur.execute(query_sql_values))
	        
	        result = cur.fetchall()
	        
	        closest_landmark_data = json.dumps(result)
	        
	        print(closest_landmark_data)
	        
	        
	        
	        
cur.close()
conn.close()


with open('Individual_Landmarks.json', 'r') as landmarks:
    # Get the service resource
    sqs = boto3.resource('sqs')

    # Get the queue
    queue = sqs.get_queue_by_name(QueueName='spj322')

    # Create a new message
    response_1 = queue.send_message(MessageBody='landmark_data')

    # The response is NOT a resource, but gives you a message ID and MD5
    print(response_1.get('MessageId'))
    print(response_1.get('MD5OfMessageBody'))


    queue.send_message(MessageBody='boto3', MessageAttributes={
    
    'Author': {
    'StringValue': 'landmarks.readlines(0)',
    'DataType': 'String'
	    }
	})

    response_1 = queue.send_messages(Entries=[	
	    	{
		'Id': '1',
		'MessageBody': 'landmark data'
	    },
	    {
		'Id': '2',
		'MessageBody': 'landmark data',
		'MessageAttributes': {
		    'Author': {
		        'StringValue': landmarks.readlines()[0],
		        'DataType': 'String'
		    }
		}
	    }
	])
	
with open('closest_landmark_data.json', 'w') as cl_landmark_data:

    cl_landmark_data = json.dumps(closest_landmark_data)
    # Get the service resource
    sqs = boto3.resource('sqs')

    # Get the queue
    queue = sqs.get_queue_by_name(QueueName='spj322')

    # Create a new message
    response_2 = queue.send_message(MessageBody='closest landmark data')

    # The response is NOT a resource, but gives you a message ID and MD5
    print(response_2.get('MessageId'))
    print(response_2.get('MD5OfMessageBody'))


    queue.send_message(MessageBody='boto3', MessageAttributes={
    
    'Author': {
    'StringValue': 'closest_landmark_data.readlines(0)',
    'DataType': 'String'
	    }
	})

    response_2 = queue.send_messages(Entries=[	
	    	{
		'Id': '1',
		'MessageBody': 'closest_landmark_data'
	    },
	    {
		'Id': '2',
		'MessageBody': 'closest_landmark_data',
		'MessageAttributes': {
		    'Author': {
		        'StringValue': closest_landmark_data[:],
		        'DataType': 'String'
		    }
		}
	    }
	])

    print(response_1)

    print(response_2)
    # Print out any failures
    print(response_1.get('Failed'))
    print(response_2.get('Failed'))

