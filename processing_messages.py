import boto3

# Get the service resource
sqs = boto3.resource('sqs')

# Get the queue
queue = sqs.get_queue_by_name(QueueName='spj322')

# Process messages by printing out body and optional author name
for message in queue.receive_messages(MessageAttributeNames=["landmarks.readlines(0)"]):
    # Get the custom author message attribute if it was set
    author_text = ''
    if message.message_attributes is not None:
        author_name = message.message_attributes.get("Author").get('StringValue')
        if author_name:
            author_text = ' ({0})'.format(author_name)

    # Print out the body and author (if set)
    print('Here is the, {0}!{1}'.format(message.body, author_text))

    # Let the queue know that the message is processed
    message.delete() 
   
