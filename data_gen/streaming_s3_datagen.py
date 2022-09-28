import boto3
import json
from datetime import datetime
import time
import string
import random

################################
## MESSAGE CONFIG SETTINGS 
##
## number of messages to push
num_messages = 9000
##
## delay between messages in sec
delay=.1 #100 MS or 10 per sec
##
#s3 target bucket
bucket = '<your-bucket-name>' 
##
#s3 target prefix, aka path
bucket_prefix= '<your-bucket-directory>'
##################################


#create a new s3 resource 
s3 = boto3.resource('s3')

#random date generator function
def random_date(seed):
    random.seed(seed)
    d = random.randint(1, int(time.time()))
    return datetime.fromtimestamp(d).strftime('%Y-%m-%d')

i=1
#now we will create messages for the number of product messages specified in num_messages 
while i <= num_messages:
    json_raw = {
        "transaction_id": f'{i:09}',
        "transaction_date": datetime.now().strftime("%m/%d/%Y %I:%M:%S.%f %p"),
        "vendor_id": random.randint(1, 6),
        "product_id": random.randint(10000000, 99999999),
        "product_price": random.randint(9, 2000),
        "quantity": random.randint(1, 20),
        "product_name": ''.join(random.choices(string.ascii_lowercase, k=random.randint(4, 10))).capitalize(),
        "product_desc": ''.join(random.choices(string.ascii_lowercase, k=random.randint(6, 20))).capitalize()
    }

    # convert into JSON:
    json_data = json.dumps(json_raw)

    # debug to view the result is a JSON object
    #print(json_data)
    #lets send the message to s3 one at a time - eek 
    object = s3.Object(bucket, bucket_prefix + 'json_data_' + str(i) + '.json')
    #print (object)
    object.put(Body=json_data)
    #wait 100MS and repeat 
    time.sleep(delay)
    i +=1


##
## unused code but useful for reference #
##
# Print out bucket names and files 
#response = s3.list_objects_v2(Bucket=bucket, Prefix=bucket_prefix)
#files = response.get("Contents")
#for file in files:
#    print(f"file_name: {file['Key']}, size: {file['Size']}")

#put a local file to s3
##s3 = boto3.resource('s3')    
#s3.Bucket(bucket).upload_file('hello_from_local_2.txt', bucket_prefix + 'hello_from_local_2.txt')
