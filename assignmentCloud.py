import boto3
import csv

s3 = boto3.resource('s3',
 aws_access_key_id='AKIARZTUCNKQQFO2SSPW',
 aws_secret_access_key='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'
)

try:
    s3.create_bucket(Bucket='dmhgftiucvg642957556677', CreateBucketConfiguration={
    'LocationConstraint': 'us-west-2'})
except Exception as e:
    print("this may already exist")

bucket = s3.Bucket("dmhgftiucvg642957556677")
bucket.Acl().put(ACL='public-read')

body = open('experiments.csv', 'rb')
o = s3.Object('dmhgftiucvg642957556677', 'test.csv').put(Body=body)
s3.Object('dmhgftiucvg642957556677', 'test.csv').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
 region_name='us-west-2',
 aws_access_key_id='AKIARZTUCNKQQFO2SSPW',
 aws_secret_access_key='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'
)

try:
    table = dyndb.create_table(
    TableName='DataTable',
    KeySchema=[
    {
    'AttributeName': 'PartitionKey',
    'KeyType': 'HASH'
    },
    {
    'AttributeName': 'RowKey',
    'KeyType': 'RANGE'
    }
    ],
    AttributeDefinitions=[
    {
    'AttributeName': 'PartitionKey',
    'AttributeType': 'S'
    },
    {
    'AttributeName': 'RowKey',
    'AttributeType': 'S'
    },
    ],
    ProvisionedThroughput={
    'ReadCapacityUnits': 5,
    'WriteCapacityUnits': 5
    }
    )
except:
    #if there is an exception, the table may already exist. if so...
    table = dyndb.Table("DataTable")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)

with open('experiments.csv') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    headers = next(csvf, None)
    for item in csvf:
        print(item)
        body = open(item[3], 'rb')
        s3.Object('dmhgftiucvg642957556677', item[3]).put(Body=body )
        md = s3.Object('dmhgftiucvg642957556677', item[3]).Acl().put(ACL='public-read')

        url = " https://s3-us-west-2.amazonaws.com/dmhgftiucvg642957556677/"+item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
        'description' : item[4], 'date' : item[2], 'url':url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

response = table.get_item(
 Key={
 'PartitionKey': 'The second',
 'RowKey': '2'
 }
)
item = response['Item']
print(item)

