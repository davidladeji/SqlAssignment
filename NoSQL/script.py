import boto3

s3 = boto3.resource('s3',
 aws_access_key_id='AKIAZQH5ER33MBZJYRFM',
 aws_secret_access_key='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'
)

try:
	s3.create_bucket(Bucket='data-bucket-bylads2', CreateBucketConfiguration={
	'LocationConstraint': 'us-west-2'})
except Exception as e:
	print(e)

bucket = s3.Bucket("data-bucket-bylads2")

bucket.Acl().put(ACL='public-read')

body = open("C:\\Users\\dlade\\OneDrive\\Desktop\\Cloud assignments\\SqlAssignment\\NoSQL\\files\\exp1.csv", 'rb')

o = s3.Object('data-bucket-bylads2', 'test').put(Body=body)

s3.Object('data-bucket-bylads2', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
	region_name='us-west-2',
	aws_access_key_id='AKIAZQH5ER33MBZJYRFM',
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
except Exception as e:
	print (e)
	#if there is an exception, the table may already exist. if so...
	table = dyndb.Table("DataTable")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)

import csv

with open('C:\\Users\\dlade\\OneDrive\\Desktop\\Cloud assignments\\SqlAssignment\\NoSQL\\files\\experiments.csv', 'rt') as csvfile:
	csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
	count = 0
	for item in csvf:
		count = count + 1
		if (count == 1):
			continue
		print(item)
		body = open('C:\\Users\\dlade\\OneDrive\\Desktop\\Cloud assignments\\SqlAssignment\\NoSQL\\files\\'+item[4], 'rb')
		s3.Object('data-bucket-bylads2', item[4]).put(Body=body )
		md = s3.Object('data-bucket-bylads2', item[4]).Acl().put(ACL='public-read')

		url = "https://s3-us-west-2.amazonaws.com/data-bucket-bylads2/"+item[4]
		metadata_item = {'PartitionKey': item[4], 'RowKey': item[0], 'Temp': item[1],
		'Concentration': item[3], 'Conductivity': item[2], 'url':url}

		try:
			table.put_item(Item=metadata_item)
		except Exception as e:
			print("item may already be there or another failure")
			print(e)

response = table.get_item(
	Key={
	'PartitionKey': 'exp3.csv',
	'RowKey': '3'
	}
)
item = response['Item']
print(item)

print(response)
