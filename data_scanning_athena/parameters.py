project = "athenablog"             #Project name
env = "dev"                      #Environment name
app = "app"                     #App name

 
###   S3 Buckets Parameters   ###
 
dataLakeBucketName = "employdataset-datalake"   #Name of the datalake employ dataset datalake bucket
sourceBucketName = "employdataset-source" #Name of the employ dataset source bucket
stagingBucketName = "employdataset-staging" #Name of the employ dataset staging bucket
queryOutputBucketName = "athena-blog-query-output" ##Name of the athena query output bucket

###   Lambda Function Parameters   ###

serviceLambdaName = "data-service-function" #Data service lambda function name
notifierLambdaName = "query-status-notify-function" #Query status notify lambda function name
processorLambdaName = "ctas-processor-function" #CTAS processor lambda function name
lambdaBucketName = "athena-blog-artifacts-us-east-1-570720296911" #Premade bucket name to store the lambda(python) code files 
lambdaFunctionHandler = "lambda_function.lambda_handler" #Lambda Function Handler
lambdaRuntimeVersion = "python3.12" #Lambda function version
lambdaServiceLayerVersion = "Python312:8" #Lambda layer version
lambdaMemorySize = 10240 #Lambda memory size
lambdaEphemeralStorageSize = 10240 #Lambda storage size

###   AWS Glue Parameters   ###

glueDatabaseName = "employee-database" #Amazon glue database name
glueClassifierName = "employee-dataset-classifier" #Amazon glue classifier name
classifierDelimeter = ','
classifierQuoteSymbol = '"'
classifierAllowSingleColoumn = False #Allow the single coloumn or not
classifierDisableTrimValue = False #Disable the trim value or not
classifierContainsHeader = "PRESENT"
classifierHeader = ["dept_no", "dept_name"]
glueCrawlerName = "employee-db-crawler" #Amazon glue crawler name
glueRecrawlBehavior = "CRAWL_EVERYTHING"

###   DynamoDB Parameters   ###

dynamoPartitionKey = "table_name" #dynamodb partition key
dynamoDbTableName = "employee-database-CTAS-quries" #dynamodb table name
dynamoPartitionKeyType = "STRING"
dynamoCapacityMode = "PROVISIONED"
dynamoDbTableClass = "STANDARD"
dynamoTableWriteCapacityUnit = 1 #Specify the write capacity unit for dynamodb table
dynamoTableReadCapacityUnit = 1 #Specify the read capacity unit for dynamodb table

###   Event Rules Parameters   ###

queryStatusRuleName = "athena-query-status" #Specify the athena query status rule name
crawlerStatusRuleName = "employee-dataset-crawler-status" #Specify the crawler status rule name

###   SNS Topic   ###

snsTopicName = "data-update-notification" #Specify the topic name
snsSubscriptionEndpoint = "zain.ali@netsoltech.com" #Specify the subscription endpoint email