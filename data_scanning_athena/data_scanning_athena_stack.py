from aws_cdk import (
    Duration,
    Stack,
    Size,
    CfnOutput,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    Tags as tags,
    Aws as AWS,
    aws_glue as glue,
    aws_dynamodb as dynamo,
    aws_events as events,
    aws_events_targets as targets,
    aws_sns_subscriptions as subscriptions,
    aws_sns as sns,
    RemovalPolicy
)
from constructs import Construct
from . import parameters

class DataScanningAthenaStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

#############       S3 Buuckets Configurations      #############

        s3DataLakeBucket = s3.Bucket(self, "s3DataLakeBucket",
                bucket_name = f"{parameters.project}-{parameters.env}-{parameters.app}-{parameters.dataLakeBucketName}-{AWS.REGION}",
                removal_policy=RemovalPolicy.DESTROY
         
        )
        tags.of(s3DataLakeBucket).add("name", f"{parameters.project}-{parameters.env}-{parameters.app}-s3DataLakeBucket")
        tags.of(s3DataLakeBucket).add("project", parameters.project)
        tags.of(s3DataLakeBucket).add("env", parameters.env)
        tags.of(s3DataLakeBucket).add("app", parameters.app)

        s3StagingBucket = s3.Bucket(self, "s3StagingBucket",
                bucket_name = f"{parameters.project}-{parameters.env}-{parameters.app}-{parameters.stagingBucketName}-{AWS.REGION}",
                removal_policy=RemovalPolicy.DESTROY
        )
        tags.of(s3StagingBucket).add("name", f"{parameters.project}-{parameters.env}-{parameters.app}-s3StagingBucket")
        tags.of(s3StagingBucket).add("project", parameters.project)
        tags.of(s3StagingBucket).add("env", parameters.env)
        tags.of(s3StagingBucket).add("app", parameters.app)

        s3SourceBucket = s3.Bucket(self, "s3SourceBucket",
                bucket_name = f"{parameters.project}-{parameters.env}-{parameters.app}-{parameters.sourceBucketName}-{AWS.REGION}",
                removal_policy=RemovalPolicy.DESTROY
        )
        tags.of(s3SourceBucket).add("name", f"{parameters.project}-{parameters.env}-{parameters.app}-s3SourceBucket")
        tags.of(s3SourceBucket).add("project", parameters.project)
        tags.of(s3SourceBucket).add("env", parameters.env)
        tags.of(s3SourceBucket).add("app", parameters.app)

        s3QueryOutputBucket = s3.Bucket(self, "s3QueryOutputBucket",
                bucket_name = f"{parameters.project}-{parameters.env}-{parameters.app}-{parameters.queryOutputBucketName}-{AWS.REGION}",
                removal_policy=RemovalPolicy.DESTROY
        )
        tags.of(s3QueryOutputBucket).add("name", f"{parameters.project}-{parameters.env}-{parameters.app}-s3SourceBucket")
        tags.of(s3QueryOutputBucket).add("project", parameters.project)
        tags.of(s3QueryOutputBucket).add("env", parameters.env)
        tags.of(s3QueryOutputBucket).add("app", parameters.app)     

#############       IAM Roles and Policies Configurations      #############
                          #Lambda Execution Role#
 
        lambdaExecutionRole = iam.Role(self, "lambdaExecutionRole",
            role_name = f"{parameters.project}-{parameters.env}-{parameters.app}-lambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )
        lambdaExecutionRole.add_managed_policy(
             iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
             )
        lambdaExecutionRole.add_managed_policy(
             iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
             )
        tags.of(lambdaExecutionRole).add("name", f"{parameters.project}-{parameters.env}-{parameters.app}-lambdaExecutionRole")
        tags.of(lambdaExecutionRole).add("project", parameters.project)
        tags.of(lambdaExecutionRole).add("env", parameters.env)
        tags.of(lambdaExecutionRole).add("app", parameters.app)
        lambdaExecutionRole.attach_inline_policy(
            iam.Policy(self, 'lambdaExecutionPolicy',
                statements = [
                    iam.PolicyStatement(
                        effect = iam.Effect.ALLOW,
                        actions=[
                            "s3:DescribeJob",
                            "s3:GetObject",
                            "s3:ListBucket",
                            "s3:PutObject",
                        ],
                        resources= [f"arn:aws:s3:::*"]
                    ),
                    iam.PolicyStatement(
                        effect = iam.Effect.ALLOW,
                        actions=[
                            "cloudwatch:GetMetricStatistics",
                            "cloudwatch:ListMetrics",
                            "cloudwatch:PutMetricData",
                            "cloudwatch:GetMetricData",
                            "cloudwatch:GetMetricWidgetImage"
                        ],
                        resources= ["*"]
                    ),
                    iam.PolicyStatement(
                        effect = iam.Effect.ALLOW,
                        actions=[
                            "sns:Publish",
                            "sns:Subscribe",
                            "sns:Unsubscribe"
                        ],
                        resources= ["*"]
                    ),
                    iam.PolicyStatement(
                        effect = iam.Effect.ALLOW,
                        actions=[
                            "athena:StartQueryExecution",
                            "athena:GetQueryExecution",
                            "athena:GetQueryResults"
                        ],
                        resources= ["*"]
                    )

                   
                ]
            )
        )
                               ## Glue Crawler IAM Role ##

        glueCrawlerRole = iam.Role(self, "glueCrawlerRole",
            role_name = f"{parameters.project}-{parameters.env}-{parameters.app}-glueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
        )
        glueCrawlerRole.add_managed_policy(
             iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
             )
        tags.of(glueCrawlerRole).add("name", f"{parameters.project}-{parameters.env}-{parameters.app}-glueCrawlerRole")
        tags.of(glueCrawlerRole).add("project", parameters.project)
        tags.of(glueCrawlerRole).add("env", parameters.env)
        tags.of(glueCrawlerRole).add("app", parameters.app)
        glueCrawlerRole.attach_inline_policy(
            iam.Policy(self, 'glueCrawlerPolicy',
                statements = [
                    iam.PolicyStatement(
                        effect = iam.Effect.ALLOW,
                        actions=[
                            "s3:GetObject",
                            "s3:PutObject"
                        ],
                        resources= [f"arn:aws:s3:::{s3DataLakeBucket.bucket_name}/*"]
                    )
                ]
            )
        )
#############       SNS Topic And Subscription   #############

        snsTopic = sns.Topic(self, "snsTopic",
            topic_name = f"{parameters.project}-{parameters.env}-{parameters.app}-{parameters.snsTopicName}",
        )
        snsSubscription = sns.Subscription(self, "Subscription",
            topic = snsTopic,
            endpoint = parameters.snsSubscriptionEndpoint,
            protocol = sns.SubscriptionProtocol.EMAIL
        )
        tags.of(snsTopic).add("name", f"{parameters.project}-{parameters.env}-{parameters.app}-snsTopic")
        tags.of(snsTopic).add("project", parameters.project)
        tags.of(snsTopic).add("env", parameters.env)
        tags.of(snsTopic).add("app", parameters.app)

#############       AWS Glue Database      #############    
    
        glueDatabase = glue.CfnDatabase(self, "glueDatabase",
            catalog_id = AWS.ACCOUNT_ID,
            database_input = glue.CfnDatabase.DatabaseInputProperty(
                name = f"{parameters.project}-{parameters.env}-{parameters.app}-{parameters.glueDatabaseName}"
            )
        )
#############       AWS Glue Classifier      #############
        glueClassifier = glue.CfnClassifier(self, "glueClassifier",
            csv_classifier=glue.CfnClassifier.CsvClassifierProperty(
                allow_single_column = parameters.classifierAllowSingleColoumn,
                contains_header = parameters.classifierContainsHeader,
                delimiter = parameters.classifierDelimeter,
                disable_value_trimming = parameters.classifierDisableTrimValue,
                header = parameters.classifierHeader,
                name = f"{parameters.project}-{parameters.env}-{parameters.app}-{parameters.glueClassifierName}",
                quote_symbol = parameters.classifierQuoteSymbol
            )
        )

#############       AWS Glue Crawler      #############

        glueCrawler = glue.CfnCrawler(self, "glueCrawler",
            name = f"{parameters.project}-{parameters.env}-{parameters.app}-{parameters.glueCrawlerName}",
            classifiers = [glueClassifier.ref],
            database_name = glueDatabase.ref,
            role = glueCrawlerRole.role_name,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path = f"s3://{s3DataLakeBucket.bucket_name}"
                )]
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior = parameters.glueRecrawlBehavior
            )
        )
        tags.of(glueCrawler).add("name", f"{parameters.project}-{parameters.env}-{parameters.app}-glueCrawler")
        tags.of(glueCrawler).add("project", parameters.project)
        tags.of(glueCrawler).add("env", parameters.env)
        tags.of(glueCrawler).add("app", parameters.app)

#############       AWS DynamoDB     #############

        dynamoDbTable = dynamo.Table(self, "dynamoDbTable",
            table_name = f"{parameters.project}-{parameters.env}-{parameters.app}-{parameters.dynamoDbTableName}",
            partition_key=dynamo.Attribute(
                name = parameters.dynamoPartitionKey,
                type = dynamo.AttributeType(parameters.dynamoPartitionKeyType)
            ),
            table_class = dynamo.TableClass(parameters.dynamoDbTableClass),
            billing_mode = dynamo.BillingMode(parameters.dynamoCapacityMode),
            read_capacity = parameters.dynamoTableReadCapacityUnit,
            write_capacity = parameters.dynamoTableWriteCapacityUnit,
            removal_policy = RemovalPolicy.DESTROY
        )
        tags.of(dynamoDbTable).add("name", f"{parameters.project}-{parameters.env}-{parameters.app}-dynamoDbTable")
        tags.of(dynamoDbTable).add("project", parameters.project)
        tags.of(dynamoDbTable).add("env", parameters.env)
        tags.of(dynamoDbTable).add("app", parameters.app)

#############       Service Lambda Function Configurations      #############        
        #bucket is common for all lambda function
        
        lambdaBucket = s3.Bucket.from_bucket_name(self, "LambdaBucket", parameters.lambdaBucketName)
        lambdaServiceFunction = _lambda.Function(self, "lambdaServiceFunction",
            function_name = f"{parameters.project}-{parameters.env}-{parameters.app}-{parameters.serviceLambdaName}",
            runtime = _lambda.Runtime(parameters.lambdaRuntimeVersion),
            handler = parameters.lambdaFunctionHandler,
            timeout = Duration.minutes(15),
            layers = [_lambda.LayerVersion.from_layer_version_arn(
                self, "AwsSdkPandaLayer",
                f"arn:aws:lambda:{AWS.REGION}:336392948345:layer:AWSSDKPandas-{parameters.lambdaServiceLayerVersion}")],
            memory_size = parameters.lambdaMemorySize,
            ephemeral_storage_size = Size.mebibytes(parameters.lambdaEphemeralStorageSize),
            role = iam.Role.from_role_arn(self, 'importServiceRole', role_arn=lambdaExecutionRole.role_arn),
            environment = {
                  'BUCKET_NAME': s3SourceBucket.bucket_name,
                  'CRAWLER_NAME': glueCrawler.name,
                  'DW_BUCKET_NAME': s3DataLakeBucket.bucket_name,
                  'STAGING_BUCKET_NAME': s3StagingBucket.bucket_name
                   },
            code = _lambda.Code.from_bucket(bucket = lambdaBucket,key = "ServiceLambdaFunction.zip")
        )
        tags.of(lambdaServiceFunction).add("name", f"{parameters.project}-{parameters.env}-{parameters.app}-lambdaServiceFunction")
        tags.of(lambdaServiceFunction).add("project", parameters.project)
        tags.of(lambdaServiceFunction).add("env", parameters.env)
        tags.of(lambdaServiceFunction).add("app", parameters.app)

#############       Notifier Lambda Function Configurations      #############

        lambdaNotifierFunction = _lambda.Function(self, "lambdaNotifierFunction",
            function_name = f"{parameters.project}-{parameters.env}-{parameters.app}-{parameters.notifierLambdaName}",
            runtime = _lambda.Runtime(parameters.lambdaRuntimeVersion),
            handler = parameters.lambdaFunctionHandler,
            timeout = Duration.minutes(15),
            memory_size = parameters.lambdaMemorySize,
            ephemeral_storage_size = Size.mebibytes(parameters.lambdaEphemeralStorageSize),
            role = iam.Role.from_role_arn(self, 'importNotifierRole', role_arn=lambdaExecutionRole.role_arn),
            environment = {
                  'TOPIC_ARN': snsTopic.topic_arn
                   },
            code = _lambda.Code.from_bucket(bucket = lambdaBucket,key = "NotifierLambdaFunction.zip")
        )
        tags.of(lambdaNotifierFunction).add("name", f"{parameters.project}-{parameters.env}-{parameters.app}-lambdaNotifierFunction")
        tags.of(lambdaNotifierFunction).add("project", parameters.project)
        tags.of(lambdaNotifierFunction).add("env", parameters.env)
        tags.of(lambdaNotifierFunction).add("app", parameters.app)

#############       CTAS Processor Lambda Function Configurations      #############

        lambdaProcessorFunction = _lambda.Function(self, "lambdaProcessorFunction",
            function_name = f"{parameters.project}-{parameters.env}-{parameters.app}-{parameters.processorLambdaName}",
            runtime = _lambda.Runtime(parameters.lambdaRuntimeVersion),
            handler = parameters.lambdaFunctionHandler,
            timeout = Duration.minutes(15),
            memory_size = parameters.lambdaMemorySize,
            ephemeral_storage_size = Size.mebibytes(parameters.lambdaEphemeralStorageSize),
            role = iam.Role.from_role_arn(self, 'importProcessorRole', role_arn=lambdaExecutionRole.role_arn),
            environment = {
                  'DATABASE_NAME': glueDatabase.ref,
                  'DYNAMO_DB_TABLE': dynamoDbTable.table_name,
                  'QUERY_OUTPUT_LOCATION': f"s3://{s3QueryOutputBucket.bucket_name}"

                   },
            code = _lambda.Code.from_bucket(bucket = lambdaBucket,key = "ProcessorLambdaFunction.zip")
        )
        tags.of(lambdaProcessorFunction).add("name", f"{parameters.project}-{parameters.env}-{parameters.app}-lambdaProcessorFunction")
        tags.of(lambdaProcessorFunction).add("project", parameters.project)
        tags.of(lambdaProcessorFunction).add("env", parameters.env)
        tags.of(lambdaProcessorFunction).add("app", parameters.app)


#############       EventBridge Rules     #############

        queryStatusRule = events.Rule(self, "queryStatusRule",
            rule_name = f"{parameters.project}-{parameters.env}-{parameters.app}-{parameters.queryStatusRuleName}", 
            event_pattern=events.EventPattern(
                source = ["aws.athena"],
                detail_type = ["Athena Query State Change"],
                detail = {
                    "currentState": ["SUCCEEDED", "FAILED"]
                }
            )
        )
        queryStatusRule.add_target(targets.LambdaFunction(lambdaNotifierFunction))
        tags.of(queryStatusRule).add("name", f"{parameters.project}-{parameters.env}-{parameters.app}-queryStatusRule")
        tags.of(queryStatusRule).add("project", parameters.project)
        tags.of(queryStatusRule).add("env", parameters.env)
        tags.of(queryStatusRule).add("app", parameters.app)

        crawlerStatusRule = events.Rule(self, "crawlerStatusRule",
            rule_name = f"{parameters.project}-{parameters.env}-{parameters.app}-{parameters.crawlerStatusRuleName}", 
            event_pattern=events.EventPattern(
                source = ["aws.glue"],
                detail_type = ["Glue Crawler State Change"],
                detail = {
                    "state": ["Succeeded"]
                }
            )
        )
        crawlerStatusRule.add_target(targets.LambdaFunction(lambdaProcessorFunction))
        tags.of(crawlerStatusRule).add("name", f"{parameters.project}-{parameters.env}-{parameters.app}-crawlerStatusRule")
        tags.of(crawlerStatusRule).add("project", parameters.project)
        tags.of(crawlerStatusRule).add("env", parameters.env)
        tags.of(crawlerStatusRule).add("app", parameters.app)

#############       Output Values      #############

        CfnOutput(self, "s3DataLakeBucketName",
            value = s3DataLakeBucket.bucket_name,
            description = "The S3 DataLake Bucket Name",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-s3DataLakeBucketName"
        )
        CfnOutput(self, "s3StagingBucketName",
            value = s3StagingBucket.bucket_name,
            description = "The S3 Staging Bucket Name",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-s3StagingBucketName"
        )
        CfnOutput(self, "s3SourceBucketName",
            value = s3SourceBucket.bucket_name,
            description = "The S3 Source Bucket Name",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-s3SourceBucketName"
        )
        CfnOutput(self, "s3QueryOutputBucketName",
            value = s3QueryOutputBucket.bucket_name,
            description = "The S3 Query Output Bucket Name",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-s3QueryOutputBucketName"
        )
        CfnOutput(self, "lambdaExecutionRoleArn",
            value = lambdaExecutionRole.role_arn,
            description = "The Lambda Execuction Role ARN",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-lambdaExecutionRoleArn"
        )
        CfnOutput(self, "glueCrawlerRoleArn",
            value = glueCrawlerRole.role_arn,
            description = "The Glue Crawler Role ARN",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-glueCrawlerRoleArn"
        )
        CfnOutput(self, "snsTopicName",
            value = snsTopic.topic_name,
            description = "The SNS Topic Name",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-snsTopicName"
        )
        CfnOutput(self, "glueDatabaseName",
            value = glueDatabase.ref,
            description = "The Glue Database Name",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-glueDatabaseName"
        )
        CfnOutput(self, "glueClassifierName",
            value = glueClassifier.ref,
            description = "The Glue Classifier Name",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-glueClassifierName"
        )
        CfnOutput(self, "glueCrawlerName",
            value = glueCrawler.name,
            description = "The Glue Crawler Name",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-glueCrawlerName"
        )
        CfnOutput(self, "dynamoDbTableName",
            value = dynamoDbTable.table_name,
            description = "The DynamoDB Table Name",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-dynamoDbTableName"
        )
        CfnOutput(self, "lambdaServiceFunctionName",
            value = lambdaServiceFunction.function_name,
            description = "The Lambda Service Function Name",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-lambdaServiceFunctionName"
        )
        CfnOutput(self, "lambdaNotifierFunctionName",
            value = lambdaNotifierFunction.function_name,
            description = "The Lambda Notifier Function Name",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-lambdaNotifierFunctionName"
        )
        CfnOutput(self, "lambdaProcessorFunctionName",
            value = lambdaProcessorFunction.function_name,
            description = "The Lambda Processor Function Name",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-lambdaProcessorFunctionName"
        )
        CfnOutput(self, "queryStatusRuleName",
            value = queryStatusRule.rule_name,
            description = "The Query Status Rule Name",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-queryStatusRuleName"
        )
        CfnOutput(self, "crawlerStatusRuleName",
            value = crawlerStatusRule.rule_name,
            description = "The Crawler Status Rule Name",
            export_name = f"{parameters.project}-{parameters.env}-{parameters.app}-crawlerStatusRuleName"
        )

