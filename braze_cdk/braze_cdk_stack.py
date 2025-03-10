from aws_cdk import (
    aws_wafv2 as wafv2,
    Stack,
    Duration,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_apigateway as apigateway,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_logs as logs,
    aws_cognito as cognito
)
from constructs import Construct

class BrazeCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        # Create S3 bucket for logs
        log_bucket = s3.Bucket(
            self,
            "BrazeCurrentsExportLogBucket",
            removal_policy=RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True
        )

        # Create S3 bucket with server access logging enabled
        bucket = s3.Bucket(
            self, 
            "BrazeCurrentsExportBucket",
            removal_policy=RemovalPolicy.DESTROY,
            server_access_logs_bucket=log_bucket,
            server_access_logs_prefix="access-logs/",
            enforce_ssl=True
        )

        # Create Lambda layer for fastavro
        fastavro_layer = lambda_.LayerVersion(
            self,
            "FastavroLayer",
            code=lambda_.Code.from_asset("braze_cdk/lambda_layer/fastavro_layer.zip"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Fastavro library layer"
        )

        # Create Lambda function
        lambda_fn = lambda_.Function(
            self,
            "BrazeCurrentsProcessor",
            runtime=lambda_.Runtime.PYTHON_3_12,  # Python 3.13 not yet available in AWS Lambda
            handler="index.lambda_handler",
            code=lambda_.Code.from_asset("braze_cdk/lambda"),
            layers=[fastavro_layer],
            timeout=Duration.seconds(30)
        )

        # Create DynamoDB table
        table = dynamodb.Table(
            self,
            "BrazeUserPersonalization",
            table_name="braze_user_personalization",
            partition_key=dynamodb.Attribute(
                name="user_id",
                type=dynamodb.AttributeType.STRING
            ),
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery=True
        )

        # Add custom Bedrock permissions to Lambda role
        lambda_fn.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "bedrock:InvokeModel",
                "bedrock:ListFoundationModels"
            ],
            resources=["arn:aws:bedrock:*::foundation-model/anthropic.claude-v2"]
        ))

        # Add DynamoDB permissions to Lambda role
        table.grant_write_data(lambda_fn)

        # Create Cognito User Pool
        user_pool = cognito.UserPool(
            self, "BrazeApiUserPool",
            self_sign_up_enabled=False,
            sign_in_aliases=cognito.SignInAliases(email=True),
            password_policy=cognito.PasswordPolicy(
                min_length=8,
                require_lowercase=True,
                require_uppercase=True,
                require_digits=True,
                require_symbols=True
            ),
            mfa=cognito.Mfa.REQUIRED,
            mfa_second_factor=cognito.MfaSecondFactor(
                sms=True,
                otp=True
            ),
            advanced_security_mode=cognito.AdvancedSecurityMode.ENFORCED
        )

        # Create User Pool Client
        user_pool_client = user_pool.add_client("BrazeApiClient",
            generate_secret=True,
            auth_flows=cognito.AuthFlow(
                user_password=True,
                user_srp=True
            ),
            o_auth=cognito.OAuthSettings(
                flows=cognito.OAuthFlows(
                    client_credentials=True
                ),
                scopes=[cognito.OAuthScope.custom("braze-api/read")]
            )
        )

        # Create API Gateway
        # Create log group for API Gateway access logs
        api_log_group = logs.LogGroup(
            self, "ApiAccessLogs",
            retention=logs.RetentionDays.ONE_MONTH
        )

        # Create WAFv2 Web ACL
        web_acl = wafv2.CfnWebACL(
            self, "ApiWAF",
            default_action=wafv2.CfnWebACL.DefaultActionProperty(allow={}),
            scope="REGIONAL",
            visibility_config=wafv2.CfnWebACL.VisibilityConfigProperty(
                cloud_watch_metrics_enabled=True,
                metric_name="ApiWafMetrics",
                sampled_requests_enabled=True
            ),
            rules=[
                wafv2.CfnWebACL.RuleProperty(
                    name="LimitRequests",
                    priority=1,
                    statement=wafv2.CfnWebACL.StatementProperty(
                        rate_based_statement=wafv2.CfnWebACL.RateBasedStatementProperty(
                            limit=2000,
                            aggregate_key_type="IP"
                        )
                    ),
                    visibility_config=wafv2.CfnWebACL.VisibilityConfigProperty(
                        cloud_watch_metrics_enabled=True,
                        metric_name="LimitRequestsRule",
                        sampled_requests_enabled=True
                    ),
                    override_action=wafv2.CfnWebACL.OverrideActionProperty(none={})
                )
            ]
        )

        # Create Cognito Authorizer
        auth = apigateway.CognitoUserPoolsAuthorizer(
            self, "BrazeApiAuthorizer",
            cognito_user_pools=[user_pool]
        )

        api = apigateway.RestApi(
            self, 'BrazePersonalizationApi',
            rest_api_name='Braze Personalization API',
            deploy_options=apigateway.StageOptions(
                stage_name='prod',
                access_log_destination=apigateway.LogGroupLogDestination(api_log_group),
                access_log_format=apigateway.AccessLogFormat.clf(),
                logging_level=apigateway.MethodLoggingLevel.INFO,
                data_trace_enabled=True,
            )
        )

       
        
        # Associate WAF WebACL with the API Gateway stage
        wafv2.CfnWebACLAssociation(
            self, "ApiWafAssociation",
            resource_arn=f"arn:aws:apigateway:{self.region}:{self.account}:/restapis/{api.rest_api_id}/stages/prod",
            web_acl_arn=web_acl.attr_arn
        )

        # Create integration with DynamoDB
        dynamo_integration = apigateway.AwsIntegration(
            service='dynamodb',
            action='GetItem',
            options=apigateway.IntegrationOptions(
                credentials_role=iam.Role(
                    self, 'DynamoGetItemRole',
                    assumed_by=iam.ServicePrincipal('apigateway.amazonaws.com'),
                    inline_policies={
                        'DynamoGetItemPolicy': iam.PolicyDocument(
                            statements=[
                                iam.PolicyStatement(
                                    actions=['dynamodb:GetItem'],
                                    resources=[table.table_arn]
                                )
                            ]
                        )
                    }
                ),
                request_templates={
                    'application/json': '{' +
                    '"TableName": "braze_user_personalization",' +
                    '"Key":{"user_id": {' +
                    '"S": "$input.params(\'user_id\')"' +
                    '}}}'
                },
                integration_responses=[{
                    'statusCode': '200',
                    'responseTemplates': {
                        'application/json': '$input.json(\'$\')'
                    }
                }]
            )
        )

        # Add GET method to root resource
        api.root.add_method(
            'GET',
            dynamo_integration,
            method_responses=[apigateway.MethodResponse(status_code='200')],
            request_parameters={
                'method.request.querystring.user_id': True,
                'method.request.header.Authorization': True
            },
            request_validator_options=apigateway.RequestValidatorOptions(
                validate_request_parameters=True,
                validate_request_body=False
            ),
            authorizer=auth,
            authorization_type=apigateway.AuthorizationType.COGNITO,
            authorization_scopes=["braze-api/read"]
        )

        # Add S3 notification to trigger Lambda
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(lambda_fn)
        )

        # Grant read permissions to Lambda
        bucket.grant_read(lambda_fn)
     
