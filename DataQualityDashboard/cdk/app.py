#!/usr/bin/env python3
"""
AWS CDK App for Data Quality Dashboard
Infrastructure as Code deployment
"""

import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_apigateway as apigateway,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_secretsmanager as secretsmanager,
    aws_iam as iam,
    aws_logs as logs,
    Duration,
    RemovalPolicy
)
from constructs import Construct
import json

class DataQualityDashboardStack(Stack):
    """CDK Stack for Data Quality Dashboard with complete infrastructure"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create VPC for secure networking
        vpc = ec2.Vpc(
            self, "DataQualityVPC",
            max_azs=2,
            nat_gateways=1,
            cidr="10.0.0.0/16",
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24
                ),
                ec2.SubnetConfiguration(
                    name="Database",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24
                )
            ]
        )

        # Create security group for RDS
        db_security_group = ec2.SecurityGroup(
            self, "DatabaseSecurityGroup",
            vpc=vpc,
            description="Security group for RDS PostgreSQL",
            allow_all_outbound=False
        )

        # Create security group for Lambda
        lambda_security_group = ec2.SecurityGroup(
            self, "LambdaSecurityGroup",
            vpc=vpc,
            description="Security group for Lambda functions",
            allow_all_outbound=True
        )

        # Allow Lambda to connect to RDS
        db_security_group.add_ingress_rule(
            peer=lambda_security_group,
            connection=ec2.Port.tcp(5432),
            description="Allow Lambda to connect to PostgreSQL"
        )

        # Create database credentials secret
        db_credentials = secretsmanager.Secret(
            self, "DatabaseCredentials",
            description="Credentials for Data Quality Dashboard database",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template=json.dumps({"username": "admin"}),
                generate_string_key="password",
                exclude_characters=' %+~`#$&*()|[]{}:;<>?!\'/@"\\',
                password_length=32
            )
        )

        # Create RDS PostgreSQL instance
        database = rds.DatabaseInstance(
            self, "DataQualityDatabase",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15_4
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3,
                ec2.InstanceSize.MICRO
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ),
            security_groups=[db_security_group],
            credentials=rds.Credentials.from_secret(db_credentials),
            database_name="dataquality",
            allocated_storage=20,
            storage_type=rds.StorageType.GP2,
            backup_retention=Duration.days(7),
            deletion_protection=False,  # Set to True for production
            removal_policy=RemovalPolicy.DESTROY  # Change for production
        )

        # Create session secret
        session_secret = secretsmanager.Secret(
            self, "SessionSecret",
            description="Flask session secret key",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                password_length=64,
                exclude_characters=' %+~`#$&*()|[]{}:;<>?!\'/@"\\'
            )
        )

        # Create Lambda execution role
        lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        # Grant Lambda access to secrets
        db_credentials.grant_read(lambda_role)
        session_secret.grant_read(lambda_role)

        # Create Lambda function
        lambda_function = _lambda.Function(
            self, "DataQualityDashboardFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="wsgi_handler.handler",
            code=_lambda.Code.from_asset("../", 
                exclude=[
                    "cdk/**",
                    ".git/**",
                    "__pycache__/**",
                    "*.pyc",
                    ".env*",
                    "venv/**",
                    "node_modules/**",
                    ".replit",
                    "uv.lock"
                ]
            ),
            environment={
                "PGHOST": database.instance_endpoint.hostname,
                "PGPORT": "5432",
                "PGDATABASE": "dataquality",
                "DB_CREDENTIALS_SECRET": db_credentials.secret_arn,
                "SESSION_SECRET_ARN": session_secret.secret_arn,
                "FLASK_DEBUG": "false",
                "LOG_LEVEL": "INFO"
            },
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            security_groups=[lambda_security_group],
            timeout=Duration.seconds(30),
            memory_size=1024,
            role=lambda_role,
            log_retention=logs.RetentionDays.ONE_MONTH
        )

        # Create API Gateway
        api = apigateway.LambdaRestApi(
            self, "DataQualityDashboardAPI",
            handler=lambda_function,
            proxy=True,
            description="Data Quality Dashboard API",
            deploy_options=apigateway.StageOptions(
                stage_name="prod",
                throttling_rate_limit=100,
                throttling_burst_limit=200,
                logging_level=apigateway.MethodLoggingLevel.INFO,
                data_trace_enabled=True,
                metrics_enabled=True
            )
        )

        # Create CloudWatch dashboard
        from aws_cdk import aws_cloudwatch as cloudwatch
        
        dashboard = cloudwatch.Dashboard(
            self, "DataQualityDashboard",
            dashboard_name="DataQualityDashboard-Monitoring"
        )

        # Add Lambda metrics to dashboard
        dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="Lambda Function Metrics",
                left=[
                    lambda_function.metric_invocations(),
                    lambda_function.metric_errors(),
                    lambda_function.metric_duration()
                ]
            ),
            cloudwatch.GraphWidget(
                title="API Gateway Metrics",
                left=[
                    api.metric_count(),
                    api.metric_latency(),
                    api.metric_client_error(),
                    api.metric_server_error()
                ]
            )
        )

        # Output important values
        cdk.CfnOutput(
            self, "APIGatewayURL",
            value=api.url,
            description="API Gateway endpoint URL"
        )

        cdk.CfnOutput(
            self, "DatabaseEndpoint",
            value=database.instance_endpoint.hostname,
            description="RDS PostgreSQL endpoint"
        )

        cdk.CfnOutput(
            self, "DatabaseCredentialsSecret",
            value=db_credentials.secret_arn,
            description="ARN of the database credentials secret"
        )

app = cdk.App()
DataQualityDashboardStack(
    app, "DataQualityDashboardStack",
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region")
    ),
    description="Data Quality Dashboard infrastructure with Lambda, API Gateway, and RDS"
)

app.synth()