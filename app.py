#!/usr/bin/env python3
import os

import aws_cdk as cdk

from data_scanning_athena.data_scanning_athena_stack import DataScanningAthenaStack
from aws_cdk import (
    Aws as AWS
)

app = cdk.App()
DataScanningAthenaStack(app, "athenablog-dev-app-main-stack",

    env=cdk.Environment(account=AWS.ACCOUNT_ID, region=AWS.REGION)

    )

app.synth()
