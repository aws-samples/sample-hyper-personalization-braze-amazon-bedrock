#!/usr/bin/env python3
import os
import cdk_nag
import aws_cdk as cdk

from braze_cdk.braze_cdk_stack import BrazeCdkStack


app = cdk.App()
BrazeCdkStack(app, "BrazeCdkStack",
    )

app.synth()
