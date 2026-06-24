# lambda_handler.py
#
# Lambda requires a specific entry point signature: handler(event, context)
# - event:   dict passed by the trigger (EventBridge sends schedule metadata)
# - context: Lambda runtime info (function name, remaining time, etc.)
#            We don't use context here, but it must be in the signature.
#
# When running locally:   python src/pipeline.py
# When running on Lambda: Lambda calls handler(event, context)

import sys
import os

# Lambda unzips the deployment package to /var/task/
# Our source files are at the root of the zip, so they're importable directly.
# No sys.path manipulation needed.

def handler(event, context):
    """
    Lambda entry point.
    EventBridge will call this function once per day.
    """
    print(f"Lambda invoked. Event: {event}")

    try:
        # Import here (not at module level) so import errors surface clearly
        # in CloudWatch logs rather than as cryptic Lambda init failures.
        from pipeline import ETLPipeline

        pipeline = ETLPipeline()
        pipeline.run()

        return {
            'statusCode': 200,
            'body': 'Pipeline completed successfully.'
        }

    except Exception as e:
        print(f"Pipeline failed: {e}")
        # Re-raising causes Lambda to mark the invocation as failed,
        # which triggers CloudWatch alarms and EventBridge retry logic.
        raise
