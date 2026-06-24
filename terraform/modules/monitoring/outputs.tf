output "sns_topic_arn" {
  description = "ARN of the SNS alert topic"
  value       = aws_sns_topic.pipeline_alerts.arn
}

output "log_group_name" {
  description = "CloudWatch Log Group name"
  value       = aws_cloudwatch_log_group.lambda_logs.name
}

output "alarm_names" {
  description = "Names of all CloudWatch alarms created"
  value = [
    aws_cloudwatch_metric_alarm.lambda_errors.alarm_name,
    aws_cloudwatch_metric_alarm.pipeline_failed.alarm_name,
    aws_cloudwatch_metric_alarm.pipeline_silence.alarm_name,
  ]
}