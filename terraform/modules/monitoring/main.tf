# ──────────────────────────────────────────────────────────
# CloudWatch Log Group
#
# Lambda在首次运行时自动创建了这个Log Group，但没有设置
# 保留期（默认永久，会持续产生费用）。
# import block让Terraform接管已有资源而不是重新创建，
# 然后把 retention_in_days 设为30天。
# ──────────────────────────────────────────────────────────
import {
  to = aws_cloudwatch_log_group.lambda_logs
  id = "/aws/lambda/nl-rail-etl-pipeline"
}

resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${var.lambda_function_name}"
  retention_in_days = var.log_retention_days

  tags = {
    Environment = var.environment
  }
}


# ──────────────────────────────────────────────────────────
# SNS Topic + Email Subscription
# ──────────────────────────────────────────────────────────
resource "aws_sns_topic" "pipeline_alerts" {
  name = "nl-rail-pipeline-alerts"

  tags = {
    Environment = var.environment
  }
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}


# ──────────────────────────────────────────────────────────
# Metric Filter: 应用级别失败
#
# 从Lambda日志里检测 "Pipeline failed" 字符串，
# 转换成自定义指标 NLRail/ETL::PipelineFailedCount。
# 覆盖Lambda本身不报错但业务逻辑失败的情况。
# ──────────────────────────────────────────────────────────
resource "aws_cloudwatch_log_metric_filter" "pipeline_failed" {
  name           = "nl-rail-pipeline-failed-filter"
  log_group_name = aws_cloudwatch_log_group.lambda_logs.name
  pattern        = "\"Pipeline failed\""

  metric_transformation {
    name          = "PipelineFailedCount"
    namespace     = "NLRail/ETL"
    value         = "1"
    default_value = "0"
  }
}


# ──────────────────────────────────────────────────────────
# Alarm 1: Lambda级别报错
#
# 使用AWS内置指标 AWS/Lambda::Errors。
# 覆盖：未捕获异常、超时、OOM、import失败、re-raise。
# ok_actions: 恢复正常时也发一封邮件确认。
# ──────────────────────────────────────────────────────────
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "nl-rail-lambda-errors"
  alarm_description   = "Lambda invocation returned an error"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching" # 没调用 ≠ 报错，静默失败由Alarm 3负责

  dimensions = {
    FunctionName = var.lambda_function_name
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
  ok_actions    = [aws_sns_topic.pipeline_alerts.arn]

  tags = {
    Environment = var.environment
  }
}


# ──────────────────────────────────────────────────────────
# Alarm 2: 应用级别失败
#
# 使用上面 Metric Filter 产生的自定义指标。
# 双保险：万一Lambda本身返回成功但日志里有 "Pipeline failed"，
# Alarm 1不会触发，这个alarm兜底。
# ──────────────────────────────────────────────────────────
resource "aws_cloudwatch_metric_alarm" "pipeline_failed" {
  alarm_name          = "nl-rail-pipeline-failed"
  alarm_description   = "Pipeline logged a failure message"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "PipelineFailedCount"
  namespace           = "NLRail/ETL"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
  ok_actions    = [aws_sns_topic.pipeline_alerts.arn]

  tags = {
    Environment = var.environment
  }
}


# ──────────────────────────────────────────────────────────
# Alarm 3: 静默失败（Lambda根本没跑）
#
# EventBridge Scheduler被禁用/删除时，Lambda不会报错，
# Alarm 1/2都不会触发。
# 这个alarm检测24小时内Invocations总数 < 1。
# treat_missing_data = "breaching"：
#   没有数据点 = Lambda没运行 = 直接告警。
# ──────────────────────────────────────────────────────────
resource "aws_cloudwatch_metric_alarm" "pipeline_silence" {
  alarm_name          = "nl-rail-pipeline-not-invoked"
  alarm_description   = "Lambda was not invoked in the past 24 hours"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Invocations"
  namespace           = "AWS/Lambda"
  period              = 86400
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "breaching"

  dimensions = {
    FunctionName = var.lambda_function_name
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]

  tags = {
    Environment = var.environment
  }
}