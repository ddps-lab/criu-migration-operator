# EventBridge rule for spot instance interruption warnings
resource "aws_cloudwatch_event_rule" "spot_interruption" {
  name        = "${var.prefix}-spot-interruption-rule"
  description = "Capture EC2 spot instance interruption warnings"

  event_pattern = jsonencode({
    source      = ["aws.ec2"]
    detail-type = ["EC2 Spot Instance Interruption Warning"]
    detail = {
      instance-action = ["terminate"]
    }
  })

  tags = merge(
    var.tags,
    {
      "Purpose" = "criu-spot-monitoring"
    }
  )
}

# EventBridge Target (webhook)
# NOTE: Initially commented out - uncomment after EventBridge consumer is deployed
# The webhook URL will be the LoadBalancer DNS of the eventbridge-consumer service
# resource "aws_cloudwatch_event_target" "webhook" {
#   rule      = aws_cloudwatch_event_rule.spot_interruption.name
#   target_id = "criu-eventbridge-consumer"
#   arn       = "arn:aws:events:us-east-1:${data.aws_caller_identity.current.account_id}:event-bus/default"
#   http_parameters = {
#     path_parameter_values = {}
#     query_string_parameters = {}
#     header_parameters = {
#       "Content-Type" = "application/json"
#     }
#   }
#   role_arn = aws_iam_role.eventbridge.arn
# }

# Get current AWS account ID
data "aws_caller_identity" "current" {}

output "eventbridge_rule_name" {
  description = "Name of the EventBridge rule for spot interruptions"
  value       = aws_cloudwatch_event_rule.spot_interruption.name
}

output "aws_account_id" {
  description = "AWS account ID"
  value       = data.aws_caller_identity.current.account_id
}
