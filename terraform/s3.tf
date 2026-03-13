# S3 Express One Zone directory bucket for CRIU checkpoints
resource "aws_s3_directory_bucket" "criu_checkpoints" {
  bucket = "${var.prefix}-criu-checkpoints--${var.spot_az}--x-s3"

  location {
    name = var.spot_az
    type = "AvailabilityZone"
  }

  force_destroy = true  # Allow destruction for testing
}

# NOTE: S3 Express One Zone directory buckets do not support:
# - Tags
# - Public access block configuration
# - These are managed at the account level automatically
