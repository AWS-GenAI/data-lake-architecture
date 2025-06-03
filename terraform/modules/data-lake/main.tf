/**
 * Data Lake Core Infrastructure Module
 * 
 * This module creates the core infrastructure for the data lake architecture including:
 * - S3 buckets for different data lake zones
 * - IAM roles and policies
 * - AWS Glue Data Catalog resources
 * - Lake Formation permissions
 */

provider "aws" {
  region = var.region
}

# ---------------------------------------------------------------------------------------------------------------------
# S3 Buckets for Data Lake Zones
# ---------------------------------------------------------------------------------------------------------------------

resource "aws_s3_bucket" "landing_zone" {
  bucket = "${var.prefix}-landing-zone-${var.environment}"
  
  tags = merge(
    var.tags,
    {
      Name = "${var.prefix}-landing-zone-${var.environment}"
      DataLakeZone = "landing"
    }
  )
}

resource "aws_s3_bucket" "processing_zone" {
  bucket = "${var.prefix}-processing-zone-${var.environment}"
  
  tags = merge(
    var.tags,
    {
      Name = "${var.prefix}-processing-zone-${var.environment}"
      DataLakeZone = "processing"
    }
  )
}

resource "aws_s3_bucket" "enrichment_zone" {
  bucket = "${var.prefix}-enrichment-zone-${var.environment}"
  
  tags = merge(
    var.tags,
    {
      Name = "${var.prefix}-enrichment-zone-${var.environment}"
      DataLakeZone = "enrichment"
    }
  )
}

resource "aws_s3_bucket" "consumption_zone" {
  bucket = "${var.prefix}-consumption-zone-${var.environment}"
  
  tags = merge(
    var.tags,
    {
      Name = "${var.prefix}-consumption-zone-${var.environment}"
      DataLakeZone = "consumption"
    }
  )
}

# ---------------------------------------------------------------------------------------------------------------------
# S3 Bucket Policies
# ---------------------------------------------------------------------------------------------------------------------

resource "aws_s3_bucket_server_side_encryption_configuration" "landing_zone" {
  bucket = aws_s3_bucket.landing_zone.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processing_zone" {
  bucket = aws_s3_bucket.processing_zone.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "enrichment_zone" {
  bucket = aws_s3_bucket.enrichment_zone.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "consumption_zone" {
  bucket = aws_s3_bucket.consumption_zone.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "landing_zone" {
  bucket = aws_s3_bucket.landing_zone.id

  rule {
    id = "transition-to-standard-ia"
    status = "Enabled"

    transition {
      days = 30
      storage_class = "STANDARD_IA"
    }
  }
}

resource "aws_s3_bucket_versioning" "landing_zone" {
  bucket = aws_s3_bucket.landing_zone.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# IAM Roles for Data Lake Access
# ---------------------------------------------------------------------------------------------------------------------

resource "aws_iam_role" "data_lake_admin" {
  name = "${var.prefix}-data-lake-admin-${var.environment}"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.account_id}:root"
        }
      }
    ]
  })
  
  tags = var.tags
}

resource "aws_iam_role" "data_engineer" {
  name = "${var.prefix}-data-engineer-${var.environment}"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.account_id}:root"
        }
      }
    ]
  })
  
  tags = var.tags
}

resource "aws_iam_role" "data_analyst" {
  name = "${var.prefix}-data-analyst-${var.environment}"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${var.account_id}:root"
        }
      }
    ]
  })
  
  tags = var.tags
}

# ---------------------------------------------------------------------------------------------------------------------
# IAM Policies for Data Lake Access
# ---------------------------------------------------------------------------------------------------------------------

resource "aws_iam_policy" "data_lake_admin_policy" {
  name        = "${var.prefix}-data-lake-admin-policy-${var.environment}"
  description = "Policy for Data Lake Administrators"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:*"
        ]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.landing_zone.arn,
          "${aws_s3_bucket.landing_zone.arn}/*",
          aws_s3_bucket.processing_zone.arn,
          "${aws_s3_bucket.processing_zone.arn}/*",
          aws_s3_bucket.enrichment_zone.arn,
          "${aws_s3_bucket.enrichment_zone.arn}/*",
          aws_s3_bucket.consumption_zone.arn,
          "${aws_s3_bucket.consumption_zone.arn}/*"
        ]
      },
      {
        Action = [
          "glue:*"
        ]
        Effect = "Allow"
        Resource = "*"
      },
      {
        Action = [
          "lakeformation:*"
        ]
        Effect = "Allow"
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_policy" "data_engineer_policy" {
  name        = "${var.prefix}-data-engineer-policy-${var.environment}"
  description = "Policy for Data Engineers"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.landing_zone.arn,
          "${aws_s3_bucket.landing_zone.arn}/*",
          aws_s3_bucket.processing_zone.arn,
          "${aws_s3_bucket.processing_zone.arn}/*",
          aws_s3_bucket.enrichment_zone.arn,
          "${aws_s3_bucket.enrichment_zone.arn}/*"
        ]
      },
      {
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.consumption_zone.arn,
          "${aws_s3_bucket.consumption_zone.arn}/*"
        ]
      },
      {
        Action = [
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:BatchCreatePartition",
          "glue:GetPartitions",
          "glue:GetPartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:GetUserDefinedFunctions"
        ]
        Effect = "Allow"
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_policy" "data_analyst_policy" {
  name        = "${var.prefix}-data-analyst-policy-${var.environment}"
  description = "Policy for Data Analysts"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.consumption_zone.arn,
          "${aws_s3_bucket.consumption_zone.arn}/*"
        ]
      },
      {
        Action = [
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetPartitions",
          "glue:GetPartition",
          "glue:GetUserDefinedFunctions"
        ]
        Effect = "Allow"
        Resource = "*"
      }
    ]
  })
}

# ---------------------------------------------------------------------------------------------------------------------
# IAM Role Policy Attachments
# ---------------------------------------------------------------------------------------------------------------------

resource "aws_iam_role_policy_attachment" "data_lake_admin_attachment" {
  role       = aws_iam_role.data_lake_admin.name
  policy_arn = aws_iam_policy.data_lake_admin_policy.arn
}

resource "aws_iam_role_policy_attachment" "data_engineer_attachment" {
  role       = aws_iam_role.data_engineer.name
  policy_arn = aws_iam_policy.data_engineer_policy.arn
}

resource "aws_iam_role_policy_attachment" "data_analyst_attachment" {
  role       = aws_iam_role.data_analyst.name
  policy_arn = aws_iam_policy.data_analyst_policy.arn
}

# ---------------------------------------------------------------------------------------------------------------------
# AWS Glue Data Catalog Resources
# ---------------------------------------------------------------------------------------------------------------------

resource "aws_glue_catalog_database" "landing_zone_db" {
  name = "${var.prefix}_landing_zone_${var.environment}"
  
  description = "Landing zone database for raw data"
}

resource "aws_glue_catalog_database" "processing_zone_db" {
  name = "${var.prefix}_processing_zone_${var.environment}"
  
  description = "Processing zone database for validated data"
}

resource "aws_glue_catalog_database" "enrichment_zone_db" {
  name = "${var.prefix}_enrichment_zone_${var.environment}"
  
  description = "Enrichment zone database for transformed data"
}

resource "aws_glue_catalog_database" "consumption_zone_db" {
  name = "${var.prefix}_consumption_zone_${var.environment}"
  
  description = "Consumption zone database for analytics-ready data"
}

# ---------------------------------------------------------------------------------------------------------------------
# Lake Formation Resources
# ---------------------------------------------------------------------------------------------------------------------

resource "aws_lakeformation_resource" "landing_zone" {
  arn = aws_s3_bucket.landing_zone.arn
}

resource "aws_lakeformation_resource" "processing_zone" {
  arn = aws_s3_bucket.processing_zone.arn
}

resource "aws_lakeformation_resource" "enrichment_zone" {
  arn = aws_s3_bucket.enrichment_zone.arn
}

resource "aws_lakeformation_resource" "consumption_zone" {
  arn = aws_s3_bucket.consumption_zone.arn
}

# ---------------------------------------------------------------------------------------------------------------------
# Data Quality Monitoring Resources
# ---------------------------------------------------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "data_quality_logs" {
  name = "/aws/datalake/${var.prefix}-data-quality-${var.environment}"
  retention_in_days = 90
  
  tags = merge(
    var.tags,
    {
      Name = "${var.prefix}-data-quality-logs-${var.environment}"
    }
  )
}
