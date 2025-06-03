"""
Data Quality Monitoring Framework

This module implements the data quality monitoring framework described in DATA-520.
It provides functionality for:
- Defining and executing data quality rules
- Collecting data quality metrics
- Generating data quality reports
- Alerting on data quality issues
"""

import os
import json
import logging
import datetime
from typing import Dict, List, Any, Optional, Union
import boto3
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum, when, isnan, isnull

class DataQualityMonitor:
    """
    Main class for the Data Quality Monitoring Framework.
    
    This class provides methods for defining and executing data quality checks,
    collecting metrics, and generating reports.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        database_name: str,
        table_name: str,
        rules_path: Optional[str] = None,
        metrics_path: Optional[str] = None,
        log_level: str = "INFO"
    ):
        """
        Initialize the Data Quality Monitor.
        
        Args:
            spark: SparkSession to use for data processing
            database_name: Glue database name
            table_name: Glue table name
            rules_path: Path to JSON file containing data quality rules
            metrics_path: Path to store metrics results
            log_level: Logging level
        """
        self.spark = spark
        self.database_name = database_name
        self.table_name = table_name
        self.rules_path = rules_path
        self.metrics_path = metrics_path
        
        # Set up logging
        self.logger = logging.getLogger("DataQualityMonitor")
        self.logger.setLevel(getattr(logging, log_level))
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        # Initialize AWS clients
        self.glue_client = boto3.client('glue')
        self.cloudwatch_client = boto3.client('cloudwatch')
        self.sns_client = boto3.client('sns')
        
        # Load rules if path is provided
        self.rules = []
        if self.rules_path:
            self.load_rules()
    
    def load_rules(self) -> None:
        """
        Load data quality rules from the specified JSON file.
        """
        try:
            self.logger.info(f"Loading rules from {self.rules_path}")
            with open(self.rules_path, 'r') as f:
                self.rules = json.load(f)
            self.logger.info(f"Loaded {len(self.rules)} rules")
        except Exception as e:
            self.logger.error(f"Error loading rules: {str(e)}")
            raise
    
    def load_data(self) -> DataFrame:
        """
        Load data from the specified Glue table.
        
        Returns:
            DataFrame: Spark DataFrame containing the table data
        """
        try:
            self.logger.info(f"Loading data from {self.database_name}.{self.table_name}")
            df = self.spark.table(f"{self.database_name}.{self.table_name}")
            self.logger.info(f"Loaded {df.count()} rows")
            return df
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise
    
    def run_completeness_check(self, df: DataFrame, column: str, threshold: float) -> Dict[str, Any]:
        """
        Check for null/missing values in a column.
        
        Args:
            df: Spark DataFrame to check
            column: Column name to check
            threshold: Maximum allowed percentage of null values
            
        Returns:
            Dict containing check results
        """
        self.logger.info(f"Running completeness check on column {column}")
        
        total_count = df.count()
        if total_count == 0:
            self.logger.warning("DataFrame is empty")
            return {
                "check_type": "completeness",
                "column": column,
                "threshold": threshold,
                "null_count": 0,
                "total_count": 0,
                "null_percentage": 0,
                "passed": True,
                "timestamp": datetime.datetime.now().isoformat()
            }
        
        null_count = df.filter(col(column).isNull() | isnan(column)).count()
        null_percentage = (null_count / total_count) * 100
        passed = null_percentage <= threshold
        
        result = {
            "check_type": "completeness",
            "column": column,
            "threshold": threshold,
            "null_count": null_count,
            "total_count": total_count,
            "null_percentage": null_percentage,
            "passed": passed,
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        self.logger.info(f"Completeness check result: {result}")
        return result
    
    def run_uniqueness_check(self, df: DataFrame, column: str, threshold: float) -> Dict[str, Any]:
        """
        Check for duplicate values in a column.
        
        Args:
            df: Spark DataFrame to check
            column: Column name to check
            threshold: Minimum required percentage of unique values
            
        Returns:
            Dict containing check results
        """
        self.logger.info(f"Running uniqueness check on column {column}")
        
        total_count = df.count()
        if total_count == 0:
            self.logger.warning("DataFrame is empty")
            return {
                "check_type": "uniqueness",
                "column": column,
                "threshold": threshold,
                "unique_count": 0,
                "total_count": 0,
                "unique_percentage": 0,
                "passed": True,
                "timestamp": datetime.datetime.now().isoformat()
            }
        
        unique_count = df.select(column).distinct().count()
        unique_percentage = (unique_count / total_count) * 100
        passed = unique_percentage >= threshold
        
        result = {
            "check_type": "uniqueness",
            "column": column,
            "threshold": threshold,
            "unique_count": unique_count,
            "total_count": total_count,
            "unique_percentage": unique_percentage,
            "passed": passed,
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        self.logger.info(f"Uniqueness check result: {result}")
        return result
    
    def run_value_range_check(
        self, 
        df: DataFrame, 
        column: str, 
        min_value: Optional[Union[int, float]] = None, 
        max_value: Optional[Union[int, float]] = None
    ) -> Dict[str, Any]:
        """
        Check if values in a column are within the specified range.
        
        Args:
            df: Spark DataFrame to check
            column: Column name to check
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            
        Returns:
            Dict containing check results
        """
        self.logger.info(f"Running value range check on column {column}")
        
        # Filter out null values
        filtered_df = df.filter(col(column).isNotNull())
        
        # Get min and max values
        if filtered_df.count() == 0:
            self.logger.warning(f"No non-null values in column {column}")
            return {
                "check_type": "value_range",
                "column": column,
                "min_value": min_value,
                "max_value": max_value,
                "actual_min": None,
                "actual_max": None,
                "passed": True,
                "timestamp": datetime.datetime.now().isoformat()
            }
        
        stats = filtered_df.agg({"*": "count", column: "min", column: "max"}).collect()[0]
        actual_min = stats[f"min({column})"]
        actual_max = stats[f"max({column})"]
        
        # Check if values are within range
        min_check = True if min_value is None else actual_min >= min_value
        max_check = True if max_value is None else actual_max <= max_value
        passed = min_check and max_check
        
        result = {
            "check_type": "value_range",
            "column": column,
            "min_value": min_value,
            "max_value": max_value,
            "actual_min": actual_min,
            "actual_max": actual_max,
            "passed": passed,
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        self.logger.info(f"Value range check result: {result}")
        return result
    
    def run_pattern_check(self, df: DataFrame, column: str, pattern: str) -> Dict[str, Any]:
        """
        Check if values in a column match the specified regex pattern.
        
        Args:
            df: Spark DataFrame to check
            column: Column name to check
            pattern: Regex pattern to match
            
        Returns:
            Dict containing check results
        """
        self.logger.info(f"Running pattern check on column {column}")
        
        # Filter out null values
        filtered_df = df.filter(col(column).isNotNull())
        
        if filtered_df.count() == 0:
            self.logger.warning(f"No non-null values in column {column}")
            return {
                "check_type": "pattern",
                "column": column,
                "pattern": pattern,
                "match_count": 0,
                "total_count": 0,
                "match_percentage": 0,
                "passed": True,
                "timestamp": datetime.datetime.now().isoformat()
            }
        
        # Count matches
        total_count = filtered_df.count()
        match_count = filtered_df.filter(col(column).rlike(pattern)).count()
        match_percentage = (match_count / total_count) * 100
        passed = match_count == total_count
        
        result = {
            "check_type": "pattern",
            "column": column,
            "pattern": pattern,
            "match_count": match_count,
            "total_count": total_count,
            "match_percentage": match_percentage,
            "passed": passed,
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        self.logger.info(f"Pattern check result: {result}")
        return result
    
    def run_referential_integrity_check(
        self, 
        df: DataFrame, 
        column: str, 
        ref_database: str, 
        ref_table: str, 
        ref_column: str
    ) -> Dict[str, Any]:
        """
        Check if values in a column exist in a reference table.
        
        Args:
            df: Spark DataFrame to check
            column: Column name to check
            ref_database: Reference database name
            ref_table: Reference table name
            ref_column: Reference column name
            
        Returns:
            Dict containing check results
        """
        self.logger.info(f"Running referential integrity check on column {column}")
        
        # Load reference table
        try:
            ref_df = self.spark.table(f"{ref_database}.{ref_table}")
        except Exception as e:
            self.logger.error(f"Error loading reference table: {str(e)}")
            return {
                "check_type": "referential_integrity",
                "column": column,
                "ref_database": ref_database,
                "ref_table": ref_table,
                "ref_column": ref_column,
                "invalid_count": None,
                "total_count": None,
                "valid_percentage": None,
                "passed": False,
                "error": str(e),
                "timestamp": datetime.datetime.now().isoformat()
            }
        
        # Filter out null values
        filtered_df = df.filter(col(column).isNotNull())
        
        if filtered_df.count() == 0:
            self.logger.warning(f"No non-null values in column {column}")
            return {
                "check_type": "referential_integrity",
                "column": column,
                "ref_database": ref_database,
                "ref_table": ref_table,
                "ref_column": ref_column,
                "invalid_count": 0,
                "total_count": 0,
                "valid_percentage": 100,
                "passed": True,
                "timestamp": datetime.datetime.now().isoformat()
            }
        
        # Get distinct values from both tables
        source_values = filtered_df.select(column).distinct()
        ref_values = ref_df.select(ref_column).distinct()
        
        # Find values that don't exist in reference table
        invalid_values = source_values.join(
            ref_values,
            source_values[column] == ref_values[ref_column],
            "left_anti"
        )
        
        # Count invalid values
        total_count = filtered_df.count()
        invalid_count = invalid_values.count()
        valid_percentage = ((total_count - invalid_count) / total_count) * 100
        passed = invalid_count == 0
        
        result = {
            "check_type": "referential_integrity",
            "column": column,
            "ref_database": ref_database,
            "ref_table": ref_table,
            "ref_column": ref_column,
            "invalid_count": invalid_count,
            "total_count": total_count,
            "valid_percentage": valid_percentage,
            "passed": passed,
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        self.logger.info(f"Referential integrity check result: {result}")
        return result
    
    def run_all_checks(self) -> List[Dict[str, Any]]:
        """
        Run all data quality checks defined in the rules.
        
        Returns:
            List of dictionaries containing check results
        """
        self.logger.info("Running all data quality checks")
        
        # Load data
        df = self.load_data()
        
        # Run checks
        results = []
        for rule in self.rules:
            try:
                check_type = rule.get("check_type")
                column = rule.get("column")
                
                if check_type == "completeness":
                    threshold = rule.get("threshold", 0)
                    result = self.run_completeness_check(df, column, threshold)
                
                elif check_type == "uniqueness":
                    threshold = rule.get("threshold", 100)
                    result = self.run_uniqueness_check(df, column, threshold)
                
                elif check_type == "value_range":
                    min_value = rule.get("min_value")
                    max_value = rule.get("max_value")
                    result = self.run_value_range_check(df, column, min_value, max_value)
                
                elif check_type == "pattern":
                    pattern = rule.get("pattern")
                    result = self.run_pattern_check(df, column, pattern)
                
                elif check_type == "referential_integrity":
                    ref_database = rule.get("ref_database")
                    ref_table = rule.get("ref_table")
                    ref_column = rule.get("ref_column")
                    result = self.run_referential_integrity_check(
                        df, column, ref_database, ref_table, ref_column
                    )
                
                else:
                    self.logger.warning(f"Unknown check type: {check_type}")
                    continue
                
                # Add rule metadata to result
                result["rule_id"] = rule.get("rule_id")
                result["rule_name"] = rule.get("rule_name")
                result["rule_description"] = rule.get("rule_description")
                result["severity"] = rule.get("severity", "medium")
                
                results.append(result)
                
                # Send metrics to CloudWatch
                self.send_metrics_to_cloudwatch(result)
                
                # Send alert if check failed
                if not result["passed"] and rule.get("alert", False):
                    self.send_alert(result)
                
            except Exception as e:
                self.logger.error(f"Error running check: {str(e)}")
                results.append({
                    "rule_id": rule.get("rule_id"),
                    "rule_name": rule.get("rule_name"),
                    "check_type": rule.get("check_type"),
                    "column": rule.get("column"),
                    "passed": False,
                    "error": str(e),
                    "timestamp": datetime.datetime.now().isoformat()
                })
        
        # Save results if metrics path is provided
        if self.metrics_path:
            self.save_results(results)
        
        return results
    
    def send_metrics_to_cloudwatch(self, result: Dict[str, Any]) -> None:
        """
        Send data quality metrics to CloudWatch.
        
        Args:
            result: Check result to send as metrics
        """
        try:
            metric_data = []
            
            # Common dimensions
            dimensions = [
                {"Name": "Database", "Value": self.database_name},
                {"Name": "Table", "Value": self.table_name},
                {"Name": "Column", "Value": result.get("column", "unknown")}
            ]
            
            # Add check-specific metrics
            check_type = result.get("check_type")
            
            if check_type == "completeness":
                metric_data.append({
                    "MetricName": "NullPercentage",
                    "Dimensions": dimensions,
                    "Value": result.get("null_percentage", 0),
                    "Unit": "Percent"
                })
            
            elif check_type == "uniqueness":
                metric_data.append({
                    "MetricName": "UniquePercentage",
                    "Dimensions": dimensions,
                    "Value": result.get("unique_percentage", 0),
                    "Unit": "Percent"
                })
            
            elif check_type == "pattern":
                metric_data.append({
                    "MetricName": "PatternMatchPercentage",
                    "Dimensions": dimensions,
                    "Value": result.get("match_percentage", 0),
                    "Unit": "Percent"
                })
            
            elif check_type == "referential_integrity":
                metric_data.append({
                    "MetricName": "ReferentialIntegrityPercentage",
                    "Dimensions": dimensions,
                    "Value": result.get("valid_percentage", 0),
                    "Unit": "Percent"
                })
            
            # Add pass/fail metric
            metric_data.append({
                "MetricName": "CheckPassed",
                "Dimensions": dimensions + [{"Name": "CheckType", "Value": check_type}],
                "Value": 1 if result.get("passed", False) else 0,
                "Unit": "Count"
            })
            
            # Send metrics to CloudWatch
            if metric_data:
                self.cloudwatch_client.put_metric_data(
                    Namespace="DataQuality",
                    MetricData=metric_data
                )
                self.logger.info(f"Sent {len(metric_data)} metrics to CloudWatch")
        
        except Exception as e:
            self.logger.error(f"Error sending metrics to CloudWatch: {str(e)}")
    
    def send_alert(self, result: Dict[str, Any]) -> None:
        """
        Send an alert for a failed data quality check.
        
        Args:
            result: Check result to send an alert for
        """
        try:
            # Get SNS topic ARN from environment variable
            sns_topic_arn = os.environ.get("DATA_QUALITY_ALERT_TOPIC_ARN")
            if not sns_topic_arn:
                self.logger.warning("DATA_QUALITY_ALERT_TOPIC_ARN not set, skipping alert")
                return
            
            # Create message
            subject = f"Data Quality Alert: {result.get('rule_name', 'Unknown Rule')}"
            
            message = {
                "database": self.database_name,
                "table": self.table_name,
                "rule_id": result.get("rule_id"),
                "rule_name": result.get("rule_name"),
                "check_type": result.get("check_type"),
                "column": result.get("column"),
                "severity": result.get("severity", "medium"),
                "timestamp": result.get("timestamp"),
                "details": result
            }
            
            # Send message
            self.sns_client.publish(
                TopicArn=sns_topic_arn,
                Subject=subject,
                Message=json.dumps(message, indent=2)
            )
            
            self.logger.info(f"Sent alert to SNS topic: {sns_topic_arn}")
        
        except Exception as e:
            self.logger.error(f"Error sending alert: {str(e)}")
    
    def save_results(self, results: List[Dict[str, Any]]) -> None:
        """
        Save check results to the specified path.
        
        Args:
            results: List of check results to save
        """
        try:
            # Create timestamp for filename
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.database_name}_{self.table_name}_{timestamp}.json"
            filepath = os.path.join(self.metrics_path, filename)
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            # Save results
            with open(filepath, 'w') as f:
                json.dump(results, f, indent=2)
            
            self.logger.info(f"Saved results to {filepath}")
        
        except Exception as e:
            self.logger.error(f"Error saving results: {str(e)}")
    
    def generate_report(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate a summary report from check results.
        
        Args:
            results: List of check results
            
        Returns:
            Dict containing report summary
        """
        try:
            # Count checks by type and status
            total_checks = len(results)
            passed_checks = sum(1 for r in results if r.get("passed", False))
            failed_checks = total_checks - passed_checks
            
            check_types = {}
            for result in results:
                check_type = result.get("check_type")
                if check_type not in check_types:
                    check_types[check_type] = {"total": 0, "passed": 0, "failed": 0}
                
                check_types[check_type]["total"] += 1
                if result.get("passed", False):
                    check_types[check_type]["passed"] += 1
                else:
                    check_types[check_type]["failed"] += 1
            
            # Generate report
            report = {
                "database": self.database_name,
                "table": self.table_name,
                "timestamp": datetime.datetime.now().isoformat(),
                "summary": {
                    "total_checks": total_checks,
                    "passed_checks": passed_checks,
                    "failed_checks": failed_checks,
                    "pass_percentage": (passed_checks / total_checks * 100) if total_checks > 0 else 100
                },
                "by_check_type": check_types,
                "failed_checks": [r for r in results if not r.get("passed", False)]
            }
            
            self.logger.info(f"Generated report: {report['summary']}")
            return report
        
        except Exception as e:
            self.logger.error(f"Error generating report: {str(e)}")
            return {
                "error": str(e),
                "timestamp": datetime.datetime.now().isoformat()
            }
