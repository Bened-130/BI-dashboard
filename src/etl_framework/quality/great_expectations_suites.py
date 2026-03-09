import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectTableRowCountToBeBetween,
    ExpectColumnPairValuesToBeEqual,
    ExpectColumnValuesToMatchRegex
)
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)


class ProductQualitySuite:
    """Great Expectations suite for product data"""
    
    def __init__(self):
        self.context = gx.get_context()
        self.suite_name = "product_quality_suite"
    
    def create_suite(self) -> ExpectationSuite:
        """Create comprehensive quality suite"""
        suite = self.context.create_expectation_suite(
            expectation_suite_name=self.suite_name,
            overwrite_existing=True
        )
        
        # Completeness expectations
        suite.add_expectation(
            ExpectColumnValuesToNotBeNull(column="product_id")
        )
        suite.add_expectation(
            ExpectColumnValuesToNotBeNull(column="product_name")
        )
        
        # Accuracy expectations
        suite.add_expectation(
            ExpectColumnValuesToBeBetween(
                column="price",
                min_value=0,
                max_value=100000
            )
        )
        
        # Validity expectations
        suite.add_expectation(
            ExpectColumnValuesToBeInSet(
                column="category",
                value_set=["ELECTRONICS", "CLOTHING", "FOOD", "HOME", "SPORTS"]
            )
        )
        
        # Consistency expectations
        suite.add_expectation(
            ExpectColumnValuesToMatchRegex(
                column="product_id",
                regex=r"^PROD-[0-9]{6}$"
            )
        )
        
        # Volume expectations
        suite.add_expectation(
            ExpectTableRowCountToBeBetween(min_value=1, max_value=10000000)
        )
        
        return suite
    
    def validate(self, df: DataFrame) -> dict:
        """Run validation against DataFrame"""
        suite = self.create_suite()
        
        batch = self.context.add_or_update_expectation_suite(
            expectation_suite=suite
        )
        
        validator = self.context.get_validator(
            batch_request={"dataframe": df},
            expectation_suite=suite
        )
        
        results = validator.validate()
        
        # Log results
        success_rate = results.statistics["successful_expectations"] / results.statistics["evaluated_expectations"]
        logger.info(f"Validation success rate: {success_rate:.2%}")
        
        if success_rate < 0.95:
            logger.error("Data quality check failed!")
            raise ValueError(f"Quality check failed: {success_rate:.2%} success rate")
        
        return results.to_json_dict()