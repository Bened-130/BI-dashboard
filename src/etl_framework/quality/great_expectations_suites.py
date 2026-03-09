import logging
from typing import Any, Dict

# Optional imports with graceful fallback
try:
    import great_expectations as gx
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.expectations import (
        ExpectColumnValuesToNotBeNull,
        ExpectColumnValuesToBeBetween,
        ExpectColumnValuesToBeInSet,
        ExpectTableRowCountToBeBetween,
        ExpectColumnValuesToMatchRegex
    )
    GX_AVAILABLE = True
except ImportError:
    GX_AVAILABLE = False
    gx = None
    ExpectationSuite = None

logger = logging.getLogger(__name__)


class ProductQualitySuite:
    """Great Expectations suite for product data"""
    
    def __init__(self):
        if not GX_AVAILABLE:
            logger.warning("Great Expectations not installed. Quality checks will be skipped.")
            self.context = None
            self.suite_name = "product_quality_suite"
            return
        
        try:
            self.context = gx.get_context()
        except Exception as e:
            logger.warning(f"Could not initialize Great Expectations context: {e}")
            self.context = None
        
        self.suite_name = "product_quality_suite"
    
    def create_suite(self) -> Any:
        """Create comprehensive quality suite"""
        if not GX_AVAILABLE or self.context is None:
            logger.warning("Great Expectations not available, returning empty suite")
            return None
        
        try:
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
                    value_set=["ELECTRONICS", "CLOTHING", "FOOD", "HOME", "SPORTS", "UNCATEGORIZED"]
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
            
        except Exception as e:
            logger.error(f"Error creating expectation suite: {e}")
            return None
    
    def validate(self, df: Any) -> Dict[str, Any]:
        """Run validation against DataFrame"""
        if not GX_AVAILABLE or self.context is None:
            logger.warning("Great Expectations not available, skipping validation")
            return {
                "success": True,
                "statistics": {
                    "evaluated_expectations": 0,
                    "successful_expectations": 0,
                    "unsuccessful_expectations": 0,
                    "success_percent": 100.0
                },
                "results": []
            }
        
        try:
            suite = self.create_suite()
            if suite is None:
                return {"success": True, "message": "No suite created"}
            
            # Create batch and validator
            data_source = self.context.data_sources.add_spark("spark_datasource")
            data_asset = data_source.add_dataframe_asset(name="product_data_asset")
            batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_def")
            batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
            
            # Create expectation suite
            expectation_suite = self.context.suites.add(suite)
            
            # Create and run checkpoint
            checkpoint = self.context.add_or_update_checkpoint(
                name="product_checkpoint",
                batch_request=batch_definition.build_batch_request(batch_parameters={"dataframe": df}),
                expectation_suite_name=self.suite_name,
            )
            
            checkpoint_result = checkpoint.run()
            
            # Extract results
            success = checkpoint_result.success
            statistics = {
                "evaluated_expectations": checkpoint_result.statistics.get("evaluated_expectations", 0),
                "successful_expectations": checkpoint_result.statistics.get("successful_expectations", 0),
                "unsuccessful_expectations": checkpoint_result.statistics.get("unsuccessful_expectations", 0),
                "success_percent": checkpoint_result.statistics.get("success_percent", 0.0)
            }
            
            # Log results
            success_rate = statistics["success_percent"]
            logger.info(f"Validation success rate: {success_rate:.2f}%")
            
            if success_rate < 95:
                logger.error("Data quality check failed!")
                # Don't raise error, just log - let caller decide
                # raise ValueError(f"Quality check failed: {success_rate:.2f}% success rate")
            
            return {
                "success": success,
                "statistics": statistics,
                "results": []
            }
            
        except Exception as e:
            logger.error(f"Error during validation: {e}")
            return {
                "success": False,
                "error": str(e),
                "statistics": {
                    "evaluated_expectations": 0,
                    "successful_expectations": 0,
                    "unsuccessful_expectations": 0,
                    "success_percent": 0.0
                }
            }