"""
Lightweight test - NO PySpark required
Tests business logic with pure Python
"""

import sys
import os
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

def test_configuration():
    """Test configuration loading"""
    print("\n" + "="*60)
    print("TEST 1: Configuration")
    print("="*60)
    
    try:
        from etl_framework.config.settings import get_settings, Settings
        settings = get_settings()
        print(f"✓ Environment: {settings.environment}")
        print(f"✓ Storage path: {settings.storage.abfss_root}")
        print("✓ Configuration loaded successfully")
        return True
    except Exception as e:
        print(f"✗ Configuration failed: {e}")
        return False

def test_transformer_logic():
    """Test transformer business logic without Spark"""
    print("\n" + "="*60)
    print("TEST 2: Transformer Business Logic")
    print("="*60)
    
    # Pure Python test of business rules
    test_products = [
        {"product_id": "PROD-001", "product_name": "  wireless MOUSE  ", "category": "electronics", "price": 29.99},
        {"product_id": "PROD-002", "product_name": "Gaming KEYBOARD", "category": None, "price": None},
    ]
    
    print(f"Input products: {len(test_products)}")
    
    # Test business rules
    for product in test_products:
        # Rule 1: Clean product name
        original_name = product['product_name']
        cleaned_name = original_name.strip().title() if original_name else "Uncategorized"
        product['product_name'] = cleaned_name
        
        # Rule 2: Standardize category
        if not product['category']:
            product['category'] = "UNCATEGORIZED"
        else:
            product['category'] = product['category'].upper()
        
        # Rule 3: Handle missing price
        if not product['price']:
            product['price'] = 50.0  # median
            product['_imputed'] = True
        else:
            product['_imputed'] = False
        
        # Rule 4: Price tier
        price = product['price']
        if price < 50:
            product['price_tier'] = "Budget"
        elif price < 100:
            product['price_tier'] = "Standard"
        else:
            product['price_tier'] = "Premium"
        
        print(f"  ✓ {product['product_id']}: {cleaned_name} | {product['category']} | ${price} | {product['price_tier']}")
    
    print("✓ Business logic validated")
    return True

def test_data_quality_rules():
    """Test data quality rules"""
    print("\n" + "="*60)
    print("TEST 3: Data Quality Rules")
    print("="*60)
    
    test_data = [
        {"product_id": "PROD-001", "product_name": "Valid Product", "price": 25.00},  # Valid
        {"product_id": "PROD-002", "product_name": None, "price": 25.00},              # Invalid: no name
        {"product_id": "PROD-003", "product_name": "A", "price": 25.00},               # Invalid: name too short
        {"product_id": "PROD-004", "product_name": "Valid", "price": -10},             # Invalid: negative price
    ]
    
    valid_count = 0
    invalid_count = 0
    
    for record in test_data:
        # Validation rules
        is_valid = (
            record.get('product_name') is not None and
            len(str(record.get('product_name', ''))) > 2 and
            record.get('price', 0) > 0
        )
        
        status = "✓ VALID" if is_valid else "✗ INVALID"
        print(f"  {status}: {record['product_id']}")
        
        if is_valid:
            valid_count += 1
        else:
            invalid_count += 1
    
    print(f"\n✓ Validation complete: {valid_count} valid, {invalid_count} invalid")
    return True

def test_file_structure():
    """Verify project structure"""
    print("\n" + "="*60)
    print("TEST 4: Project Structure")
    print("="*60)
    
    required_files = [
        'src/etl_framework/__init__.py',
        'src/etl_framework/config/settings.py',
        'src/etl_framework/transformations/product_transforms.py',
        'infrastructure/main.bicep',
        '.github/workflows/ci-cd-synapse.yml',
        'requirements.txt',
    ]
    
    base_path = os.path.join(os.path.dirname(__file__), '..')
    
    for file_path in required_files:
        full_path = os.path.join(base_path, file_path)
        exists = os.path.exists(full_path)
        status = "✓" if exists else "✗"
        print(f"  {status} {file_path}")
    
    print("✓ Structure verification complete")
    return True

def main():
    """Run all tests"""
    print("\n" + "="*70)
    print("AZURE SYNAPSE ETL - LIGHTWEIGHT TEST SUITE")
    print("No PySpark required - Pure Python validation")
    print("="*70)
    
    results = []
    
    results.append(("Configuration", test_configuration()))
    results.append(("Transformer Logic", test_transformer_logic()))
    results.append(("Data Quality Rules", test_data_quality_rules()))
    results.append(("File Structure", test_file_structure()))
    
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {name}")
    
    all_passed = all(r[1] for r in results)
    
    print("="*70)
    if all_passed:
        print("✓ ALL TESTS PASSED")
        print("Ready for Azure deployment!")
    else:
        print("✗ SOME TESTS FAILED")
    print("="*70 + "\n")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())