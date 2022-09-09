
from tap_jira.transformers import FlattenCFTransformer


transformer: FlattenCFTransformer = FlattenCFTransformer()

def test_no_cf():
    """Test CF transformer in case there is no information for CF in record"""
    record = {
        "id": "test"
    }
    new_record = transformer.transform(record)
    assert record == new_record

def test_cf_with_value_2():
    record = {
        "id": "test",
        "keyed_custom_fields": {
            "test_custom_field": {
                "name": "test egi currency range",
                "type": "currency_range",
                "value": {
                    "min_value": "12.15",
                    "max_value": "15.462352353453",
                    "unit": "EUR"
                }
            }
        }
    }
    new_record = transformer.transform(record)
    assert {"id": "test", "test_custom_field": {
        "min_value": "12.15",
        "max_value": "15.462352353453",
        "unit": "EUR"
    }} == new_record

def test_cf_with_value():
    record = {
        "id": "test",
        "keyed_custom_fields": {
            "test_custom_field": {
                "value": {
                    "min_value": "12423523.34534",
                    "max_value": "34215345.346"
                }
            }
        }
    }
    new_record = transformer.transform(record)
    assert {"id": "test", "test_custom_field": {
        "min_value": "12423523.34534",
        "max_value": "34215345.346"
    }} == new_record

def test_cf_none_value():
    record = {
        "id": "test",
        "keyed_custom_fields": {
            "test_custom_field": None
        }
    }
    new_record = transformer.transform(record)
    assert {"id": "test", "test_custom_field": None} == new_record

def test_cf_empty():
    record = {
        "id": "test",
        "keyed_custom_fields": {}
    }
    new_record = transformer.transform(record)
    assert {"id": "test"} == new_record

def test_cf_none():
    record = {
        "id": "test",
        "keyed_custom_fields": None
    }
    new_record = transformer.transform(record)
    assert {"id": "test"} == new_record