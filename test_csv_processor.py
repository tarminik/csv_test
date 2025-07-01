import pytest
import os
from csv_processor import process_csv, AggregationError, FilterError

TEST_CSV = """name,brand,price,rating
iphone 15 pro,apple,999,4.9
galaxy s23 ultra,samsung,1199,4.8
redmi note 12,xiaomi,199,4.6
poco x5 pro,xiaomi,299,4.4
"""

@pytest.fixture(scope="module")
def csv_file(tmp_path_factory):
    file = tmp_path_factory.mktemp("data") / "products.csv"
    file.write_text(TEST_CSV, encoding="utf-8")
    return str(file)

def test_no_filter(csv_file):
    data, headers = process_csv(csv_file, where=None, aggregate=None)
    assert len(data) == 4
    assert headers == ["name", "brand", "price", "rating"]

def test_filter_numeric(csv_file):
    data, _ = process_csv(csv_file, where="rating>4.7", aggregate=None)
    assert len(data) == 2
    assert any("iphone 15 pro" in row for row in data)
    assert any("galaxy s23 ultra" in row for row in data)

def test_filter_string(csv_file):
    data, _ = process_csv(csv_file, where="brand=apple", aggregate=None)
    assert len(data) == 1
    assert data[0][0] == "iphone 15 pro"

def test_aggregate_avg(csv_file):
    data, headers = process_csv(csv_file, where=None, aggregate="rating=avg")
    assert headers == ["avg"]
    assert abs(data[0][0] - 4.67) < 0.01

def test_aggregate_min(csv_file):
    data, headers = process_csv(csv_file, where="brand=xiaomi", aggregate="rating=min")
    assert headers == ["min"]
    assert data[0][0] == 4.4

def test_aggregate_max(csv_file):
    data, headers = process_csv(csv_file, where=None, aggregate="price=max")
    assert headers == ["max"]
    assert data[0][0] == 1199

def test_filter_column_not_found(csv_file):
    with pytest.raises(FilterError):
        process_csv(csv_file, where="notacolumn=1", aggregate=None)

def test_aggregate_column_not_found(csv_file):
    with pytest.raises(AggregationError):
        process_csv(csv_file, where=None, aggregate="notacolumn=avg")

def test_aggregate_not_numeric(csv_file):
    with pytest.raises(AggregationError):
        process_csv(csv_file, where=None, aggregate="brand=avg")

def test_aggregate_invalid_func(csv_file):
    with pytest.raises(AggregationError):
        process_csv(csv_file, where=None, aggregate="rating=median")

def test_filter_invalid_operator(csv_file):
    with pytest.raises(FilterError):
        process_csv(csv_file, where="rating>=4.7", aggregate=None)

def test_aggregate_empty_result(csv_file):
    with pytest.raises(AggregationError):
        process_csv(csv_file, where="brand=notfound", aggregate="rating=avg") 