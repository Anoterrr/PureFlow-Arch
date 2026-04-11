"""Unit tests for core utilities and generators."""
import pytest
from src.utils.generators import generate_base_customers, generate_base_vendas
from src.core.config import get_s3_paths

def test_generate_base_customers():
    """Validates customer generator logic."""
    n_customers = 10
    customers = generate_base_customers(n_customers)
    
    assert len(customers["customer_id"]) == n_customers
    assert "name" in customers
    assert "region" in customers
    assert customers["region"][0] in ["North", "South", "East", "West", "Central"]

def test_generate_base_vendas():
    """Validates sales generator logic and column naming."""
    n_vendas = 20
    n_customers = 5
    vendas = generate_base_vendas(n_vendas, n_customers, (10, 100), "2024-03-29")
    
    assert len(vendas["id"]) == n_vendas
    assert "sale_value" in vendas
    assert "sale_date" in vendas
    assert all(val >= 10 for val in vendas["sale_value"])

def test_get_s3_paths():
    """Ensures S3 paths are correctly formatted."""
    paths = get_s3_paths("2024-01-01")
    assert "s3://landing-zone/erp_vendas/dt=2024-01-01/vendas.csv" == paths["vendas_landing"]
    assert "s3://silver/erp_vendas/dt=2024-01-01/" == paths["vendas_silver"]
