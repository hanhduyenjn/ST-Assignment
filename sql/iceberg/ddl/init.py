#!/usr/bin/env python3
"""
Initialize Iceberg tables from DDL files in this directory.

Usage:
    python init.py

Environment Variables (with defaults):
    ICEBERG_REST_CATALOG_URL: http://localhost:8181
    MINIO_ENDPOINT: http://localhost:9000
    MINIO_ROOT_USER: minioadmin
    MINIO_ROOT_PASSWORD: minioadmin123
"""

import re
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.common.spark_session import SparkSessionFactory
from src.common.logging import get_logger

log = get_logger()


def init_iceberg_tables():
    """Initialize all bronze and silver tables from DDL files in this directory."""
    
    spark = SparkSessionFactory.create(mode='batch', app_name='init-iceberg-tables')
    
    # Create namespaces
    spark.sql('CREATE NAMESPACE IF NOT EXISTS rest.bronze')
    spark.sql('CREATE NAMESPACE IF NOT EXISTS rest.silver')
    log.info("Created namespaces: rest.bronze, rest.silver")
    
    # Read and execute DDL files in this directory
    ddl_dir = Path(__file__).parent
    ddl_files = sorted([f for f in ddl_dir.glob('*.sql') if f.is_file()])
    log.info(f"Found {len(ddl_files)} DDL files")
    
    created_tables = []
    failed_tables = []
    
    for sql_file in ddl_files:
        with open(sql_file) as f:
            sql_content = f.read()
        
        # Qualify table names with catalog prefix
        sql_content = sql_content.replace(
            'CREATE TABLE IF NOT EXISTS silver.',
            'CREATE TABLE IF NOT EXISTS rest.silver.'
        )
        sql_content = sql_content.replace(
            'CREATE TABLE IF NOT EXISTS bronze.',
            'CREATE TABLE IF NOT EXISTS rest.bronze.'
        )
        
        # Remove ORDER BY clauses (not supported in Spark 3.5 CREATE TABLE)
        sql_content = re.sub(r'ORDER BY\s*\([^)]+\)\s*', '', sql_content)
        
        try:
            spark.sql(sql_content)
            created_tables.append(sql_file.stem)
            log.info(f"✓ {sql_file.name}")
        except Exception as e:
            failed_tables.append((sql_file.stem, str(e)[:150]))
            log.error(f"✗ {sql_file.name}: {str(e)[:150]}")
    
    # Verify
    log.info("\n" + "="*60)
    log.info("Tables in rest.bronze:")
    log.info("="*60)
    try:
        spark.sql('SHOW TABLES IN rest.bronze').show(truncate=False)
    except:
        pass
    
    log.info("\n" + "="*60)
    log.info("Tables in rest.silver:")
    log.info("="*60)
    try:
        spark.sql('SHOW TABLES IN rest.silver').show(truncate=False)
    except:
        pass
    
    spark.stop()
    
    # Summary
    log.info("\n" + "="*60)
    log.info(f"✓ Initialization complete: {len(created_tables)} tables created")
    if failed_tables:
        log.warning(f"⚠ {len(failed_tables)} tables failed:")
        for table, error in failed_tables:
            log.warning(f"  - {table}")
        return False
    return True


if __name__ == '__main__':
    success = init_iceberg_tables()
    sys.exit(0 if success else 1)
