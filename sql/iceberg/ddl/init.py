import re
from pathlib import Path

from src.common.logging import log
from src.common.spark_session import SparkSessionFactory


def init_iceberg_tables():
    """Initialize all bronze and silver tables from DDL files in this directory."""

    spark = SparkSessionFactory.create(mode='batch', app_name='init-iceberg-tables')

    # Create namespaces
    spark.sql('CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze')
    spark.sql('CREATE NAMESPACE IF NOT EXISTS lakehouse.silver')
    log.info("Created namespaces: lakehouse.bronze, lakehouse.silver")

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
            'CREATE TABLE IF NOT EXISTS lakehouse.silver.'
        )
        sql_content = sql_content.replace(
            'CREATE TABLE IF NOT EXISTS bronze.',
            'CREATE TABLE IF NOT EXISTS lakehouse.bronze.'
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
    log.info("Tables in lakehouse.bronze:")
    log.info("="*60)
    try:
        spark.sql('SHOW TABLES IN lakehouse.bronze').show(truncate=False)
    except Exception as e:
        log.error(f"Error occurred while fetching bronze tables: {e}")

    log.info("\n" + "="*60)
    log.info("Tables in lakehouse.silver:")
    log.info("="*60)
    try:
        spark.sql('SHOW TABLES IN lakehouse.silver').show(truncate=False)
    except Exception as e:
        log.error(f"Error occurred while fetching silver tables: {e}")

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
