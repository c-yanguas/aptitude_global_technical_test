import os
import sys

# add path to src
test_dir = os.path.dirname(os.path.abspath(__file__))
src_path = f"{test_dir}/../src"
if src_path not in sys.path:
    sys.path.append(src_path)


from  data_transform import DataTransformer



import pytest
from pyspark.sql import SparkSession

# Fixture para crear una sesión de Spark
@pytest.fixture(scope="session")
def spark_session(request):
    spark = SparkSession.builder.appName("pytest-pyspark").getOrCreate()
    request.addfinalizer(lambda: spark.stop())
    return spark

# Test para la función cast_column de DataTransformer
def test_cast_column(spark_session):
    # Crear una instancia de la clase DataTransformer
    data_transformer = DataTransformer()
    
    # Crear un DataFrame de prueba
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    columns = ["name", "value"]
    df = spark_session.createDataFrame(data, columns)

    # Castear la columna 'value' a tipo 'string'
    new_df = data_transformer.cast_column(df, 'value', 'string')

    # Verificar si el tipo de la columna 'value' es 'string' en el nuevo DataFrame
    assert new_df.schema['value'].dataType.typeName() == 'string'
