from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.types import ArrayType, FloatType

class VectorArrayConverter:
    @staticmethod
    @udf(returnType = VectorUDT())
    def array_to_vector(array):
        return Vectors.dense(array)

    @staticmethod
    @udf(returnType = ArrayType(FloatType()))
    def vector_to_array(vector):
        return vector.toArray().tolist() if vector else None