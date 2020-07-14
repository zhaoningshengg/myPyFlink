from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, BatchTableEnvironment, StreamTableEnvironment
from pyflink.table.udf import udf


@udf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_type=DataTypes.BIGINT(), udf_type="pandas")
def add(i, j):
    return i + j


env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# configure the off-heap memory of current taskmanager to enable the python worker uses off-heap memory.
table_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

# Register Source
table_env.execute_sql("""
        CREATE TABLE mySource (
          a BIGINT,
          b BIGINT
        ) WITH (
          'connector' = 'filesystem',
          'format' = 'csv',
          'path' = './input/udf.input'
        )
    """)

# Register Sink
table_env.execute_sql("""
        CREATE TABLE print_table (
 a bigint,
 b bigint,
 c bigint
) WITH (
 'connector' = 'print'
)
    """)

# register the vectorized Python scalar function
table_env.register_function("add", add)

my_table = table_env.from_path("mySource")
# use the vectorized Python scalar function in Python Table API
my_table.select("a,b,add(a, b) as c").insert_into("print_table")

# use the vectorized Python scalar function in SQL API
# table_env.sql_query("SELECT add(bigint, bigint) FROM mySource")

table_env.execute("pandas udf job")