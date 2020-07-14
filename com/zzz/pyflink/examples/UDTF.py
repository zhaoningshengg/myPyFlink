from pyflink.dataset import ExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, EnvironmentSettings, StreamTableEnvironment, BatchTableEnvironment
from pyflink.table.udf import udf, udtf, TableFunction


class Split(TableFunction):
    def eval(self, string):
        for s in string.split(" "):
            yield s, len(s)


env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

table_env.execute_sql("""
        CREATE TABLE mySource (
          a STRING
        ) WITH (
          'connector' = 'filesystem',
          'format' = 'csv',
          'path' = './input/udtf.input'
        )
    """)

table_env.execute_sql("""
        CREATE TABLE mySink1 (
          a STRING,
          word STRING,
          length INT
        ) WITH (
          'connector' = 'filesystem',
          'format' = 'csv',
          'path' = './output/udtf.output'
        )
    """)

table_env.execute_sql("""
        CREATE TABLE mySink2 (
          a STRING,
          word STRING,
          length INT
        ) WITH (
          'connector' = 'filesystem',
          'format' = 'csv',
          'path' = './output/udtf.output2'
        )
    """)

my_table = table_env.from_path("mySource")
# configure the off-heap memory of current taskmanager to enable the python worker uses off-heap memory.
table_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

# register the Python Table Function
table_env.register_function("split", udtf(Split(), DataTypes.STRING(), [DataTypes.STRING(), DataTypes.INT()]))

# use the Python Table Function in Python Table API
# my_table.join_lateral("split(a) as (word, length)").insert_into("mySink1")
my_table.join_lateral("split(a) as (word, length)").execute_insert(
    "mySink1").get_job_client().get_job_execution_result().result()

# my_table.left_outer_join_lateral("split(a) as (word, length)").execute_insert(
#     "mySink2").get_job_client().get_job_execution_result().result()
# my_table.left_outer_join_lateral("split(a) as (word, length)")

# table_env.execute("udtf job")
