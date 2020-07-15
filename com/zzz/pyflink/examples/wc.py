from pyflink.dataset import ExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment, StreamTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# configure the off-heap memory of current taskmanager to enable the python worker uses off-heap memory.
table_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

# Register Sink
table_env.execute_sql("""
        CREATE TABLE print_table (
 word string,
 wc bigint
) WITH (
 'connector' = 'print'
)
    """)

table_env.from_elements(
    [('flink',), ('flink',), ('pyflink',)],
    DataTypes.ROW([DataTypes.FIELD("word", DataTypes.STRING())])
).group_by('word') \
    .select('word, count(1) as wc') \
    .insert_into('print_table')

table_env.execute("tutorial_job")

# flink run -m zzz.flink:80 -py wc.py
