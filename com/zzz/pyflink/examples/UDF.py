from pyflink.table import DataTypes, EnvironmentSettings, BatchTableEnvironment
from pyflink.table.udf import udf

# Init environment
environment_settings = EnvironmentSettings.new_instance().use_blink_planner().in_batch_mode().build()
t_env = BatchTableEnvironment.create(environment_settings=environment_settings)
t_env._j_tenv.getPlanner().getExecEnv().setParallelism(1)

t_env.get_config().get_configuration().set_boolean('python.fn-execution.memory.managed', True)


@udf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_type=DataTypes.ROW())
def split(i, j):
    return {'a': i + 1, 'b': j + 1}


# Define and register UDF
add = udf(lambda i, j: {'a': i, 'b': j}, [DataTypes.BIGINT(), DataTypes.BIGINT()],
          DataTypes.ROW([DataTypes.FIELD('a', DataTypes.BIGINT()),
                         DataTypes.FIELD('b', DataTypes.BIGINT())]))
get = udf(lambda x, y: x[y], [DataTypes.ROW(), DataTypes.STRING()], DataTypes.BIGINT())

t_env.register_function("split", split)
t_env.register_function("add", add)
t_env.register_function('get', get)

# Register Source
t_env.execute_sql("""
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
t_env.execute_sql("""
        CREATE TABLE mySink (
          `a` BIGINT,
          `b` BIGINT
        ) WITH (
          'connector' = 'filesystem',
          'format' = 'csv',
          'path' = './output/udf.output'
        )
    """)

# Query
t_env.from_path('mySource') \
    .select("split(a, b) as dic").select('get(dic,"a") as a,get(dic,"b") as b') \
    .insert_into('mySink')

t_env.execute("3-udf_add")

"""
t_env.from_table_source(SocketTableSource(port=9999)).alias("line").select("split(line) as str_array").select("get(str_array, 3) as city, "                     "get(str_array, 1).cast(LONG) as count, "                     "get(str_array, 2).cast(LONG) as unit_price")\        .select("city, count, count * unit_price as total_price")\       
        .group_by("city").select("city, sum(count) as sales_volume, sum(total_price)   
         as sales").insert_into("sink")
t_env.execute("Sales Statistic")
"""