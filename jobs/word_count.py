from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types

env = StreamExecutionEnvironment.get_execution_environment()

# source: simple in-memory collection
ds = env.from_collection(
    collection=[("hello", 1), ("world", 1), ("hello", 1)],
    type_info=Types.TUPLE([Types.STRING(), Types.INT()])
)

# simple sum by key
ds.key_by(lambda x: x[0]).sum(1).print()

env.execute("word_count_job")