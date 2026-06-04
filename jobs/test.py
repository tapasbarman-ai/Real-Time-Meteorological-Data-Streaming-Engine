# pyflink working check 
"""from pyflink.datastream import StreamExecutionEnvironment
print("pyflink working correctly!")"""

# pyflink enviroment

"""from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1) # set parallelism to 1 for testing

print("pyflink environment set up correctly!")
"""

"""from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment


env = StreamExecutionEnvironment.get_execution_environment()

data = [1, 2, 3, 4, 5]

ds = env.from_collection(
    collection = data,
    type_info = Types.INT()
)

ds.print()
env.execute("test_job")"""


# Map function test

"""from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment


env = StreamExecutionEnvironment.get_execution_environment()

data = [1, 2, 3, 4, 5]

ds = env.from_collection(
    collection = data,
    type_info = Types.INT()
)

map = ds.map(lambda x: x*2,output_type = Types.INT())

map.print()
env.execute("test_job")"""