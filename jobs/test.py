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


# filter function test

"""from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()

data = [10, 15, 20, 25, 30]

ds = env.from_collection(
    collection = data,
    type_info = Types.INT()
)

filtered = ds.filter(lambda x:x >20)
filtered.print()
env.execute("test_job")"""


# keyed stream test

"""from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()

data = [("Alice", 10), ("Bob", 15), ("Alice", 20), ("Bob", 25), ("Alice", 30)]

ds = env.from_collection(
    collection = data,
    type_info = Types.TUPLE([Types.STRING(), Types.INT()])
)
keyed = ds.key_by(lambda x: x[0]) # key by name
keyed.print()
env.execute("test_job")
"""

# Handeling late data with watermarks

"""from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

env = StreamExecutionEnvironment.get_execution_environment()

data = [
    (1000,10),
    (2000,20),
    (3000,30),
    (4000,40),
    (5000,50)
]

ds = env.from_collection(
    collection = data,
)

watermark = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
stream = ds.assign_timestamps_and_watermarks(watermark)

stream.print()
env.execute("test_job")"""

# Window Processing

# from pyflink.datastream.window import TumblingEventTimeWindows
# from pyflink.common.typeinfo import Types
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.time import Time

# env = StreamExecutionEnvironment.get_execution_environment()
# data = [
#         ("A",10),
#         ("A",20),
#         ("A",50)
#     ]
# ds = env.from_collection(
#     collection = data,
#     type_info = Types.TUPLE([Types.STRING(), Types.INT()])
# )


# windowed = ds.key_by(lambda x: x[0]).window(TumblingEventTimeWindows.of(Time.seconds(5))).reduce(lambda a,b: (a[0], a[1]+b[1]))

# windowed.print()
# env.execute("test_job")


# Process Function


from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream import ProcessFunction

env = StreamExecutionEnvironment.get_execution_environment()

data = [10, 20, 30, 40, 50]

ds = env.from_collection(
    collection = data,  
)
class MyProcessFunction(ProcessFunction):
    def process_element(self,value,ctx):
        if value > 25:
            return [f"ALERT: {value}"]
process = ds.process(MyProcessFunction())
process.print()
env.execute("test_job")

