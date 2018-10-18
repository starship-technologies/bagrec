bagrec: a fast ROS bag writer
-------------------------------------------------

### What is it?

An alternative to `rosbag record` for recording [ROS](http://www.ros.org/)
bag files from given topics. This actually uses `librosbag` for serialisation,
the primary difference is in the elision of the expensive queuing and
dispatch implied by ROS's message subscription system.

This simply queues all messages in memory in one thread and periodically
writes them in another using a simple mutex. In our use case, this uses
roughly 50% of the CPU of `rosbag record`.

It implements one additional bespoke feature: forwarding of latched messages
across split bags. With this enabled (by default) the last message on a latched
topic is written at the start of a bag after split, allowing 'state' messages
to be available in each split bag.

### Dependencies

Requires a C++11 conformant compiler and STL implementation, and
pulls in [starship-technologies/common_cxx](https://github.com/starship-technologies/common_cxx)
as a git submodule.

Is packaged as a ROS catkin module depending on roscpp and rosbag, as well
as boost_program_options (already a ROS dependency).

   `$ git clone https://github.com/starship-technologies/bagrec --recurse-submodules`

### Example

```
rosrun bagrec bagrec
       -o bag_file_prefix \
       --split-interval 120 \
       --lz4 \
       --topic-substring camera_info image \
       --topic /odometry
```

### Arguments

```
options:
  --help                       show help
  -o [ --output-filename ] arg output bag filename
  -t [ --topic ] arg           topics to record (exact match)
  -s [ --topic-substring ] arg topics to record (substring)
  -j [ --bz2 ]                 use BZ2 compression
  --lz4                        use LZ4 compression
  -L [ --split-interval ] arg  split bags every X seconds
  --min-free-space arg         stop recording if free space drops below (MiB)
  --disable-latching           disable rewrite of last latched message 
                               per-topic at start of split bags
```

