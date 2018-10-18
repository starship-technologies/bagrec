/*
 * Copyright (c) 2018 Starship Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "common/array_formatter.hpp"
#include "common/common_timestamp.hpp"
#include "common/common_timestamp_fmt.hpp"

#include <ros/ros.h>
#include <ros/spinner.h>
#include <ros/subscribe_options.h>
#include <ros/callback_queue.h>

#include <rosbag/bag.h>

#include <topic_tools/shape_shifter.h>

#include <boost/program_options.hpp>

#include <mutex>
#include <atomic>
#include <list>
#include <future>
#include <unordered_set>

#include <unistd.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

template <typename T>
void mark_unused(T &&)
{ }
using common::string_view;
using PathBuf = common::array_formatter<4096>;

struct NoQueue : public ros::CallbackQueueInterface
{
  void addCallback(const ros::CallbackInterfacePtr &callback, uint64_t owner_id=0)
  {
    mark_unused(owner_id);
    // immediately call, do not queue!
    callback->call();
  }

  void removeByID(uint64_t owner_id)
  {
    mark_unused(owner_id);
  }
};

static uid_t owner_uid(const char* path)
{
  struct stat info;
  if (::stat(path, &info) == -1)
      return 0;
  return info.st_uid;
}

static uint64_t available_space_mb(const char* path)
{
  struct statvfs vfsstats;
  if (::statvfs(path, &vfsstats) == -1)
    {
      ROS_ERROR("bagrec: Stating file system for '%s' failed (%m)", path);
      return 0;
    }
  return uint64_t(vfsstats.f_bavail) * vfsstats.f_bsize / (1024*1024);
}

struct message_entry
{
  std::string topic;
  ros::Time time;
  topic_tools::ShapeShifter::ConstPtr mp;
  boost::shared_ptr<ros::M_string> connection_header;
};

struct bagrec
{
  std::list<ros::Subscriber> m_subscribers;

  NoQueue m_noQueue;
  ros::NodeHandle m_nodeHandle;

  std::mutex m_mutex;
  std::deque<message_entry> m_messages;
  std::map<std::string, message_entry> m_latchedMessages;
  std::atomic<bool> m_shutdown { false };

  struct {
      std::unique_ptr<rosbag::Bag> bag;
      int counter = 0;
      ros::Time startTime;
      PathBuf path;
      bool diskFullNotified = false;
  } m_writerData;

  std::unordered_set<std::string> m_checked_topics;

  void subscribe(const std::string& topic);
  void runner();
  void writeMessage(const message_entry& msg);
  void openBag();
  void finishBag();
  void msgCallback(const std::string& topic, const ros::MessageEvent<topic_tools::ShapeShifter const>& event);

  struct arguments_t {
      std::string outputFilename;
      bool useBZ2 = false;
      bool useLZ4 = false;
      std::vector<std::string> topics;
      std::vector<std::string> topicsSubstring;
      double splitInterval = 0.0;
      uint32_t minMBFree = 1024;
      bool disableLatching = false;
  } m_arguments;

  bagrec(arguments_t args) : m_arguments(args)
  {
    m_checked_topics.reserve(1000);
  }
  void run();

  void subscribeSubstring();
};


static const size_t g_max_unread_messages = 10000;

void bagrec::msgCallback(const std::string& topic, const ros::MessageEvent<topic_tools::ShapeShifter const>& event)
{
  ros::Time rectime = ros::Time::now();
  std::lock_guard<std::mutex> lock(m_mutex);
  if ( m_messages.size() >= g_max_unread_messages ) // abort, scream, die?!?
    {
      ROS_ERROR("bagrec::msgCallback: message queue size exceeded, aborting!");
      abort();
    }
  m_messages.push_back({topic, rectime, event.getMessage(), event.getConnectionHeaderPtr()});

  auto& msg = m_messages.back();
  static const std::string latching{"latching"};
  if ( !m_arguments.disableLatching && msg.connection_header )
    {
      auto it = msg.connection_header->find(latching);
      if ( (it != msg.connection_header->end()) && (it->second == "1") )
          m_latchedMessages[msg.topic] = msg;
    }
}

void bagrec::writeMessage(const message_entry& msg)
{
  bool shouldSplit = m_arguments.splitInterval && ((msg.time - m_writerData.startTime).toSec() > m_arguments.splitInterval);

  if ( !m_writerData.bag || shouldSplit )
    {
      auto dirname = string_view{m_arguments.outputFilename}.dirname();
      if ( owner_uid(dirname.to_string().c_str()) == 0 ) // quietly do nothing if directory does not exist or is owned by root
          return;

      const size_t avail = available_space_mb(dirname.size() ? dirname.to_string().c_str() : ".");
      if ( avail < m_arguments.minMBFree )
        {
          if ( !m_writerData.diskFullNotified )
              ROS_WARN("bagrec: disk has (%zu < %u) MiB free, not writing.", avail, m_arguments.minMBFree);
          m_writerData.diskFullNotified = true;
          return;
        }
      m_writerData.diskFullNotified = false;
      m_writerData.startTime = msg.time;
      openBag();

      if ( !m_arguments.disableLatching )
        {
          m_mutex.lock();
          auto latchedMessages = m_latchedMessages;
          m_mutex.unlock();

          for ( auto& pair : latchedMessages )
            {
              auto& latched = pair.second;
              m_writerData.bag->write(latched.topic, m_writerData.startTime, *latched.mp, latched.connection_header);
            }
        }
    }
  m_writerData.bag->write(msg.topic, msg.time, *msg.mp, msg.connection_header);
}

void bagrec::openBag()
{
  string_view filename_root = string_view{m_arguments.outputFilename}.clip_tail(".bag");
  ros::WallTime wt = ros::WallTime::now();

  std::array<char, 256> datebuf{'\0'};
  string_view timestr = common::timestamp_fmt_str(datebuf, "%Y-%m-%d-%H-%M-%S", {wt.sec, wt.nsec});

  PathBuf path{"%.*s_%.*s_%d.bag", filename_root.sizei(), filename_root.data(), timestr.sizei(), timestr.data(), m_writerData.counter++};
  PathBuf pathActive{"%s.active", path.c_str()};

  finishBag();
  m_writerData.bag.reset(new rosbag::Bag());
  if ( m_arguments.useLZ4 )
      m_writerData.bag->setCompression(rosbag::compression::LZ4);
  else if ( m_arguments.useBZ2 )
      m_writerData.bag->setCompression(rosbag::compression::BZ2);
  m_writerData.bag->setChunkThreshold(1024 * 768);
  m_writerData.bag->open(pathActive.to_string(), rosbag::bagmode::Write);
  ROS_INFO("bagrec: writing bag [%s]", path.c_str());
  m_writerData.path = path;
}

void bagrec::finishBag()
{
  if ( !m_writerData.bag )
      return;

  PathBuf pathActive{"%s.active", m_writerData.path.c_str()};

  m_writerData.bag.reset();

  int res = ::rename(pathActive.c_str(), m_writerData.path.c_str());
  if ( res )
      fprintf(stderr, "failed to rename '%s' => '%s': %m\n", pathActive.c_str(), m_writerData.path.c_str());
}

void bagrec::runner()
{
  ROS_INFO("bagrec: runner starting up..");
  decltype(m_messages) msgs;
  while ( !m_shutdown.load(std::memory_order_relaxed) )
    {
      m_mutex.lock();
      // fast move switch the entire queue
      msgs = std::move(m_messages);
      m_mutex.unlock();
      for ( auto& m : msgs )
          writeMessage(m);
      msgs.clear();
      ::usleep(10000);
    }
  finishBag();
  ROS_INFO("bagrec: runner terminating..");
}

void bagrec::subscribe(const std::string& topic)
{
  ros::SubscribeOptions ops;
  ops.topic = topic;
  ops.queue_size = 1000;
  ops.md5sum = ros::message_traits::md5sum<topic_tools::ShapeShifter>();
  ops.datatype = ros::message_traits::datatype<topic_tools::ShapeShifter>();
  ops.helper = ros::SubscriptionCallbackHelperPtr(
      new ros::SubscriptionCallbackHelperT<const ros::MessageEvent<topic_tools::ShapeShifter const>& >(
          [this, topic] (const ros::MessageEvent<topic_tools::ShapeShifter const>& ev) {
              msgCallback(topic, ev);
          }
      )
  );
  ops.tracked_object = ros::VoidPtr();
  ops.callback_queue = &m_noQueue;
  m_subscribers.push_back(m_nodeHandle.subscribe(ops));
  ROS_INFO("bagrec: subscribing to [%s]", topic.c_str());
}

// Fast rosbag recorder. 2X faster than rosbag record when testing on x86.
// Lean implementation
int main(int argc, char *argv[])
{
  ros::init(argc, argv, "bacrec");

  bagrec::arguments_t arguments;

  namespace po = boost::program_options;
  po::options_description desc("options");

  desc.add_options()
    ("help", "show help")
    ("output-filename,o", po::value(&arguments.outputFilename)->required(), "output bag filename")
    ("topic,t", po::value<std::vector<std::string>>(&arguments.topics)->multitoken(), "topics to record (exact match)")
    ("topic-substring,s", po::value<std::vector<std::string>>(&arguments.topicsSubstring)->multitoken(), "topics to record (substring)")
    ("bz2,j", po::bool_switch(&arguments.useBZ2), "use BZ2 compression")
    ("lz4", po::bool_switch(&arguments.useLZ4), "use LZ4 compression")
    ("split-interval,L", po::value(&arguments.splitInterval), "split bags every X seconds")
    ("min-free-space", po::value(&arguments.minMBFree), "stop recording if free space drops below (MiB)")
    ("disable-latching", po::bool_switch(&arguments.disableLatching), "disable rewrite of last latched message per-topic at start of split bags");

  po::positional_options_description pd;
  pd.add("topic", -1);

  po::variables_map vm;
  try
    {
      po::store(po::command_line_parser(argc, argv).options(desc).positional(pd).run(), vm);

      if ( vm.count("help") ) {
          std::cout << desc << "\n";
          return 0;
      }

      po::notify(vm);
    }
  catch (const po::error& e)
    {
      std::cerr << desc << "\n\n";
      std::cerr << "bagrec: " << e.what() << "\n";
      return -1;
    }

  if ( !arguments.topics.size() && !arguments.topicsSubstring.size() )
    {
      ROS_ERROR("bagrec: no topics provided, exiting.");
      return -1;
    }

  bagrec rec{arguments};
  rec.run();

  return 0;
}

void bagrec::run()
{
  std::future<void> runner_thread = std::async(std::launch::async,
    [this]()
    {
      try
        {
          runner();
        }
      catch (const std::exception& e)
        {
          ROS_ERROR("bagrec: caught exception '%s' in inner thread, aborting..", e.what());
          abort();
        }
    }
  );

  for ( auto& topic : m_arguments.topics )
      subscribe(topic);

  ros::Timer checkTopicsTimer;
  if ( m_arguments.topicsSubstring.size() )
    {
      subscribeSubstring();
      checkTopicsTimer = m_nodeHandle.createTimer(ros::Duration(1.0), [this] (const ros::TimerEvent&) {
          subscribeSubstring();
      });
    }

  auto dirname = common::string_view{m_arguments.outputFilename}.dirname();
  if ( owner_uid(dirname.to_string().c_str()) == 0 )
    {
      ROS_ERROR("bagrec: recording directory does not exist or is owned by root, not writing..");
    }

  ROS_INFO("bagrec: spinning..");
  while ( ros::ok() )
    {
      usleep(100000);
      ros::spinOnce();
    }

  m_shutdown = true;

  runner_thread.get();
}

void bagrec::subscribeSubstring()
{
    ros::master::V_TopicInfo topics;
    if ( !ros::master::getTopics(topics) )
      {
        ROS_WARN("bagrec: failed to list topics");
        return;
      }

    for ( const auto& active_topic: topics )
      {
        if ( m_checked_topics.find(active_topic.name) != m_checked_topics.end() )
            continue;
        m_checked_topics.insert(active_topic.name);

        auto cmp = [&active_topic](const ros::Subscriber& sub)
          {
            return sub.getTopic() == active_topic.name;
          };

        if ( std::find_if(m_subscribers.begin(), m_subscribers.end(), cmp) != m_subscribers.end() )
            continue;

        for ( const auto& substr : m_arguments.topicsSubstring )
          {
            if ( active_topic.name.find(substr) == std::string::npos )
                continue;
            subscribe(active_topic.name);
            break;
          }
      }
}

