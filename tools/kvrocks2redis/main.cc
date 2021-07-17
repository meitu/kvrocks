#include <getopt.h>
#include <event2/thread.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <csignal>

#include "../../src/config.h"
#include "../../src/storage.h"

#include "sync.h"
#include "redis_writer.h"
#include "parser.h"
#include "config.h"
#include "version.h"

const char *kDefaultConfPath = "../kvrocks2redis.conf";

std::function<void()> hup_handler;

struct Options {
  std::string conf_file = kDefaultConfPath;
  bool show_usage = false;
};

extern "C" void signal_handler(int sig) {
  if (hup_handler) hup_handler();
}

static void usage(const char *program) {
  std::cout << program << " sync kvrocks to redis\n"
            << "\t-c config file, default is " << kDefaultConfPath << "\n"
            << "\t-h help\n";
  exit(0);
}

static Options parseCommandLineOptions(int argc, char **argv) {
  int ch;
  Options opts;
  while ((ch = ::getopt(argc, argv, "c:p:hv")) != -1) {
    switch (ch) {
      case 'c': opts.conf_file = optarg;
        break;
      case 'h': opts.show_usage = true;
        break;
      case 'v': exit(0);
      default: usage(argv[0]);
    }
  }
  return opts;
}

static void initGoogleLog(const Kvrocks2redis::Config *config) {
  FLAGS_minloglevel = config->loglevel;
  FLAGS_max_log_size = 100;
  FLAGS_logbufsecs = 0;
  FLAGS_log_dir = config->dir;
}

static Status createPidFile(const std::string &path) {
  int fd = open(path.data(), O_RDWR | O_CREAT | O_EXCL, 0660);
  if (fd < 0) {
    return Status(Status::NotOK, strerror(errno));
  }
  std::string pid_str = std::to_string(getpid());
  write(fd, pid_str.data(), pid_str.size());
  close(fd);
  return Status::OK();
}

static void removePidFile(const std::string &path) {
  std::remove(path.data());
}

static void daemonize() {
  pid_t pid;

  pid = fork();
  if (pid < 0) {
    LOG(ERROR) << "Failed to fork the process, err: " << strerror(errno);
    exit(1);
  }
  if (pid > 0) exit(EXIT_SUCCESS);  // parent process
  // change the file mode
  umask(0);
  if (setsid() < 0) {
    LOG(ERROR) << "Failed to setsid, err: %s" << strerror(errno);
    exit(1);
  }
  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  close(STDERR_FILENO);
}

int main(int argc, char *argv[]) {
  google::InitGoogleLogging("kvrocks2redis");
#ifndef DISABLE_USE_PTHREADS
  evthread_use_pthreads();
#endif

  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

  std::cout << "Version: " << VERSION << " @" << GIT_COMMIT << std::endl;
  auto opts = parseCommandLineOptions(argc, argv);
  if (opts.show_usage) usage(argv[0]);
  std::string config_file_path = std::move(opts.conf_file);

  Kvrocks2redis::Config config;
  Status s = config.Load(config_file_path);
  if (!s.IsOK()) {
    std::cout << "Failed to load config, err: " << s.Msg() << std::endl;
    exit(1);
  }
  initGoogleLog(&config);

  if (config.daemonize) daemonize();
  s = createPidFile(config.pidfile);
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to create pidfile: " << s.Msg();
    exit(1);
  }

  Config kvrocks_config;
  kvrocks_config.db_dir = config.db_dir;

  Engine::Storage storage(&kvrocks_config);
  s = storage.OpenForReadOnly();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to open: " << s.Msg();
    exit(1);
  }

  RedisWriter writer(&config);
  Parser parser(&storage, &writer);

  Sync sync(&storage, &writer, &parser, &config);
  hup_handler = [&sync]() {
    if (!sync.IsStopped()) {
      LOG(INFO) << "Bye Bye";
      sync.Stop();
    }
  };
  sync.Start();
  removePidFile(config.pidfile);
  return 0;
}
