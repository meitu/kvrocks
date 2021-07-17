#include <getopt.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dlfcn.h>
#ifdef __linux__
#define _XOPEN_SOURCE 700
#else
#define _XOPEN_SOURCE
#endif
#include <sys/un.h>
#include <signal.h>
#include <execinfo.h>
#include <ucontext.h>
#include <event2/thread.h>
#include <glog/logging.h>
#include "worker.h"
#include "storage.h"
#include "version.h"
#include "config.h"
#include "server.h"
#include "util.h"

#if defined(__APPLE__) || defined(__linux__)
#define HAVE_BACKTRACE 1
#endif

const char *kDefaultConfPath = "../kvrocks.conf";

std::function<void()> hup_handler;

struct Options {
  std::string conf_file;
  bool show_usage = false;
};

extern "C" void signal_handler(int sig) {
  if (hup_handler) hup_handler();
}

#ifdef HAVE_BACKTRACE
void *getMcontextEip(ucontext_t *uc) {
#ifdef __x86_64__
#define REG_EIP REG_RIP
#endif
#if defined(__FreeBSD__)
        return reinterpret_cast<void*>(uc->uc_mcontext.mc_eip);
#elif defined(__dietlibc__)
        return reinterpret_cast<void*>(uc->uc_mcontext.eip);
#elif defined(__APPLE__) && !defined(MAC_OS_X_VERSION_10_6)
#if __x86_64__
        return reinterpret_cast<void*>(uc->uc_mcontext->__ss.__rip);
#else
        return reinterpret_cast<void*>(uc->uc_mcontext->__ss.__eip);
#endif
#elif defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)
#if defined(_STRUCT_X86_THREAD_STATE64) && !defined(__i386__)
        return reinterpret_cast<void*>(uc->uc_mcontext->__ss.__rip);
#elif defined(__i386__)
        return reinterpret_cast<void*>(uc->uc_mcontext->__ss.__eip);
#else
        // OSX ARM64
        return reinterpret_cast<void*>(uc->uc_mcontext->__ss.__pc);
#endif
#elif defined(__i386__) || defined(__X86_64__) || defined(__x86_64__)
        return reinterpret_cast<void*>(uc->uc_mcontext.gregs[REG_EIP]); /* Linux 32/64 bit */
#elif defined(__ia64__) /* Linux IA64 */
        return reinterpret_cast<void*>(uc->uc_mcontext.sc_ip);
#endif
  return nullptr;
}

extern "C" void segvHandler(int sig, siginfo_t *info, void *secret) {
  void *trace[100];
  char **messages = nullptr;
  struct sigaction act;
  auto uc = reinterpret_cast<ucontext_t*>(secret);

  LOG(WARNING) << "======= Ooops! kvrocks "<< VERSION << " got signal: "  << sig << " =======";
  int trace_size = backtrace(trace, 100);
  /* overwrite sigaction with caller's address */
  if (getMcontextEip(uc) != nullptr) {
    trace[1] = getMcontextEip(uc);
  }
  messages = backtrace_symbols(trace, trace_size);
  for (int i = 1; i < trace_size; ++i) {
    LOG(WARNING) << messages[i];
  }
  /* Make sure we exit with the right signal at the end. So for instance
   * the core will be dumped if enabled.
   */
  sigemptyset(&act.sa_mask);
  /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
   * is used. Otherwise, sa_handler is used
   */
  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
  act.sa_handler = SIG_DFL;
  sigaction(sig, &act, nullptr);
  kill(getpid(), sig);
}

void setupSigSegvAction() {
  struct sigaction act;

  sigemptyset(&act.sa_mask);
  /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
   * is used. Otherwise, sa_handler is used */
  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND | SA_SIGINFO;
  act.sa_sigaction = segvHandler;
  sigaction(SIGSEGV, &act, nullptr);
  sigaction(SIGBUS, &act, nullptr);
  sigaction(SIGFPE, &act, nullptr);
  sigaction(SIGILL, &act, nullptr);
  sigaction(SIGBUS, &act, nullptr);

  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
  act.sa_handler = signal_handler;
  sigaction(SIGTERM, &act, nullptr);
  sigaction(SIGINT, &act, nullptr);
}

#else /* HAVE_BACKTRACE */
void setupSigSegvAction() {
}
#endif /* HAVE_BACKTRACE */

static void usage(const char* program) {
  std::cout << program << " implements the Redis protocol based on rocksdb\n"
            << "\t-c config file, default is " << kDefaultConfPath << "\n"
            << "\t-h help\n";
  exit(0);
}

static Options parseCommandLineOptions(int argc, char **argv) {
  int ch;
  Options opts;
  while ((ch = ::getopt(argc, argv, "c:hv")) != -1) {
    switch (ch) {
      case 'c': opts.conf_file = optarg; break;
      case 'h': opts.show_usage = true; break;
      case 'v': exit(0);
      default: usage(argv[0]);
    }
  }
  return opts;
}

static void initGoogleLog(const Config *config) {
  FLAGS_minloglevel = config->loglevel;
  FLAGS_max_log_size = 100;
  FLAGS_logbufsecs = 0;
  FLAGS_log_dir = config->log_dir;
}

bool supervisedUpstart() {
  const char *upstart_job = getenv("UPSTART_JOB");
  if (!upstart_job) {
    LOG(WARNING) << "upstart supervision requested, but UPSTART_JOB not found";
    return false;
  }
  LOG(INFO) << "supervised by upstart, will stop to signal readiness";
  raise(SIGSTOP);
  unsetenv("UPSTART_JOB");
  return true;
}

bool supervisedSystemd() {
  const char *notify_socket = getenv("NOTIFY_SOCKET");
  if (!notify_socket) {
    LOG(WARNING) << "systemd supervision requested, but NOTIFY_SOCKET not found";
    return false;
  }

  int fd = 1;
  if ((fd = socket(AF_UNIX, SOCK_DGRAM, 0)) == -1) {
    LOG(WARNING) << "Can't connect to systemd socket " << notify_socket;
    return false;
  }

  struct sockaddr_un su;
  memset(&su, 0, sizeof(su));
  su.sun_family = AF_UNIX;
  strncpy (su.sun_path, notify_socket, sizeof(su.sun_path) -1);
  su.sun_path[sizeof(su.sun_path) - 1] = '\0';
  if (notify_socket[0] == '@')
    su.sun_path[0] = '\0';

  struct iovec iov;
  memset(&iov, 0, sizeof(iov));
  std::string ready = "READY=1";
  iov.iov_base = &ready[0];
  iov.iov_len = ready.size();

  struct msghdr hdr;
  memset(&hdr, 0, sizeof(hdr));
  hdr.msg_name = &su;
  hdr.msg_namelen = offsetof(struct sockaddr_un, sun_path) + strlen(notify_socket);
  hdr.msg_iov = &iov;
  hdr.msg_iovlen = 1;

  int sendto_flags = 0;
  unsetenv("NOTIFY_SOCKET");
#ifdef HAVE_MSG_NOSIGNAL
  sendto_flags |= MSG_NOSIGNAL;
#endif
  if (sendmsg(fd, &hdr, sendto_flags) < 0) {
    LOG(WARNING) << "Can't send notification to systemd";
    close(fd);
    return false;
  }
  close(fd);
  return true;
}

bool isSupervisedMode(int mode) {
  if (mode == SUPERVISED_AUTODETECT)  {
    const char *upstart_job = getenv("UPSTART_JOB");
    const char *notify_socket = getenv("NOTIFY_SOCKET");
    if (upstart_job) {
      mode = SUPERVISED_UPSTART;
    }  else if (notify_socket) {
      mode = SUPERVISED_SYSTEMD;
    }
  }
  if (mode == SUPERVISED_UPSTART) {
    return supervisedUpstart();
  } else if (mode == SUPERVISED_SYSTEMD) {
    return supervisedSystemd();
  }
  return false;
}

static Status createPidFile(const std::string &path) {
  int fd = open(path.data(), O_RDWR|O_CREAT, 0660);
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

int main(int argc, char* argv[]) {
  google::InitGoogleLogging("kvrocks");
  evthread_use_pthreads();

  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
  setupSigSegvAction();

  std::cout << "Version: " << VERSION << " @" << GIT_COMMIT << std::endl;
  auto opts = parseCommandLineOptions(argc, argv);
  if (opts.show_usage) usage(argv[0]);

  Redis::InitCommandsTable();
  Redis::PopulateCommands();

  Config config;
  Status s = config.Load(opts.conf_file);
  if (!s.IsOK()) {
    std::cout << "Failed to load config, err: " << s.Msg() << std::endl;
    exit(1);
  }
  initGoogleLog(&config);
  // Tricky: We don't expect that different instances running on the same port,
  // but the server use REUSE_PORT to support the multi listeners. So we connect
  // the listen port to check if the port has already listened or not.
  if (Util::IsPortInUse(config.port)) {
    std::cout << "Failed to start the server, the specified port["
              << config.port << "] is already in use" << std::endl;
    exit(1);
  }
  bool is_supervised = isSupervisedMode(config.supervised_mode);
  if (config.daemonize && !is_supervised) daemonize();
  s = createPidFile(config.pidfile);
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to create pidfile: " << s.Msg();
    exit(1);
  }

  LOG(INFO) << "Version: " << VERSION << " @" << GIT_COMMIT;
  Engine::Storage storage(&config);
  s = storage.Open();
  if (!s.IsOK()) {
    LOG(ERROR) << "Failed to open: " << s.Msg();
    removePidFile(config.pidfile);
    exit(1);
  }
  Server svr(&storage, &config);
  hup_handler = [&svr] {
    if (!svr.IsStopped()) {
      LOG(INFO) << "Bye Bye";
      svr.Stop();
    }
  };
  svr.Start();
  svr.Join();

  removePidFile(config.pidfile);
  google::ShutdownGoogleLogging();
  libevent_global_shutdown();
  return 0;
}
