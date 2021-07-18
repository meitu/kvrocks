#define __STDC_FORMAT_MACROS
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <netdb.h>
#include <poll.h>
#include <errno.h>
#include <pthread.h>
#include <fcntl.h>
#include <math.h>
#include <string>
#include <algorithm>
#include <event2/util.h>
#include <event2/buffer.h>
#include <glog/logging.h>
#ifdef __linux__
#include <sys/sendfile.h>
#endif


#include "util.h"
#include "status.h"

#ifndef POLLIN
# define POLLIN      0x0001    /* There is data to read */
# define POLLPRI     0x0002    /* There is urgent data to read */
# define POLLOUT     0x0004    /* Writing now will not block */
# define POLLERR     0x0008    /* Error condition */
# define POLLHUP     0x0010    /* Hung up */
# define POLLNVAL    0x0020    /* Invalid request: fd not open */
#endif

#define AE_READABLE 1
#define AE_WRITABLE 2
#define AE_ERROR 4
#define AE_HUP 8

namespace Util {
Status SockConnect(std::string host, uint32_t port, int *fd) {
  int rv, cfd;
  char portstr[6];  /* strlen("65535") + 1; */
  addrinfo hints, *servinfo, *p;

  snprintf(portstr, sizeof(portstr), "%u", port);
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if ((rv = getaddrinfo(host.c_str(), portstr, &hints, &servinfo)) != 0) {
    return Status(Status::NotOK, gai_strerror(rv));
  }

  for (p = servinfo; p != nullptr ; p = p->ai_next) {
    if ((cfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
      continue;
    if (connect(cfd, p->ai_addr, p->ai_addrlen) == -1) {
      close(cfd);
      continue;
    }
    Status s = SockSetTcpKeepalive(cfd, 120);
    if (s.IsOK()) {
      s = SockSetTcpNoDelay(cfd, 1);
    }
    if (!s.IsOK()) {
      close(cfd);
      continue;
    }

    *fd = cfd;
    freeaddrinfo(servinfo);
    return Status::OK();
  }

  freeaddrinfo(servinfo);
  return Status(Status::NotOK, strerror(errno));
}

const std::string Float2String(double d) {
  if (isinf(d)) {
    return d > 0 ? "inf" : "-inf";
  }

  char buf[128];
  snprintf(buf, sizeof(buf), "%.17g", d);
  return buf;
}

Status SockSetTcpNoDelay(int fd, int val) {
  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) == -1) {
    return Status(Status::NotOK, strerror(errno));
  }
  return Status::OK();
}

Status SockSetTcpKeepalive(int fd, int interval) {
  int val = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val)) == -1) {
    return Status(Status::NotOK, strerror(errno));
  }

#ifdef __linux__
  // Default settings are more or less garbage, with the keepalive time
  // set to 7200 by default on Linux. Modify settings to make the feature
  // actually useful.

  // Send first probe after interval.
  val = interval;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &val, sizeof(val)) < 0) {
    return Status(Status::NotOK, std::string("setsockopt TCP_KEEPIDLE: ")+strerror(errno));
  }

  // Send next probes after the specified interval. Note that we set the
  // delay as interval / 3, as we send three probes before detecting
  // an error (see the next setsockopt call).
  val = interval / 3;
  if (val == 0) val = 1;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof(val)) < 0) {
    return Status(Status::NotOK, std::string("setsockopt TCP_KEEPINTVL: ")+strerror(errno));
  }

  // Consider the socket in error state after three we send three ACK
  // probes without getting a reply.
  val = 3;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &val, sizeof(val)) < 0) {
    return Status(Status::NotOK, std::string("setsockopt TCP_KEEPCNT: ")+strerror(errno));
  }
#else
  ((void) interval);  // Avoid unused var warning for non Linux systems.
#endif

  return Status::OK();
}

Status SockConnect(std::string host, uint32_t port, int *fd, uint64_t conn_timeout, uint64_t timeout) {
  if (conn_timeout == 0) {
    auto s = SockConnect(host, port, fd);
    if (!s.IsOK()) return s;
  } else {
    sockaddr_in sin{};
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = inet_addr(host.c_str());
    sin.sin_port = htons(port);
    *fd = socket(AF_INET, SOCK_STREAM, 0);

    fcntl(*fd, F_SETFL, O_NONBLOCK);
    connect(*fd, reinterpret_cast<sockaddr *>(&sin), sizeof(sin));

    auto retmask = Util::aeWait(*fd, AE_WRITABLE, conn_timeout);
    if ((retmask & AE_WRITABLE) == 0 ||
        (retmask & AE_ERROR) != 0 ||
        (retmask & AE_HUP) != 0
        ) {
      close(*fd);
      *fd = -1;
      return Status(Status::NotOK, strerror(errno));
    }

    int socket_arg;
    // Set to blocking mode again...
    if ((socket_arg = fcntl(*fd, F_GETFL, NULL)) < 0) {
      close(*fd);
      *fd = -1;
      return Status(Status::NotOK, strerror(errno));
    }
    socket_arg &= (~O_NONBLOCK);
    if (fcntl(*fd, F_SETFL, socket_arg) < 0) {
      close(*fd);
      *fd = -1;
      return Status(Status::NotOK, strerror(errno));
    }
    SockSetTcpNoDelay(*fd, 1);
  }
  if (timeout > 0) {
    struct timeval tv;
    tv.tv_sec = timeout / 1000;
    tv.tv_usec = (timeout % 1000) * 1000;
    if (setsockopt(*fd, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<char *>(&tv), sizeof(tv)) < 0) {
      close(*fd);
      *fd = -1;
      return Status(Status::NotOK, std::string("setsockopt failed: ") + strerror(errno));
    }
  }
  return Status::OK();
}

// NOTE: fd should be blocking here
Status SockSend(int fd, const std::string &data) {
  ssize_t n = 0;
  while (n < static_cast<ssize_t>(data.size())) {
    ssize_t nwritten = write(fd, data.c_str()+n, data.size()-n);
    if (nwritten == -1) {
      return Status(Status::NotOK, strerror(errno));
    }
    n += nwritten;
  }
  return Status::OK();
}

// Implements SockSendFileCore to transfer data between file descriptors and
// avoid transferring data to and from user space.
//
// The function prototype is just like sendfile(2) on Linux. in_fd is a file
// descriptor opened for reading and out_fd is a descriptor opened for writing.
// offset specifies where to start reading data from in_fd. count is the number
// of bytes to copy between the file descriptors.
//
// The return value is the number of bytes written to out_fd, if the transfer
// was successful. On error, -1 is returned, and errno is set appropriately.
ssize_t SockSendFileCore(int out_fd, int in_fd, off_t offset, size_t count) {
#if defined(__linux__)
    return sendfile(out_fd, in_fd, &offset, count);

#elif defined(__APPLE__)
    off_t len = count;
    if (sendfile(in_fd, out_fd, offset, &len, NULL, 0) == -1)
      return -1;
    else
      return (ssize_t)len;

#endif
    errno = ENOSYS;
    return -1;
}

// Send file by sendfile actually according to different operation systems,
// please note that, the out socket fd should be in blocking mode.
Status SockSendFile(int out_fd, int in_fd, size_t size) {
  ssize_t nwritten = 0;
  off_t  offset = 0;
  while (size != 0) {
    size_t n = size <= 16*1024 ? size : 16*1024;
    nwritten = SockSendFileCore(out_fd, in_fd, offset, n);
    if (nwritten == -1) {
      if (errno == EINTR)
        continue;
      else
        return Status(Status::NotOK, strerror(errno));
    }
    size -= nwritten;
    offset += nwritten;
  }
  return Status::OK();
}

Status SockSetBlocking(int fd, int blocking) {
  int flags;
  // Old flags
  if ((flags = fcntl(fd, F_GETFL)) == -1) {
    return Status(Status::NotOK,
           std::string("fcntl(F_GETFL): ") + strerror(errno));
  }

  // New flags
  if (blocking)
    flags &= ~O_NONBLOCK;
  else
    flags |= O_NONBLOCK;

  if (fcntl(fd, F_SETFL, flags) == -1) {
    return Status(Status::NotOK,
           std::string("fcntl(F_SETFL,O_BLOCK): ") + strerror(errno));
  }
  return Status::OK();
}

Status SockReadLine(int fd, std::string *data) {
  size_t line_len;
  evbuffer *evbuf = evbuffer_new();
  if (evbuffer_read(evbuf, fd, -1) <= 0) {
    evbuffer_free(evbuf);
    return Status(Status::NotOK, std::string("read response err: ") + strerror(errno));
  }
  char *line = evbuffer_readln(evbuf, &line_len, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) {
    free(line);
    evbuffer_free(evbuf);
    return Status(Status::NotOK, std::string("read response err(empty): ") + strerror(errno));
  }
  *data = std::string(line, line_len);
  free(line);
  evbuffer_free(evbuf);
  return Status::OK();
}

int GetPeerAddr(int fd, std::string *addr, uint32_t *port) {
  sockaddr_storage sa{};
  socklen_t sa_len = sizeof(sa);
  if (getpeername(fd, reinterpret_cast<sockaddr *>(&sa), &sa_len) < 0) {
    return -1;
  }
  if (sa.ss_family == AF_INET) {
    auto sa4 = reinterpret_cast<sockaddr_in *>(&sa);
    char buf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, reinterpret_cast<void *>(&sa4->sin_addr), buf, INET_ADDRSTRLEN);
    addr->clear();
    addr->append(buf);
    *port = ntohs(sa4->sin_port);
    return 0;
  }
  return -2;  // only support AF_INET currently
}

Status StringToNum(const std::string &str, int64_t *n, int64_t min, int64_t max) {
  try {
    *n = static_cast<int64_t>(std::stoll(str));
    if (max > min && (*n < min || *n > max)) {
      return Status(Status::NotOK, "value shoud between "+std::to_string(min)+" and "+std::to_string(max));
    }
  } catch (std::exception &e) {
    return Status(Status::NotOK, "value is not an integer or out of range");
  }
  return Status::OK();
}

std::string ToLower(std::string in) {
  std::transform(in.begin(), in.end(), in.begin(),
                 [](char c) -> char { return static_cast<char>(std::tolower(c)); });
  return in;
}

void Trim(const std::string &in, const std::string &chars, std::string *out) {
  out->clear();
  if (in.empty()) return;
  out->assign(in);
  out->erase(0, out->find_first_not_of(chars));
  out->erase(out->find_last_not_of(chars)+1);
}

void Split(std::string in, std::string delim, std::vector<std::string> *out) {
  if (in.empty() || !out) return;
  out->clear();

  std::string::size_type pos = 0;
  std::string elem, trimed_elem;
  do {
    pos = in.find_first_of(delim);
    elem = in.substr(0, pos);
    Trim(elem, delim, &trimed_elem);
    if (!trimed_elem.empty()) out->push_back(trimed_elem);
    in = in.substr(pos+1);
  } while (pos != std::string::npos);
}

void Split2KV(const std::string&in, std::string delim, std::vector<std::string> *out) {
  out->clear();
  std::string::size_type pos;
  if ((pos = in.find_first_of(delim)) != std::string::npos) {
    pos = in.find_first_of(delim);
    std::string str, key, value;
    str = in.substr(0, pos);
    Util::Trim(str, delim, &key);
    if (!key.empty()) out->push_back(key);
    str = in.substr(pos+1);
    Util::Trim(str, delim, &value);
    if (!value.empty()) out->push_back(value);
  }
}

bool HasPrefix(const std::string &str, const std::string &prefix) {
  if (str.empty() || prefix.empty()) return false;
  return !strncasecmp(str.data(), prefix.data(), prefix.size());
}

int StringMatch(const std::string &pattern, const std::string &in, int nocase) {
  return StringMatchLen(pattern.c_str(), pattern.length(), in.c_str(), in.length(), nocase);
}

// Glob-style pattern matching.
int StringMatchLen(const char *pattern, int patternLen,
                   const char *string, int stringLen, int nocase) {
  while (patternLen && stringLen) {
    switch (pattern[0]) {
      case '*':
        while (pattern[1] == '*') {
          pattern++;
          patternLen--;
        }
        if (patternLen == 1)
          return 1; /* match */
        while (stringLen) {
          if (StringMatchLen(pattern + 1, patternLen - 1,
                             string, stringLen, nocase))
            return 1; /* match */
          string++;
          stringLen--;
        }
        return 0; /* no match */
        break;
      case '?':
        if (stringLen == 0)
          return 0; /* no match */
        string++;
        stringLen--;
        break;
      case '[': {
        int not_symbol, match;

        pattern++;
        patternLen--;
        not_symbol = pattern[0] == '^';
        if (not_symbol) {
          pattern++;
          patternLen--;
        }
        match = 0;
        while (1) {
          if (pattern[0] == '\\' && patternLen >= 2) {
            pattern++;
            patternLen--;
            if (pattern[0] == string[0])
              match = 1;
          } else if (pattern[0] == ']') {
            break;
          } else if (patternLen == 0) {
            pattern--;
            patternLen++;
            break;
          } else if (pattern[1] == '-' && patternLen >= 3) {
            int start = pattern[0];
            int end = pattern[2];
            int c = string[0];
            if (start > end) {
              int t = start;
              start = end;
              end = t;
            }
            if (nocase) {
              start = tolower(start);
              end = tolower(end);
              c = tolower(c);
            }
            pattern += 2;
            patternLen -= 2;
            if (c >= start && c <= end)
              match = 1;
          } else {
            if (!nocase) {
              if (pattern[0] == string[0])
                match = 1;
            } else {
              if (tolower(static_cast<int>(pattern[0])) == tolower(static_cast<int>(string[0])))
                match = 1;
            }
          }
          pattern++;
          patternLen--;
        }
        if (not_symbol)
          match = !match;
        if (!match)
          return 0; /* no match */
        string++;
        stringLen--;
        break;
      }
      case '\\':
        if (patternLen >= 2) {
          pattern++;
          patternLen--;
        }
        /* fall through */
      default:
        if (!nocase) {
          if (pattern[0] != string[0])
            return 0; /* no match */
        } else {
          if (tolower(static_cast<int>(pattern[0])) != tolower(static_cast<int>(string[0])))
            return 0; /* no match */
        }
        string++;
        stringLen--;
        break;
    }
    pattern++;
    patternLen--;
    if (stringLen == 0) {
      while (*pattern == '*') {
        pattern++;
        patternLen--;
      }
      break;
    }
  }
  if (patternLen == 0 && stringLen == 0)
    return 1;
  return 0;
}

std::string StringToHex(const std::string &input) {
  static const char hex_digits[] = "0123456789ABCDEF";
  std::string output;
  output.reserve(input.length() * 2);
  for (unsigned char c : input) {
    output.push_back(hex_digits[c >> 4]);
    output.push_back(hex_digits[c & 15]);
  }
  return output;
}

void BytesToHuman(char *buf, size_t size, uint64_t n) {
  double d;

  if (n < 1024) {
    snprintf(buf, size, "%" PRIu64 "B", n);
  } else if (n < (1024*1024)) {
    d = static_cast<double>(n)/(1024);
    snprintf(buf, size, "%.2fK", d);
  } else if (n < (1024LL*1024*1024)) {
    d = static_cast<double>(n)/(1024*1024);
    snprintf(buf, size, "%.2fM", d);
  } else if (n < (1024LL*1024*1024*1024)) {
    d = static_cast<double>(n)/(1024LL*1024*1024);
    snprintf(buf, size, "%.2fG", d);
  } else if (n < (1024LL*1024*1024*1024*1024)) {
    d = static_cast<double>(n)/(1024LL*1024*1024*1024);
    snprintf(buf, size, "%.2fT", d);
  } else if (n < (1024LL*1024*1024*1024*1024*1024)) {
    d = static_cast<double>(n)/(1024LL*1024*1024*1024*1024);
    snprintf(buf, size, "%.2fP", d);
  } else {
    snprintf(buf, size, "%" PRIu64 "B", n);
  }
}

bool IsPortInUse(int port) {
  int fd;
  Status s = SockConnect("0.0.0.0", static_cast<uint32_t>(port), &fd);
  if (fd > 0) close(fd);
  return s.IsOK();
}

void ThreadSetName(const char *name) {
#ifdef __APPLE__
  pthread_setname_np(name);
#else
  pthread_setname_np(pthread_self(), name);
#endif
}

/* Wait for milliseconds until the given file descriptor becomes
 * writable/readable/exception */
int aeWait(int fd, int mask, uint64_t timeout) {
  struct pollfd pfd;
  int retmask = 0, retval;

  memset(&pfd, 0, sizeof(pfd));
  pfd.fd = fd;
  if (mask & AE_READABLE) pfd.events |= POLLIN;
  if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

  if ((retval = poll(&pfd, 1, timeout)) == 1) {
    if (pfd.revents & POLLIN) retmask |= AE_READABLE;
    if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
    if (pfd.revents & POLLERR) retmask |= AE_ERROR;
    if (pfd.revents & POLLHUP) retmask |= AE_HUP;
    return retmask;
  } else {
    return retval;
  }
}

}  // namespace Util
