#include "cron.h"
#include <stdexcept>
#include <utility>

std::string Scheduler::ToString() const {
  auto param2String = [](int n)->std::string {
    return n == -1 ? "*" : std::to_string(n);
  };
  return param2String(minute) + " " +
      param2String(hour) + " " +
      param2String(mday) + " " +
      param2String(month) + " " +
      param2String(wday);
}

Status Cron::SetScheduleTime(const std::vector<std::string> &args) {
  if (args.empty()) {
    schedulers_.clear();
    return Status::OK();
  }
  if (args.size() % 5 != 0) {
    return Status(Status::NotOK, "time expression format error,should only contain 5x fields");
  }

  std::vector<Scheduler> new_schedulers;
  Scheduler st;
  for (size_t i = 0; i < args.size(); i += 5) {
    Status s = convertToScheduleTime(args[i], args[i+1], args[i+2], args[i+3], args[i+4], &st);
    if (!s.IsOK()) {
      return Status(Status::NotOK, "time expression format error : " + s.Msg());
    }
    new_schedulers.emplace_back(std::move(st));
  }
  schedulers_ = std::move(new_schedulers);
  return Status::OK();
}

bool Cron::IsTimeMatch(struct tm *tm) {
  if (tm->tm_min == last_tm_.tm_min &&
      tm->tm_hour == last_tm_.tm_hour &&
      tm->tm_mday == last_tm_.tm_mday &&
      tm->tm_mon == last_tm_.tm_mon &&
      tm->tm_wday == last_tm_.tm_wday) {
    return false;
  }
  for (const auto &st : schedulers_) {
    if ((st.minute == -1 || tm->tm_min == st.minute) &&
        (st.hour == -1 || tm->tm_hour == st.hour) &&
        (st.mday == -1 || tm->tm_mday == st.mday) &&
        (st.month == -1 || (tm->tm_mon + 1) == st.month) &&
        (st.wday == -1 || tm->tm_wday == st.wday)) {
      last_tm_ = *tm;
      return true;
    }
  }
  return false;
}

bool Cron::IsEnabled() {
  return !schedulers_.empty();
}

std::string Cron::ToString() {
  std::string ret;
  for (size_t i = 0; i < schedulers_.size(); i++) {
    ret += schedulers_[i].ToString();
    if (i != schedulers_.size()-1) ret += " ";
  }
  return ret;
}

Status Cron::convertToScheduleTime(const std::string &minute,
                                   const std::string &hour,
                                   const std::string &mday,
                                   const std::string &month,
                                   const std::string &wday,
                                   Scheduler *st) {
  Status s;
  s = convertParam(minute, 0, 59, &st->minute);
  if (!s.IsOK()) return s;
  s = convertParam(hour, 0, 23, &st->hour);
  if (!s.IsOK()) return s;
  s = convertParam(mday, 1, 31, &st->mday);
  if (!s.IsOK()) return s;
  s = convertParam(month, 1, 12, &st->month);
  if (!s.IsOK()) return s;
  s = convertParam(wday, 0, 6, &st->wday);
  return s;
}

Status Cron::convertParam(const std::string &param, int lower_bound, int upper_bound, int *value) {
  if (param == "*") {
    *value = -1;
    return Status::OK();
  }

  try {
    *value = std::stoi(param);
  } catch (const std::invalid_argument &e) {
    return Status(Status::NotOK, "malformed token(`" + param + "`) not an integer or *");
  } catch (const std::out_of_range &e) {
    return Status(Status::NotOK, "malformed token(`" + param + "`) not convertable to int");
  }
  if (*value < lower_bound || *value > upper_bound) {
    return Status(Status::NotOK, "malformed token(`" + param + "`) out of bound");
  }
  return Status::OK();
}

