#pragma once

#include <vector>
#include <iostream>
#include <algorithm>
#include <functional>
#include <unordered_map>
#include <typeindex>

class Observable;

class Observer {
 public:
  class Event {
   public:
    virtual ~Event() {}
  };
  virtual ~Observer() {}
  virtual void OnNotify(Observable const& subject, Observer::Event const& event) = 0;
};

using ObserverEvent = Observer::Event;

class Observable {
 public:
  virtual ~Observable() {}
  void RegisterObserver(Observer* observer) {
    if (std::find(std::begin(observers_), std::end(observers_), observer) == std::end(observers_)) {
      observers_.push_back(observer);
    }
  }
  void UnregisterObserver(Observer* observer) {
    observers_.erase(std::remove(std::begin(observers_), std::end(observers_), observer), std::end(observers_));
  }
  void NotifyObservers(ObserverEvent const& event) {
    for (Observer* observer : observers_) {
      observer->OnNotify(*this, event);
    }
  }
  int ObserverCount() { return observers_.size(); }

 private:
  std::vector<Observer*> observers_;
};

class EventHandler : public Observer {
 public:
  void OnNotify(Observable const& subject, ObserverEvent const& event) override {
    auto find = handlers_.find(std::type_index(typeid(event)));
    if (find != handlers_.end()) {
      find->second(subject, event);
    }
  }
  template<typename T, typename std::enable_if<std::is_base_of<ObserverEvent, T>::value>::type* = nullptr>
  void registerEventHandler(std::function<void(Observable const&, ObserverEvent const&)> handler) {
    handlers_[std::type_index(typeid(T))] = handler;
  }

 private:
  std::unordered_map<std::type_index, std::function<void(Observable const&, ObserverEvent const&)>> handlers_;
};
