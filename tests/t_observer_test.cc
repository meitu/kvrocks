#include <string>
#include <gtest/gtest.h>
#include "observer.h"

class TestObserable : public Observable {
 public:
  TestObserable() : Observable() {};
};

class ObserverTest : public testing::Test {
 protected:
  ~ObserverTest() {
  }
  static void SetUpTestCase() {
  }
  static void TearDownTestCase() {
  }

  virtual void SetUp() {
    observable = new TestObserable();
  }

  virtual void TearDown() {
    delete observable;
  }

  TestObserable* observable;
};

class TestEvent : public ObserverEvent {
 public:
  TestEvent() : ObserverEvent() {}
  explicit TestEvent(const ObserverEvent&) {}
};

class TestHandler : public EventHandler {
 public:
  TestHandler() : EventHandler() {
    registerEventHandler<TestEvent>(
      std::bind(&TestHandler::event_up, this, std::placeholders::_1, std::placeholders::_2));
  }
  std::string result_str;

 private:
  void event_up(Observable subject, ObserverEvent const& event) {
    result_str = "exec event up";
  }
};

TEST_F(ObserverTest, RegisterObserver) {
  auto* handler = new TestHandler();
  observable->RegisterObserver(handler);
  EXPECT_EQ(observable->ObserverCount(), 1);

  TestEvent ev;
  observable->NotifyObservers(ev);
  EXPECT_EQ(handler->result_str, "exec event up");
  handler->result_str = "";

  observable->UnregisterObserver(handler);
  EXPECT_EQ(observable->ObserverCount(), 0);
}