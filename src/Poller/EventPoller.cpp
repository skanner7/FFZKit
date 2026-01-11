//
// Created by FFZero on 2025-01-18.
//

#include "EventPoller.h"

#include "SelectWrap.h"
#include "Util/util.h"
#include "Util/TimeTicker.h"
#include "Util/uv_errno.h"
#include "Network/sockutil.h"

#if defined(HAS_EPOLL)
#include <sys/epoll.h>

//Prevent epoll thundering ?
#if !defined(EPOLLEXCLUSIVE)
#define EPOLLEXCLUSIVE 0  
#endif

#define EPOLL_SIZE 1024

#define toEpoll(event)        (((event) & Event_Read)  ? EPOLLIN : 0) \
                            | (((event) & Event_Write) ? EPOLLOUT : 0) \
                            | (((event) & Event_Error) ? (EPOLLHUP | EPOLLERR) : 0) \
                            | (((event) & Event_LT)    ? 0 : EPOLLET)

                            
#define toPoller(epoll_event)     (((epoll_event) & (EPOLLIN | EPOLLRDNORM | EPOLLHUP)) ? Event_Read   : 0) \
                                | (((epoll_event) & (EPOLLOUT | EPOLLWRNORM)) ? Event_Write : 0) \
                                | (((epoll_event) & EPOLLHUP) ? Event_Error : 0) \
                                | (((epoll_event) & EPOLLERR) ? Event_Error : 0)                            

#define create_event() epoll_create(EPOLL_SIZE)
#if !defined(_WIN32)
#define close_event(fd) close(fd)
#else
#define close_event(fd) epoll_close(fd)
#endif

#endif // HAS_EPOLL

#if defined(HAS_KQUEUE)
#include <sys/event.h>
#define KEVENT_SIZE 1024
#define create_event() kqueue()
#define close_event(fd) close(fd)
#endif // HAS_KQUEUE

using namespace std;

namespace FFZKit {

static thread_local std::weak_ptr<EventPoller> s_current_poller;

void EventPoller::addEventPipe() {
    SockUtil::setNoBlocked(pipe_.readFD());
    // 对于唤醒机制来说，写入失败是可以接受的
    // 因为如果管道满了，说明管道里已经有数据了
    // 如果强制阻塞写，反而会拖慢任务提交者的速度
    SockUtil::setNoBlocked(pipe_.writeFD());

    // 添加内部管道事件 
    if (addEvent(pipe_.readFD(), EventPoller::Event_Read, [this](int event) { onPipeEvent(); }) == -1) {
        throw std::runtime_error("Add pipe fd to poller failed");
    }
}

EventPoller::EventPoller(string name) {
#if defined(HAS_EPOLL) || defined(HAS_KQUEUE)
    event_fd_ = create_event();
    if (event_fd_ == INVALID_EVENT_FD) {
        throw runtime_error(StrPrinter << "Create event fd failed: " << get_uv_errmsg());
    }
#if !defined(_WIN32)
    SockUtil::setCloExec(event_fd_);
#endif
 #endif //HAS_EPOLL

    name_ = std::move(name);
    logger_ = Logger::Instance().shared_from_this();
    addEventPipe();
}

size_t EventPoller::fdCount() const {
    return fd_count_;
} 

// static
EventPoller::Ptr EventPoller::getCurrentPoller() {
    return s_current_poller.lock();
}

thread::id EventPoller::getThreadId() const {
    return loop_thread_ ? loop_thread_->get_id() : thread::id();
}

const string& EventPoller::getThreadName() const {
    return name_;
}

void EventPoller::shutdown() {
    async_I([this]() {
        throw ExitException();
    }, false, true);

    if(loop_thread_) {
        try {
            loop_thread_->join();
        } catch (...) {
            loop_thread_->detach();
        }
        delete loop_thread_;
        loop_thread_ = nullptr;
    }
}

EventPoller::~EventPoller() {
    shutdown();

#if defined(HAS_EPOLL) || defined(HAS_KQUEUE)
    if (event_fd_ != INVALID_EVENT_FD) {
        close_event(event_fd_);
        event_fd_ = INVALID_EVENT_FD;
    }
#endif // HAS_EPOLL

    onPipeEvent(true);
    InfoL << getThreadName() << " destroyed!";
}

void EventPoller::runLoop(bool blocked, bool ref_self) {
    if (blocked) {
        if(ref_self) {
            s_current_poller = shared_from_this();
        }
        sem_loop_start_.post();
        exit_flag_ = false;
        int64_t minDelay;

#if defined(HAS_EPOLL)
        struct epoll_event events[EPOLL_SIZE];
        while (!exit_flag_) {
            minDelay = getMinDelay();
            startSleep(); // 用于统计当前线程负载情况
            int nfds = epoll_wait(event_fd_, events, EPOLL_SIZE, minDelay);
            sleepWakeUp(); // 结束统计当前线程负载情况
            if (nfds < 0) {
                // Timed out or interrupted
                continue;
            }
            
            event_cache_expired_.clear();
            for(int i = 0; i < nfds; ++i) {
                struct epoll_event &ev = events[i];
                int fd = ev.data.fd;
                if (event_cache_expired_.count(fd)) {
                    // event cache refresh
                    continue;
                }

                auto it = event_map_.find(fd);
                if(it == event_map_.end()) {
                    // 该fd已经被删除
                    epoll_ctl(event_fd_, EPOLL_CTL_DEL, fd, nullptr);
                    continue;
                }
                auto cb = it->second;
                try {
                    (*cb)(toPoller(ev.events));
                } catch (std::exception &ex) {
                    ErrorL << "Exception occurred when do event task: " << ex.what();
                }
            }
        }
#elif defined(HAS_KQUEUE)
        struct kevent kevents[KEVENT_SIZE];
        while (!exit_flag_) {
            minDelay = getMinDelay();
            struct timespec timeout = { (long)minDelay / 1000, (long)minDelay % 1000 * 1000000 };

            startSleep();
            int ret = kevent(event_fd_, nullptr, 0, kevents, KEVENT_SIZE, minDelay == -1 ? nullptr : &timeout);
            sleepWakeUp();
            if (ret <= 0) {
                continue;
            }

            event_cache_expired_.clear();

            for (int i = 0; i < ret; ++i) {
                auto &kev = kevents[i];
                auto fd = kev.ident;
                if (event_cache_expired_.count(fd)) {
                    // event cache refresh
                    continue;
                }

                auto it = event_map_.find(fd);
                if (it == event_map_.end()) {
                    EV_SET(&kev, fd, kev.filter, EV_DELETE, 0, 0, nullptr);
                    kevent(event_fd_, &kev, 1, nullptr, 0, nullptr);
                    continue;
                }
                auto cb = it->second;
                int event = 0;
                switch (kev.filter) {
                    case EVFILT_READ: event = Event_Read; break;
                    case EVFILT_WRITE: event = Event_Write; break;
                    default: WarnL << "unknown kevent filter: " << kev.filter; break;
                }

                try {
                    (*cb)(event);
                } catch (std::exception &ex) {
                    ErrorL << "Exception occurred when do event task: " << ex.what();
                }
            }
        }
#else 
        int ret, max_fd;
        FdSet set_read, set_write, set_err;
        List<Poll_Record::Ptr> callback_list;
        struct timeval tv;

        while (!exit_flag_) {
            minDelay = getMinDelay();
            tv.tv_sec = (decltype(tv.tv_sec))(minDelay / 1000);
            tv.tv_usec = 1000 * (minDelay % 1000);

            set_read.fdZero();
            set_write.fdZero();
            set_err.fdZero();
            max_fd = 0;

            for(auto &pr : event_map_) {
                int fd = pr.first;
                auto record = pr.second;
                if(record->event & Event_Read) {
                    set_read.fdSet(fd);
                }
                if(record->event & Event_Write) {
                    set_write.fdSet(fd);
                }
                if (pr.second->event & Event_Error) {
                    set_err.fdSet(pr.first); // 监听管道错误事件
                }
                if(fd > max_fd) {
                    max_fd = fd;
                }
            }

            startSleep();
            ret = fz_select(max_fd + 1, &set_read, &set_write, &set_err, minDelay == -1 ? nullptr : &tv);
            sleepWakeUp();

            if (ret < 0) {
                // Timed out or interrupted
                continue;
            }

            event_cache_expired_.clear();

            for(auto &pr : event_map_) {
                int fd = pr.first;
                auto record = pr.second;
                int event = 0;
                if(set_read.isSet(fd)) {
                    event |= Event_Read;
                }
                if(set_write.isSet(fd)) {
                    event |= Event_Write;
                }
                if(set_err.isSet(fd)) {
                    event |= Event_Error;
                }
                if(event) {
                    pr.second->attach = event;
                    callback_list.emplace_back(record);
                }
            }

            callback_list.for_each([&](const Poll_Record::Ptr &record) {
                if(event_cache_expired_.count(record->fd)) {
                    // event cache refresh
                    return;
                }

                try {
                    record->call_back(record->attach);
                } catch (std::exception &ex) {
                    ErrorL << "Exception occurred when do event task: " << ex.what();
                }
            });
            callback_list.clear();
        }
#endif // HAS_EPOLL
    } else {
        loop_thread_ = new std::thread(&EventPoller::runLoop, this, true, ref_self);
        sem_loop_start_.wait();
    }
}

bool EventPoller::isCurrentThread() {
    return !loop_thread_  || this_thread::get_id() == loop_thread_->get_id();
}

Task::Ptr EventPoller::async(TaskIn task, bool may_sync) {
    return async_I(std::move(task), may_sync, false);
}

Task::Ptr EventPoller::async_first(TaskIn task, bool may_sync) {
    return async_I(std::move(task), may_sync, true);
}

Task::Ptr EventPoller::async_I(TaskIn task, bool may_sync, bool first) {
    TimeTicker();
    if(may_sync && isCurrentThread()) {
        task();
        return nullptr;
    }
    auto task_ptr = std::make_shared<Task>(std::move(task));
    {
        std::lock_guard<std::mutex> lock(mtx_task_);
        if (first) {
            list_task_.emplace_front(task_ptr);
        } else {
            list_task_.emplace_back(task_ptr);
        }
    } 
    //通知poller线程执行任务
    pipe_.write("", 1);
    return task_ptr;
}


int EventPoller::addEvent(int fd, int event, PollEventCB cb) {
    TimeTicker();
    if (!cb) {
        WarnL << "PollEventCB is empty";
        return -1;
    }

    if (isCurrentThread()) {
#if defined(HAS_EPOLL)
        struct epoll_event ev = {0};
        ev.events = toEpoll(event) ;
        ev.data.fd = fd;
        int ret = epoll_ctl(event_fd_, EPOLL_CTL_ADD, fd, &ev);
        if (ret != -1) {
            event_map_.emplace(fd, std::make_shared<PollEventCB>(std::move(cb)));
        }
        fd_count_ = event_map_.size();
        return ret;
#elif defined(HAS_KQUEUE)
        struct kevent kev[2];
        int index = 0;
        if (event & Event_Read) {
            EV_SET(&kev[index++], fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, nullptr);
        }
        if (event & Event_Write) {
            EV_SET(&kev[index++], fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, 0, nullptr);
        }
        int ret = kevent(event_fd_, kev, index, nullptr, 0, nullptr);
        if (ret != -1) {
            event_map_.emplace(fd, std::make_shared<PollEventCB>(std::move(cb)));
        }
        fd_count_ = event_map_.size();
        return ret;        
#else
#ifndef _WIN32
        // win32平台，socket套接字不等于文件描述符，所以可能不适用这个限制
        if (fd >= FD_SETSIZE) {
            WarnL << "select() can not watch fd bigger than " << FD_SETSIZE;
            return -1;
        }
#endif  //  _WIN32
        auto record = std::make_shared<Poll_Record>();
        record->fd = fd;
        record->event = event;
        record->call_back = std::move(cb);
        event_map_.emplace(fd, record);
        fd_count_ = event_map_.size();
        return 0;
#endif // HAS_EPOLL
    } 

    async([this, fd, event, cb]() mutable {
        addEvent(fd, event, std::move(cb));
    });
    return 0;
}


int EventPoller::delEvent(int fd, PollCompleteCB cb) {
    TimeTicker();
    if (!cb) {
        cb = [](bool success) {};
    }

    if(isCurrentThread()) {
#if defined(HAS_EPOLL)
        int ret = -1;
        if (event_map_.erase(fd)) {
            event_cache_expired_.emplace(fd);
            ret = epoll_ctl(event_fd_, EPOLL_CTL_DEL, fd, nullptr);
        }
        cb(ret != -1);
        fd_count_ = event_map_.size();
        return ret;

#elif defined(HAS_KQUEUE)
        int ret = -1;
        if (event_map_.erase(fd)) {
            event_cache_expired_.emplace(fd);
            struct kevent kev[2];
            int index = 0;
            EV_SET(&kev[index++], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
            EV_SET(&kev[index++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
            ret = kevent(event_fd_, kev, index, nullptr, 0, nullptr);
        }
        cb(ret != -1);
        fd_count_ = event_map_.size();
        return ret;
#else 
        int ret = -1;
        if (event_map_.erase(fd)) {
            event_cache_expired_.emplace(fd);
            ret = 0;
        }
        cb(ret != -1);
        fd_count_ = event_map_.size();
        return ret;    
#endif // HAS_EPOLL
    }

    //Cross-thread operation
    async([this, fd, cb]() mutable {
        delEvent(fd, std::move(cb));
    });
    return 0;
}

int EventPoller::modifyEvent(int fd, int event, PollCompleteCB cb) {
    TimeTicker();
    if (!cb) {
        cb = [](bool success) {};
    }

    if(isCurrentThread()) {
#if defined(HAS_EPOLL)
        struct epoll_event ev = { 0 };
        ev.events = toEpoll(event);
        ev.data.fd = fd;
        auto ret = epoll_ctl(event_fd_, EPOLL_CTL_MOD, fd, &ev);
        cb(ret != -1);
        return ret;
#elif defined(HAS_KQUEUE)
        struct kevent kev[2];
        int index = 0;
        EV_SET(&kev[index++], fd, EVFILT_READ, event & Event_Read ? EV_ADD | EV_CLEAR : EV_DELETE, 0, 0, nullptr);
        EV_SET(&kev[index++], fd, EVFILT_WRITE, event & Event_Write ? EV_ADD | EV_CLEAR : EV_DELETE, 0, 0, nullptr);
        int ret = kevent(event_fd_, kev, index, nullptr, 0, nullptr);
        cb(ret != -1);
        return ret;
#else 
        auto it = event_map_.find(fd);
        if (it != event_map_.end()) {
            it->second->event = event;
        }
        cb(it != event_map_.end());
        return it != event_map_.end() ? 0 : -1;
#endif // HAS_EPOLL
    }

    async([this, fd, event, cb]() mutable {
        modifyEvent(fd, event, std::move(cb));
    });
    return 0;
}

EventPoller::DelayTask::Ptr EventPoller::doDelayTask(uint64_t delay_ms, function<uint64_t()> task) {
    auto delay_task = std::make_shared<DelayTask>(std::move(task));
    auto time_line = getCurrentMillisecond() + delay_ms;
    async_first([this, time_line, delay_task]() {
        // 刷新select或epoll的休眠时间
        delay_task_map_.emplace(time_line, delay_task);
    });
    return delay_task;
}

int64_t EventPoller::flushDelayTask(uint64_t now_time) {
    decltype(delay_task_map_) task_copy;
    task_copy.swap(delay_task_map_);

    for(auto it = task_copy.begin(); it != task_copy.end() && it->first <= now_time; it = task_copy.erase(it)) {
        //Expired tasks
        try {
            auto next_delay = (*(it->second))();
            if (next_delay) { 
                delay_task_map_.emplace(next_delay + now_time, std::move(it->second));
            }
        } catch (std::exception &ex) {
            ErrorL << "Exception occurred when do delay task: " << ex.what();
        }
    }

    task_copy.insert(delay_task_map_.begin(), delay_task_map_.end());
    task_copy.swap(delay_task_map_);
    auto it = delay_task_map_.begin();
    if (it == delay_task_map_.end()) {
        //No remaining timers
        return -1;
    }
    //Delay in execution of the last timer
    return it->first - now_time; 
}

int64_t EventPoller::getMinDelay() {
    if (delay_task_map_.empty()) {
        //No remaining timers
        return -1;
    }
    auto it = delay_task_map_.begin();
    uint64_t now = getCurrentMillisecond();
    if (it->first > now) {
        //All tasks have not expired
        return it->first - now;
    }
    //执行已到期的任务并刷新休眠延时
    return flushDelayTask(now);
}


inline void EventPoller::onPipeEvent(bool flush) {
    char buf[1024];
    int err = 0;

    if (!flush) {
       for (;;) {
            if ((err = pipe_.read(buf, sizeof(buf))) > 0) {
                // 读到管道数据,继续读,直到读空为止
                continue;
            }
            if (err == 0 || get_uv_error(true) != UV_EAGAIN) {
                // 收到eof或非EAGAIN(无更多数据)错误,说明管道无效了,重新打开管道  
                ErrorL << "Invalid pipe fd of event poller, reopen it";
                delEvent(pipe_.readFD());
                pipe_.reOpen();
                addEventPipe();
            }
            break;
        }
    }

    decltype(list_task_) list_swap_;
    {
        lock_guard<mutex> lck(mtx_task_);
        list_swap_.swap(list_task_);
    }
    
    list_swap_.for_each([&](const Task::Ptr &task) {
        try {
            (*task)();
        } catch (ExitException &) {
            exit_flag_ = true;
        } catch (std::exception &ex) {
            ErrorL << "Exception occurred when do async task: " << ex.what();
        }
    });
}


////////////////////////////////////////////////////////

static size_t s_pool_size = 0;
static bool s_enable_cpu_affinity = true;

INSTANCE_IMP(EventPollerPool)

EventPoller::Ptr EventPollerPool::getFirstPoller() {
    return static_pointer_cast<EventPoller>(threads_.front());
}

EventPoller::Ptr EventPollerPool::getPoller(bool prefer_current_thread) {
    auto poller = EventPoller::getCurrentPoller();
    if(prefer_current_thread && prefer_current_thread_ && poller) {
        return poller;
    }
    return static_pointer_cast<EventPoller>(getExecutor());
}

EventPollerPool::EventPollerPool() {
    auto size = addPoller("event-poller", s_pool_size, ThreadPool::PRIORITY_HIGHEST, true, s_enable_cpu_affinity);
    InfoL << "EventPoller created size: " << size;
}

void EventPollerPool::setPoolSize(size_t size) {
    s_pool_size = size;
}

void EventPollerPool::enableCpuAffinity(bool enable) {
    s_enable_cpu_affinity = enable;
}

void EventPollerPool::preferCurrentThread(bool flag) {
    prefer_current_thread_ = flag;
}

} // FFZKit