/*
 * Copyright 2016 ScyllaDB
 */


#include <libaio.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>
#include <cstdlib>
#include <type_traits>
#include <functional>
#include <vector>
#include <algorithm>
#include <random>
#include <chrono>
#include <stdint.h>
#define min min    /* prevent xfs.h from defining min() as a macro */
#include <xfs/xfs.h>

static size_t nr = 10000;
static constexpr size_t block_size = 4096;
static constexpr size_t alignment = block_size;

template <typename Counter, typename Func>
typename std::result_of<Func()>::type
with_ctxsw_counting(Counter& counter, Func&& func) {
    struct count_guard {
        Counter& counter;
        count_guard(Counter& counter) : counter(counter) {
            counter -= nvcsw();
        }
        ~count_guard() {
            counter += nvcsw();
        }
        static Counter nvcsw() {
            struct rusage usage;
            getrusage(RUSAGE_THREAD, &usage);
            return usage.ru_nvcsw;
        }
    };
    count_guard g(counter);
    return func();
}

void test_concurrent_append(io_context_t ioctx, int fd, unsigned iodepth, std::string mode) {
    auto bufsize = block_size;
    auto ctxsw = 0;
    auto buf = aligned_alloc(alignment, block_size);
    auto current_depth = unsigned(0);
    auto initiated = 0;
    auto completed = 0;
    auto iocbs = std::vector<iocb>(iodepth);
    auto iocbps = std::vector<iocb*>(iodepth);
    std::iota(iocbps.begin(), iocbps.end(), iocbs.data());
    auto ioevs = std::vector<io_event>(iodepth);
    std::random_device random_device;
 
    auto start = std::chrono::high_resolution_clock::now();

    while (completed < nr) {
        auto i = unsigned(0);
        while (initiated < nr && current_depth < iodepth) {
            io_prep_pwrite(&iocbs[i++], fd, buf, bufsize, bufsize*initiated++);
            ++current_depth;
        }
        std::shuffle(iocbs.begin(), iocbs.begin() + i, random_device);
        if (i) {
            with_ctxsw_counting(ctxsw, [&] {
                if (io_submit(ioctx, i, iocbps.data()) < 0) {
                    throw std::runtime_error("Could not submit io request");
                }
            });
        }
        auto n = io_getevents(ioctx, 1, iodepth, ioevs.data(), nullptr);
        if (n < 0) {
            throw std::runtime_error("Error getting ioevents");
        }
        std::for_each(ioevs.begin(), ioevs.begin() + n, [](io_event& e) {
            if (e.res <= 0) {
                throw std::runtime_error("IO error");
            }
        });
        current_depth -= n;
        completed += n;
    }

    using float_ms = std::chrono::duration<float, std::chrono::milliseconds::period>;
    
    auto dur = std::chrono::high_resolution_clock::now() - start;
    auto time = std::chrono::duration_cast<float_ms>(dur);

    auto rate = float(ctxsw) / nr;
    auto verdict = rate < 0.1 ? "GOOD" : "BAD";
    std::cout << "context switch per appending io (mode " << mode << ", iodepth " << iodepth << "): " << rate
          << " (" << verdict << ")"  << " (" << time.count() << "ms)" << std::endl;
    auto ptr = mmap(nullptr, nr * block_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    auto incore = std::vector<uint8_t>(nr);
    mincore(ptr, nr * block_size, incore.data());
    if (std::any_of(incore.begin(), incore.end(), [] (uint8_t m) { return m & 1; })) {
        std::cout << "Seen data in page cache (BAD)\n";
    }
}

void test_concurrent_append_size_unchanging(io_context_t ioctx, int fd, unsigned iodepth, std::string mode) {
    ftruncate(fd, nr * block_size);
    test_concurrent_append(ioctx, fd, iodepth, mode);
}

void test_concurrent_append_size_allocated(io_context_t ioctx, int fd, unsigned iodepth, std::string mode) {
    fallocate(fd, FALLOC_FL_ZERO_RANGE, 0, nr * block_size);
    test_concurrent_append(ioctx, fd, iodepth, mode);
}

void test_concurrent_append_size_written(io_context_t ioctx, int fd, unsigned iodepth, std::string mode) {
    auto size = nr * block_size;
    fallocate(fd, 0, 0, size);

    auto w = 0;
    auto buf = aligned_alloc(alignment, block_size);
    memset(buf, 0, block_size);

    while (w < size) {
        // just assume it writes the while block
        write(fd, buf, block_size);
        w += block_size;
    }
    test_concurrent_append(ioctx, fd, iodepth, mode);
}


void run_test(std::function<void (io_context_t ioctx, int fd)> func, int flags = 0) {
    io_context_t ioctx = {};
    io_setup(128, &ioctx);
    auto fname = "fsqual.tmp";
    int fd = open(fname, flags|O_CREAT|O_EXCL|O_RDWR|O_DIRECT, 0600);
    fsxattr attr = {};
    attr.fsx_xflags |= XFS_XFLAG_EXTSIZE;
    attr.fsx_extsize = nr * block_size; // 32MB
    // Ignore error; may be !xfs, and just a hint anyway
    ::ioctl(fd, XFS_IOC_FSSETXATTR, &attr);
    unlink(fname);
    func(ioctx, fd);
    close(fd);
    io_destroy(ioctx);
}

void test_dio_info() {
    auto fname = "fsqual.tmp";
    int fd = open(fname, O_CREAT|O_EXCL|O_RDWR|O_DIRECT, 0600);
    if (fd == -1) {
        std::cout << "failed to create file\n";
        return;
    }
    unlink(fname);
    struct dioattr da;
    auto r = ioctl(fd, XFS_IOC_DIOINFO, &da);
    if (r == 0) {
        std::cout << "memory DMA alignment:         " << da.d_mem << "\n";
        std::cout << "disk DMA read alignment:      " << da.d_miniosz << "\n";
        std::cout << "disk DMA write alignment:      " << da.d_miniosz << "\n";
    }
}

int main(int ac, char** av) {
    if (ac > 1) {
        nr = std::stoi(av[1]);
    }
    std::cout << "Using " << nr << " iterations..." << std::endl;
    test_dio_info();

    using test_fptr = void (*)(io_context_t, int, unsigned, std::string);
    
    struct args_type {
        test_fptr func;
        std::string name;
        int iodepth;
    } args[] = {
		{ &test_concurrent_append, "size-changing", 1 },
		{ &test_concurrent_append, "size-changing", 2 },
		{ &test_concurrent_append_size_unchanging, "size-unchanging", 3 },
		{ &test_concurrent_append_size_unchanging, "size-unchanging", 7 },
		{ &test_concurrent_append_size_allocated, "size-allocated", 3 },
		{ &test_concurrent_append_size_allocated, "size-allocated", 7 },
		{ &test_concurrent_append_size_written, "size-written", 3 },
		{ &test_concurrent_append_size_written, "size-written", 7 },
    };

    struct open_mode_type {
        std::string suffix;
        int open_mode;
    } open_modes[] = {
        { {}, 0 }, 
        { " (O_DSYNC)", O_DSYNC },
    };

    for (auto& m : open_modes) {
        for (auto& a : args) {
            run_test(std::bind(a.func, std::placeholders::_1, std::placeholders::_2, a.iodepth, a.name + m.suffix), m.open_mode);
        }
    }
    
    return 0;
}
