#include <cstdlib>
#include <sys/time.h>
#include <errno.h>
#include <iostream>
#include <leveldb/db.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

using namespace std;
using namespace leveldb;

DEFINE_string(dir, "", "db directory");
DEFINE_int64(n, 100000, "requests");
DEFINE_uint32(length, 1024, "value length");
DEFINE_uint32(c, 1, "concurrency");

void randStr(unsigned *seedp, int length, std::string& out) {
    out.resize(length);
    for (int i = 0; i < length; i++) {
        out[i] = rand_r(seedp) % 93 + 33;
    }
}

leveldb::DB* openDB() {
    // 如果没有指定db目录，使用临时目录
    std::string dir = FLAGS_dir;
    if (dir.empty()) {
        char tmp_template[] = "/tmp/leveldb_bench_XXXXXX";
        char *tmpdir = mkdtemp(tmp_template);
        PCHECK(tmpdir != NULL) << "call mktemp failed.";
        if (NULL == tmpdir) {
            exit(EXIT_FAILURE);
        } else {
            dir = std::string(tmpdir);
        }
    }
    LOG(INFO) << "db directory: " << dir;

    DB* db = NULL;
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 1024 * 1024 * 16;
    Status status = DB::Open(options, dir, &db);
    if (!status.ok()) {
        LOG(FATAL) << "open db failed(" << status.ToString() << ").";
        exit(EXIT_FAILURE);
    }
    return db;
}

int64_t count = 0;

void *benchRoutine(void *arg) {
    LOG(INFO) << "thread start.";
    leveldb::DB* db = (leveldb::DB *)(arg);
    unsigned seed = time(NULL);
    while (__sync_fetch_and_sub(&count,1) > 0) {
        std::string key;
        std::string value;
        randStr(&seed, 20, key);
        randStr(&seed, FLAGS_length, value);
        Status status = db->Put(WriteOptions(), key, value);
        if (!status.ok()) {
            LOG(FATAL) << "put failed(" << status.ToString() << ").";
            exit(EXIT_FAILURE);
        }
    }
    return NULL;
}

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, true);
    count = FLAGS_n;

    LOG(INFO) << "concurency: " << FLAGS_c << ", reqeust: " << FLAGS_n << ", value length: " << FLAGS_length;

    leveldb::DB* db = openDB();

    struct timeval start;
    int ret = gettimeofday(&start, NULL);
    if (ret != 0) {
        LOG(FATAL) << "get start time failed(" << strerror(errno) << ")";
        exit(EXIT_FAILURE);
    }

    std::vector<pthread_t> threads;
    threads.reserve(FLAGS_c);
    for (int i = 0; i < FLAGS_c; i++) {
        pthread_t t;
        int ret = pthread_create(&t, NULL, benchRoutine, db);
        if (ret != 0) {
            LOG(FATAL) << "create thread failed(" << strerror(errno) << ")";
            exit(EXIT_FAILURE);
        }
        threads.push_back(t);
    }

    for (int i = 0; i < FLAGS_c; i++) {
        pthread_join(threads[i], NULL);
    }

    struct timeval end;
    ret = gettimeofday(&end, NULL);
    if (ret != 0) {
        LOG(FATAL) << "get end time failed(" << strerror(errno) << ")";
        exit(EXIT_FAILURE);
    }

    struct timeval taken;
    timersub(&end, &start, &taken);

    LOG(INFO) << "finish " << FLAGS_n << " requests taken " << taken.tv_sec << "s" << taken.tv_usec / 1000
              << "ms.  ops: " << (FLAGS_n * 1000)/(taken.tv_sec*1000 + taken.tv_usec/1000);

    return 0;
}
