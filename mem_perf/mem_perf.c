#define _GNU_SOURCE

#include <errno.h>
#include <getopt.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define CACHELINE_BYTES 64UL
#define U64_PER_LINE (CACHELINE_BYTES / sizeof(uint64_t))

typedef struct __attribute__((aligned(64))) {
    int *cores;
    int num_cores;
    int duration_sec;
    int interval_sec;
    int read_percent;
    size_t per_thread_buffer_bytes;
    bool random_access;
    bool latency_sweep;
    int sweep_seconds;
    int probe_core;
    size_t probe_buffer_bytes;
    int *sweep_pcts;
    int num_sweep_pcts;
} config_t;

typedef struct {
    int tid;
    int core_id;
    uint64_t *base;
    size_t region_u64;
    int read_percent;
    bool random_access;
    atomic_ullong bytes_done;
    atomic_ullong checksum;
    atomic_int *ready_count;
    atomic_bool *start_flag;
    atomic_bool *stop_flag;
} worker_ctx_t;

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [options]\n"
            "Options:\n"
            "  -c, --cores <list>         CPU core list, e.g. 0,2-4,7 (default: all available)\n"
            "  -r, --read-percent <n>     Read ratio [0-100], default 50\n"
            "  -t, --time <sec>           Run time in seconds, default 10\n"
            "  -i, --interval <sec>       Realtime print interval in seconds, default 1\n"
            "  -b, --buffer-mb <mb>       Buffer size per thread in MB, default 100\n"
            "  -a, --access <mode>        Access mode: random|seq, default random\n"
            "  -L, --latency-sweep        Print latency vs bandwidth table (MLC-like)\n"
            "  -S, --sweep-seconds <sec>  Seconds per sweep point, default 2\n"
            "  -p, --probe-core <id>      Core used by latency probe (default: last selected core)\n"
            "  -m, --probe-mb <mb>        Probe working set size in MB, default 256\n"
            "  -P, --sweep-pcts <list>    Load points in percent, default 0,25,50,75,90,100\n"
            "  -h, --help                 Show this message\n",
            prog);
}

static int parse_nonneg_int(const char *s, const char *name) {
    char *end = NULL;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || v < 0 || v > 1 << 30) {
        fprintf(stderr, "Invalid %s: %s\n", name, s);
        return -1;
    }
    return (int)v;
}

static int cmp_int(const void *a, const void *b) {
    const int ia = *(const int *)a;
    const int ib = *(const int *)b;
    return (ia > ib) - (ia < ib);
}

static int parse_core_list(const char *s, int **out_cores, int *out_n) {
    char *tmp = strdup(s);
    char *save = NULL;
    int cap = 16;
    int n = 0;
    int *cores = (int *)malloc((size_t)cap * sizeof(int));
    if (!tmp || !cores) {
        free(tmp);
        free(cores);
        return -1;
    }

    for (char *tok = strtok_r(tmp, ",", &save); tok != NULL;
         tok = strtok_r(NULL, ",", &save)) {
        char *dash = strchr(tok, '-');
        if (!dash) {
            int c = parse_nonneg_int(tok, "core id");
            if (c < 0) {
                free(tmp);
                free(cores);
                return -1;
            }
            if (n == cap) {
                cap *= 2;
                int *new_cores = (int *)realloc(cores, (size_t)cap * sizeof(int));
                if (!new_cores) {
                    free(tmp);
                    free(cores);
                    return -1;
                }
                cores = new_cores;
            }
            cores[n++] = c;
            continue;
        }

        *dash = '\0';
        int a = parse_nonneg_int(tok, "core range start");
        int b = parse_nonneg_int(dash + 1, "core range end");
        if (a < 0 || b < 0 || a > b) {
            fprintf(stderr, "Invalid core range: %s-%s\n", tok, dash + 1);
            free(tmp);
            free(cores);
            return -1;
        }
        for (int c = a; c <= b; c++) {
            if (n == cap) {
                cap *= 2;
                int *new_cores = (int *)realloc(cores, (size_t)cap * sizeof(int));
                if (!new_cores) {
                    free(tmp);
                    free(cores);
                    return -1;
                }
                cores = new_cores;
            }
            cores[n++] = c;
        }
    }

    free(tmp);
    if (n == 0) {
        free(cores);
        return -1;
    }

    qsort(cores, (size_t)n, sizeof(int), cmp_int);
    int uniq_n = 0;
    for (int i = 0; i < n; i++) {
        if (i == 0 || cores[i] != cores[i - 1]) {
            cores[uniq_n++] = cores[i];
        }
    }

    *out_cores = cores;
    *out_n = uniq_n;
    return 0;
}

static int parse_percent_list(const char *s, int **out_vals, int *out_n) {
    char *tmp = strdup(s);
    char *save = NULL;
    int cap = 16;
    int n = 0;
    int *vals = (int *)malloc((size_t)cap * sizeof(int));
    if (!tmp || !vals) {
        free(tmp);
        free(vals);
        return -1;
    }

    for (char *tok = strtok_r(tmp, ",", &save); tok != NULL;
         tok = strtok_r(NULL, ",", &save)) {
        int v = parse_nonneg_int(tok, "sweep percent");
        if (v < 0 || v > 100) {
            free(tmp);
            free(vals);
            return -1;
        }
        if (n == cap) {
            cap *= 2;
            int *new_vals = (int *)realloc(vals, (size_t)cap * sizeof(int));
            if (!new_vals) {
                free(tmp);
                free(vals);
                return -1;
            }
            vals = new_vals;
        }
        vals[n++] = v;
    }
    free(tmp);
    if (n == 0) {
        free(vals);
        return -1;
    }
    *out_vals = vals;
    *out_n = n;
    return 0;
}

static int detect_all_available_cores(int **out_cores, int *out_n) {
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == 0) {
        int n = CPU_COUNT(&set);
        if (n <= 0) {
            return -1;
        }
        int *cores = (int *)malloc((size_t)n * sizeof(int));
        if (!cores) {
            return -1;
        }
        int idx = 0;
        for (int c = 0; c < CPU_SETSIZE; c++) {
            if (CPU_ISSET(c, &set)) {
                cores[idx++] = c;
            }
        }
        *out_cores = cores;
        *out_n = idx;
        return idx > 0 ? 0 : -1;
    }

    long nproc = sysconf(_SC_NPROCESSORS_ONLN);
    if (nproc <= 0) {
        return -1;
    }
    int n = (int)nproc;
    int *cores = (int *)malloc((size_t)n * sizeof(int));
    if (!cores) {
        return -1;
    }
    for (int i = 0; i < n; i++) {
        cores[i] = i;
    }
    *out_cores = cores;
    *out_n = n;
    return 0;
}

static inline uint64_t xorshift64(uint64_t *state) {
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}

static bool parse_access_mode(const char *s, bool *out_random) {
    if (strcmp(s, "random") == 0 || strcmp(s, "rand") == 0) {
        *out_random = true;
        return true;
    }
    if (strcmp(s, "seq") == 0 || strcmp(s, "sequential") == 0) {
        *out_random = false;
        return true;
    }
    return false;
}

static void bind_core_or_die(int core) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
    if (rc != 0) {
        fprintf(stderr, "Failed to pin thread to core %d: %s\n", core, strerror(rc));
        exit(1);
    }
}

static void *worker_main(void *arg) {
    worker_ctx_t *ctx = (worker_ctx_t *)arg;
    bind_core_or_die(ctx->core_id);

    // First-touch initialization by the pinned worker thread helps NUMA placement.
    const size_t lines_for_init = ctx->region_u64 / U64_PER_LINE;
    for (size_t l = 0; l < lines_for_init; l++) {
        ctx->base[l * U64_PER_LINE] = (uint64_t)(ctx->tid + l);
    }
    atomic_fetch_add_explicit(ctx->ready_count, 1, memory_order_release);

    while (!atomic_load_explicit(ctx->start_flag, memory_order_acquire)) {
    }

    size_t idx = 0;
    uint64_t seed = (uint64_t)(ctx->tid + 1) * 0x9e3779b97f4a7c15ULL;
    uint64_t local_checksum = 0;
    uint64_t local_bytes = 0;
    const size_t step = U64_PER_LINE;
    const size_t lines = ctx->region_u64 / step;
    const size_t batch_lines = 16384;
    if (lines == 0) {
        return NULL;
    }

    while (!atomic_load_explicit(ctx->stop_flag, memory_order_relaxed)) {
        if (ctx->read_percent == 100) {
            for (size_t n = 0; n < batch_lines; n++) {
                if (ctx->random_access) {
                    idx = (size_t)(xorshift64(&seed) % (uint64_t)lines);
                }
                uint64_t *p = ctx->base + idx * step;
                local_checksum ^= p[0];
                local_bytes += CACHELINE_BYTES;
                if (!ctx->random_access) {
                    idx++;
                    if (idx == lines) {
                        idx = 0;
                    }
                }
            }
        } else if (ctx->read_percent == 0) {
            for (size_t n = 0; n < batch_lines; n++) {
                if (ctx->random_access) {
                    idx = (size_t)(xorshift64(&seed) % (uint64_t)lines);
                }
                uint64_t *p = ctx->base + idx * step;
                uint64_t v = seed ^ (uint64_t)idx;
                for (size_t i = 0; i < U64_PER_LINE; i++) {
                    p[i] = v + i;
                }
                local_bytes += CACHELINE_BYTES;
                if (!ctx->random_access) {
                    idx++;
                    if (idx == lines) {
                        idx = 0;
                    }
                }
            }
        } else {
            for (size_t n = 0; n < batch_lines; n++) {
                uint64_t r = xorshift64(&seed) % 100ULL;
                if (ctx->random_access) {
                    idx = (size_t)(xorshift64(&seed) % (uint64_t)lines);
                }
                uint64_t *p = ctx->base + idx * step;

                if ((int)r < ctx->read_percent) {
                    for (size_t i = 0; i < U64_PER_LINE; i++) {
                        local_checksum += p[i];
                    }
                } else {
                    uint64_t v = seed ^ (uint64_t)idx;
                    for (size_t i = 0; i < U64_PER_LINE; i++) {
                        p[i] = v + i;
                    }
                }

                local_bytes += CACHELINE_BYTES;
                if (!ctx->random_access) {
                    idx++;
                    if (idx == lines) {
                        idx = 0;
                    }
                }
            }
        }
        atomic_store_explicit(&ctx->bytes_done, local_bytes, memory_order_relaxed);
    }

    atomic_store_explicit(&ctx->bytes_done, local_bytes, memory_order_relaxed);
    atomic_fetch_add_explicit(&ctx->checksum, local_checksum, memory_order_relaxed);
    return NULL;
}

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static long long round_to_ll(double x) {
    return (long long)(x >= 0.0 ? x + 0.5 : x - 0.5);
}

static int pin_current_thread(int core) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    return pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
}

static void build_random_cycle(uint32_t *next_idx, size_t n, uint64_t seed) {
    uint32_t *perm = (uint32_t *)malloc(n * sizeof(uint32_t));
    if (!perm) {
        fprintf(stderr, "Failed to allocate permutation buffer\n");
        exit(1);
    }
    for (size_t i = 0; i < n; i++) {
        perm[i] = (uint32_t)i;
    }
    for (size_t i = n - 1; i > 0; i--) {
        size_t j = (size_t)(xorshift64(&seed) % (uint64_t)(i + 1));
        uint32_t tmp = perm[i];
        perm[i] = perm[j];
        perm[j] = tmp;
    }
    for (size_t i = 0; i + 1 < n; i++) {
        next_idx[perm[i]] = perm[i + 1];
    }
    next_idx[perm[n - 1]] = perm[0];
    free(perm);
}

static double run_probe_latency_ns(int probe_core, size_t probe_bytes, int duration_sec) {
    if (pin_current_thread(probe_core) != 0) {
        fprintf(stderr, "Failed to pin probe thread to core %d\n", probe_core);
        return -1.0;
    }

    size_t n = probe_bytes / sizeof(uint32_t);
    if (n < 1024) {
        n = 1024;
    }
    uint32_t *next_idx = NULL;
    if (posix_memalign((void **)&next_idx, CACHELINE_BYTES, n * sizeof(uint32_t)) != 0 || !next_idx) {
        fprintf(stderr, "Probe allocation failed\n");
        return -1.0;
    }

    build_random_cycle(next_idx, n, 0x1234abcd55aaULL);
    volatile uint32_t idx = 0;
    uint64_t iters = 0;

    uint64_t start = now_ns();
    while (true) {
        for (int k = 0; k < 10000; k++) {
            idx = next_idx[idx];
            iters++;
        }
        uint64_t now = now_ns();
        if ((double)(now - start) / 1e9 >= (double)duration_sec) {
            break;
        }
    }
    uint64_t end = now_ns();
    free(next_idx);

    if (iters == 0) {
        return -1.0;
    }
    return (double)(end - start) / (double)iters;
}

static int run_bw_latency_point(const config_t *cfg, const int *bg_cores, int bg_ncores,
                                int probe_core, double *out_bw_gbps, double *out_lat_ns) {
    if (bg_ncores == 0) {
        *out_bw_gbps = 0.0;
        *out_lat_ns = run_probe_latency_ns(probe_core, cfg->probe_buffer_bytes, cfg->sweep_seconds);
        return (*out_lat_ns < 0.0) ? -1 : 0;
    }

    size_t per_thread_u64 = cfg->per_thread_buffer_bytes / sizeof(uint64_t);
    per_thread_u64 = (per_thread_u64 / U64_PER_LINE) * U64_PER_LINE;
    if (per_thread_u64 < U64_PER_LINE) {
        return -1;
    }
    if ((size_t)bg_ncores > SIZE_MAX / per_thread_u64) {
        return -1;
    }
    size_t total_u64 = per_thread_u64 * (size_t)bg_ncores;

    uint64_t *buf = NULL;
    if (posix_memalign((void **)&buf, CACHELINE_BYTES, total_u64 * sizeof(uint64_t)) != 0 || !buf) {
        return -1;
    }

    pthread_t *threads = (pthread_t *)calloc((size_t)bg_ncores, sizeof(pthread_t));
    worker_ctx_t *ctxs = (worker_ctx_t *)calloc((size_t)bg_ncores, sizeof(worker_ctx_t));
    if (!threads || !ctxs) {
        free(buf);
        free(threads);
        free(ctxs);
        return -1;
    }

    atomic_bool start_flag;
    atomic_bool stop_flag;
    atomic_int ready_count;
    atomic_init(&start_flag, false);
    atomic_init(&stop_flag, false);
    atomic_init(&ready_count, 0);

    for (int i = 0; i < bg_ncores; i++) {
        ctxs[i].tid = i;
        ctxs[i].core_id = bg_cores[i];
        ctxs[i].base = buf + (size_t)i * per_thread_u64;
        ctxs[i].region_u64 = per_thread_u64;
        ctxs[i].read_percent = cfg->read_percent;
        ctxs[i].random_access = cfg->random_access;
        atomic_init(&ctxs[i].bytes_done, 0ULL);
        atomic_init(&ctxs[i].checksum, 0ULL);
        ctxs[i].ready_count = &ready_count;
        ctxs[i].start_flag = &start_flag;
        ctxs[i].stop_flag = &stop_flag;

        if (pthread_create(&threads[i], NULL, worker_main, &ctxs[i]) != 0) {
            atomic_store(&stop_flag, true);
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j], NULL);
            }
            free(buf);
            free(threads);
            free(ctxs);
            return -1;
        }
    }

    while (atomic_load_explicit(&ready_count, memory_order_acquire) < bg_ncores) {
        usleep(1000);
    }
    atomic_store_explicit(&start_flag, true, memory_order_release);

    uint64_t start_ns = now_ns();
    double lat_ns = run_probe_latency_ns(probe_core, cfg->probe_buffer_bytes, cfg->sweep_seconds);
    uint64_t end_ns = now_ns();

    atomic_store_explicit(&stop_flag, true, memory_order_release);
    for (int i = 0; i < bg_ncores; i++) {
        pthread_join(threads[i], NULL);
    }

    uint64_t total_bytes = 0ULL;
    for (int i = 0; i < bg_ncores; i++) {
        total_bytes += atomic_load_explicit(&ctxs[i].bytes_done, memory_order_relaxed);
    }
    double total_sec = (double)(end_ns - start_ns) / 1e9;
    *out_bw_gbps = (total_sec > 0.0) ? ((double)total_bytes / total_sec) / 1e9 : 0.0;
    *out_lat_ns = lat_ns;

    free(buf);
    free(threads);
    free(ctxs);
    return (lat_ns < 0.0) ? -1 : 0;
}

int main(int argc, char **argv) {
    int default_sweep_pcts[] = {0, 25, 50, 75, 90, 100};
    config_t cfg = {
        .cores = NULL,
        .num_cores = 0,
        .duration_sec = 10,
        .interval_sec = 1,
        .read_percent = 50,
        .per_thread_buffer_bytes = 100UL * 1024UL * 1024UL,
        .random_access = true,
        .latency_sweep = false,
        .sweep_seconds = 2,
        .probe_core = -1,
        .probe_buffer_bytes = 256UL * 1024UL * 1024UL,
        .sweep_pcts = NULL,
        .num_sweep_pcts = 0,
    };

    static struct option long_opts[] = {
        {"cores", required_argument, NULL, 'c'},
        {"read-percent", required_argument, NULL, 'r'},
        {"time", required_argument, NULL, 't'},
        {"interval", required_argument, NULL, 'i'},
        {"buffer-mb", required_argument, NULL, 'b'},
        {"access", required_argument, NULL, 'a'},
        {"latency-sweep", no_argument, NULL, 'L'},
        {"sweep-seconds", required_argument, NULL, 'S'},
        {"probe-core", required_argument, NULL, 'p'},
        {"probe-mb", required_argument, NULL, 'm'},
        {"sweep-pcts", required_argument, NULL, 'P'},
        {"help", no_argument, NULL, 'h'},
        {0, 0, 0, 0},
    };

    int opt = 0;
    while ((opt = getopt_long(argc, argv, "c:r:t:i:b:a:LS:p:m:P:h", long_opts, NULL)) != -1) {
        switch (opt) {
            case 'c':
                free(cfg.cores);
                cfg.cores = NULL;
                cfg.num_cores = 0;
                if (parse_core_list(optarg, &cfg.cores, &cfg.num_cores) != 0) {
                    fprintf(stderr, "Failed to parse --cores/-c\n");
                    return 1;
                }
                break;
            case 'r': {
                int v = parse_nonneg_int(optarg, "read percent");
                if (v < 0 || v > 100) {
                    fprintf(stderr, "--read-percent/-r must be in [0,100]\n");
                    return 1;
                }
                cfg.read_percent = v;
                break;
            }
            case 't': {
                int v = parse_nonneg_int(optarg, "time");
                if (v <= 0) {
                    fprintf(stderr, "--time/-t must be > 0\n");
                    return 1;
                }
                cfg.duration_sec = v;
                break;
            }
            case 'i': {
                int v = parse_nonneg_int(optarg, "interval");
                if (v <= 0) {
                    fprintf(stderr, "--interval/-i must be > 0\n");
                    return 1;
                }
                cfg.interval_sec = v;
                break;
            }
            case 'b': {
                int v = parse_nonneg_int(optarg, "buffer MB");
                if (v <= 0) {
                    fprintf(stderr, "--buffer-mb/-b must be > 0\n");
                    return 1;
                }
                cfg.per_thread_buffer_bytes = (size_t)v * 1024UL * 1024UL;
                break;
            }
            case 'a':
                if (!parse_access_mode(optarg, &cfg.random_access)) {
                    fprintf(stderr, "--access/-a must be one of: random, seq\n");
                    return 1;
                }
                break;
            case 'L':
                cfg.latency_sweep = true;
                break;
            case 'S': {
                int v = parse_nonneg_int(optarg, "sweep seconds");
                if (v <= 0) {
                    fprintf(stderr, "--sweep-seconds/-S must be > 0\n");
                    return 1;
                }
                cfg.sweep_seconds = v;
                break;
            }
            case 'p': {
                int v = parse_nonneg_int(optarg, "probe core");
                if (v < 0) {
                    fprintf(stderr, "--probe-core/-p must be >= 0\n");
                    return 1;
                }
                cfg.probe_core = v;
                break;
            }
            case 'm': {
                int v = parse_nonneg_int(optarg, "probe MB");
                if (v <= 0) {
                    fprintf(stderr, "--probe-mb/-m must be > 0\n");
                    return 1;
                }
                cfg.probe_buffer_bytes = (size_t)v * 1024UL * 1024UL;
                break;
            }
            case 'P':
                free(cfg.sweep_pcts);
                cfg.sweep_pcts = NULL;
                cfg.num_sweep_pcts = 0;
                if (parse_percent_list(optarg, &cfg.sweep_pcts, &cfg.num_sweep_pcts) != 0) {
                    fprintf(stderr, "Failed to parse --sweep-pcts/-P\n");
                    return 1;
                }
                break;
            case 'h':
                usage(argv[0]);
                return 0;
            default:
                usage(argv[0]);
                return 1;
        }
    }

    if (optind < argc) {
        fprintf(stderr, "Unexpected positional argument: %s\n", argv[optind]);
        usage(argv[0]);
        return 1;
    }

    if (cfg.num_cores == 0) {
        if (detect_all_available_cores(&cfg.cores, &cfg.num_cores) != 0) {
            fprintf(stderr, "Failed to detect available CPU cores\n");
            return 1;
        }
    }

    if (cfg.latency_sweep) {
        if (cfg.num_sweep_pcts == 0) {
            cfg.num_sweep_pcts = (int)(sizeof(default_sweep_pcts) / sizeof(default_sweep_pcts[0]));
            cfg.sweep_pcts = (int *)malloc((size_t)cfg.num_sweep_pcts * sizeof(int));
            if (!cfg.sweep_pcts) {
                fprintf(stderr, "Allocation failed for sweep points\n");
                free(cfg.cores);
                return 1;
            }
            memcpy(cfg.sweep_pcts, default_sweep_pcts, sizeof(default_sweep_pcts));
        }

        if (cfg.num_cores < 2) {
            fprintf(stderr, "Latency sweep needs at least 2 cores (1 probe + >=1 background)\n");
            free(cfg.cores);
            free(cfg.sweep_pcts);
            return 1;
        }

        int probe_core = cfg.probe_core >= 0 ? cfg.probe_core : cfg.cores[cfg.num_cores - 1];
        int probe_found = 0;
        int *bg_cores = (int *)malloc((size_t)cfg.num_cores * sizeof(int));
        if (!bg_cores) {
            free(cfg.cores);
            free(cfg.sweep_pcts);
            return 1;
        }
        int bg_n = 0;
        for (int i = 0; i < cfg.num_cores; i++) {
            if (cfg.cores[i] == probe_core) {
                probe_found = 1;
                continue;
            }
            bg_cores[bg_n++] = cfg.cores[i];
        }
        if (!probe_found || bg_n == 0) {
            fprintf(stderr, "Probe core must be in selected core set and leave at least one background core\n");
            free(bg_cores);
            free(cfg.cores);
            free(cfg.sweep_pcts);
            return 1;
        }

        printf("=== mem_perf latency sweep ===\n");
        printf("probe_core=%d | bg_cores=%d | per_thread_buffer=%zuMB | probe_buffer=%zuMB | access=%s | read:write=%d:%d | point_duration=%ds\n",
               probe_core, bg_n, cfg.per_thread_buffer_bytes / (1024UL * 1024UL),
               cfg.probe_buffer_bytes / (1024UL * 1024UL), cfg.random_access ? "random" : "seq",
               cfg.read_percent, 100 - cfg.read_percent, cfg.sweep_seconds);
        printf("load_pct,active_bg_threads,measured_bw_GBps,probe_latency_ns\n");
        fflush(stdout);

        for (int i = 0; i < cfg.num_sweep_pcts; i++) {
            int pct = cfg.sweep_pcts[i];
            int active = (pct == 0) ? 0 : (pct * bg_n + 99) / 100;
            if (active > bg_n) {
                active = bg_n;
            }
            double bw = 0.0;
            double lat_ns = 0.0;
            if (run_bw_latency_point(&cfg, bg_cores, active, probe_core, &bw, &lat_ns) != 0) {
                fprintf(stderr, "Failed at sweep point %d%%\n", pct);
                free(bg_cores);
                free(cfg.cores);
                free(cfg.sweep_pcts);
                return 1;
            }
            printf("%d,%d,%lld,%lld\n", pct, active, round_to_ll(bw), round_to_ll(lat_ns));
            fflush(stdout);
        }

        free(bg_cores);
        free(cfg.cores);
        free(cfg.sweep_pcts);
        return 0;
    }

    size_t per_thread_u64 = cfg.per_thread_buffer_bytes / sizeof(uint64_t);
    per_thread_u64 = (per_thread_u64 / U64_PER_LINE) * U64_PER_LINE;
    if (per_thread_u64 < U64_PER_LINE) {
        fprintf(stderr, "Per-thread buffer too small\n");
        return 1;
    }

    if ((size_t)cfg.num_cores > SIZE_MAX / per_thread_u64) {
        fprintf(stderr, "Requested total buffer is too large\n");
        return 1;
    }
    size_t total_u64 = per_thread_u64 * (size_t)cfg.num_cores;

    uint64_t *buf = NULL;
    if (posix_memalign((void **)&buf, CACHELINE_BYTES, total_u64 * sizeof(uint64_t)) != 0 || !buf) {
        fprintf(stderr, "Buffer allocation failed\n");
        return 1;
    }

    pthread_t *threads = (pthread_t *)calloc((size_t)cfg.num_cores, sizeof(pthread_t));
    worker_ctx_t *ctxs = (worker_ctx_t *)calloc((size_t)cfg.num_cores, sizeof(worker_ctx_t));
    if (!threads || !ctxs) {
        fprintf(stderr, "Allocation failed for thread structures\n");
        free(buf);
        free(threads);
        free(ctxs);
        return 1;
    }

    atomic_bool start_flag;
    atomic_bool stop_flag;
    atomic_int ready_count;
    atomic_init(&start_flag, false);
    atomic_init(&stop_flag, false);
    atomic_init(&ready_count, 0);

    size_t region_u64 = per_thread_u64;

    for (int i = 0; i < cfg.num_cores; i++) {
        ctxs[i].tid = i;
        ctxs[i].core_id = cfg.cores[i];
        ctxs[i].base = buf + (size_t)i * region_u64;
        ctxs[i].region_u64 = region_u64;
        ctxs[i].read_percent = cfg.read_percent;
        ctxs[i].random_access = cfg.random_access;
        atomic_init(&ctxs[i].bytes_done, 0ULL);
        atomic_init(&ctxs[i].checksum, 0ULL);
        ctxs[i].ready_count = &ready_count;
        ctxs[i].start_flag = &start_flag;
        ctxs[i].stop_flag = &stop_flag;

        if (pthread_create(&threads[i], NULL, worker_main, &ctxs[i]) != 0) {
            fprintf(stderr, "Failed to create thread %d\n", i);
            atomic_store(&stop_flag, true);
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j], NULL);
            }
            free(cfg.cores);
            free(buf);
            free(threads);
            free(ctxs);
            return 1;
        }
    }

    printf("=== mem_perf bandwidth test ===\n");
    printf("cores=");
    for (int i = 0; i < cfg.num_cores; i++) {
        printf("%d%s", cfg.cores[i], (i == cfg.num_cores - 1) ? "" : ",");
    }
    printf(" | read:write=%d:%d | access=%s | duration=%ds | interval=%ds | per_thread_buffer=%zuMB | total_buffer=%zuMB\n",
           cfg.read_percent, 100 - cfg.read_percent, cfg.random_access ? "random" : "seq",
           cfg.duration_sec, cfg.interval_sec,
           cfg.per_thread_buffer_bytes / (1024UL * 1024UL),
           (cfg.per_thread_buffer_bytes * (size_t)cfg.num_cores) / (1024UL * 1024UL));
    printf("time_s,inst_bw_GBps,total_bytes_GB\n");
    fflush(stdout);

    while (atomic_load_explicit(&ready_count, memory_order_acquire) < cfg.num_cores) {
        usleep(1000);
    }

    atomic_store_explicit(&start_flag, true, memory_order_release);

    uint64_t start_ns = now_ns();
    uint64_t last_ns = start_ns;
    uint64_t last_bytes = 0ULL;
    uint64_t total_bytes = 0ULL;

    while (true) {
        sleep((unsigned int)cfg.interval_sec);
        uint64_t now = now_ns();

        total_bytes = 0ULL;
        for (int i = 0; i < cfg.num_cores; i++) {
            total_bytes += atomic_load_explicit(&ctxs[i].bytes_done, memory_order_relaxed);
        }

        double dt = (double)(now - last_ns) / 1e9;
        uint64_t dbytes = total_bytes - last_bytes;
        double inst_gbps = dt > 0.0 ? ((double)dbytes / dt) / 1e9 : 0.0;
        double elapsed = (double)(now - start_ns) / 1e9;
        double total_gb = (double)total_bytes / 1e9;
        printf("%lld,%lld,%lld\n", round_to_ll(elapsed), round_to_ll(inst_gbps), round_to_ll(total_gb));
        fflush(stdout);

        last_ns = now;
        last_bytes = total_bytes;

        if (elapsed >= (double)cfg.duration_sec) {
            break;
        }
    }

    atomic_store_explicit(&stop_flag, true, memory_order_release);
    for (int i = 0; i < cfg.num_cores; i++) {
        pthread_join(threads[i], NULL);
    }

    uint64_t end_ns = now_ns();
    double total_sec = (double)(end_ns - start_ns) / 1e9;
    total_bytes = 0ULL;
    uint64_t checksum = 0ULL;
    for (int i = 0; i < cfg.num_cores; i++) {
        total_bytes += atomic_load_explicit(&ctxs[i].bytes_done, memory_order_relaxed);
        checksum += atomic_load_explicit(&ctxs[i].checksum, memory_order_relaxed);
    }
    double avg_gbps = ((double)total_bytes / total_sec) / 1e9;

    printf("SUMMARY: duration=%llds total_bytes=%lldGB avg_bw=%lldGB/s checksum=%llu\n",
           round_to_ll(total_sec), round_to_ll((double)total_bytes / 1e9),
           round_to_ll(avg_gbps), (unsigned long long)checksum);

    free(cfg.cores);
    free(cfg.sweep_pcts);
    free(buf);
    free(threads);
    free(ctxs);
    return 0;
}
