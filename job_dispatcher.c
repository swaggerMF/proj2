#define _POSIX_C_SOURCE 200809L

#include <mpi.h>
#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MASTER_RANK 0
#define CLIENT_ID_LEN 32
#define NAME_LEN 9
#define LINE_BUF 256

enum {
    CMD_PRIMES = 1,
    CMD_PRIMEDIVISORS = 2,
    CMD_ANAGRAMS = 3
};

enum {
    TAG_WORK = 1,
    TAG_STOP = 2,
    TAG_RESULT_HEADER = 3,
    TAG_RESULT_STRING = 4
};

typedef struct {
    int job_id;
    int cmd;
    long long n;
    char client[CLIENT_ID_LEN];
    char name[NAME_LEN];
} JobPacket;

typedef struct {
    int job_id;
    int cmd;
    long long num_result;
    int anagram_count;
    int str_len;
    char client[CLIENT_ID_LEN];
} ResultPacket;

typedef struct {
    int job_id;
    int cmd;
    long long n;
    char client[CLIENT_ID_LEN];
    char name[NAME_LEN];
    int worker_rank;
    double received;
    double dispatched;
    double finished;
} JobLog;

typedef struct {
    JobLog *items;
    int cap;
} LogBook;

typedef enum {
    LINE_EMPTY = 0,
    LINE_WAIT,
    LINE_JOB,
    LINE_BAD
} LineKind;

typedef struct {
    LineKind kind;
    int wait_seconds;
    int cmd;
    long long n;
    char client[CLIENT_ID_LEN];
    char name[NAME_LEN];
} ParsedCmd;

const char *cmd_label(int cmd) {
    switch (cmd) {
        case CMD_PRIMES: return "PRIMES";
        case CMD_PRIMEDIVISORS: return "PRIMEDIVISORS";
        case CMD_ANAGRAMS: return "ANAGRAMS";
        default: return "UNKNOWN";
    }
}

void logbook_ensure(LogBook *lb, int job_id) {
    if (job_id < lb->cap) return;
    int new_cap = lb->cap == 0 ? 1024 : lb->cap;
    while (job_id >= new_cap) new_cap *= 2;
    JobLog *next = realloc(lb->items, (size_t)new_cap * sizeof(*next));
    if (!next) {
        perror("realloc logbook");
        exit(EXIT_FAILURE);
    }
    memset(next + lb->cap, 0, (size_t)(new_cap - lb->cap) * sizeof(*next));
    lb->items = next;
    lb->cap = new_cap;
}

void write_client_line(const char *client, const char *text) {
    char fname[64];
    snprintf(fname, sizeof(fname), "%s.out", client);
    FILE *f = fopen(fname, "a");
    if (!f) {
        perror("fopen client output");
        return;
    }
    fputs(text, f);
    fclose(f);
}

void rstrip_inplace(char *s) {
    size_t len = strlen(s);
    while (len > 0) {
        unsigned char ch = (unsigned char)s[len - 1];
        if (ch == '\n' || ch == '\r' || isspace(ch)) {
            s[len - 1] = '\0';
            len--;
        } else {
            break;
        }
    }
}

char *next_token(char **cursor) {
    char *s = *cursor;
    while (*s && isspace((unsigned char)*s)) s++;
    if (*s == '\0') {
        *cursor = s;
        return NULL;
    }
    char *start = s;
    while (*s && !isspace((unsigned char)*s)) s++;
    if (*s) {
        *s = '\0';
        s++;
    }
    *cursor = s;
    return start;
}

int parse_nonneg_int(const char *s, int *out) {
    char *end = NULL;
    long val = strtol(s, &end, 10);
    if (end == s) return 0;
    while (*end && isspace((unsigned char)*end)) end++;
    if (*end != '\0') return 0;
    if (val < 0 || val > INT_MAX) return 0;
    *out = (int)val;
    return 1;
}

int parse_nonneg_ll(const char *s, long long *out) {
    char *end = NULL;
    long long val = strtoll(s, &end, 10);
    if (end == s) return 0;
    while (*end && isspace((unsigned char)*end)) end++;
    if (*end != '\0') return 0;
    if (val < 0) return 0;
    *out = val;
    return 1;
}

LineKind parse_line(const char *line, ParsedCmd *out) {
    if (!line || !out) return LINE_BAD;
    char buf[LINE_BUF];
    snprintf(buf, sizeof(buf), "%s", line);
    rstrip_inplace(buf);
    char *cur = buf;
    char *t1 = next_token(&cur);
    if (!t1) {
        out->kind = LINE_EMPTY;
        return LINE_EMPTY;
    }
    if (strcmp(t1, "WAIT") == 0) {
        char *t2 = next_token(&cur);
        char *extra = next_token(&cur);
        int seconds = 0;
        if (!t2 || extra || !parse_nonneg_int(t2, &seconds)) {
            out->kind = LINE_BAD;
            return LINE_BAD;
        }
        out->kind = LINE_WAIT;
        out->wait_seconds = seconds;
        return LINE_WAIT;
    }

    char *cmd = next_token(&cur);
    char *arg = next_token(&cur);
    char *extra = next_token(&cur);
    if (!cmd || !arg || extra) {
        out->kind = LINE_BAD;
        return LINE_BAD;
    }

    memset(out, 0, sizeof(*out));
    snprintf(out->client, sizeof(out->client), "%s", t1);

    if (strcmp(cmd, "PRIMES") == 0) {
        int n = 0;
        if (!parse_nonneg_int(arg, &n)) {
            out->kind = LINE_BAD;
            return LINE_BAD;
        }
        out->cmd = CMD_PRIMES;
        out->n = n;
    } else if (strcmp(cmd, "PRIMEDIVISORS") == 0) {
        long long n = 0;
        if (!parse_nonneg_ll(arg, &n)) {
            out->kind = LINE_BAD;
            return LINE_BAD;
        }
        out->cmd = CMD_PRIMEDIVISORS;
        out->n = n;
    } else if (strcmp(cmd, "ANAGRAMS") == 0) {
        if (strlen(arg) > NAME_LEN - 1) {
            out->kind = LINE_BAD;
            return LINE_BAD;
        }
        out->cmd = CMD_ANAGRAMS;
        snprintf(out->name, sizeof(out->name), "%s", arg);
    } else {
        out->kind = LINE_BAD;
        return LINE_BAD;
    }

    out->kind = LINE_JOB;
    return LINE_JOB;
}

long long count_primes_sieve(int n) {
    if (n < 2) return 0;
    unsigned char *flags = malloc((size_t)n + 1);
    if (!flags) {
        perror("malloc primes");
        exit(EXIT_FAILURE);
    }
    memset(flags, 1, (size_t)n + 1);
    flags[0] = 0;
    flags[1] = 0;
    for (int p = 2; (long long)p * p <= n; p++) {
        if (!flags[p]) continue;
        for (int k = p * p; k <= n; k += p) flags[k] = 0;
    }
    long long count = 0;
    for (int i = 2; i <= n; i++) {
        if (flags[i]) count++;
    }
    free(flags);
    return count;
}

long long count_prime_divisors(long long n) {
    if (n <= 1) return 0;
    long long count = 0;
    if (n % 2 == 0) {
        count++;
        while (n % 2 == 0) n /= 2;
    }
    for (long long p = 3; p * p <= n; p += 2) {
        if (n % p == 0) {
            count++;
            while (n % p == 0) n /= p;
        }
    }
    if (n > 1) count++;
    return count;
}

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} StrBuf;

void sb_init(StrBuf *sb, size_t cap) {
    sb->data = malloc(cap);
    if (!sb->data) {
        perror("malloc anagrams");
        exit(EXIT_FAILURE);
    }
    sb->len = 0;
    sb->cap = cap;
}

void sb_reserve(StrBuf *sb, size_t extra) {
    if (sb->len + extra <= sb->cap) return;
    size_t new_cap = sb->cap == 0 ? 128 : sb->cap;
    while (new_cap < sb->len + extra) new_cap *= 2;
    char *next = realloc(sb->data, new_cap);
    if (!next) {
        perror("realloc anagrams");
        exit(EXIT_FAILURE);
    }
    sb->data = next;
    sb->cap = new_cap;
}

void sb_append(StrBuf *sb, const char *src, size_t n) {
    sb_reserve(sb, n);
    memcpy(sb->data + sb->len, src, n);
    sb->len += n;
}

int cmp_char(const void *a, const void *b) {
    char ca = *(const char *)a;
    char cb = *(const char *)b;
    return (ca > cb) - (ca < cb);
}

int next_permutation(char *a, int n) {
    int i = n - 2;
    while (i >= 0 && a[i] >= a[i + 1]) i--;
    if (i < 0) return 0;
    int j = n - 1;
    while (a[j] <= a[i]) j--;
    char tmp = a[i];
    a[i] = a[j];
    a[j] = tmp;
    for (int l = i + 1, r = n - 1; l < r; l++, r--) {
        char t = a[l];
        a[l] = a[r];
        a[r] = t;
    }
    return 1;
}

int build_anagrams(const char *input, char **out, size_t *out_len, int *count) {
    if (!input || !out || !out_len || !count) return 0;
    size_t n = strlen(input);
    if (n > NAME_LEN - 1) return 0;
    if (n == 0) {
        *out = malloc(1);
        if (!*out) {
            perror("malloc anagrams");
            exit(EXIT_FAILURE);
        }
        (*out)[0] = '\0';
        *out_len = 0;
        *count = 0;
        return 1;
    }
    char *chars = malloc(n);
    if (!chars) {
        perror("malloc anagrams");
        exit(EXIT_FAILURE);
    }
    memcpy(chars, input, n);
    qsort(chars, n, sizeof(char), cmp_char);

    StrBuf sb;
    sb_init(&sb, 128);
    int total = 0;
    do {
        sb_append(&sb, chars, n);
        sb_append(&sb, "\n", 1);
        total++;
    } while (next_permutation(chars, (int)n));

    free(chars);
    sb_reserve(&sb, 1);
    sb.data[sb.len] = '\0';
    *out = sb.data;
    *out_len = sb.len;
    *count = total;
    return 1;
}

void send_job_packet(const JobPacket *job, int dest, int tag) {
    MPI_Send((void *)job, (int)sizeof(*job), MPI_BYTE, dest, tag, MPI_COMM_WORLD);
}

int recv_job_packet(JobPacket *job, int src, MPI_Status *status) {
    MPI_Status st;
    MPI_Recv(job, (int)sizeof(*job), MPI_BYTE, src, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
    if (status) *status = st;
    return st.MPI_TAG != TAG_STOP;
}

void send_result_packet(const ResultPacket *res, int dest) {
    MPI_Send((void *)res, (int)sizeof(*res), MPI_BYTE, dest, TAG_RESULT_HEADER, MPI_COMM_WORLD);
}

int recv_result_packet(ResultPacket *res, int *src) {
    MPI_Status st;
    MPI_Recv(res, (int)sizeof(*res), MPI_BYTE, MPI_ANY_SOURCE, TAG_RESULT_HEADER, MPI_COMM_WORLD, &st);
    if (src) *src = st.MPI_SOURCE;
    return 1;
}

int find_free_worker(const int *worker_free, int comm_sz) {
    for (int r = 1; r < comm_sz; r++) {
        if (worker_free[r]) return r;
    }
    return -1;
}

void handle_result(int *worker_free, LogBook *lb, FILE *logf, double t0) {
    ResultPacket res;
    int src = -1;
    recv_result_packet(&res, &src);
    worker_free[src] = 1;

    logbook_ensure(lb, res.job_id);
    JobLog *log = &lb->items[res.job_id];
    log->finished = MPI_Wtime() - t0;

    if (logf) {
        if (log->cmd == CMD_ANAGRAMS) {
            fprintf(logf,
                    "job=%d client=%s cmd=%s arg=%s worker=%d received=%.6f dispatched=%.6f finished=%.6f\n",
                    log->job_id, log->client, cmd_label(log->cmd), log->name,
                    log->worker_rank, log->received, log->dispatched, log->finished);
        } else {
            fprintf(logf,
                    "job=%d client=%s cmd=%s arg=%lld worker=%d received=%.6f dispatched=%.6f finished=%.6f\n",
                    log->job_id, log->client, cmd_label(log->cmd), log->n,
                    log->worker_rank, log->received, log->dispatched, log->finished);
        }
        fflush(logf);
    }

    if (res.cmd == CMD_ANAGRAMS && res.str_len > 0) {
        char *buf = malloc((size_t)res.str_len + 1);
        if (!buf) {
            perror("malloc result");
            return;
        }
        MPI_Recv(buf, res.str_len, MPI_CHAR, src, TAG_RESULT_STRING, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        buf[res.str_len] = '\0';

        char header[256];
        snprintf(header, sizeof(header), "JOB %d ANAGRAMS => %d anagrams\n",
                 res.job_id, res.anagram_count);
        write_client_line(res.client, header);
        write_client_line(res.client, buf);
        write_client_line(res.client, "\n");
        free(buf);
    } else {
        char line[256];
        if (res.cmd == CMD_PRIMES) {
            snprintf(line, sizeof(line), "JOB %d PRIMES => %lld\n", res.job_id, res.num_result);
        } else {
            snprintf(line, sizeof(line), "JOB %d PRIMEDIVISORS => %lld\n", res.job_id, res.num_result);
        }
        write_client_line(res.client, line);
    }
}

void run_master(const char *cmd_path, int comm_sz) {
    FILE *fp = fopen(cmd_path, "r");
    if (!fp) {
        perror("fopen command file");
        return;
    }

    FILE *logf = fopen("dispatcher.log", "w");
    if (!logf) {
        perror("fopen dispatcher log");
        fclose(fp);
        return;
    }

    int *worker_free = calloc((size_t)comm_sz, sizeof(int));
    if (!worker_free) {
        perror("calloc worker state");
        fclose(fp);
        fclose(logf);
        return;
    }
    for (int r = 1; r < comm_sz; r++) worker_free[r] = 1;

    double t0 = MPI_Wtime();
    double t_start = t0;

    LogBook lb = {0};
    char line[LINE_BUF];
    int job_id = 1;
    int active_jobs = 0;

    while (fgets(line, sizeof(line), fp)) {
        ParsedCmd pc;
        LineKind kind = parse_line(line, &pc);
        if (kind == LINE_EMPTY) continue;
        if (kind == LINE_BAD) {
            fprintf(stderr, "Invalid command: %s", line);
            continue;
        }
        if (kind == LINE_WAIT) {
            sleep((unsigned int)pc.wait_seconds);
            continue;
        }

        int worker = find_free_worker(worker_free, comm_sz);
        while (worker == -1) {
            handle_result(worker_free, &lb, logf, t0);
            active_jobs--;
            worker = find_free_worker(worker_free, comm_sz);
        }

        JobPacket job;
        memset(&job, 0, sizeof(job));
        job.job_id = job_id++;
        job.cmd = pc.cmd;
        job.n = pc.n;
        snprintf(job.client, sizeof(job.client), "%s", pc.client);
        snprintf(job.name, sizeof(job.name), "%s", pc.name);

        logbook_ensure(&lb, job.job_id);
        JobLog *log = &lb.items[job.job_id];
        memset(log, 0, sizeof(*log));
        log->job_id = job.job_id;
        log->cmd = job.cmd;
        log->n = job.n;
        snprintf(log->client, sizeof(log->client), "%s", job.client);
        snprintf(log->name, sizeof(log->name), "%s", job.name);
        log->received = MPI_Wtime() - t0;
        log->worker_rank = worker;
        log->dispatched = MPI_Wtime() - t0;

        send_job_packet(&job, worker, TAG_WORK);
        worker_free[worker] = 0;
        active_jobs++;
    }

    fclose(fp);

    while (active_jobs > 0) {
        handle_result(worker_free, &lb, logf, t0);
        active_jobs--;
    }

    JobPacket stop = {0};
    for (int r = 1; r < comm_sz; r++) {
        send_job_packet(&stop, r, TAG_STOP);
    }

    double t_end = MPI_Wtime();
    FILE *mes = fopen("time_measurements.txt", "a");
    if (mes) {
        fprintf(mes, "parallel_total_seconds=%.6f, workers=%d\n",
                (t_end - t_start), comm_sz - 1);
        fclose(mes);
    }

    free(lb.items);
    free(worker_free);
    fclose(logf);
}

void run_worker(void) {
    while (1) {
        JobPacket job;
        MPI_Status st;
        int ok = recv_job_packet(&job, MASTER_RANK, &st);
        if (!ok) break;

        ResultPacket res;
        memset(&res, 0, sizeof(res));
        res.job_id = job.job_id;
        res.cmd = job.cmd;
        snprintf(res.client, sizeof(res.client), "%s", job.client);

        if (job.cmd == CMD_PRIMES) {
            res.num_result = count_primes_sieve((int)job.n);
            send_result_packet(&res, MASTER_RANK);
        } else if (job.cmd == CMD_PRIMEDIVISORS) {
            res.num_result = count_prime_divisors(job.n);
            send_result_packet(&res, MASTER_RANK);
        } else if (job.cmd == CMD_ANAGRAMS) {
            char *out = NULL;
            size_t len = 0;
            int count = 0;
            if (!build_anagrams(job.name, &out, &len, &count) || !out) {
                send_result_packet(&res, MASTER_RANK);
            } else {
                res.anagram_count = count;
                res.str_len = (int)len;
                send_result_packet(&res, MASTER_RANK);
                if (res.str_len > 0) {
                    MPI_Send(out, res.str_len, MPI_CHAR, MASTER_RANK,
                             TAG_RESULT_STRING, MPI_COMM_WORLD);
                }
                free(out);
            }
        }
    }
}

void run_serial(const char *cmd_path) {
    FILE *fp = fopen(cmd_path, "r");
    if (!fp) {
        perror("fopen command file");
        return;
    }

    FILE *mes = fopen("time_measurements.txt", "a");
    if (!mes) {
        perror("fopen time measurements");
        fclose(fp);
        return;
    }

    double t_start = MPI_Wtime();
    char line[LINE_BUF];
    int job_id = 1;

    while (fgets(line, sizeof(line), fp)) {
        ParsedCmd pc;
        LineKind kind = parse_line(line, &pc);
        if (kind == LINE_EMPTY || kind == LINE_BAD) continue;
        if (kind == LINE_WAIT) continue;

        if (pc.cmd == CMD_PRIMES) {
            long long ans = count_primes_sieve((int)pc.n);
            char out[256];
            snprintf(out, sizeof(out), "JOB %d PRIMES => %lld\n", job_id, ans);
            write_client_line(pc.client, out);
        } else if (pc.cmd == CMD_PRIMEDIVISORS) {
            long long ans = count_prime_divisors(pc.n);
            char out[256];
            snprintf(out, sizeof(out), "JOB %d PRIMEDIVISORS => %lld\n", job_id, ans);
            write_client_line(pc.client, out);
        } else if (pc.cmd == CMD_ANAGRAMS) {
            char *out = NULL;
            size_t len = 0;
            int count = 0;
            build_anagrams(pc.name, &out, &len, &count);
            char header[256];
            snprintf(header, sizeof(header), "JOB %d ANAGRAMS => %d anagrams\n", job_id, count);
            write_client_line(pc.client, header);
            if (out) {
                write_client_line(pc.client, out);
                write_client_line(pc.client, "\n");
                free(out);
            }
        }
        job_id++;
    }

    double t_end = MPI_Wtime();
    fprintf(mes, "serial_total_seconds=%.6f\n", (t_end - t_start));
    fclose(mes);
    fclose(fp);
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int rank = -1;
    int comm_sz = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    if (rank == MASTER_RANK) {
        if (argc != 2) {
            fprintf(stderr, "Usage: mpirun -np <P> %s <command_file>\n", argv[0]);
        } else if (comm_sz == 1) {
            run_serial(argv[1]);
        } else {
            run_master(argv[1], comm_sz);
        }
    } else {
        run_worker();
    }

    MPI_Finalize();
    return 0;
}
