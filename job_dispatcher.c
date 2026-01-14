#include <mpi.h>
#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

void run_master(const char *cmd_path, int comm_sz) {
    (void)cmd_path;
    (void)comm_sz;
    fprintf(stdout, "master placeholder\n");
}

void run_worker(void) {
    fprintf(stdout, "worker placeholder\n");
}

void run_serial(const char *cmd_path) {
    (void)cmd_path;
    fprintf(stdout, "serial placeholder\n");
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
