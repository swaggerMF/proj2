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
    (void)n;
    return 0;
}

long long count_prime_divisors(long long n) {
    (void)n;
    return 0;
}

int build_anagrams(const char *input, char **out, size_t *out_len, int *count) {
    (void)input;
    if (out) *out = NULL;
    if (out_len) *out_len = 0;
    if (count) *count = 0;
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
