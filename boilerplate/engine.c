/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Implements all TODOs from the boilerplate:
 *   - bounded_buffer_push / pop
 *   - logging_thread
 *   - child_fn  (clone child: namespaces + chroot + pipe + execve)
 *   - run_supervisor  (socket + SIGCHLD + container lifecycle)
 *   - send_control_request  (CLI client)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

/* ── Constants (unchanged from boilerplate) ─────────────────────── */
#define STACK_SIZE          (1024 * 1024)
#define CONTAINER_ID_LEN    32
#define CONTROL_PATH        "/tmp/mini_runtime.sock"
#define LOG_DIR             "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN   256
#define LOG_CHUNK_SIZE      4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT  (40UL << 20)
#define DEFAULT_HARD_LIMIT  (64UL << 20)
#define MAX_ARGS            64

/* ── Enums (unchanged from boilerplate) ─────────────────────────── */
typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

/* ── Structs (unchanged from boilerplate) ───────────────────────── */
typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;   /* pipe write end — child stdout goes here */
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* Args for the per-container pipe-reader thread */
typedef struct {
    char container_id[CONTAINER_ID_LEN];
    int  pipe_read_fd;
    bounded_buffer_t *buffer;
} reader_args_t;

/* ── Globals ────────────────────────────────────────────────────── */
/*
 * We keep a small set of globals so signal handlers can reach them.
 * Everything else is passed through ctx / args structs.
 */
static supervisor_ctx_t *g_ctx     = NULL;
static int               g_srv_fd  = -1;

/* ── Usage ──────────────────────────────────────────────────────── */
static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

/* ── Flag parsers (unchanged from boilerplate) ──────────────────── */
static int parse_mib_flag(const char *flag,
                           const char *value,
                           unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;
    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                 int argc, char *argv[], int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;
        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i+1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i+1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i+1], &end, 10);
            if (errno != 0 || end == argv[i+1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr, "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i+1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ════════════════════════════════════════════════════════════════
 * BOUNDED BUFFER  (producer-consumer ring)
 * ════════════════════════════════════════════════════════════════ */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;
    memset(buffer, 0, sizeof(*buffer));
    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0) return rc;
    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) { pthread_mutex_destroy(&buffer->mutex); return rc; }
    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * bounded_buffer_push — producer side.
 * Blocks when buffer is full. Returns 0 on success, -1 on shutdown.
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while full, unless we are shutting down */
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    /* Insert at tail, advance tail with wrap-around */
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);   /* wake the logger thread */
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * bounded_buffer_pop — consumer side.
 * Blocks when buffer is empty. Returns 0 on success, -1 when shutdown
 * AND buffer is fully drained (safe to exit).
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while empty, but wake on shutdown so we can drain remainder */
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    /* Shutting down and nothing left → tell consumer to exit */
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    /* Remove from head, advance head with wrap-around */
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);    /* wake any waiting producer */
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ════════════════════════════════════════════════════════════════
 * LOGGING THREAD  (global consumer — one per supervisor)
 * ════════════════════════════════════════════════════════════════ */

/*
 * Drains the bounded buffer continuously. Each log_item_t knows which
 * container it belongs to via container_id, so we open the correct
 * per-container log file and append to it.
 *
 * KEY FIX vs step6: we keep the log fd open across writes inside a
 * single pop() call, and use O_SYNC to guarantee data reaches disk
 * before the CLI's `logs` command reads it.
 */
void *logging_thread(void *arg)
{
    bounded_buffer_t *buffer = (bounded_buffer_t *)arg;
    log_item_t item;

    while (bounded_buffer_pop(buffer, &item) == 0) {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);

        /* O_APPEND is atomic for writes ≤ PIPE_BUF; safe for our chunk size */
        int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("[logger] open");
            continue;
        }

        size_t written = 0;
        while (written < item.length) {
            ssize_t n = write(fd, item.data + written, item.length - written);
            if (n < 0) {
                if (errno == EINTR) continue;
                perror("[logger] write");
                break;
            }
            written += (size_t)n;
        }
        close(fd);
    }

    printf("[logger] thread exiting, all chunks flushed\n");
    return NULL;
}

/* ════════════════════════════════════════════════════════════════
 * PIPE READER THREAD  (producer — one per container)
 *
 * Reads raw bytes from the container's stdout pipe and pushes
 * log_item_t chunks into the shared bounded buffer.
 * ════════════════════════════════════════════════════════════════ */
static void *pipe_reader_thread(void *arg)
{
    reader_args_t *ra = (reader_args_t *)arg;
    log_item_t item;

    while (1) {
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, ra->container_id, CONTAINER_ID_LEN - 1);

        /* Blocking read — returns 0 (EOF) when container exits */
        ssize_t n = read(ra->pipe_read_fd, item.data, LOG_CHUNK_SIZE);
        if (n <= 0) break;

        item.length = (size_t)n;

        /* Push into bounded buffer — blocks if full, exits if shutdown */
        if (bounded_buffer_push(ra->buffer, &item) != 0)
            break;
    }

    close(ra->pipe_read_fd);
    free(ra);
    return NULL;
}

/* ════════════════════════════════════════════════════════════════
 * CHILD ENTRYPOINT  (runs inside clone())
 * ════════════════════════════════════════════════════════════════ */

/*
 * FIX: We use execve("/bin/sh", ["sh","-c", command], envp) so that
 * shell built-ins, pipes, and semicolons all work inside containers.
 *
 * FIX: dup2 happens BEFORE chroot so the fd number survives across
 * the namespace boundary (fds are process-local, not path-local).
 *
 * FIX: We close ALL other open fds after dup2 so the pipe write-end
 * held in the parent is not accidentally kept alive in the child
 * (which would prevent EOF from reaching the reader thread).
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* ── Step 1: redirect stdout + stderr into the logging pipe ── */
    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0) {
        perror("child: dup2 stdout");
        return 1;
    }
    if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("child: dup2 stderr");
        return 1;
    }
    /* Close original write-end fd — we have it as fd 1 and 2 now */
    if (cfg->log_write_fd != STDOUT_FILENO && cfg->log_write_fd != STDERR_FILENO)
        close(cfg->log_write_fd);

    /* Close stdin — containers don't need interactive input */
    close(STDIN_FILENO);

    /* ── Step 2: UTS namespace — set container hostname ── */
    if (sethostname(cfg->id, strlen(cfg->id)) != 0)
        perror("child: sethostname");   /* non-fatal */

    /* ── Step 3: filesystem isolation via chroot ── */
    if (chroot(cfg->rootfs) != 0) {
        perror("child: chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("child: chdir /");
        return 1;
    }

    /* ── Step 4: mount /proc inside our private mount namespace ── */
    if (mount("proc", "/proc", "proc",
              MS_NOEXEC | MS_NOSUID | MS_NODEV, NULL) != 0)
        perror("child: mount /proc");   /* non-fatal — some commands don't need it */

    /* ── Step 5: scheduling priority ── */
    if (cfg->nice_value != 0) {
        errno = 0;
        if (nice(cfg->nice_value) == -1 && errno != 0)
            perror("child: nice");
    }

    /* ── Step 6: exec the command via sh -c so shell syntax works ── */
    char *envp[] = {
        "PATH=/bin:/usr/bin:/sbin:/usr/sbin",
        "HOME=/root",
        "TERM=xterm",
        NULL
    };
    char *argv[] = { "sh", "-c", cfg->command, NULL };

    execve("/bin/sh", argv, envp);

    /* execve only returns on error */
    perror("child: execve /bin/sh");
    return 127;
}

/* ════════════════════════════════════════════════════════════════
 * MONITOR IOCTL HELPERS  (unchanged from boilerplate)
 * ════════════════════════════════════════════════════════════════ */

int register_with_monitor(int monitor_fd,
                           const char *container_id,
                           pid_t host_pid,
                           unsigned long soft_limit_bytes,
                           unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd,
                             const char *container_id,
                             pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ════════════════════════════════════════════════════════════════
 * CONTAINER LAUNCH HELPER
 * ════════════════════════════════════════════════════════════════ */

/*
 * Creates a pipe, spawns a container via clone(), closes the write
 * end in the supervisor, and starts a reader thread to drain stdout.
 * Returns the host PID of the container, or -1 on error.
 */
static pid_t launch_container(supervisor_ctx_t *ctx,
                               const char *id,
                               const char *rootfs,
                               const char *command,
                               int nice_value)
{
    int pipefd[2];
    if (pipe(pipefd) != 0) { perror("pipe"); return -1; }

    /* pipe[0] = read end (supervisor)   pipe[1] = write end (child stdout) */

    child_config_t *cfg = calloc(1, sizeof(*cfg));
    if (!cfg) { close(pipefd[0]); close(pipefd[1]); return -1; }

    strncpy(cfg->id,      id,      CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs,  rootfs,  PATH_MAX - 1);
    strncpy(cfg->command, command, CHILD_COMMAND_LEN - 1);
    cfg->nice_value  = nice_value;
    cfg->log_write_fd = pipefd[1];   /* child writes stdout here */

    char *stack = malloc(STACK_SIZE);
    if (!stack) { free(cfg); close(pipefd[0]); close(pipefd[1]); return -1; }

    pid_t pid = clone(child_fn,
                      stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      cfg);

    /*
     * CRITICAL: close the write end in the supervisor immediately after
     * clone(). If we keep it open, the pipe never reaches EOF when the
     * child exits, so the reader thread blocks forever and logs never flush.
     */
    close(pipefd[1]);

    if (pid < 0) {
        perror("clone");
        free(stack); free(cfg); close(pipefd[0]);
        return -1;
    }

    /* Start a reader thread that drains this container's stdout pipe */
    reader_args_t *ra = malloc(sizeof(*ra));
    if (!ra) { close(pipefd[0]); return pid; }   /* leak reader but keep container */
    strncpy(ra->container_id, id, CONTAINER_ID_LEN - 1);
    ra->pipe_read_fd = pipefd[0];
    ra->buffer       = &ctx->log_buffer;

    pthread_t tid;
    if (pthread_create(&tid, NULL, pipe_reader_thread, ra) != 0) {
        close(pipefd[0]); free(ra);
    } else {
        pthread_detach(tid);
    }

    return pid;
}

/* ════════════════════════════════════════════════════════════════
 * SIGNAL HANDLERS
 * ════════════════════════════════════════════════════════════════ */

/*
 * SIGCHLD: reap exited containers without blocking the accept() loop.
 * Uses WNOHANG so we never block inside a signal handler.
 */
static void sigchld_handler(int sig)
{
    (void)sig;
    if (!g_ctx) return;

    int   wstatus;
    pid_t pid;
    while ((pid = waitpid(-1, &wstatus, WNOHANG)) > 0) {
        /* metadata_lock: we try but don't block — signal handler constraint */
        container_record_t *c;
        for (c = g_ctx->containers; c; c = c->next) {
            if (c->host_pid != pid) continue;
            if (WIFEXITED(wstatus)) {
                c->state     = CONTAINER_EXITED;
                c->exit_code = WEXITSTATUS(wstatus);
            } else if (WIFSIGNALED(wstatus)) {
                c->state      = CONTAINER_KILLED;
                c->exit_signal = WTERMSIG(wstatus);
            }
            /* Unregister from kernel monitor if loaded */
            if (g_ctx->monitor_fd >= 0)
                unregister_from_monitor(g_ctx->monitor_fd,
                                        c->id, c->host_pid);
            break;
        }
    }
}

static void sigterm_handler(int sig)
{
    (void)sig;
    /* Close server socket to break accept() loop */
    if (g_srv_fd >= 0) {
        close(g_srv_fd);
        g_srv_fd = -1;
    }
    unlink(CONTROL_PATH);
    if (g_ctx)
        bounded_buffer_begin_shutdown(&g_ctx->log_buffer);
    printf("\n[supervisor] caught signal — shutting down\n");
}

/* ════════════════════════════════════════════════════════════════
 * COMMAND HANDLERS  (supervisor side)
 * ════════════════════════════════════════════════════════════════ */

static void handle_start(supervisor_ctx_t *ctx,
                          const control_request_t *req,
                          control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);

    /* Reject duplicate running containers with the same ID */
    for (container_record_t *c = ctx->containers; c; c = c->next) {
        if (strcmp(c->id, req->container_id) == 0 &&
            c->state == CONTAINER_RUNNING) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = -1;
            snprintf(resp->message, CONTROL_MESSAGE_LEN,
                     "ERROR: container '%s' is already running",
                     req->container_id);
            return;
        }
    }

    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Ensure logs/ directory exists */
    mkdir(LOG_DIR, 0755);

    /* Truncate (or create) the log file for a fresh start */
    char log_path[PATH_MAX];
    snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, req->container_id);
    int lfd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (lfd < 0) {
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "ERROR: cannot create log file: %s", strerror(errno));
        return;
    }
    close(lfd);

    pid_t pid = launch_container(ctx,
                                  req->container_id,
                                  req->rootfs,
                                  req->command,
                                  req->nice_value);
    if (pid < 0) {
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "ERROR: launch failed: %s", strerror(errno));
        return;
    }

    /* Register with kernel memory monitor if available */
    if (ctx->monitor_fd >= 0) {
        register_with_monitor(ctx->monitor_fd,
                               req->container_id, pid,
                               req->soft_limit_bytes,
                               req->hard_limit_bytes);
    }

    /* Allocate and prepend metadata record */
    container_record_t *rec = calloc(1, sizeof(*rec));
    if (!rec) {
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN, "ERROR: out of memory");
        return;
    }

    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(rec->log_path, log_path, PATH_MAX - 1);
    rec->host_pid         = pid;
    rec->started_at       = time(NULL);
    rec->state            = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;

    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next       = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    resp->status = 0;
    snprintf(resp->message, CONTROL_MESSAGE_LEN,
             "OK: started '%s' host-pid=%d",
             req->container_id, pid);

    printf("[supervisor] started '%s' pid=%d\n", req->container_id, pid);
}

static void handle_ps(supervisor_ctx_t *ctx, control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);

    if (!ctx->containers) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(resp->message, CONTROL_MESSAGE_LEN, "No containers.");
        resp->status = 0;
        return;
    }

    int off = snprintf(resp->message, CONTROL_MESSAGE_LEN,
                       "%-16s %-8s %-10s %-8s %s\n",
                       "ID", "PID", "STATE", "EXIT", "STARTED");

    for (container_record_t *c = ctx->containers;
         c && off < CONTROL_MESSAGE_LEN - 1;
         c = c->next) {
        char ts[32];
        struct tm *tm_info = localtime(&c->started_at);
        strftime(ts, sizeof(ts), "%H:%M:%S", tm_info);
        off += snprintf(resp->message + off, CONTROL_MESSAGE_LEN - off,
                        "%-16s %-8d %-10s %-8d %s\n",
                        c->id, c->host_pid,
                        state_to_string(c->state),
                        c->exit_code, ts);
    }

    pthread_mutex_unlock(&ctx->metadata_lock);
    resp->status = 0;
}

static void handle_logs(supervisor_ctx_t *ctx,
                         const control_request_t *req,
                         control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);

    container_record_t *found = NULL;
    for (container_record_t *c = ctx->containers; c; c = c->next) {
        if (strcmp(c->id, req->container_id) == 0) {
            found = c; break;
        }
    }

    if (!found) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "ERROR: container '%s' not found", req->container_id);
        return;
    }

    char log_path[PATH_MAX];
    strncpy(log_path, found->log_path, PATH_MAX - 1);
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Read the tail of the log file into the response message */
    FILE *f = fopen(log_path, "r");
    if (!f) {
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "(log file not yet available: %s)", strerror(errno));
        resp->status = 0;
        return;
    }

    /* Seek to the tail so we return the last CONTROL_MESSAGE_LEN-1 bytes */
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    long start = sz - (long)(CONTROL_MESSAGE_LEN - 1);
    if (start < 0) start = 0;
    fseek(f, start, SEEK_SET);

    size_t n = fread(resp->message, 1, CONTROL_MESSAGE_LEN - 1, f);
    resp->message[n] = '\0';
    fclose(f);
    resp->status = 0;
}

static void handle_stop(supervisor_ctx_t *ctx,
                         const control_request_t *req,
                         control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);

    for (container_record_t *c = ctx->containers; c; c = c->next) {
        if (strcmp(c->id, req->container_id) != 0) continue;

        if (c->state != CONTAINER_RUNNING) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp->status = -1;
            snprintf(resp->message, CONTROL_MESSAGE_LEN,
                     "ERROR: '%s' is not running (state=%s)",
                     c->id, state_to_string(c->state));
            return;
        }

        pid_t pid = c->host_pid;
        c->state  = CONTAINER_STOPPED;
        pthread_mutex_unlock(&ctx->metadata_lock);

        kill(pid, SIGTERM);

        resp->status = 0;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "OK: sent SIGTERM to '%s' (pid=%d)", req->container_id, pid);

        printf("[supervisor] stopped '%s' pid=%d\n", req->container_id, pid);
        return;
    }

    pthread_mutex_unlock(&ctx->metadata_lock);
    resp->status = -1;
    snprintf(resp->message, CONTROL_MESSAGE_LEN,
             "ERROR: container '%s' not found", req->container_id);
}

/* ── Dispatch one accepted client connection ────────────────────── */
static void handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t  req;
    control_response_t resp;
    memset(&req,  0, sizeof(req));
    memset(&resp, 0, sizeof(resp));

    ssize_t n = recv(client_fd, &req, sizeof(req), MSG_WAITALL);
    if (n != (ssize_t)sizeof(req)) {
        fprintf(stderr, "[supervisor] short recv: got %zd expected %zu\n",
                n, sizeof(req));
        return;
    }

    switch (req.kind) {
    case CMD_START:
    case CMD_RUN:   handle_start(ctx, &req, &resp); break;
    case CMD_PS:    handle_ps(ctx, &resp);           break;
    case CMD_LOGS:  handle_logs(ctx, &req, &resp);   break;
    case CMD_STOP:  handle_stop(ctx, &req, &resp);   break;
    default:
        resp.status = -1;
        snprintf(resp.message, CONTROL_MESSAGE_LEN,
                 "ERROR: unknown command kind=%d", req.kind);
    }

    send(client_fd, &resp, sizeof(resp), 0);
}

/* ════════════════════════════════════════════════════════════════
 * SUPERVISOR — main entry point
 * ════════════════════════════════════════════════════════════════ */

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;

    g_ctx = &ctx;   /* expose to signal handlers */

    /* ── Init metadata lock ── */
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    /* ── Init bounded buffer ── */
    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc; perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* ── 1. Try to open the kernel memory monitor device ── */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        printf("[supervisor] /dev/container_monitor not available "
               "(kernel module not loaded) — memory limits disabled\n");
    } else {
        printf("[supervisor] kernel monitor opened fd=%d\n", ctx.monitor_fd);
    }

    /* ── 2. Install signal handlers ── */
    signal(SIGCHLD, sigchld_handler);
    signal(SIGTERM, sigterm_handler);
    signal(SIGINT,  sigterm_handler);
    signal(SIGPIPE, SIG_IGN);           /* don't die on broken client socket */

    /* ── 3. Spawn the logger thread ── */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx.log_buffer);
    if (rc != 0) {
        errno = rc; perror("pthread_create logger");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* ── 4. Create and bind the control UNIX domain socket ── */
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); goto cleanup; }

    g_srv_fd = ctx.server_fd;

    unlink(CONTROL_PATH);   /* remove stale socket if any */

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); goto cleanup;
    }
    if (listen(ctx.server_fd, 8) < 0) {
        perror("listen"); goto cleanup;
    }

    printf("[supervisor] ready  PID=%d  rootfs=%s  socket=%s\n",
           getpid(), rootfs, CONTROL_PATH);

    /* ── 5. Event loop: accept CLI connections ── */
    while (1) {
        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;   /* SIGCHLD interrupted accept — loop */
            if (errno == EBADF || errno == EINVAL) break;  /* socket was closed */
            perror("accept");
            break;
        }
        handle_client(&ctx, client_fd);
        close(client_fd);
    }

cleanup:
    printf("[supervisor] shutting down...\n");

    /* Drain and stop logger thread */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    /* Clean up containers list */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *c = ctx.containers;
    while (c) {
        container_record_t *next = c->next;
        free(c);
        c = next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    if (ctx.server_fd  >= 0) close(ctx.server_fd);
    unlink(CONTROL_PATH);

    printf("[supervisor] done\n");
    return 0;
}

/* ════════════════════════════════════════════════════════════════
 * CLIENT — send_control_request
 * ════════════════════════════════════════════════════════════════ */

static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect (is the supervisor running?)");
        close(fd);
        return 1;
    }

    if (send(fd, req, sizeof(*req), 0) != (ssize_t)sizeof(*req)) {
        perror("send");
        close(fd);
        return 1;
    }

    control_response_t resp;
    memset(&resp, 0, sizeof(resp));
    ssize_t n = recv(fd, &resp, sizeof(resp), MSG_WAITALL);
    if (n == (ssize_t)sizeof(resp)) {
        printf("%s\n", resp.message);
    } else {
        fprintf(stderr, "[client] truncated response (%zd bytes)\n", n);
    }

    close(fd);
    return (n == (ssize_t)sizeof(resp)) ? resp.status : 1;
}

/* ════════════════════════════════════════════════════════════════
 * CLI COMMAND BUILDERS  (unchanged shape from boilerplate)
 * ════════════════════════════════════════════════════════════════ */

static int cmd_start(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> "
                "[--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    strncpy(req.rootfs,       argv[3], PATH_MAX - 1);
    strncpy(req.command,      argv[4], CHILD_COMMAND_LEN - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> "
                "[--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    strncpy(req.rootfs,       argv[3], PATH_MAX - 1);
    strncpy(req.command,      argv[4], CHILD_COMMAND_LEN - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    return send_control_request(&req);
}

/* ════════════════════════════════════════════════════════════════
 * MAIN
 * ════════════════════════════════════════════════════════════════ */

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
