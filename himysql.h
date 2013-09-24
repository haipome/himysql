# pragma once

# include <time.h>
# include <pthread.h>
# include <mysql.h>
# include <errmsg.h>
# include <mysqld_error.h>

struct himysql
{
    MYSQL       *conn;
    size_t      buf_size;
    char        *sql;
    size_t      len;

    char        *host;
    int         port;
    char        *db;
    char        *user;
    char        *passwd;
    char        *charset;

    int         use_thread;
    pthread_t   thread;
    int         running;
    int         pipefd[2];
    void        *list;
    size_t      list_max;

    MYSQL_RES   *result;
    size_t      num_fields;
    size_t      num_rows;
    MYSQL_FIELD *fields;
    MYSQL_ROW   row;
    void        *fail_cb;

    pthread_mutex_t lock;
};

typedef struct himysql himysql_t;

# define HM_USE_THREAD 0x1

himysql_t *himysql_init(const char *host, int port, const char *db,
        const char *user, const char *passwd, const char *charset, int flag);

# define HM_OK 0
# define HM_ERROR (-1)
# define HM_LOST  (-2)

int himysql(himysql_t *hm, const char *format, ...);

int himysql_query(himysql_t *hm, const char *sql, size_t len);

char *himysql_error(himysql_t *hm);

unsigned int himysql_errno(himysql_t *hm);

typedef void himysql_fail_cb(himysql_t *hm);
int himysql_set_fail_cb(himysql_t *hm, himysql_fail_cb *cb);

int himysql_set_list_max(himysql_t *hm, size_t max);

int himysql_fetch_fields(himysql_t *hm);

int himysql_fetch_row(himysql_t *hm);

void himysql_free_result(himysql_t *hm);

void himysql_fini(himysql_t *hm);

