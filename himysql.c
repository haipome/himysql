# include <stdio.h>
# include <string.h>
# include <stdlib.h>
# include <stdint.h>
# include <stdarg.h>
# include <error.h>
# include <errno.h>
# include <unistd.h>
# include <sys/time.h>

# include "himysql.h"

# define KB 1024
# define MB 1024 * KB

# define HM_BUF_SIZE_INIT (64 * KB)
# define HM_LIST_MAX_DEFAULT (100 * MB)

static int hm_exec(himysql_t *hm, const char *sql, size_t len);
static void hm_fini(himysql_t *hm);

static int hm_expand_buf(himysql_t *hm, size_t size)
{
    size_t new_size = hm->buf_size * 2;
    while ((new_size - hm->buf_size) < size)
        new_size *= 2;

    void *p = realloc(hm->sql, new_size);
    if (p == NULL)
        return HM_ERROR;

    hm->sql = p;
    hm->buf_size = new_size;

    return HM_OK;
}

static int hm_cat_vprintf(himysql_t *hm, const char *format, va_list ap)
{
    while (1)
    {
        hm->sql[hm->buf_size - 2] = '\0';
        int n = vsnprintf(hm->sql + hm->len, hm->buf_size - hm->len, format, ap);
        if (hm->sql[hm->buf_size - 2] != '\0')
        {
            if (hm_expand_buf(hm, 1) != HM_OK)
                return HM_ERROR;
            continue;
        }
        hm->len += n;
        break;
    }

    return HM_OK;
}

static int hm_cat_hexstr(himysql_t *hm, const char *p, size_t size)
{
    if ((hm->buf_size - hm->len - 2) < size * 2)
    {
        if (hm_expand_buf(hm, size * 2) != HM_OK)
            return HM_ERROR;
    }

    static char hex[16] = {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', };

    size_t i;
    for (i = 0; i < size; ++i)
    {
        hm->sql[hm->len++] = hex[((uint8_t)(p[i]) & 0xf0) >> 4];
        hm->sql[hm->len++] = hex[(uint8_t)(p[i]) & 0x0f];
    }

    hm->sql[hm->len] = '\0';

    return HM_OK;
}

static int hm_cat_escape(himysql_t *hm, char *s)
{
    size_t slen = strlen(s);
    if ((hm->buf_size - hm->len - 2) < slen * 2)
    {
        if (hm_expand_buf(hm, slen * 2) != HM_OK)
            return HM_ERROR;
    }

    hm->len += mysql_real_escape_string(hm->conn, hm->sql + hm->len, s, slen);

    return HM_OK;
}

static int hm_cat_str(himysql_t *hm, char *s)
{
    size_t slen = strlen(s);
    if ((hm->buf_size - hm->len - 2) < slen)
    {
        if (hm_expand_buf(hm, slen) != HM_OK)
            return HM_ERROR;
    }

    memcpy(hm->sql + hm->len, s, slen);
    hm->len += slen;
    hm->sql[hm->len] = '\0';

    return HM_OK;
}

static int hm_cat_char(himysql_t *hm, char c)
{
    if ((hm->buf_size - hm->len - 2) < 1)
    {
        if (hm_expand_buf(hm, 1) != HM_OK)
            return HM_ERROR;
    }

    hm->sql[hm->len++] = c;
    hm->sql[hm->len] = '\0';

    return HM_OK;
}

# define IS_DIGIT(c) ((c) >= '0' && (c) <= '9')

static int hm_format_sql(himysql_t *hm, const char *format, va_list ap)
{
    hm->len = 0;

    const char *f = format;
    while (*f)
    {
        if (*f != '%' || f[1] == '\0')
        {
            if (hm_cat_char(hm, *f) != HM_OK)
                return HM_ERROR;

            ++f;
            continue;
        }

        char *arg;
        size_t size;

        switch (f[1])
        {
            case '%':
                if (hm_cat_char(hm, '%') != HM_OK)
                    return HM_ERROR;
                break;
            case 's':
                arg = va_arg(ap, char *);
                if (hm_cat_char(hm, '\'') != HM_OK)
                    return HM_ERROR;
                if (hm_cat_escape(hm, arg) != HM_OK)
                    return HM_ERROR;
                if (hm_cat_char(hm, '\'') != HM_OK)
                    return HM_ERROR;
                break;
            case 'b':
                arg = va_arg(ap, char *);
                size = va_arg(ap, size_t);
                if (hm_cat_str(hm, "unhex('") != HM_OK)
                    return HM_ERROR;
                if (hm_cat_hexstr(hm, arg, size) != HM_OK)
                    return HM_ERROR;
                if (hm_cat_str(hm, "')") != HM_OK)
                    return HM_ERROR;
                break;
            default:
                /* Try to detect printf format */
                {
                    static const char int_fmts[] = "diouxX";
                    char _format[16];
                    const char *_f = f + 1;
                    size_t _l = 0;
                    va_list _ap;

                    /* Flags */
                    if (*_f != '\0' && *_f == '#') ++_f;
                    if (*_f != '\0' && *_f == '0') ++_f;
                    if (*_f != '\0' && *_f == '-') ++_f;
                    if (*_f != '\0' && *_f == ' ') ++_f;
                    if (*_f != '\0' && *_f == '+') ++_f;

                    /* Field width */
                    while (*_f != '\0' && IS_DIGIT(*_f)) ++_f;

                    /* Precision */
                    if (*f == '.')
                    {
                        ++_f;
                        while (*_f != '\0' && IS_DIGIT(*_f)) ++_f;
                    }

                    /* Copy va_list before consuming with va_arg */
                    va_copy(_ap, ap);

                    /* Integer conversion */
                    if (strchr(int_fmts, *_f) != NULL)
                    {
                        va_arg(ap, int);
                        goto fmt_valid_lable;
                    }

                    /* Double  conversion */
                    if (strchr("eEfFgGaA", *_f) != NULL)
                    {
                        va_arg(ap, double);
                        goto fmt_valid_lable;
                    }

                    /* Size: char */
                    if (_f[0] == 'h' && _f[1] == 'h')
                    {
                        _f += 2;
                        if (*_f != '\0' && strchr(int_fmts, *_f) != NULL)
                        {
                            va_arg(ap, int); /* char gets promoted to int */
                            goto fmt_valid_lable;
                        }
                        goto fmt_invalid_lable;
                    }

                    /* Size: short */
                    if (_f[0] == 'h')
                    {
                        _f += 1;
                        if (*_f != '\0' && strchr(int_fmts, *_f) != NULL)
                        {
                            va_arg(ap, int); /* short gets promoted to int */
                            goto fmt_valid_lable;
                        }
                        goto fmt_invalid_lable;
                    }

                    /* Size: long long */
                    if (_f[0] == 'l' && _f[1] == 'l')
                    {
                        _f += 2;
                        if (*f != '\0' && strchr(int_fmts, *_f) != NULL)
                        {
                            va_arg(ap, long long);
                            goto fmt_valid_lable;
                        }
                        goto fmt_invalid_lable;
                    }

                    /* Size: long */
                    if (_f[0] == 'l')
                    {
                        _f += 1;
                        if (*_f != '\0' && strchr(int_fmts, *_f) != NULL)
                        {
                            va_arg(ap, long);
                            goto fmt_valid_lable;
                        }
                        goto fmt_invalid_lable;
                    }

                    /* Size: size_t */
                    if (_f[0] == 'z')
                    {
                        _f += 1;
                        if (*_f != '\0' && strchr(int_fmts, *_f) != NULL)
                        {
                            va_arg(ap, size_t);
                            goto fmt_valid_lable;
                        }
                        goto fmt_invalid_lable;
                    }
fmt_invalid_lable:
                    if (hm_cat_char(hm, '%') != HM_OK)
                        return HM_ERROR;

                    if (*_f)
                        f = _f - 1;
                    else
                        f = _f - 2;

                    va_end(_ap);
                    break;
fmt_valid_lable:
                    _l = _f - f + 1;
                    if (_l <= sizeof(_format - 1))
                    {
                        memcpy(_format, f, _l);
                        _format[_l] = '\0';
                        if (hm_cat_vprintf(hm, _format, _ap) != HM_OK)
                            return HM_ERROR;

                        /* Update current position (note: outer blocks
                         * increment c 2 so compensate here */
                        f = _f - 1;

                        va_end(_ap);
                        break;
                    }
                    else
                    {
                        goto fmt_invalid_lable;
                    }
                }
        }

        f += 2;
    }

    return HM_OK;
}

typedef struct list_node
{
    struct list_node *next;
    size_t size;
    char value[];
} list_node;

typedef struct list
{
    pthread_mutex_t lock;
    list_node *head;
    list_node *tail;
    size_t len;
    size_t size;
} list;

static int list_add_tail(list *list, const void *value, size_t size, size_t max)
{
    if (list->len >= 1 && (list->size + size) > max)
        return HM_ERROR;

    list_node *node = malloc(sizeof(list_node) + size);
    if (node == NULL)
        return HM_ERROR;

    node->next = NULL;
    node->size = size;
    memcpy(node->value, value, size);

    if (list->len == 0)
    {
        list->head = list->tail = node;
    }
    else
    {
        list->tail->next = node;
        list->tail = node;
    }
    list->len += 1;
    list->size += size;

    return HM_OK;
}

static int hm_list_add_tail(himysql_t *hm, const void *value, size_t size)
{
    list *list = hm->list;
    pthread_mutex_lock(&list->lock);
    int ret = list_add_tail(list, value, size, hm->list_max);
    pthread_mutex_unlock(&list->lock);

    return ret;
}

static void list_del_head(list *list)
{
    list_node *node = list->head;
    list->head = node->next;

    list->len -= 1;
    list->size -= node->size;
    free(node);
}

static void hm_list_del_head(himysql_t *hm)
{
    list *list = hm->list;
    pthread_mutex_lock(&list->lock);
    list_del_head(list);
    pthread_mutex_unlock(&list->lock);
}

static void hm_exec_list(himysql_t *hm, int sleep)
{
    himysql_fail_cb *fail_cb = hm->fail_cb;

    list *list = hm->list;
    list_node *head;
    while (list->len)
    {
        head = list->head;
        int ret = hm_exec(hm, head->value, head->size);
        if (ret == HM_LOST)
        {
            if (sleep)
            {
                usleep(100 * 1000);
            }
            else
            {
                if (fail_cb)
                    fail_cb(hm);

                hm_list_del_head(hm);
            }
        }
        else
        {
            if (ret == HM_OK)
            {
                MYSQL_RES *result = mysql_store_result(hm->conn);
                if (result)
                    mysql_free_result(result);
            }

            hm_list_del_head(hm);
        }
    }
}

static void hm_thread_loop(himysql_t *hm)
{
    fd_set r_set;
    FD_ZERO(&r_set);
    FD_SET(hm->pipefd[0], &r_set);

    struct timeval timeout = { 0, 100 * 1000 };

    int ret = select(hm->pipefd[0] + 1, &r_set, NULL, NULL, &timeout);
    if (ret > 0 && FD_ISSET(hm->pipefd[0], &r_set))
    {
        char c;
        if (read(hm->pipefd[0], &c, 1) != 1) ;
    }

    hm_exec_list(hm, 1);
}

static void *hm_thread_routine(void *arg)
{
    himysql_t *hm = (himysql_t *)arg;

    while (hm->running)
    {
        hm_thread_loop(hm);
    }

    hm_exec_list(hm, 0);

    return NULL;
}

static void hm_free(himysql_t *hm)
{
    free(hm->host);
    free(hm->db);
    free(hm->user);
    free(hm->passwd);
    free(hm->charset);
    free(hm->sql);
    free(hm);
}

static int hm_connect(himysql_t *hm)
{
    if ((hm->conn = mysql_init(NULL)) == NULL)
        return HM_ERROR;

    /* set mysql auto reconnect */
    my_bool reconnect = 1;
    if (mysql_options(hm->conn, MYSQL_OPT_RECONNECT, &reconnect) != 0)
    {
        mysql_close(hm->conn);
        return HM_ERROR;
    }

    /* set mysql charset */
    if (mysql_options(hm->conn, MYSQL_SET_CHARSET_NAME, hm->charset) != 0)
    {
        mysql_close(hm->conn);
        return HM_ERROR;
    }

    if (mysql_real_connect(hm->conn, hm->host, hm->user, hm->passwd, hm->db, \
                hm->port, NULL, 0) == NULL)
    {
        mysql_close(hm->conn);
        return HM_ERROR;
    }

    return HM_OK;
}

himysql_t *himysql_init(const char *host, int port, const char *db,
        const char *user, const char *passwd, const char *charset, int flag)
{
    himysql_t *hm;
    if ((hm = calloc(1, sizeof(himysql_t))) == NULL)
        return NULL;

    hm->port = port ? port : 3306;
    hm->host = strdup(host);
    hm->user = strdup(user ? user : "root");
    hm->charset = strdup(charset ? charset : "utf8");
    if (hm->host == NULL || hm->user == NULL || hm->charset == NULL)
    {
        hm_free(hm);
        return NULL;
    }
    if (db && (hm->db = strdup(db)) == NULL)
    {
        hm_free(hm);
        return NULL;
    }
    if (passwd && (hm->passwd = strdup(passwd)) == NULL)
    {
        hm_free(hm);
        return NULL;
    }
    hm->buf_size = HM_BUF_SIZE_INIT;
    if ((hm->sql = malloc(hm->buf_size)) == NULL)
    {
        hm_free(hm);
        return NULL;
    }

    if (hm_connect(hm) != HM_OK)
    {
        hm_free(hm);
        return NULL;
    }

    if (flag & HM_USE_THREAD)
        hm->use_thread = 1;

    if (hm->use_thread)
    {
        pthread_mutex_init(&hm->lock, NULL);

        if ((hm->list = calloc(1, sizeof(list))) == NULL)
        {
            mysql_close(hm->conn);
            hm_free(hm);
            return NULL;
        }
        pthread_mutex_init(&((list *)hm->list)->lock, NULL);
        hm->list_max = HM_LIST_MAX_DEFAULT;

        if (pipe(hm->pipefd) != 0)
        {
            mysql_close(hm->conn);
            hm_free(hm);
            return NULL;
        }

        hm->running = 1;
        if (pthread_create(&hm->thread, NULL, hm_thread_routine, hm) < 0)
        {
            hm_fini(hm);
            return NULL;
        }
    }

    return hm;
}

int himysql_set_fail_cb(himysql_t *hm, himysql_fail_cb *cb)
{
    hm->fail_cb = (void *)cb;

    return HM_OK;
}

int himysql_set_list_max(himysql_t *hm, size_t max)
{
    hm->list_max = max;

    return HM_OK;
}

static int hm_exec(himysql_t *hm, const char *sql, size_t len)
{
    int ret = mysql_real_query(hm->conn, sql, len);
    if (ret != 0)
    {
        himysql_fail_cb *fail_cb = hm->fail_cb;

        unsigned errcode = mysql_errno(hm->conn);
        if (
                errcode == CR_SERVER_LOST       ||  /* 2013 */
                errcode == CR_SERVER_GONE_ERROR ||  /* 2006 */
                errcode == CR_CONNECTION_ERROR  ||  /* 2002 */
                errcode == CR_CONN_HOST_ERROR   ||  /* 2003 */
                errcode == ER_SERVER_SHUTDOWN   ||  /* 1053 */
                errcode == ER_QUERY_INTERRUPTED)    /* 1317 */
        {
            if (!hm->use_thread && fail_cb)
                fail_cb(hm);

            return HM_LOST;
        }
        else
        {
            if (fail_cb)
                fail_cb(hm);

            return HM_ERROR;
        }
    }

    return HM_OK;
}

static int hm_sql(himysql_t *hm, const char *sql, size_t len)
{
    if (hm->use_thread)
    {
        int ret = hm_list_add_tail(hm, sql, len);
        if (ret != HM_OK)
            return ret;

        write(hm->pipefd[1], "", 1);
    }
    else
    {
        int ret = hm_exec(hm, sql, len);
        if (ret != HM_OK)
            return ret;

        himysql_free_result(hm);

        hm->result = mysql_store_result(hm->conn);
        if (hm->result)
        {
            hm->num_fields = mysql_num_fields(hm->result);
            hm->num_rows = mysql_num_rows(hm->result);
            hm->num_affected = mysql_affected_rows(hm->conn);
        }
    }

    return HM_OK;
}

static int _himysql(himysql_t *hm, const char *format, va_list ap)
{
    int ret = hm_format_sql(hm, format, ap);
    if (ret != HM_OK)
        return HM_ERROR;

    return hm_sql(hm, hm->sql, hm->len);
}

int himysql(himysql_t *hm, const char *format, ...)
{
    if (hm->use_thread)
        pthread_mutex_lock(&hm->lock);

    va_list ap;
    va_start(ap, format);
    int ret = _himysql(hm, format, ap);
    va_end(ap);

    if (hm->use_thread)
        pthread_mutex_unlock(&hm->lock);

    return ret;
}

int himysql_query(himysql_t *hm, const char *sql, size_t len)
{
    return hm_sql(hm, sql, len ? len : strlen(sql));
}

char *himysql_error(himysql_t *hm)
{
    if (hm && hm->conn)
        return (char *)mysql_error(hm->conn);

    return "";
}

unsigned int himysql_errno(himysql_t *hm)
{
    if (hm && hm->conn)
        return mysql_errno(hm->conn);

    return 0;
}

int himysql_fetch_fields(himysql_t *hm)
{
    if (hm->result == NULL)
        return HM_ERROR;

    hm->fields = mysql_fetch_fields(hm->result);
    if (hm->fields == NULL)
        return HM_ERROR;

    return HM_OK;
}

int himysql_fetch_row(himysql_t *hm)
{
    if (hm->result == NULL)
        return HM_ERROR;

    hm->row = mysql_fetch_row(hm->result);
    if (hm->row == NULL)
        return HM_ERROR;

    return HM_OK;
}

void himysql_free_result(himysql_t *hm)
{
    if (hm->result)
        mysql_free_result(hm->result);

    hm->result = NULL;
    hm->num_fields = 0;
    hm->num_rows = 0;
    hm->num_affected = 0;
}

static void hm_fini(himysql_t *hm)
{
    himysql_free_result(hm);
    mysql_close(hm->conn);
    if (hm->use_thread)
    {
        close(hm->pipefd[0]);
        close(hm->pipefd[1]);
    }
    hm_free(hm);
}

void himysql_fini(himysql_t *hm)
{
    if (hm == NULL)
        return;

    if (hm->use_thread)
    {
        hm->running = 0;
        write(hm->pipefd[1], "", 1);
        pthread_join(hm->thread, NULL);
    }

    hm_fini(hm);
}

