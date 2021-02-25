# UniCon: United Connections (for databases)
import json
import pymysql
from .UniLog import UniLog, NoopLog


class UniDefaultInvoker(object):
    def __init__(self):
        pass

    def get_names(self, instance_name, db_name):
        return instance_name, db_name


class UniTransaction(object):
    def __init__(self, uni_con, auto_commit):
        self.uni_con = uni_con
        self.auto_commit = auto_commit

    def commit(self, no_delay=False):
        if self.auto_commit and not no_delay:
            self.uni_con.logger.info("UniTransaction: Auto commit enabled and no delay set. Skip committing here.")
        else:
            self.uni_con.commit()

    def __enter__(self):
        self.uni_con.logger.info("UniTransaction: Begin transaction.")

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_tb is not None:
            self.uni_con.logger.info("UniTransaction: Exception detected, start rollback.")
            self.uni_con.rollback()
        elif self.auto_commit:
            self.uni_con.logger.info("UniTransaction: Leave transaction with auto commit enabled, start commiting...")
            self.uni_con.commit()
        else:
            self.uni_con.logger.info("UniTransaction: Leave transaction, start rollback.")
            self.uni_con.rollback()


class UniCon(object):
    @staticmethod
    def from_mysql(mysql_conn, logger=None, cursor_type="dict", managed=False):
        conn = UniCon("", _blankInit=True)
        conn._managed = managed
        conn.instance_name = "<MySQL Connection>"
        conn._host = "<existing_connection>"
        conn.logger = logger if logger is not None else UniLog()
        conn.conn = mysql_conn
        if cursor_type == "dict":
            conn.cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
        else:
            conn.cursor = mysql_conn.cursor()
        conn.conn_type = "mysql"
        return conn

    @staticmethod
    def from_config(config_params, logger=None, cursor_type="dict"):
        conn = UniCon("", _blankInit=True)
        conn._managed = True
        conn.instance_name = "<MySQL Connection>"
        conn._host = config_params["host"] if "host" in config_params else "<from_config>"
        conn.logger = logger if logger is not None else UniLog()

        conn.logger.info("Connecting to {}...".format(conn._host))
        conn.conn = pymysql.connect(**config_params)
        if cursor_type == "dict":
            conn.cursor = conn.conn.cursor(pymysql.cursors.DictCursor)
        else:
            conn.cursor = conn.conn.cursor()
        conn.conn_type = "mysql"
        return conn

    @staticmethod
    def connect_mysql(host, port, username, password, database, charset="utf8", logger=UniLog(), cursor_type="dict"):
        conn = UniCon("", _blankInit=True)
        conn._managed = True
        conn.instance_name = "<MySQL Connection>"
        conn._host = host
        conn.logger = logger or NoopLog()

        conn.logger.info("Connecting to {}...".format(conn._host))
        conn.conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database, charset=charset)
        if cursor_type == "dict":
            conn.cursor = conn.conn.cursor(pymysql.cursors.DictCursor)
        else:
            conn.cursor = conn.conn.cursor()
        conn.conn_type = "mysql"
        return conn

    def _do_create_db_conn(self, instance_name, db_name=None, cursor_type="dict", config_paths=None):
        filepaths = config_paths or [
            "unicon_pwd.json",
            "config/unicon_pwd.json",
            ".config/unicon_pwd.json"
        ]

        for filepath in filepaths:
            try:
                with open(filepath, "r") as f:
                    content_raw = f.read()
                instance_config = json.loads(content_raw)
            except Exception:
                pass
            else:
                self.logger.info("Found UniCon configure: {}".format(filepath))
                break
        else:
            raise Exception("Cannot find UniCon configure file in: {}.".format(",".join(filepaths)))

        if instance_name not in instance_config:
            raise Exception("Unknown instance name: {}".format(instance_name))

        self._host = instance_config[instance_name]['host']
        self.logger.info("Opening connection to instance: {} ({})...".format(instance_name, self._host))

        conn = pymysql.connect(db=db_name, **instance_config[instance_name])
        if cursor_type == "dict":
            cursor = conn.cursor(pymysql.cursors.DictCursor)
        else:
            cursor = conn.cursor()  # By default, pymysql use class Cursor. (returns rows as tuples and stores the result set in the client)

        return conn, cursor

    def __init__(self, instance_name, db_name=None, file_path=None,  # Connection params
                 invoker=None, # UniCon settings
                 logger=None, cursor_type="dict",  # Basic settings
                 _blankInit=False, config_paths=None):
        if _blankInit:
            self._managed = False
            return
        else:
            self._managed = True

        self.logger = logger or NoopLog()

        if invoker:
            self.logger.info("UniCon: Invoker present.")
            self.instance_name, db_name = invoker.get_names(instance_name, db_name)  # db_name will not be stored in self.
            self.logger.info("UniCon: Invoker set names to: ({}, {})".format(self.instance_name, db_name))
        else:
            self.instance_name = instance_name

        self.conn, self.cursor = self._do_create_db_conn(self.instance_name, db_name=db_name, cursor_type=cursor_type, config_paths=config_paths)
        self.conn_type = "mysql"

    def __del__(self):
        if self._managed:
            self.conn.close()
            self.logger.info("Connection to instance closed: {} ({})".format(self.instance_name, self._host))

    def new_transaction(self, auto_commit=False):
        return UniTransaction(self, auto_commit)

    def _check_params(self, params, output=True):
        if params is not None and type(params) is not list:
            raise Exception("Invalid params type: {}".format(type(params)))
        if output and params:
            self.logger.info(params)

    def execute(self, sql, params=None):
        self.logger.info(sql)
        self._check_params(params)
        return self.cursor.execute(sql, params)

    def executemany(self, sql, params):
        self.logger.info(sql)
        self._check_params(params, output=False)
        return self.cursor.executemany(sql, params)

    def query(self, sql, params=None):
        self.logger.info(sql)
        self._check_params(params)
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    def commit(self):
        self.logger.debug("UniCon: Commiting...")
        self.conn.commit()

    def rollback(self):
        self.logger.debug("UniCon: Rollback.")
        self.conn.rollback()

    @property
    def lastrowid(self):
        return self.cursor.lastrowid

    @property
    def rowcount(self):
        return self.cursor.rowcount

    # Helper Functions
    def query_one(self, sql, params=None):
        result = self.query(sql, params)
        if result:
            return result[0]
        else:
            return None

    def insert_into(self, table_name, sql_fields, unique_arr=None):
        table_struct = sorted(sql_fields.keys())
        if unique_arr:
            left_fields = [k for k in table_struct if k not in unique_arr]
            if not left_fields:
                left_fields = table_struct
            return self.execute("insert into {}({}) values ({}) on duplicate key update {}".format(
                table_name,
                ','.join(table_struct),
                ','.join(["%s"] * len(table_struct)),
                ','.join(["{}=values({})".format(k, k) for k in left_fields])
            ), [sql_fields[k] for k in table_struct])
        else:
            return self.execute("insert into {}({}) values ({})".format(
                table_name,
                ','.join(table_struct),
                ','.join(["%s"] * len(table_struct))
            ), [sql_fields[k] for k in table_struct])

    def insert_many(self, table_name, sql_fields_arr, unique_arr=None):
        table_struct = sorted(sql_fields_arr[0].keys())
        if unique_arr:
            left_fields = [k for k in table_struct if k not in unique_arr]
            if not left_fields:
                left_fields = table_struct
            return self.executemany("insert into {}({}) values ({}) on duplicate key update {}".format(
                table_name,
                ','.join(table_struct),
                ','.join(["%s"] * len(table_struct)),
                ','.join(["{}=values({})".format(k, k) for k in left_fields])
            ), [tuple([sql_fields[k] for k in table_struct]) for sql_fields in sql_fields_arr])
        else:
            return self.executemany("insert into {}({}) values ({})".format(
                table_name,
                ','.join(table_struct),
                ','.join(["%s"] * len(table_struct))
            ), [tuple([sql_fields[k] for k in table_struct]) for sql_fields in sql_fields_arr])

    def get_table_struct(self, table_name):
        return self.query("desc {}".format(table_name))
