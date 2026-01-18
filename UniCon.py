# UniCon: United Connections (for databases)
from __future__ import annotations
import json
import logging
from typing import Any
import pymysql
from UniTools.UniLog import get_or_create_logger


class UniTransaction:
    def __init__(self, conn: UniCon, auto_commit: bool):
        self.conn = conn
        self.auto_commit = auto_commit

    def commit(self):
        if self.auto_commit:
            self.conn.commit()

    def __enter__(self):
        if self.conn.logger is not None:
            self.conn.logger.info("BEGIN TRANSACTION")

    def __exit__(self, exc_type, exc_value, exc_tb): # type: ignore
        if exc_tb is not None:
            self.conn.rollback()
        elif self.auto_commit:
            self.conn.commit()
        else:
            self.conn.rollback()


def from_mysql(mysql_conn: pymysql.Connection, logger: logging.Logger | None =None, cursor_type: str ="dict", disable_logger: bool = False, managed: bool=False):
    if cursor_type == "dict":
        mysql_cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    else:
        mysql_cursor = mysql_conn.cursor()

    return UniCon(mysql_conn, mysql_cursor, None if disable_logger else logger if logger is not None else get_or_create_logger(), managed)


def connect_mysql(host: str, port: int, username: str, password: str, database: str,
                  charset: str ="utf8", logger: logging.Logger | None = None, cursor_type: str ="dict", disable_logger: bool=False):
    mysql_conn = pymysql.connect(host=host, port=port, user=username, password=password, database=database, charset=charset)
    if cursor_type == "dict":
        mysql_cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
    else:
        mysql_cursor = mysql_conn.cursor()
    return UniCon(mysql_conn, mysql_cursor, None if disable_logger else logger if logger is not None else get_or_create_logger(), True)


class UniCon(object):
    def __init__(self, mysql_conn: pymysql.Connection, mysql_cursor: pymysql.cursors.Cursor , logger: logging.Logger | None, managed: bool):
        self.conn = mysql_conn
        self.cursor = mysql_cursor
        self.logger = logger
        self.managed = managed

    def __del__(self):
        if self.managed:
            host = self.conn.host
            port = self.conn.port
            self.conn.close()
            if self.logger is not None:
                self.logger.debug("Connection to instance closed ({}:{})".format(host, port))

    def new_transaction(self, auto_commit: bool =False):
        return UniTransaction(self, auto_commit)

    def _check_params(self, params: Any):
        if params is not None and type(params) is not list and type(params) is not tuple:
            raise Exception("Invalid params type: {}".format(type(params).__name__))

    def execute(self, sql: str, params: tuple[Any] | list[Any] | None = None):
        self._check_params(params)
        if self.logger is not None:
            self.logger.debug("{}{}".format(sql, " [{}]".format(json.dumps(params)) if params is not None else ""))

        return self.cursor.execute(sql, params)

    def executemany(self, sql: str, params: list[list[Any]] | list[tuple[Any]] | tuple[tuple[Any]] | list[dict[Any, Any]] | None = None):
        self._check_params(params)
        if self.logger is not None:
            self.logger.debug("{}{}".format(sql, " [{}]".format(json.dumps(params)) if params is not None else ""))

        return self.cursor.executemany(sql, params or ())

    def query(self, sql: str, params: tuple[Any] | list[Any] | None = None):
        self._check_params(params)
        if self.logger is not None:
            self.logger.debug("{}{}".format(sql, " [{}]".format(json.dumps(params)) if params is not None else ""))

        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    def commit(self):
        if self.logger is not None:
            self.logger.debug("COMMIT")
        self.conn.commit()

    def rollback(self):
        if self.logger is not None:
            self.logger.debug("ROLLBACK")
        self.conn.rollback()

    @property
    def lastrowid(self):
        return self.cursor.lastrowid

    @property
    def rowcount(self):
        return self.cursor.rowcount

    # Helper Functions
    def query_one(self, sql: str, params: tuple[Any] | list[Any] | None = None):
        result = self.query(sql, params)
        if result:
            return result[0]
        else:
            return None

    def insert_into(self, table_name: str, sql_fields: dict[str, Any], unique_arr: list[str] | None = None):
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

    def insert_many(self, table_name: str, sql_fields_arr: list[dict[str, Any]], unique_arr: list[str] | None = None):
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

    def get_table_struct(self, table_name: str):
        return self.query("desc {}".format(table_name))
