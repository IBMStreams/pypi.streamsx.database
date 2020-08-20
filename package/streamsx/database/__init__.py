# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

"""
Overview
++++++++

Provides classes and functions to run SQL statements to a database.


Credentials
+++++++++++

Db2 Warehouse credentials are defined using service credentials JSON.

The mandatory JSON elements are "username", "password" and "jdbcurl"::

    {
        "username": "<JDBC_USER>",
        "password": "<JDBC_PASSWORD>",
        "jdbcurl":  "<JDBC_URL>"
    }


"""

__version__='1.5.6'

__all__ = ['JDBCStatement', 'download_toolkit', 'configure_connection', 'run_statement']
from streamsx.database._database import JDBCStatement, download_toolkit, configure_connection, run_statement
