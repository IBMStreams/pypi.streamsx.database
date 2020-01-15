# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

import datetime
import requests
import os
import json
from tempfile import gettempdir
import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.spl.types import rstring
from streamsx.toolkits import download_toolkit
import streamsx.topology.composite


_TOOLKIT_NAME = 'com.ibm.streamsx.jdbc'

def _add_driver_file_from_url(topology, url, filename):
    r = requests.get(url)
    tmpdirname = gettempdir()
    tmpfile = tmpdirname + '/' + filename
    with open(tmpfile, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)
    topology.add_file_dependency(tmpfile, 'opt')
    return 'opt/'+filename

def _add_driver_file(topology, path):
    filename = os.path.basename(path)
    topology.add_file_dependency(path, 'opt')
    return 'opt/'+filename

def _read_db2_credentials(credentials):
    jdbcurl = ""
    username = ""
    password = ""
    if isinstance(credentials, dict):
        username = credentials.get('username')
        password = credentials.get('password')
        if 'jdbcurl' in credentials:
            jdbcurl = credentials.get('jdbcurl')
        else:
            if 'class' in credentials:
                if credentials.get('class') == 'external': # CP4D external connection
                    if 'url' in credentials:
                        jdbcurl = credentials.get('url')
                    else:
                        raise TypeError(credentials)
    else:
        raise TypeError(credentials)
    return jdbcurl, username, password

def configure_connection (instance, name = 'database', credentials = None):
    """Configures IBM Streams for a certain connection.


    Creates or updates an application configuration object containing the required properties with connection information.


    Example for creating a configuration for a Streams instance with connection details::

        from streamsx.rest import Instance
        import streamsx.topology.context
        from icpd_core import icpd_util
        
        cfg = icpd_util.get_service_instance_details (name='your-streams-instance')
        cfg[context.ConfigParams.SSL_VERIFY] = False
        instance = Instance.of_service (cfg)
        app_cfg = configure_connection (instance, credentials = 'my_credentials_json')

    In Cloud Pak for Data you can configure a connection to Db2 with `Connecting to data sources <https://docs-icpdata.mybluemix.net/docs/content/SSQNUZ_current/com.ibm.icpdata.doc/igc/t_connect_data_sources.html>`_
    Example using this configured external connection with the name 'Db2-Cloud' to create an application configuration for IBM Streams::

        db_external_connection = icpd_util.get_connection('Db2-Cloud',conn_class='external')
        app_cfg = configure_connection (instance, credentials = db_external_connection)


    Args:
        instance(streamsx.rest_primitives.Instance): IBM Streams instance object.
        name(str): Name of the application configuration, default name is 'database'.
        credentials(str|dict): The service credentials, for example Db2 Warehouse service credentials.
    Returns:
        Name of the application configuration.
    """

    description = 'Database credentials'
    properties = {}
    if credentials is None:
        raise TypeError (credentials)
    
    if isinstance (credentials, dict):
        if 'class' in credentials:
            if credentials.get('class') == 'external': # CP4D external connection
                if 'url' in credentials:
                    db_json = {}
                    db_json['jdbcurl'] = credentials.get('url')
                    db_json['username'] = credentials.get('username')
                    db_json['password'] = credentials.get('password')
                    properties ['credentials'] = json.dumps (db_json)
                else:
                    raise TypeError(credentials)
        else:
            properties ['credentials'] = json.dumps (credentials)
    else:
        properties ['credentials'] = credentials
    
    # check if application configuration exists
    app_config = instance.get_application_configurations (name = name)
    if app_config:
        print ('update application configuration: ' + name)
        app_config[0].update (properties)
    else:
        print ('create application configuration: ' + name)
        instance.create_application_configuration (name, properties, description)
    return name


def download_toolkit(url=None, target_dir=None):
    r"""Downloads the latest JDBC toolkit from GitHub.

    Example for updating the JDBC toolkit for your topology with the latest toolkit from GitHub::

        import streamsx.database as db
        # download toolkit from GitHub
        jdbc_toolkit_location = db.download_toolkit()
        # add the toolkit to topology
        streamsx.spl.toolkit.add_toolkit(topology, jdbc_toolkit_location)

    Example for updating the topology with a specific version of the JDBC toolkit using a URL::

        import streamsx.database as db
        url171 = 'https://github.com/IBMStreams/streamsx.jdbc/releases/download/v1.7.1/streamsx.jdbc.toolkits-1.7.1-20190703-1017.tgz'
        jdbc_toolkit_location = db.download_toolkit(url=url171)
        streamsx.spl.toolkit.add_toolkit(topology, jdbc_toolkit_location)

    Args:
        url(str): Link to toolkit archive (\*.tgz) to be downloaded. Use this parameter to 
            download a specific version of the toolkit.
        target_dir(str): the directory where the toolkit is unpacked to. If a relative path is given,
            the path is appended to the system temporary directory, for example to /tmp on Unix/Linux systems.
            If target_dir is ``None`` a location relative to the system temporary directory is chosen.

    Returns:
        str: the location of the downloaded toolkit

    .. note:: This function requires an outgoing Internet connection
    .. versionadded:: 1.4
    """
    _toolkit_location = streamsx.toolkits.download_toolkit (toolkit_name=_TOOLKIT_NAME, url=url, target_dir=target_dir)
    return _toolkit_location


def run_statement(stream, credentials, schema=None, sql=None, sql_attribute=None, sql_params=None, transaction_size=1, jdbc_driver_class='com.ibm.db2.jcc.DB2Driver', jdbc_driver_lib=None, ssl_connection=None, truststore=None, truststore_password=None, keystore=None, keystore_password=None, keystore_type=None, truststore_type=None, plugin_name=None, security_mechanism=None, vm_arg=None, name=None):
    """Runs a SQL statement using DB2 client driver and JDBC database interface.

    The statement is called once for each input tuple received. Result sets that are produced by the statement are emitted as output stream tuples.
    
    This function includes the JDBC driver for DB2 database ('com.ibm.db2.jcc.DB2Driver') in the application bundle per default.

    Different drivers, e.g. for other databases than DB2, can be applied and the parameters ``jdbc_driver_class`` and ``jdbc_driver_lib`` must be specified.
    
    Supports two ways to specify the statement:

    * Statement is part of the input stream. You can specify which input stream attribute contains the statement with the ``sql_attribute`` argument. If input stream is of type ``CommonSchema.String``, then you don't need to specify the ``sql_attribute`` argument.
    * Statement is given with the ``sql`` argument. The statement can contain parameter markers that are set with input stream attributes specified by ``sql_params`` argument.

    Example with "insert" statement and values passed with input stream, where the input stream "sample_stream" is of type "sample_schema"::

        import streamsx.database as db
        
        sample_schema = StreamSchema('tuple<rstring A, rstring B>')
        ...
        sql_insert = 'INSERT INTO RUN_SAMPLE (A, B) VALUES (?, ?)'
        inserts = db.run_statement(sample_stream, credentials=credentials, schema=sample_schema, sql=sql_insert, sql_params="A, B")

    Example with "select count" statement and defined output schema with attribute ``TOTAL`` having the result of the query::

        sample_schema = StreamSchema('tuple<int32 TOTAL, rstring string>')
        sql_query = 'SELECT COUNT(*) AS TOTAL FROM SAMPLE.TAB1'
        query = topo.source([sql_query]).as_string()
        res = db.run_statement(query, credentials=credentials, schema=sample_schema)
    
    Example for using configured external connection with the name 'Db2-Cloud' (Cloud Pak for Data only),
    see `Connecting to data sources <https://docs-icpdata.mybluemix.net/docs/content/SSQNUZ_current/com.ibm.icpdata.doc/igc/t_connect_data_sources.html>`_::

        db_external_connection = icpd_util.get_connection('Db2-Cloud',conn_class='external')
        res = db.run_statement(query, credentials=db_external_connection, schema=sample_schema)


    Args:
        stream(Stream): Stream of tuples containing the SQL statements or SQL statement parameter values. Supports ``streamsx.topology.schema.StreamSchema`` (schema for a structured stream) or ``CommonSchema.String``  as input.
        credentials(dict|str): The credentials of the IBM cloud DB2 warehouse service as dict or configured external connection of kind "Db2 Warehouse" (Cloud Pak for Data only) as dict or the name of the application configuration.
        schema(StreamSchema): Schema for returned stream. Defaults to input stream schema if not set.             
        sql(str): String containing the SQL statement. Use this as alternative option to ``sql_attribute`` parameter.
        sql_attribute(str): Name of the input stream attribute containing the SQL statement. Use this as alternative option to ``sql`` parameter.
        sql_params(str): The values of SQL statement parameters. These values and SQL statement parameter markers are associated in lexicographic order. For example, the first parameter marker in the SQL statement is associated with the first sql_params value.
        transaction_size(int): The number of tuples to commit per transaction. The default value is 1.
        jdbc_driver_class(str): The default driver is for DB2 database 'com.ibm.db2.jcc.DB2Driver'.
        jdbc_driver_lib(str): Path to the JDBC driver library file. Specify the jar filename with absolute path, containing the class given with ``jdbc_driver_class`` parameter. Per default the 'db2jcc4.jar' is added to the 'opt' directory in the application bundle.
        ssl_connection(bool): Set to ``True`` to enable SSL connection.
        truststore(str): Path to the trust store file for the SSL connection.
        truststore_password(str): Password for the trust store file given by the truststore parameter.
        keystore(str): Path to the key store file for the SSL connection.
        keystore_password(str): Password for the key store file given by the keystore parameter.
        keystore_type(str): Type of the key store file (JKS, PKCS12).
        truststore_type(str): Type of the key store file (JKS, PKCS12).
        plugin_name(str): Name of the security plugin.
        security_mechanism(int): Value of the security mechanism.
        vm_arg(str): Arbitrary JVM arguments can be passed to the Streams operator.
        name(str): Sink name in the Streams context, defaults to a generated name.

    Returns:
        Output Stream.
    """

    if sql_attribute is None and sql is None:
        if stream.oport.schema == CommonSchema.String:
            sql_attribute = 'string'
        else:
            raise ValueError("Either sql_attribute or sql parameter must be set.")

    if jdbc_driver_lib is None and jdbc_driver_class != 'com.ibm.db2.jcc.DB2Driver':
        raise ValueError("Parameter jdbc_driver_lib must be specified containing the class from jdbc_driver_class parameter.")

    if schema is None:
        schema = stream.oport.schema

    if isinstance(credentials, dict):
        jdbcurl, username, password = _read_db2_credentials(credentials)
        app_config_name = None
    else:
        jdbcurl=None
        username=None
        password=None
        app_config_name = credentials

    _op = _JDBCRun(stream, schema, appConfigName=app_config_name, jdbcUrl=jdbcurl, jdbcUser=username, jdbcPassword=password, transactionSize=transaction_size, vmArg=vm_arg, name=name)
    if sql_attribute is not None:
        _op.params['statementAttr'] = _op.attribute(stream, sql_attribute)
    else:
        _op.params['statement'] = sql
    if sql_params is not None:
        _op.params['statementParamAttrs'] = sql_params
   
    _op.params['jdbcClassName'] = jdbc_driver_class
    if jdbc_driver_lib is None:
        _op.params['jdbcDriverLib'] = _add_driver_file_from_url(stream.topology, 'https://github.com/IBMStreams/streamsx.jdbc/raw/master/samples/JDBCSample/opt/db2jcc4.jar', 'db2jcc4.jar')
    else:
        _op.params['jdbcDriverLib'] = _add_driver_file(stream.topology, jdbc_driver_lib)

    if ssl_connection is not None:
        if ssl_connection is True:
            _op.params['sslConnection'] = _op.expression('true')
    if keystore is not None:
        _op.params['keyStore'] = _add_driver_file(stream.topology, keystore)
        if keystore_type is not None:
            _op.params['keyStoreType'] = keystore_type
    if keystore_password is not None:
        _op.params['keyStorePassword'] = keystore_password
    if truststore is not None:
        _op.params['trustStore'] = _add_driver_file(stream.topology, truststore)
        if truststore_type is not None:
            _op.params['trustStoreType'] = truststore_type
    if truststore_password is not None:
        _op.params['trustStorePassword'] = truststore_password
    if security_mechanism is not None:
        _op.params['securityMechanism'] = _op.expression(security_mechanism)
    if plugin_name is not None:
        _op.params['pluginName'] = plugin_name

    return _op.outputs[0]


class JDBCStatement(streamsx.topology.composite.Map):
    """
    Composite map transformation for JDBC statement

    The statement is called once for each input tuple received. Result sets that are produced by the statement are emitted as output stream tuples.
    
    This function includes the JDBC driver for DB2 database ('com.ibm.db2.jcc.DB2Driver') in the application bundle per default.

    Different drivers, e.g. for other databases than DB2, can be applied with the method ``set_jdbc_driver()``.
    
    There are two ways to specify the statement:

    * Statement is part of the input stream. You can specify which input stream attribute contains the statement with ``set_sql_attribute()``. If input stream is of type ``CommonSchema.String``, then you don't need to specify the ``sql_attribute`` property.
    * Statement is given with the ``set_sql()`` method. The statement can contain parameter markers that are set with input stream attributes specified by ``set_sql_params()``.

    Example with "insert" statement and values passed with input stream, where the input stream "sample_stream" is of type "sample_schema"::

        import streamsx.database as db
        
        sample_schema = StreamSchema('tuple<rstring A, rstring B>')
        ...
        sql_insert = 'INSERT INTO RUN_SAMPLE (A, B) VALUES (?, ?)'
        inserts = sample_stream.map(db.JDBCStatement(credentials), schema=sample_schema).set_sql(sql_insert).set_sql_params=("A, B")

    Example with "select count" statement and defined output schema with attribute ``TOTAL`` having the result of the query::

        sample_schema = StreamSchema('tuple<int32 TOTAL, rstring string>')
        sql_query = 'SELECT COUNT(*) AS TOTAL FROM SAMPLE.TAB1'
        query = topo.source([sql_query]).as_string()
        res = query.map(db.JDBCStatement(credentials), schema=sample_schema)
    
    Example for using configured external connection with the name 'Db2-Cloud' (Cloud Pak for Data only),
    see `Connecting to data sources <https://docs-icpdata.mybluemix.net/docs/content/SSQNUZ_current/com.ibm.icpdata.doc/igc/t_connect_data_sources.html>`_::

        db_external_connection = icpd_util.get_connection('Db2-Cloud',conn_class='external')
        res = query.map(db.JDBCStatement(db_external_connection), schema=sample_schema)

    .. versionadded:: 1.5

    Attributes
    ----------
    credentials : (dict|str)
        The credentials of the IBM cloud DB2 warehouse service as dict or configured external connection of kind "Db2 Warehouse" (Cloud Pak for Data only) as dict or the name of the application configuration.

    """


    def __init__(self, credentials):
        self.credentials = credentials
        # defaults
        self.vm_arg=None
        self.jdbc_driver_class = 'com.ibm.db2.jcc.DB2Driver'
        self.jdbc_driver_lib = None
        self.sql=None
        self.sql_attribute=None
        self.sql_params=None
        self.transaction_size=1
        self.ssl_connection=None
        self.truststore=None
        self.truststore_password=None
        self.truststore_type=None
        self.keystore=None
        self.keystore_password=None
        self.keystore_type=None
        self.plugin_name=None
        self.security_mechanism=None
        

    def set_vm_arg(self, vm_arg):
        """Sets the JVM arguments

        Args:
            vm_arg(str): Arbitrary JVM arguments can be passed to the Streams operator

        :returns: the instance
        :rtype: streamsx.database.JDBCStatment
        """
        self.vm_arg = vm_arg 
        return self

    def set_jdbc_driver(self, library, class_name='com.ibm.db2.jcc.DB2Driver'):
        """Sets the JDBC driver

        Args:
            library(str): Path to the JDBC driver library file. Specify the jar filename with absolute path, containing the class given with ``jdbc_driver_class`` parameter. Per default the 'db2jcc4.jar' is added to the 'opt' directory in the application bundle.
            class_name(str): Set the class name of the JDBC driver. The default driver is for DB2 database 'com.ibm.db2.jcc.DB2Driver'.

        :returns: the instance
        :rtype: streamsx.database.JDBCStatment
        """
        if lib is None and class_name != 'com.ibm.db2.jcc.DB2Driver':
            raise ValueError("Parameter jdbc_driver_lib must be specified containing the class from jdbc_driver_class parameter.")
        self.jdbc_driver_class = class_name
        self.jdbc_driver_lib = library
        return self

    def set_plugin_name(self, plugin_name):
        """Sets the name of the security plugin

        Args:
            plugin_name(str): Name of the security plugin

        :returns: the instance
        :rtype: streamsx.database.JDBCStatment
        """
        self.plugin_name = plugin_name 
        return self

    def set_security_mechanism(self, security_mechanism):
        """Sets the value of the security mechanism

        Args:
            security_mechanism(int): Value of the security mechanism.

        :returns: the instance
        :rtype: streamsx.database.JDBCStatment
        """
        self.security_mechanism = security_mechanism 
        return self

    def set_ssl_connection(self, ssl_connection=True):
        """Enable the SSL connection

        Args:
            ssl_connection(bool): Set to ``True`` to enable SSL connection.

        :returns: the instance
        :rtype: streamsx.database.JDBCStatment
        """
        self.ssl_connection = ssl_connection 
        return self

    def set_truststore(self, truststore, truststore_password, truststore_type=None):
        """Sets the truststore file and password

        Args:
            truststore(str): Path to the key store file for the SSL connection.
            truststore_password(str): Password for the key store file given by the keystore parameter.
            truststore_type(str): Type of the key store file (JKS, PKCS12).

        :returns: the instance
        :rtype: streamsx.database.JDBCStatment
        """
        self.truststore=truststore
        self.truststore_password=truststore_password
        self.truststore_type=truststore_type
        return self

    def set_keystore(self, keystore, keystore_password, keystore_type=None):
        """Set the keystore file and passsword

        Args:
            keystore(str): Path to the key store file for the SSL connection.
            keystore_password(str): Password for the key store file given by the keystore parameter.
            keystore_type(str): Type of the key store file (JKS, PKCS12).

        :returns: the instance
        :rtype: streamsx.database.JDBCStatment
        """
        self.keystore=keystore
        self.keystore_password=keystore_password
        self.keystore_type=keystore_type
        return self

    def set_transaction_size(self, transaction_size=1):
        """Sets the transaction size

        Args:
            transaction_size(int): The number of tuples to commit per transaction. The default value is 1.

        :returns: the instance
        :rtype: streamsx.database.JDBCStatment
        """
        self.transaction_size = transaction_size 
        return self

    def set_sql(self, sql):
        """Sets the SQL statement

        Args:
            sql(str): String containing the SQL statement. Use this as alternative option to ``sql_attribute`` parameter.


        :returns: the instance
        :rtype: streamsx.database.JDBCStatment
        """
        self.sql = sql 
        return self

    def set_sql_attribute(self, sql_attribute):
        """Sets the name of the input stream attribute containing the SQL statement. Use this as alternative option to ``sql`` parameter.
        
        Args:
            sql_attribute(str): Name of the input stream attribute containing the SQL statement. Use this as alternative option to ``sql`` parameter.

        :returns: the instance
        :rtype: streamsx.database.JDBCStatment
        """
        self.sql_attribute = sql_attribute 
        return self

    def set_sql_params(self, sql_params):
        """Sets the values of SQL statement parameters. These values and SQL statement parameter markers are associated in lexicographic order. For example, the first parameter marker in the SQL statement is associated with the first sql_params value.

        Args:
            sql_params(str): The values of SQL statement parameters. These values and SQL statement parameter markers are associated in lexicographic order. For example, the first parameter marker in the SQL statement is associated with the first sql_params value.

        :returns: the instance
        :rtype: streamsx.database.JDBCStatment
        """
        self.sql_params = sql_params 
        return self

    def populate(self, topology, stream, schema, name, **options):

        if self.sql_attribute is None and self.sql is None:
            if stream.oport.schema == CommonSchema.String:
                self.sql_attribute = 'string'
            else:
                raise ValueError("Either sql_attribute or sql parameter must be set.")

        if isinstance(self.credentials, dict):
            jdbcurl, username, password = _read_db2_credentials(self.credentials)
            app_config_name = None
        else:
            jdbcurl=None
            username=None
            password=None
            app_config_name = self.credentials

        _op = _JDBCRun(stream=stream, schema=schema, appConfigName=app_config_name, jdbcUrl=jdbcurl, jdbcUser=username, jdbcPassword=password, transactionSize=self.transaction_size, vmArg=self.vm_arg, name=name)

        if self.sql_attribute is not None:
            _op.params['statementAttr'] = _op.attribute(stream, self.sql_attribute)
        else:
            _op.params['statement'] = self.sql
        if self.sql_params is not None:
            _op.params['statementParamAttrs'] = self.sql_params

        # JDBC driver settings
        _op.params['jdbcClassName'] = self.jdbc_driver_class
        if self.jdbc_driver_lib is None:
            _op.params['jdbcDriverLib'] = _add_driver_file_from_url(stream.topology, 'https://github.com/IBMStreams/streamsx.jdbc/raw/master/samples/JDBCSample/opt/db2jcc4.jar', 'db2jcc4.jar')
        else:
            _op.params['jdbcDriverLib'] = _add_driver_file(stream.topology, self.jdbc_driver_lib)

        # SSL settings
        if self.ssl_connection is not None:
            if self.ssl_connection is True:
                _op.params['sslConnection'] = _op.expression('true')
        if self.keystore is not None:
            _op.params['keyStore'] = _add_driver_file(stream.topology, self.keystore)
            if self.keystore_type is not None:
                _op.params['keyStoreType'] = self.keystore_type
        if self.keystore_password is not None:
            _op.params['keyStorePassword'] = self.keystore_password
        if self.truststore is not None:
            _op.params['trustStore'] = _add_driver_file(stream.topology, self.truststore)
            if self.truststore_type is not None:
                _op.params['trustStoreType'] = self.truststore_type
        if self.truststore_password is not None:
            _op.params['trustStorePassword'] = self.truststore_password
        if self.security_mechanism is not None:
            _op.params['securityMechanism'] = _op.expression(self.security_mechanism)
        if self.plugin_name is not None:
            _op.params['pluginName'] = self.plugin_name

        return _op.outputs[0]


class _JDBCRun(streamsx.spl.op.Invoke):
    def __init__(self, stream, schema=None, appConfigName=None, jdbcClassName=None, jdbcDriverLib=None, jdbcUrl=None, batchSize=None, checkConnection=None, commitInterval=None, commitPolicy=None, hasResultSetAttr=None, isolationLevel=None, jdbcPassword=None, jdbcProperties=None, jdbcUser=None, keyStore=None, keyStorePassword=None, keyStoreType=None, trustStoreType=None, securityMechanism=None, pluginName=None, reconnectionBound=None, reconnectionInterval=None, reconnectionPolicy=None, sqlFailureAction=None, sqlStatusAttr=None, sslConnection=None, statement=None, statementAttr=None, statementParamAttrs=None, transactionSize=None, trustStore=None, trustStorePassword=None, vmArg=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.jdbc::JDBCRun"
        inputs=stream
        schemas=schema
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if jdbcClassName is not None:
            params['jdbcClassName'] = jdbcClassName
        if jdbcDriverLib is not None:
            params['jdbcDriverLib'] = jdbcDriverLib
        if jdbcUrl is not None:
            params['jdbcUrl'] = jdbcUrl
        if batchSize is not None:
            params['batchSize'] = batchSize
        if checkConnection is not None:
            params['checkConnection'] = checkConnection
        if commitInterval is not None:
            params['commitInterval'] = commitInterval
        if commitPolicy is not None:
            params['commitPolicy'] = commitPolicy
        if hasResultSetAttr is not None:
            params['hasResultSetAttr'] = hasResultSetAttr
        if isolationLevel is not None:
            params['isolationLevel'] = isolationLevel
        if jdbcPassword is not None:
            params['jdbcPassword'] = jdbcPassword
        if jdbcProperties is not None:
            params['jdbcProperties'] = jdbcProperties
        if jdbcUser is not None:
            params['jdbcUser'] = jdbcUser
        if keyStore is not None:
            params['keyStore'] = keyStore
        if keyStorePassword is not None:
            params['keyStorePassword'] = keyStorePassword
        if keyStoreType is not None:
            params['keyStoreType'] = keyStoreType
        if reconnectionBound is not None:
            params['reconnectionBound'] = reconnectionBound
        if reconnectionInterval is not None:
            params['reconnectionInterval'] = reconnectionInterval
        if reconnectionPolicy is not None:
            params['reconnectionPolicy'] = reconnectionPolicy
        if sqlFailureAction is not None:
            params['sqlFailureAction'] = sqlFailureAction
        if sqlStatusAttr is not None:
            params['sqlStatusAttr'] = sqlStatusAttr
        if sslConnection is not None:
            params['sslConnection'] = sslConnection
        if statement is not None:
            params['statement'] = statement
        if statementAttr is not None:
            params['statementAttr'] = statementAttr
        if statementParamAttrs is not None:
            params['statementParamAttrs'] = statementParamAttrs
        if transactionSize is not None:
            params['transactionSize'] = transactionSize
        if trustStore is not None:
            params['trustStore'] = trustStore
        if trustStorePassword is not None:
            params['trustStorePassword'] = trustStorePassword
        if trustStoreType is not None:
            params['trustStoreType'] = trustStoreType
        if securityMechanism is not None:
            params['securityMechanism'] = securityMechanism
        if pluginName is not None:
            params['pluginName'] = pluginName


        super(_JDBCRun, self).__init__(topology,kind,inputs,schema,params,name)



