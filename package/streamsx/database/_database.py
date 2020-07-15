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

        from icpd_core import icpd_util
        from streamsx.rest_primitives import Instance
        import streamsx.database as db
        
        cfg = icpd_util.get_service_instance_details (name='your-streams-instance')
        cfg[context.ConfigParams.SSL_VERIFY] = False
        instance = Instance.of_service (cfg)
        app_cfg = db.configure_connection (instance, credentials = 'my_credentials_json')

    In Cloud Pak for Data you can configure a connection to Db2 with `Connecting to data sources <https://www.ibm.com/support/producthub/icpdata/docs/content/SSQNUZ_current/cpd/access/connect-data-sources.html>`_
    Example using this configured external connection with the name 'Db2-Cloud' to create an application configuration for IBM Streams::

        db_external_connection = icpd_util.get_connection('Db2-Cloud',conn_class='external')
        app_cfg = db.configure_connection (instance, credentials = db_external_connection)


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
    see `Connecting to data sources <https://www.ibm.com/support/producthub/icpdata/docs/content/SSQNUZ_current/cpd/access/connect-data-sources.html>`_::

        db_external_connection = icpd_util.get_connection('Db2-Cloud',conn_class='external')
        res = db.run_statement(query, credentials=db_external_connection, schema=sample_schema)


    Args:
        stream(streamsx.topology.topology.Stream): Stream of tuples containing the SQL statements or SQL statement parameter values. Supports ``streamsx.topology.schema.StreamSchema`` (schema for a structured stream) or ``CommonSchema.String``  as input.
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
        :py:class:`topology_ref:streamsx.topology.topology.Stream`: Output Stream.

    .. deprecated:: 1.5.0
        Use the :py:class:`~JDBCStatement`.
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
        _op.params['jdbcDriverLib'] = _add_driver_file_from_url(stream.topology, 'https://github.com/IBMStreams/streamsx.jdbc/raw/develop/samples/JDBCSample/opt/db2jcc4.jar', 'db2jcc4.jar')
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
    
    This function includes the JDBC driver for Db2 database ('com.ibm.db2.jcc.DB2Driver') in the application bundle per default.

    Different drivers, e.g. for other databases than Db2, can be applied with the properties :attr:`jdbc_driver_lib` and :attr:`jdbc_driver_class`.
    
    There are two ways to specify the statement:

    * Statement is part of the input stream. You can specify which input stream attribute contains the statement with :attr:`sql_attribute`. If input stream is of type ``CommonSchema.String``, then you don't need to specify the :attr:`sql_attribute` property.
    * Statement is given with the :attr:`sql` property. The statement can contain parameter markers that are set with input stream attributes specified by :attr:`sql_params`.

    Example of a Streams application that inserts generated data into as rows in a table::

        from streamsx.topology.topology import *
        from streamsx.topology.schema import StreamSchema
        from streamsx.topology.context import submit
        import streamsx.database as db
        import random
        import time

        # generates some data with schema (ID, NAME, AGE)
        def generate_data():
            counter = 0
            while True:
                #yield a random id, name and age
                counter = counter +1 
                yield  {"NAME": "Name_" + str(random.randint(0,500)), "ID": counter, "AGE": random.randint(10,99)}
                time.sleep(0.10)

        topo = Topology()

        tuple_schema = StreamSchema("tuple<int64 ID, rstring NAME, int32 AGE>")
        # Generates data for a stream of three attributes. Each attribute maps to a column using the same name of the Db2 database table.
        sample_data = topo.source(generate_data, name="GeneratedData").map(lambda tpl: (tpl["ID"], tpl["NAME"], tpl["AGE"]), schema=tuple_schema)

        statement = db.JDBCStatement(credentials)
        statement.sql = 'INSERT INTO SAMPLE_DEMO (ID, NAME, AGE) VALUES (? , ?, ?)'
        statement.sql_params = 'ID, NAME, AGE'

        sample_data.map(statement, name='INSERT')

        # Use for IBM Streams including IBM Cloud Pak for Data
        submit ('DISTRIBUTED', topo, cfg)

    Example with key value arguments for the :attr:`options` parameter::

        config = {
            'sql': 'INSERT INTO SAMPLE_DEMO (ID, NAME, AGE) VALUES (? , ?, ?)'
            'sql_params': 'ID, NAME, AGE'
        }
        inserts = sample_stream.map(db.JDBCStatement(credentials, **config))

    Example with "select count" statement and defined output schema with attribute ``TOTAL`` having the result of the query::

        sample_schema = StreamSchema('tuple<int32 TOTAL, rstring string>')
        sql_query = 'SELECT COUNT(*) AS TOTAL FROM SAMPLE.TAB1'
        query = topo.source([sql_query]).as_string()
        res = query.map(db.JDBCStatement(credentials), schema=sample_schema)
  
    Example with "drop table" statement and default output schema (set to input schema)::
  
        sql_drop = 'DROP TABLE RUN_SAMPLE'
        s = topo.source([sql_drop]).as_string()
        res_sql = s.map(db.JDBCStatement(credentials))
        res_sql.print()

    Example for using configured external connection with the name 'Db2-Cloud' (Cloud Pak for Data only),
    see `Connecting to data sources <https://www.ibm.com/support/producthub/icpdata/docs/content/SSQNUZ_current/cpd/access/connect-data-sources.html>`_::

        db_external_connection = icpd_util.get_connection('Db2-Cloud',conn_class='external')
        res = query.map(db.JDBCStatement(db_external_connection), schema=sample_schema)

    .. versionadded:: 1.5

    Attributes
    ----------
    credentials : dict|str
        The credentials of the IBM cloud Db2 warehouse service as dict or configured external connection of kind "Db2 Warehouse" (Cloud Pak for Data only) as dict or the name of the application configuration.
    options : kwargs
        The additional optional parameters as variable keyword arguments.
    """


    def __init__(self, credentials, **options):
        self.credentials = credentials
        # defaults
        self.vm_arg = None
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
        if 'vm_arg' in options:
            self.vm_arg = options.get('vm_arg')
        if 'jdbc_driver_class' in options:
            self.jdbc_driver_class = options.get('jdbc_driver_class')
        if 'jdbc_driver_lib' in options:
            self.jdbc_driver_lib = options.get('jdbc_driver_lib')
        if 'sql' in options:
            self.sql = options.get('sql')
        if 'sql_attribute' in options:
            self.sql_attribute = options.get('sql_attribute')
        if 'sql_params' in options:
            self.sql_params = options.get('sql_params')
        if 'transaction_size' in options:
            self.transaction_size = options.get('transaction_size')
        if 'ssl_connection' in options:
            self.ssl_connection = options.get('ssl_connection')
        if 'truststore' in options:
            self.truststore = options.get('truststore')
        if 'truststore_password' in options:
            self.truststore_password = options.get('truststore_password')
        if 'truststore_type' in options:
            self.truststore_type = options.get('truststore_type')
        if 'keystore' in options:
            self.keystore = options.get('keystore')
        if 'keystore_password' in options:
            self.keystore_password = options.get('keystore_password')
        if 'keystore_type' in options:
            self.keystore_type = options.get('keystore_type')
        if 'plugin_name' in options:
            self.plugin_name = options.get('plugin_name')
        if 'security_mechanism' in options:
            self.security_mechanism = options.get('security_mechanism')

        
    @property
    def vm_arg(self):
        """
            str: Arbitrary JVM arguments can be passed to the Streams operator
        """
        return self._vm_arg

    @vm_arg.setter
    def vm_arg(self, value):
        self._vm_arg = value
        

    @property
    def jdbc_driver_class(self):
        """
            str: Set the class name of the JDBC driver. The default driver is for DB2 database ``com.ibm.db2.jcc.DB2Driver``.
        """
        return self._jdbc_driver_class

    @jdbc_driver_class.setter
    def jdbc_driver_class(self, value):
        self._jdbc_driver_class = value
        

    @property
    def jdbc_driver_lib(self):
        """
            str: Path to the JDBC driver library file. Specify the jar filename with absolute path, containing the class given with :attr:`jdbc_driver_class` property. Per default the ``db2jcc4.jar`` is added to the 'opt' directory in the application bundle.
        """
        return self._jdbc_driver_lib

    @jdbc_driver_lib.setter
    def jdbc_driver_lib(self, value):
        self._jdbc_driver_lib = value
        

    @property
    def plugin_name(self):
        """
            str: Name of the security plugin
        """
        return self._plugin_name

    @plugin_name.setter
    def plugin_name(self, value):
        self._plugin_name = value

        
    @property
    def security_mechanism(self):
        """
            int: Value of the security mechanism
        """
        return self._security_mechanism

    @security_mechanism.setter
    def security_mechanism(self, value):
        self._security_mechanism = value


    @property
    def ssl_connection(self):
        """
            bool: Set to ``True`` to enable SSL connection
        """
        return self._ssl_connection

    @ssl_connection.setter
    def ssl_connection(self, value):
        self._ssl_connection = value



    @property
    def truststore(self):
        """
            str: Path to the trust store file for the SSL connection
        """
        return self._truststore

    @truststore.setter
    def truststore(self, value):
        self._truststore = value



    @property
    def truststore_password(self):
        """
            str: Password for the trust store file given by the :attr:`truststore` property.
        """
        return self._truststore_password

    @truststore_password.setter
    def truststore_password(self, value):
        self._truststore_password = value



    @property
    def truststore_type(self):
        """
            str: Type of the trust store file (JKS, PKCS12).
        """
        return self._truststore_type

    @truststore_type.setter
    def truststore_type(self, value):
        self._truststore_type = value


    @property
    def keystore(self):
        """
            str: Path to the key store file for the SSL connection
        """
        return self._keystore

    @keystore.setter
    def keystore(self, value):
        self._keystore = value



    @property
    def keystore_password(self):
        """
            str: Password for the key store file given by the :attr:`keystore` property.
        """
        return self._keystore_password

    @keystore_password.setter
    def keystore_password(self, value):
        self._keystore_password = value



    @property
    def keystore_type(self):
        """
            str: Type of the key store file (JKS, PKCS12).
        """
        return self._keystore_type

    @keystore_type.setter
    def keystore_type(self, value):
        self._keystore_type = value


    @property
    def transaction_size(self):
        """
            int: The number of tuples to commit per transaction. The default value is 1.
        """
        return self._transaction_size

    @transaction_size.setter
    def transaction_size(self, value):
        self._transaction_size = value


    @property
    def sql(self):
        """
            str: String containing the SQL statement. Use this as alternative option to :attr:`sql_attribute` property.
        """
        return self._sql

    @sql.setter
    def sql(self, value):
        self._sql = value


    @property
    def sql_attribute(self):
        """
            str: Name of the input stream attribute containing the SQL statement. Use this as alternative option to :attr:`sql` property.
        """
        return self._sql_attribute

    @sql_attribute.setter
    def sql_attribute(self, value):
        self._sql_attribute = value


    @property
    def sql_params(self):
        """
            str:  The values of SQL statement parameters. These values and SQL statement parameter markers are associated in lexicographic order. For example, the first parameter marker in the SQL statement is associated with the first sql_params value.
        """
        return self._sql_params

    @sql_params.setter
    def sql_params(self, value):
        self._sql_params = value


    def populate(self, topology, stream, schema, name, **options):

        if self.sql_attribute is None and self.sql is None:
            if stream.oport.schema == CommonSchema.String:
                self.sql_attribute = 'string'
            else:
                raise ValueError("Either sql_attribute or sql parameter must be set.")

        if self.jdbc_driver_lib is None and self.jdbc_driver_class != 'com.ibm.db2.jcc.DB2Driver':
            raise ValueError("Parameter jdbc_driver_lib must be specified containing the class from jdbc_driver_class parameter.")

        if schema is None:
            schema = stream.oport.schema # output schema is the same as input schema

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
            _op.params['jdbcDriverLib'] = _add_driver_file_from_url(stream.topology, 'https://github.com/IBMStreams/streamsx.jdbc/raw/develop/samples/JDBCSample/opt/db2jcc4.jar', 'db2jcc4.jar')
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



