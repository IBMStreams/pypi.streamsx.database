
import streamsx.database as db

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.rest as sr

import unittest
import datetime
import os
import json
import random
import time

##
## Test assumptions
##
## Streaming analytics service running
## DB2 Warehouse service credentials are located in a file referenced by environment variable DB2_CREDENTIALS
##

def streams_install_env_var():
    result = True
    try:
        os.environ['STREAMS_INSTALL']
    except KeyError: 
        result = False
    return result

# generates some data with schema (ID, NAME, AGE)
def generate_data():
    counter = 0
    while True:
        #yield a random id, name and age
        counter = counter +1 
        yield  {"NAME": "Name_" + str(random.randint(0,500)), "ID": counter, "AGE": random.randint(10,99)}
        time.sleep(0.10)

class TestComposite(unittest.TestCase):

    def _build_only(self, name, topo):
        self.jdbc_toolkit_home = None
        try:
            self.jdbc_toolkit_home = os.environ['STREAMS_JDBC_TOOLKIT']
        except KeyError: 
            pass
        if self.jdbc_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.jdbc_toolkit_home)

        result = streamsx.topology.context.submit("TOOLKIT", topo.graph) # creates tk* directory
        print(name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)
        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        print(name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)

    def test_basic(self):
        print ('\n---------'+str(self))
        name = 'test_basic'
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology(name)
        s = topo.source(['DROP TABLE STR_SAMPLE']).as_string()

        res_sql = s.map(db.JDBCStatement(credentials), schema=CommonSchema.String)
        res_sql.print()

        self._build_only(name, topo)

    def test_no_out_schema(self):
        print ('\n---------'+str(self))
        name = 'test_no_out_schema'
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology(name)
        s = topo.source(['DROP TABLE STR_SAMPLE']).as_string()

        res_sql = s.map(db.JDBCStatement(credentials))
        res_sql.print()

        self._build_only(name, topo)

    def test_props(self):
        print ('\n---------'+str(self))
        name = 'test_props'
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology(name)

        pulse = op.Source(topo, "spl.utility::Beacon", 'tuple<rstring A, rstring B>', params = {'iterations':1})
        pulse.A = pulse.output('"hello"')
        pulse.B = pulse.output('"world"')

        sample_schema = StreamSchema('tuple<rstring A, rstring B>')

        sql_create = 'CREATE TABLE RUN_SAMPLE (A CHAR(10), B CHAR(10))'
        stmt = db.JDBCStatement(credentials)
        stmt.sql = sql_create
        res_sql = pulse.stream.map(stmt, schema=sample_schema)
        res_sql.print()

        self._build_only(name, topo)

    def test_props_kwargs(self):
        print ('\n---------'+str(self))
        name = 'test_props_kwargs'
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology(name)

        pulse = op.Source(topo, "spl.utility::Beacon", 'tuple<rstring A, rstring B>', params = {'iterations':1})
        pulse.A = pulse.output('"hello"')
        pulse.B = pulse.output('"world"')

        sample_schema = StreamSchema('tuple<rstring A, rstring B>')

        config = {
            "sql": 'CREATE TABLE RUN_SAMPLE (A CHAR(10), B CHAR(10))'
        }
        res_sql = pulse.stream.map(db.JDBCStatement(credentials, **config), schema=sample_schema)
        res_sql.print()

        self._build_only(name, topo)

    def test_sample(self):
        print ('\n---------'+str(self))
        name = 'test_sample'
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology(name)
        tuple_schema = StreamSchema("tuple<int64 ID, rstring NAME, int32 AGE>")
        # Generates data for a stream of three attributes. Each attribute maps to a column using the same name of the Db2 database table.
        sample_data = topo.source(generate_data, name="GeneratedData").map(lambda tpl: (tpl["ID"], tpl["NAME"], tpl["AGE"]), schema=tuple_schema)
        statement = db.JDBCStatement(credentials)
        statement.sql = 'INSERT INTO SAMPLE_DEMO (ID, NAME, AGE) VALUES (? , ?, ?)'
        statement.sql_params = 'ID, NAME, AGE'
        sample_data.map(statement, name='INSERT')

        self._build_only(name, topo)

class TestCommit(unittest.TestCase):

    def setUp(self):
        Tester.setup_distributed(self)
        self.jdbc_toolkit_home = os.environ["STREAMS_JDBC_TOOLKIT"]
        # setup test config
        self.test_config = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(self.test_config)
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False  

    def _build_only(self, name, topo):
        self.jdbc_toolkit_home = None
        try:
            self.jdbc_toolkit_home = os.environ['STREAMS_JDBC_TOOLKIT']
        except KeyError: 
            pass
        if self.jdbc_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.jdbc_toolkit_home)

        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        print(name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)

    def test_1_prepare(self):
        print ('\n---------'+str(self))
        name = 'test_1_prepare'
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology(name)

        if self.jdbc_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.jdbc_toolkit_home)

        sql_drop = 'DROP TABLE SAMPLE1'
        sql_create = 'CREATE TABLE SAMPLE1 (A CHAR(10), B CHAR(10))'
        s = topo.source([sql_drop, sql_create]).as_string()
        res_sql = db.run_statement(s, credentials)
        res_sql.print()

        self._build_only(name, topo)

    def _test_2_commit_on_punct(self):
        print ('\n---------'+str(self))
        name = 'test_2_commit_on_punct'
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology(name)

        pulse = op.Source(topo, "spl.utility::Beacon", 'tuple<rstring A, rstring B>', params = {'iterations':50, 'period':2.0})
        pulse.A = pulse.output('(rstring)IterationCount()')
        pulse.B = pulse.output('"world"')

        s = pulse.stream.punctor(lambda t : '20' == t['A'], before=True)

        sample_schema = StreamSchema('tuple<rstring A, rstring B>')

        stmt = db.JDBCStatement(credentials)
        stmt.sql = 'INSERT INTO SAMPLE1 (A, B) VALUES (? , ?)'
        stmt.sql_params = 'A,B'
        stmt.commit_on_punct = True
        res_sql = s.map(stmt, schema=sample_schema)
        res_sql.print()

        self._build_only(name, topo)

    def _test_2_commit_on_punct_sql_attr(self):
        print ('\n---------'+str(self))
        name = 'test_2_commit_on_punct_sql_attr'
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology(name)

        pulse = op.Source(topo, "spl.utility::Beacon", 'tuple<rstring idx, rstring sql>', params = {'iterations':50, 'period':2.0})
        pulse.idx = pulse.output('(rstring)IterationCount()')
        pulse.sql = pulse.output('"INSERT INTO SAMPLE1 (A, B) VALUES (\'hello\', \'world\')"')

        s = pulse.stream.punctor(lambda t : '30' == t['idx'], before=True)

        sample_schema = StreamSchema('tuple<rstring sql>')

        stmt = db.JDBCStatement(credentials)
        stmt.sql_attribute = 'sql'
        stmt.commit_on_punct = True
        res_sql = s.map(stmt, schema=sample_schema)
        res_sql.print()

        self._build_only(name, topo)

    def test_2_batch_on_punct(self):
        print ('\n---------'+str(self))
        name = 'test_2_batch_on_punct'
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology(name)

        pulse = op.Source(topo, "spl.utility::Beacon", 'tuple<rstring A, rstring B>', params = {'iterations':50, 'period':2.0})
        pulse.A = pulse.output('(rstring)IterationCount()')
        pulse.B = pulse.output('"world"')

        s = pulse.stream.punctor(lambda t : '20' == t['A'], before=True)

        sample_schema = StreamSchema('tuple<rstring A, rstring B>')

        stmt = db.JDBCStatement(credentials)
        stmt.sql = 'INSERT INTO SAMPLE1 (A, B) VALUES (? , ?)'
        stmt.sql_params = 'A,B'
        stmt.batch_on_punct = True
        res_sql = s.map(stmt, schema=sample_schema)
        res_sql.print()

        self._build_only(name, topo)

    def test_2_batch_on_punct_sql_attr(self):
        print ('\n---------'+str(self))
        name = 'test_2_batch_on_punct_sql_attr'
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology(name)

        pulse = op.Source(topo, "spl.utility::Beacon", 'tuple<rstring idx, rstring sql>', params = {'iterations':50, 'period':2.0})
        pulse.idx = pulse.output('(rstring)IterationCount()')
        pulse.sql = pulse.output('"INSERT INTO SAMPLE1 (A, B) VALUES (\'hello\', \'world\')"')

        s = pulse.stream.punctor(lambda t : '30' == t['idx'], before=True)

        sample_schema = StreamSchema('tuple<rstring sql>')

        stmt = db.JDBCStatement(credentials)
        stmt.sql_attribute = 'sql'
        stmt.batch_on_punct = True
        res_sql = s.map(stmt, schema=sample_schema)
        res_sql.print()

        self._build_only(name, topo)


    def test_3_select_count(self):
        print ('\n---------'+str(self))
        name = 'test_3_select_count'
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology(name)
        sample_schema = StreamSchema('tuple<int32 TOTAL, rstring string>')
        sql_query = 'SELECT COUNT(*) AS TOTAL FROM SAMPLE1'
        s = topo.source([sql_query]).as_string()
        res_sql = db.run_statement(s, credentials, schema=sample_schema)
        res_sql.print()
        self._build_only(name, topo)

class TestParams(unittest.TestCase):

    def test_bad_lib_param(self):
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology()
        s = topo.source(['DROP TABLE STR_SAMPLE']).as_string()
        # expect ValueError because driver class is not default and jdbc_driver_lib is missing
        self.assertRaises(ValueError, db.run_statement, s, credentials, jdbc_driver_class='com.any.DBDriver')
        # expect ValueError because jdbc_driver_lib is not a valid file
        self.assertRaises(ValueError, db.run_statement, s, credentials, jdbc_driver_class='com.any.DBDriver', jdbc_driver_lib='_any_invalid_file_')

class TestDB(unittest.TestCase):


    def test_string_type(self):
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology('test_string_type')

        if self.jdbc_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.jdbc_toolkit_home)

        sql_create = 'CREATE TABLE STR_SAMPLE (A CHAR(10), B CHAR(10))'
        sql_insert = 'INSERT INTO STR_SAMPLE (A, B) VALUES (\'hello\', \'world\')'
        sql_drop = 'DROP TABLE STR_SAMPLE'
        s = topo.source([sql_create, sql_insert, sql_drop]).as_string()
        res_sql = db.run_statement(s, credentials)
        res_sql.print()
        tester = Tester(topo)
        tester.tuple_count(res_sql, 3)
        #tester.run_for(60)
        tester.test(self.test_ctxtype, self.test_config)


    @unittest.skipIf(streams_install_env_var() == False, "Missing STREAMS_INSTALL environment variable.")
    def test_string_type_with_driver_param(self):
        streams_install = os.environ['STREAMS_INSTALL']
        jdbc_driver_lib=streams_install+'/samples/com.ibm.streamsx.jdbc/JDBCSample/opt/db2jcc4.jar'
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology('test_string_type_with_driver_param')

        if self.jdbc_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.jdbc_toolkit_home)

        sql_create = 'CREATE TABLE STR_SAMPLE (A CHAR(10), B CHAR(10))'
        sql_insert = 'INSERT INTO STR_SAMPLE (A, B) VALUES (\'hello\', \'world\')'
        sql_drop = 'DROP TABLE STR_SAMPLE'
        s = topo.source([sql_create, sql_insert, sql_drop]).as_string()
        res_sql = db.run_statement(s, credentials, jdbc_driver_class='com.ibm.db2.jcc.DB2Driver', jdbc_driver_lib=jdbc_driver_lib)
        res_sql.print()
        tester = Tester(topo)
        tester.tuple_count(res_sql, 3)
        #tester.run_for(60)
        tester.test(self.test_ctxtype, self.test_config)


    def test_mixed_types(self):
        creds_file = os.environ['DB2_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)

        topo = Topology('test_mixed_types')
        if self.jdbc_toolkit_home is not None:
            streamsx.spl.toolkit.add_toolkit(topo, self.jdbc_toolkit_home)

        pulse = op.Source(topo, "spl.utility::Beacon", 'tuple<rstring A, rstring B>', params = {'iterations':1})
        pulse.A = pulse.output('"hello"')
        pulse.B = pulse.output('"world"')

        sample_schema = StreamSchema('tuple<rstring A, rstring B>')
        query_schema = StreamSchema('tuple<rstring sql>')

        sql_create = 'CREATE TABLE RUN_SAMPLE1 (A CHAR(10), B CHAR(10))'
        create_table = db.run_statement(pulse.stream, credentials, schema=sample_schema, sql=sql_create)
 
        sql_insert = 'INSERT INTO RUN_SAMPLE1 (A, B) VALUES (?, ?)'
        inserts = db.run_statement(create_table, credentials, schema=sample_schema, sql=sql_insert, sql_params="A, B")

        query = op.Map('spl.relational::Functor', inserts, schema=query_schema)
        query.sql = query.output('"SELECT A, B FROM RUN_SAMPLE1"')

        res_sql = db.run_statement(query.stream, credentials, schema=sample_schema, sql_attribute='sql')
        res_sql.print()

        sql_drop = 'DROP TABLE RUN_SAMPLE1'
        drop_table = db.run_statement(res_sql, credentials, sql=sql_drop)

        tester = Tester(topo)
        tester.tuple_count(drop_table, 1)
        #tester.run_for(60)
        tester.test(self.test_ctxtype, self.test_config)

class TestDistributed(TestDB):
    def setUp(self):
        Tester.setup_distributed(self)
        self.jdbc_toolkit_home = os.environ["STREAMS_JDBC_TOOLKIT"]
        # setup test config
        self.test_config = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(self.test_config)
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False  

class TestDistributedRemote(TestDB):
    def setUp(self):
        Tester.setup_distributed(self)
        self.jdbc_toolkit_home = None
        # setup test config
        self.test_config = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(self.test_config)
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False 

class TestStreamingAnalytics(TestDB):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=False)
        self.jdbc_toolkit_home = os.environ["STREAMS_JDBC_TOOLKIT"]

    @classmethod
    def setUpClass(self):
        # start streams service
        connection = sr.StreamingAnalyticsConnection()
        service = connection.get_streaming_analytics()
        result = service.start_instance()

class TestStreamingAnalyticsRemote(TestStreamingAnalytics):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.jdbc_toolkit_home = None

    @classmethod
    def setUpClass(self):
        super().setUpClass()

