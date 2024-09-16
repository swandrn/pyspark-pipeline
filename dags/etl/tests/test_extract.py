import unittest
from dags.etl import awsenv
from dags.etl import extract
from dags.etl import paths
from pyspark.sql import SparkSession, DataFrame
import responses
import json
import threading
from queue import Queue

class TestReadCsv(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        print("Creating a Spark session...")  
        self.spark = SparkSession.builder \
        .master("local[*]") \
        .appName("credit_cards") \
        .config('spark.local.dir', '/tmp/spark-temp') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.6,org.apache.hadoop:hadoop-common:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.367') \
        .config('spark.driver.memory', '2g') \
        .config('spark.network.timeout', '36000s') \
        .config('spark.executor.heartbeatInterval', '3600s') \
        .getOrCreate()
        print("Created!")

        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", awsenv.ACCESS_KEY_ID)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", awsenv.SECRET_ACCESS_KEY)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com")

    def test_read_csv(self):
        self.assertIsInstance(extract.read_csv(self.spark, paths.TEST_CSV), DataFrame)

    def test_read_csv_url(self):
        with self.assertRaises(SystemExit):
            extract.read_csv(self.spark, "https://www.wikipedia.org/")

    def test_read_csv_img(self):
        with self.assertRaises(SystemExit):
            extract.read_csv(self.spark, paths.IMG_FILE)
            

    def test_read_csv_mp4(self):
        with self.assertRaises(SystemExit):
            extract.read_csv(self.spark, paths.MP4_FILE)

    def test_read_csv_int(self):
        with self.assertRaises(SystemExit):
            extract.read_csv(self.spark, 0)

    def test_read_csv_bool(self):
        with self.assertRaises(SystemExit):
            extract.read_csv(self.spark, True)

    def test_read_csv_none(self):
        with self.assertRaises(SystemExit):
            extract.read_csv(self.spark, None)

    def test_read_csv_string(self):
        with self.assertRaises(SystemExit):
            extract.read_csv(self.spark, "foo")

    @classmethod
    def tearDownClass(self):
        self.spark.stop()

class TestApiCall(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        MAX_WORKERS = 1
        self.consumers = threading.BoundedSemaphore(MAX_WORKERS)
        self.q = Queue()
    
    @responses.activate
    def test_call_random_user(self):
        mock_url = 'https://mockcall.com/api'
        valid_body = '{"results":[{"gender":"female","name":{"title":"Ms","first":"Villemo","last":"Strømnes"},"location":{"street":{"number":4939,"name":"Østensjøveien"},"city":"Torhaug","state":"Sogn og Fjordane","country":"Norway","postcode":"9587","coordinates":{"latitude":"44.3646","longitude":"149.3626"},"timezone":{"offset":"+4:30","description":"Kabul"}},"email":"villemo.stromnes@example.com","login":{"uuid":"fdda7fda-7d21-4397-b855-d1355287553f","username":"goldenswan186","password":"emilio","salt":"2jdnJD0H","md5":"ad75d06f7e0beceb94363a90e1d506a6","sha1":"648d7480f32c00dab9be8445a9b3014806708e49","sha256":"f4478b524649383fc195e74d8cedbee37d8590e3ba7755d6ac3e886aee054745"},"dob":{"date":"1958-05-23T11:56:47.176Z","age":66},"registered":{"date":"2009-07-18T01:35:20.539Z","age":15},"phone":"35433291","cell":"96604993","id":{"name":"FN","value":"23055842221"},"picture":{"large":"https://randomuser.me/api/portraits/women/36.jpg","medium":"https://randomuser.me/api/portraits/med/women/36.jpg","thumbnail":"https://randomuser.me/api/portraits/thumb/women/36.jpg"},"nat":"NO"}],"info":{"seed":"96b9cbfd85aae303","results":1,"page":1,"version":"1.4"}}'
        responses.add(**{
            'method': responses.GET,
            'url': mock_url,
            'body': valid_body,
            'status': 200,
            'content_type': 'application/json; charset=utf-8',
        })

        thread = threading.Thread(target=extract.call_random_user, args=[mock_url, self.q])
        thread.start()
        user_json = self.q.get()
        self.assertEqual(user_json, json.loads(valid_body)["results"][0])

    @responses.activate
    def test_call_random_user_not_found(self):
        mock_url = 'https://mockcall.com/api'
        not_found_body = 'Not Found'
        responses.add(**{
            'method': responses.GET,
            'url': mock_url,
            'body': not_found_body,
            'status': 404,
            'content_type': 'application/json; charset=utf-8',
        })

        with self.assertRaises(Exception):
            extract.call_random_user(mock_url)
    
    @responses.activate
    def test_call_random_user_timeout(self):
        from requests.exceptions import ConnectTimeout
        mock_url = 'https://mockcall.com/api'
        responses.add(**{
            'method': responses.GET,
            'url': mock_url,
            'body': ConnectTimeout(),
            'status': 200,
            'content_type': 'application/json; charset=utf-8',
        })

        with self.assertRaises(Exception):
            extract.call_random_user(mock_url)
    
    @responses.activate
    def test_call_random_user_none(self):
        mock_url = 'https://mockcall.com/api'
        responses.add(**{
            'method': responses.GET,
            'url': mock_url,
            'body': "",
            'content_type': 'application/json; charset=utf-8',
        })

        with self.assertRaises(Exception):
            extract.call_random_user(None)

if __name__ == '__main__':
    unittest.main()