from absl.testing import parameterized

from common.bucketinfo import BucketInfo


class TestBucketInfo(parameterized.TestCase):

  @parameterized.parameters(
      ('', 'org', 'token', 'bucket'),
      ('server-url', '', 'token', 'bucket'),
      ('server-url', 'org', '', 'bucket'),
      ('server-url', 'org', 'token', ''),
  )
  def test_emptyArg_raises(self, server_url: str, organization: str, token: str, bucket: str):
    with self.assertRaises(AssertionError):
      BucketInfo(server_url=server_url, organization=organization, token=token, bucket=bucket)

  def test_to(self):
    self.assertEqual('to(host: "server-url", org: "org1", bucket: "bucket3", token: "token2")',
                     BucketInfo(server_url='server-url', organization='org1', token='token2', bucket='bucket3').to())
