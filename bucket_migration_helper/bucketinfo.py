from dataclasses import dataclass


@dataclass(kw_only=True)
class BucketInfo:
  server_url: str
  organization: str
  bucket: str
  token: str

  def __post_init__(self) -> None:
    assert self.server_url != '', 'expected "server_url" to be non-empty'
    assert self.organization != '', 'expected "organization" to be non-empty'
    assert self.bucket != '', 'expected "bucket" to be non-empty'
    assert self.token != '', 'expected "token" to be non-empty'

  def to(self) -> str:
    return f'to(host: "{self.server_url}", org: "{self.organization}", bucket: "{self.bucket}", token: "{self.token}")'
