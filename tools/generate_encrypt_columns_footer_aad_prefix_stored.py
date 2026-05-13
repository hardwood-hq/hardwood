#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.parquet.encryption import (
    CryptoFactory,
    KmsConnectionConfig,
    EncryptionConfiguration,
    KmsClient
)
import base64

data = pa.table({
    "id": [1, 2, 3],
    "name": ["Mark", "Mary", "Mike"],
    "salary": [10001, 45345, 34345]
})

KEY = base64.b64encode(b"0123456789abcdef0123456789abcdef").decode("utf-8")

class SimpleKmsClient(KmsClient):
    def __init__(self, conf):
        super().__init__()
        self.conf = conf

    def wrap_key(self, key_bytes, master_key_identifier):
        return base64.b64encode(key_bytes).decode("utf-8")

    def unwrap_key(self, wrapped_key, master_key_identifier):
        return base64.b64decode(self.conf[master_key_identifier])

def kms_client_factory(kms_connection_config):
    return SimpleKmsClient({
        "footer_key": KEY
    })

crypto_factory = CryptoFactory(kms_client_factory)

# aad_prefix is set and store_aad_prefix defaults to True,
# so the prefix is embedded in the file — reader does not need to supply it
enc_config = EncryptionConfiguration(
    footer_key="footer_key",
    plaintext_footer=False,
    uniform_encryption=True,
    double_wrapping=False,
    internal_key_material=True,
    aad_prefix="tenant-123"
)

kms_conf = KmsConnectionConfig(kms_instance_id="local")

file_encryption_props = crypto_factory.file_encryption_properties(
    kms_conf,
    enc_config
)

pq.write_table(
    data,
    "parquet-testing-runner/src/test/resources/encryption/encrypt_columns_footer_aad_prefix_stored.parquet",
    encryption_properties=file_encryption_props
)

print("File written")