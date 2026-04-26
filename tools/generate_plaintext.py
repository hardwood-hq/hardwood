#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

import pyarrow as pa
import pyarrow.parquet as pq

data = pa.table({
    "id": [1, 2, 3],
    "name": ["Mark", "Mary", "Mike"],
    "salary": [10001, 45345, 34345]
})

pq.write_table(
    data,
    "parquet-testing-runner/src/test/resources/plain/plaintext.parquet"
)

print("File written")