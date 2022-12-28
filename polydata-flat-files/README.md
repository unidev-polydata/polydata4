# polydata-flat-files

Flat file storage for polydata. Useful to use polys stored as flat files.

Supported formats:
  * YAML

## Yaml example

Example definition of Yaml storage

```yaml
# <polydata>/polydata.yaml
# poly configuration
_metadata:
  metakey1: "metavalue1"
  metakey2: "metavalue2"

mfield1: "mvalue1"
cfield1: "cvalue1"

item_per_page: 2

```

```yaml
# <polydata>/data/file.yaml
# records
_id: "test-id-1"
_timestamp: 1
aaa: "bbb"

_metadata:
  date: "2020-01-31 11:01:21"
  xxx: "yyy"
  _index:
    - "tag1"


```
