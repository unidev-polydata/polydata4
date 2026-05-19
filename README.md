# Polydata v4

Universal storage library for Java with a single, unified interface and
multiple pluggable backends. Write your code against one contract
(`com.unidev.polydata4.api.Polydata`) and switch backends through
configuration.

## Status

| Provider           | Type id           | Module                      | Status      |
|--------------------|-------------------|-----------------------------|-------------|
| MongoDB            | `mongodb`         | `polydata-mongodb`          | Active      |
| Redis              | `redis`           | `polydata-redis`            | Active      |
| SQLite (+ Flyway)  | `sqlite`          | `polydata-sqlite`           | Active      |
| Flat files (YAML)  | `flat-file-yaml`  | `polydata-flat-files`       | Active, RO  |
| Flat files (JSON)  | `flat-file-json`  | `polydata-flat-files`       | Active, RW  |
| PostgreSQL         | --                | `polydata-postgresql`       | Placeholder |
| AWS DynamoDB       | --                | `polydata-aws-dynamodb`     | Placeholder |
| AWS S3             | --                | `polydata-aws-s3`           | Placeholder |
| GCP Datastore      | --                | `polydata-gcp-datastore`    | Placeholder |

Placeholder modules are declared in `settings.gradle` but ship no Java
sources at this time.

## Requirements

* Java 25 (LTS) -- toolchain is auto-provisioned by Gradle on first
  build (via `foojay-resolver-convention`), so a system JDK 25 is
  optional. Any JDK >= 17 on `PATH` is enough to launch Gradle itself.
* Gradle 9.5.1 (use the wrapper: `./gradlew`).
* Docker daemon -- required only for the Testcontainers-driven tests
  (MongoDB and Redis providers, plus the integration test suite).

The `polydata-model` module stays pinned to Java 1.8 source/target so
downstream Java 8 consumers can still depend on the data carriers
(`BasicPoly`, `BasicPolyList`, etc.).

## Quick start

```gradle
repositories {
    maven {
        url = "https://mvn.universal-development.com/public"
    }
}

dependencies {
    implementation 'com.unidev.polydata4:polydata-factory:<version>'
}
```

The factory module transitively pulls in every active provider. If you
only need one backend, depend on that provider's module directly:

```gradle
dependencies {
    implementation 'com.unidev.polydata4:polydata-mongodb:<version>'
}
```

Artifact versions are derived from git tags via
`com.palantir.git-version`. Tagged commits produce semver strings;
untagged commits produce `<tag>-<n>-g<sha>[.dirty]`.

## Programmatic usage

### Factory dispatch (recommended)

The factory reads a `BasicPoly` config map and returns a `Polydata`
instance wired to the requested backend. Cache configuration, if
present, is wired automatically before the instance is returned.

```java
import com.unidev.polydata4.PolydataFactory;
import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.InsertRequest;

BasicPoly config = new BasicPoly();
config.put("type", "mongodb");
config.put("uri", "mongodb://localhost:27017/myapp");

PolydataFactory factory = new PolydataFactory();
Polydata polydata = factory.create(config).get();

// Each "dataset" is a logical collection; create it once per app start.
polydata.create("articles");

polydata.insert("articles", List.of(
    InsertRequest.builder()
        .data(BasicPoly.newPoly("article-1")
            .with("title", "Hello")
            .with("body",  "First post"))
        .indexToPersist(Set.of("draft", "_date"))
        .build()
));

BasicPolyList page = polydata.query("articles",
    BasicPolyQuery.builder().build());
```

### Direct construction

Every provider can be constructed without the factory if you already
know which backend you need:

```java
PolydataMongodb mongo = new PolydataMongodb("mongodb://localhost:27017/myapp");
mongo.prepareStorage();
mongo.create("articles");
```

## Configuration examples

The factory's `type` field selects the provider. Additional keys are
provider-specific. The examples below are YAML for readability; in code
you build the same shape as a `BasicPoly`.

MongoDB:

```yaml
type: mongodb
uri:  mongodb://localhost:27017/myapp
```

Redis:

```yaml
type: redis
host: localhost
port: 6379
```

SQLite (one file per dataset, written under `root`):

```yaml
type: sqlite
root: /data/sqlite
```

Flat files, read-only YAML:

```yaml
type: flat-file-yaml
root: /data/polydata
```

Flat files, read/write JSON (one JSON file per dataset, flushed on
`close()`):

```yaml
type: flat-file-json
root: /data/polydata
```

### Cache integration (optional)

Any provider can be backed by a JSR-107 (JCache) cache. The factory
honours a top-level `cache` block and calls `setCache(...)` on the
provider before returning it.

```yaml
type: mongodb
uri:  mongodb://localhost:27017/myapp
cache:
    data:
        type:              jcache
        provider:          org.ehcache.jsr107.EhcacheCachingProvider
        implementationUri: file://./ehcache.xml
        name:              polydata
```

`provider` is the fully qualified class of a JCache `CachingProvider`.
`implementationUri` is provider-specific (here, the EhCache XML config).
`name` is the named cache to fetch from the manager.

See `polydata-factory/src/main/java/com/unidev/polydata4/factory/StorageFactory.java`
for the exact wiring.

## Architecture overview

```
+--------------------+
| PolydataFactory    |  dispatches on config["type"]
+--------------------+
        |
        v
+--------------------+      +-------------------+
| StorageFactory     |----->| Optional<Cache>   |  setCache(...)
+--------------------+      +-------------------+
        |
        v
+--------------------+
| Polydata (iface)   |  17 methods: create / insert / read /
| AbstractPolydata   |  query / count / index / remove / ...
+--------------------+
        ^
        |
+-------+---------+----------+--------+---------+
|       |         |          |        |         |
Mongo  SQLite    Redis    FlatYaml FlatJson    (placeholders)
```

* `polydata-model` -- data carriers: `BasicPoly`, `BasicPolyList`,
  `BasicPolyMap`, `BasicPolyQuery`, `InsertRequest`, `InsertOptions`,
  `DataTransform`. JDK 1.8 source compatibility.
* `polydata-api` -- the `Polydata` interface plus `AbstractPolydata`
  (cache helpers) and the `packer` subsystem.
* `polydata-factory` -- `PolydataFactory`, one `StorageFactory` per
  built-in provider, the only place that knows all provider names.
* `polydata-mongodb` / `polydata-sqlite` / `polydata-redis` /
  `polydata-flat-files` -- concrete providers, one class each.

Module dependency graph is strictly downward:

```
factory  -->  every provider  -->  api  -->  model
```

For deeper documentation see `/projects/ai-kb/code-study/polydata4/`:

* `00-architecture-overview.md` -- layers, patterns, cross-cutting concerns.
* `01-execution-flow.md` -- the configure -> create -> insert -> query lifecycle.
* `polydata-<provider>.md` -- per-provider deep dive.

## Build and test

```bash
./gradlew build                       # build everything + run unit tests
./gradlew test                        # unit tests only
./gradlew :polydata-mongodb:test      # one module
./gradlew :polydata-factory:itest     # integration tests (needs Docker)
./gradlew :polydata-factory:itest --tests MongodbIntegrationTest
./gradlew publishToMavenLocal         # install to ~/.m2 for downstream testing
```

Testcontainers tests require:

* Docker daemon running and reachable via `/var/run/docker.sock`.
* Docker Engine API version >= 1.44 (Docker 25+). The build pins
  `api.version` and `DOCKER_API_VERSION` to `1.44` for the test JVM so
  `docker-java` does not fall back to its default 1.32 (which modern
  daemons reject).

Override the API version if your environment needs it:

```bash
DOCKER_API_VERSION=1.45 ./gradlew :polydata-mongodb:test
```

## CI

Two pipelines run in parallel:

* `Jenkinsfile` -- internal Jenkins, owns publication to
  `mvn.universal-development.com/public`.
* `.github/workflows/build.yml` -- GitHub Actions. Build + test only.
  Publication is explicitly excluded (`-x publish -x publishToMavenLocal`)
  and the workflow has `permissions: contents: read`.

The two pipelines are decoupled by design: GitHub Actions guards
correctness on every push/PR, Jenkins handles releases.

## Versioning

Versions are derived from git tags by `com.palantir.git-version` 3.3.0.
Run `./gradlew :polydata-api:properties | grep version` to see the
resolved version for the current commit.

## License

```
The MIT License (MIT)
Copyright (c) 2022 UNIVERSAL DEVELOPMENT PTE. LTD.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
```
