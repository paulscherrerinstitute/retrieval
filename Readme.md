# Readme

## Requirements

* Java 11+ (older JDK may work, but has not been tested)


## Build

Create a "fat" jar with all dependencies included:

```bash
gradle bootJar
```


## Configuration file

Retrieval accepts a json configuration file, mainly to let it know it's peer nodes,
whether the nodes use central (cameras) or distributed (non-camera) storage,
the location of the database for channel search queries,
and what the name of the backend is that the node represents.

Example:

```json
{
  "splitNodes": [
    { "split":  0, "host": "sf-daqbuf-21", "port": 8371 },
    { "split":  1, "host": "sf-daqbuf-22", "port": 8371 },
    { "split":  2, "host": "sf-daqbuf-23", "port": 8371 },
    "and more ..."
  ],
  "database": {
    "host": "sf-daqbuf-33",
    "port": 5432,
    "database": "daqbuffer",
    "username": "daqbuffer",
    "password": "..."
  },
  "backend": "sf-databuffer"
}
```


## Run

```bash
java -Dserver.port=8371 \
-Dretrieval.dataBaseDir=... \
-Dretrieval.baseKeyspaceName=... \
-Dretrieval.configFile=[the json config file] \
-jar retrieval-N.N.N.jar
```


## Deployment

Please refer to: <https://github.com/paulscherrerinstitute/retrieval-deploy>
