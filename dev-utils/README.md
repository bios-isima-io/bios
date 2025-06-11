# biOS developer package

biOS Developer is a toolkit installed in a Docker container that is useful to
analyze data using biOS. The toolkit is based on Jupyter Notebook that contains

- biOS Python SDK
- pandas
- numpy
- prophet
- streamlit

# Getting Started

Build the package by running

```
./build.sh
```

The artifact is a Docker image `bios-dev`.

Start a `bios-dev` container:

```
docker run -d --name bios-dev -p 8888:8888 bios-dev:latest
```

Initial password of Jupyter Notebook is `admin`. Changing password is strongly recommended.
In order to change the password, run `jupyter notebook password` in the container, such as

```
$ docker exec -it bios-dev /bin/bash
# jupyter notebook password
...
# supervisorctl restart jupyter
# exit
```

Then access to the Jupyter Notebook using a browser at `http://localhost:8888`.
