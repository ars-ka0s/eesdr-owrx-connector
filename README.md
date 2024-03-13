# eesdr-owrx-connector
Connector to use the Expert Electronics TCI protocol to feed an OpenWebRX instance.

### Installation - OS

In an OpenWebRX+ installation on a base operating system, installation requires two steps -- installing this package and adding the source to the OpenWebRX files.  This can be achieved with:
```
pip install eesdr-owrx-connector
curl https://raw.githubusercontent.com/ars-ka0s/openwebrx/eesdr/owrx/feature.py -o /opt/openwebrx/owrx/feature.py
curl https://raw.githubusercontent.com/ars-ka0s/openwebrx/eesdr/owrx/source/eesdr.py -o /opt/openwebrx/owrx/source/eesdr.py
```

### Installation - Docker
If using the Docker image, a short `Dockerfile` can customize the image:
```
FROM slechev/openwebrxplus-nightly
RUN apt update
RUN apt install -y --no-install-recommends python3-pip git
ADD https://raw.githubusercontent.com/ars-ka0s/openwebrx/eesdr/owrx/feature.py /opt/openwebrx/owrx/feature.py
ADD https://raw.githubusercontent.com/ars-ka0s/openwebrx/eesdr/owrx/source/eesdr.py /opt/openwebrx/owrx/source/eesdr.py
RUN pip install eesdr-owrx-connector
```
which can also be used with `docker compose` by changing the `image: ...` line to `build: ./`