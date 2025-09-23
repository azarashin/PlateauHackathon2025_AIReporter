FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONPATH="/usr/lib/python3/dist-packages:$PYTHONPATH"

# 基本ツールとGDAL依存パッケージのインストール
RUN apt update && apt install -y \
  gdal-bin libgdal-dev \
  build-essential git curl cmake \
  libsqlite3-dev libgeos-dev libproj-dev libtiff-dev libjpeg-dev \
  libpng-dev libgif-dev libwebp-dev libopenjp2-7-dev \
  liblzma-dev libzstd-dev libxml2-dev libexpat1-dev \
  libcurl4-openssl-dev libfreexl-dev libspatialite-dev \
  libhdf5-dev libnetcdf-dev libpoppler-dev libpcre3-dev \
  python3.12 python3.12-dev python3-pip \
  && rm -rf /var/lib/apt/lists/*

# GDALソースの取得とビルド
RUN git clone https://github.com/OSGeo/gdal.git /opt/gdal \
  && cd /opt/gdal \
  && git checkout v3.11.4 \
  && mkdir build && cd build \
  && cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
  && make -j$(nproc) \
  && make install

# 特定のバージョンのnumpy を入れるため、既存のnumpy をいったん消す
RUN apt remove python3-numpy -y

COPY ./requirements.txt .

RUN pip install --upgrade --break-system-packages -r requirements.txt

# GDALのバージョン確認用
RUN gdal-config --version

CMD ["python3", "-m", "Viewer.app"]
