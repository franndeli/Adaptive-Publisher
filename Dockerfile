FROM python:3.8

RUN apt-get update && apt-get install -y \
    libgl1-mesa-glx \
    wget \
    git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /service

# copy requirements early for caching
ADD ./requirements.txt /service/requirements.txt
ADD ./setup.py /service/setup.py

# IMPORTANT: stub package dir so "-e file:./" in requirements.txt doesn't fail during egg_info
RUN mkdir -p /service/adaptive_publisher && \
    touch /service/adaptive_publisher/__init__.py

# YOLOv5 assets
RUN wget -O yolov5n.pt https://github.com/ultralytics/yolov5/releases/download/v7.0/yolov5n.pt

RUN mkdir -p /torchhome/hub
ENV TORCH_HOME=/torchhome

# Pin YOLOv5 to v6.2 to avoid ultralytics==8 hubconf imports
RUN git clone --branch v6.2 --depth 1 https://github.com/ultralytics/yolov5.git /torchhome/hub/ultralytics_yolov5_master && \
    rm -rf /torchhome/hub/ultralytics_yolov5_master/.git

# install imagecodecs wheel + verify jpegxl/jpegls exist
RUN pip install --upgrade "pip<25" setuptools wheel && \
    pip install --no-cache-dir --only-binary=:all: imagecodecs==2022.12.24 && \
    python -c "import imagecodecs; \
               assert hasattr(imagecodecs,'jpegxl_encode'), 'no jpegxl_encode'; \
               assert hasattr(imagecodecs,'jpegls_encode'), 'no jpegls_encode'; \
               print('OK: jpegxl + jpegls available')" && \
    rm -rf /tmp/pip* /root/.cache/pip

# install python deps (includes torch==2.0.1+cpu via -f torch_stable.html)
RUN pip install --no-cache-dir -r /service/requirements.txt && \
    pip install --no-cache-dir -r /torchhome/hub/ultralytics_yolov5_master/requirements.txt && \
    rm -rf /tmp/pip* /root/.cache/pip

# now copy the rest of the source (this is when adaptive_publisher actually exists)
ADD . /service

# editable install of your local package (replaces stub)
RUN pip install -e . && rm -rf /tmp/pip* /root/.cache/pip
