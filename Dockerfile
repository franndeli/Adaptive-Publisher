FROM python:3.8

RUN apt-get update \
    && apt-get install -y \
        libgl1-mesa-glx \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /service
RUN wget -O yolov5n.pt https://github.com/ultralytics/yolov5/releases/download/v7.0/yolov5n.pt

RUN mkdir -p /torchhome/hub
ENV TORCH_HOME=/torchhome
RUN git clone --branch v6.2 --depth 1 https://github.com/ultralytics/yolov5.git /torchhome/hub/ultralytics_yolov5_master && \
    rm -r /torchhome/hub/ultralytics_yolov5_master/.git


ADD ./requirements.txt /service/requirements.txt
ADD ./setup.py /service/setup.py
RUN mkdir -p /service/adaptive_publisher/ && \
    touch /service/adaptive_publisher/__init__.py

RUN pip install -r requirements.txt && \
    pip install -r /torchhome/hub/ultralytics_yolov5_master/requirements.txt && \
    rm -rf /tmp/pip* /root/.cached


# /root/.cache/torch/hub/
## add all the rest of the code and install the actual package
## this should keep the cached layer above if no change to the pipfile or setup.py was done.
ADD . /service
RUN pip install -e . && \
    rm -rf /tmp/pip* /root/.cache
