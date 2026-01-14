# Adaptive Publisher
A publisher for Gnosis with an early filtering system that in the future could listen for adaptation plans from the Adaptation Engine in Gnosis. Additionally it also uses the internal protocol of message communication from Gnosis


# Installation

## Add Models
The binary classifier models specific for the sub-dataset being used should be placed into `./data/models`

## Configure .env
Copy the `example.env` file to `.env`, and inside the necessary variables.
Some details on a few of the configurations:

REDIS_ADDRESS=172.17.0.1
REDIS_PORT=6379
* **REDIS_ADDRESS/PORT**: Address and port of the Gnosis MEP redis server to send the events to.
* **TRACER_REPORTING_HOST/PORT**: Address and port of the Jaeger event tracing.
* **REGISTER_EVAL_DATA**: if `True` will save a evaluation json detail in `./data/evals` after finishing processing all the dataset (or if manually terminating the process)
* **DEFAULT_OI_LIST**: list of comma separated OIs, eg: `car,person`
* **DEFAULT_CACHED_FRAME_RATE_MULTIPLIER**: multiplier applied to the calculte number of cached frames, eg: 1.0
* **DEFAULT_<PIPELINE_STEP>_THRESHOLD**: sets the thresholds for each pipeline step, (DIFF, CLS, and OBJ). Note that the classifier step has two thresholds: upper and lower, for example `0.3,0.6`
* **EVENT_GENERATOR_TYPE**: Type of event generator, OCVEventGenerator uses OpenCV to read a webcam or MP4 database file. LocalOCVEventGenerator can be used to read list of image frames from a directory (or url)
* **PUBLISHER_INPUT_SOURCE**: location to the input source, if using webcam and OCVEventGenerator, set the id of the webcam (i.e., "0"). If it's a local mp4 file and using OCVEventGenerator, then set the absolute path to the MP4 file (e.g., /home/my_user/Adaptive_publisher/data/TS-D-B-2.mp4). If using a list of image frames, then pass the location of the directory containing the frames (e.g., /home/my_user/Adaptive_publisher/data/Frames/TS-D-B-2), also accepts a url (e.g., if hosting the frames directory usign "`python2 -m SimpleHTTPServer`" on another machine)
* **LISTEN_EVENT_TYPE_EARLY_FILTERING_UPDATED**: this should be ignored for now, since the publisher still not integrated with the rest of the adaptive engine in the Gnosis MEP framework.

## Installing Dependencies

### On regular PC
*This was tested with python 3.8*

To install from the `requirements.txt` file, run the following command:
```
$ pip install -r requirements.txt
```
### On Raspberry PI
*This was tested with python 3.9*

To install from the `requirements-rpi.txt` file, run the following command:
```
$ pip install -r requirements-rpi.txt
```

# Running
First of, you should have Redis and Jaeguer services, running. This can be done by using docker with: `docker-compose up -d redis jaeger`

Enter project python environment (virtualenv or conda environment)

**ps**: It's required to have the .env variables loaded into the shell so that the project can run properly. This can be done using the `source load_env.sh` command inside your preferable python environment (eg: conda).

Then, run the service with:
```
$ ./adaptive_publisher/run.py
```

# Testing
Run the script `run_tests.sh`, it will run all tests defined in the **tests** and **non_mocked_tests** directory.
Run the script `perf_run_tests.sh`, it will run all tests defined in the **perf_test** directory (used to evaluate the performance of the transformers).


# Docker
## Manual Build
Build the docker image using: `docker-compose build`

**ps**: It's required to have the .env variables loaded into the shell so that the container can build properly. An easy way of doing this is using `pipenv shell` to start the python environment with the `.env` file loaded or using the `source load_env.sh` command inside your preferable python environment (eg: conda).

## Env Configs
When running on docker you should set the `REDIS_ADDRESS` to `redis` and `TRACER_REPORTING_HOST` to `jaeger`.

Also, the local path for datasets now should be from the perspective of inside the docker container.
So instead of having `PUBLISHER_INPUT_SOURCE` = `/home/my_user/Adaptive_publisher/data/TS-D-B-2.mp4`, you should use instead `/service/data/TS-D-B-2.mp4`

## Run
Use `docker-compose run --rm service` to run the docker image

## Accessing Event Traces
You can check the event traces at the Jaeger instance on http://localhost:16686/, then filtering by the service AdaptivePublisher. The operation `early_filtering` is the operation that represents the early filtering processing, and by opening an event trace and going to this operation, one can click to expand it to see the `filter_exit_on` tag, wich is there to indicate at which stage the early filtering exited (i.e., `cached`, `diff`, `cls` or `obj`).


# License
This projet is distributed under the AGPL license, see License file for more details.

# Instructions
1. Open docker desktop
2. Rename `example.env` to `.env`
3. put video and model files in `data/`
4. run `docker-compose build`
5. run `docker-compose up -d`
6. run `docker-compose exec service python adaptive_publisher/send_msgs_test.py`
