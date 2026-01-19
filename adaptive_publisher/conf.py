import os

from decouple import config, Csv

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SOURCE_DIR)

REDIS_ADDRESS = config('REDIS_ADDRESS', default='localhost')
REDIS_PORT = config('REDIS_PORT', default='6379')
REDIS_EXPIRATION_TIME = config('REDIS_EXPIRATION_TIME', default=30)

TRACER_REPORTING_HOST = config('TRACER_REPORTING_HOST', default='localhost')
TRACER_REPORTING_PORT = config('TRACER_REPORTING_PORT', default='6831')

SERVICE_STREAM_KEY = config('SERVICE_STREAM_KEY')

PUBLISHER_INPUT_SOURCE = config('PUBLISHER_INPUT_SOURCE')
PUBLISHER_ID = config('PUBLISHER_ID', default=os.path.basename(PUBLISHER_INPUT_SOURCE))
PUBLISHER_FPS = config('PUBLISHER_FPS', cast=float)

PUBLISHER_RESOLUTION = config('PUBLISHER_RESOLUTION')
PUBLISHER_WIDTH, PUBLISHER_HEIGHT = [int(v) for v in PUBLISHER_RESOLUTION.split('x')]

EVENT_GENERATOR_TYPE = config('EVENT_GENERATOR_TYPE', default='LocalOCVEventGenerator')
EARLY_FILTERING_PIPELINE_NAME =  config('EARLY_FILTERING_PIPELINE_NAME', default='ModelPipeline')

USE_OCV_TRANSFORMS = config('USE_OCV_TRANSFORMS', default=True, cast=bool)

DEFAULT_TARGET_FPS = config('DEFAULT_TARGET_FPS', default=PUBLISHER_FPS, cast=float)
DEFAULT_CACHED_FRAME_RATE_MULTIPLIER = config('DEFAULT_CACHED_FRAME_RATE_MULTIPLIER', default=1.0, cast=float)
DEFAULT_OI_LIST = config('DEFAULT_OI_LIST', default='person', cast=Csv())

DEFAULT_DIFF_THRESHOLD =  config('DEFAULT_DIFF_THRESHOLD', default=0.05, cast=float)
DEFAULT_CLS_THRESHOLDS =  config(
    'DEFAULT_CLS_THRESHOLDS', default='0.3, 0.7', cast=lambda x: [float(t) for t in x.split(',')])
DEFAULT_OBJ_THRESHOLD =  config('DEFAULT_OBJ_THRESHOLD', default=0.5, cast=float)

DEFAULT_THRESHOLDS = {
    'diff': DEFAULT_DIFF_THRESHOLD,
    'oi_cls': DEFAULT_CLS_THRESHOLDS,
    'oi_obj': DEFAULT_OBJ_THRESHOLD,
}

MODELS_PATH = config('MODELS_PATH', default=os.path.join(PROJECT_ROOT, 'data', 'models'))
EXAMPLE_IMAGES_PATH = config('EXAMPLE_IMAGES_PATH', default=os.path.join(PROJECT_ROOT, 'data', 'example_images'))
CLS_MODEL_ID =  config('CLS_MODEL_ID', default=f'TS-D-Q-1-10S_-300_car_person-bird-dog')
# OBJ_MODEL_NAME = config('OBJ_MODEL_NAME', default=os.path.join(MODELS_PATH, 'yolov5n'))
OBJ_MODEL_NAME = config('OBJ_MODEL_NAME', default='yolov5n')

TEMP_IMG_PATH = os.path.join(PROJECT_ROOT, 'data', 'eval', f'tmp.png')

EVAL_ID = f'{PUBLISHER_ID}-{int(PUBLISHER_FPS)}-{PUBLISHER_RESOLUTION}-{EARLY_FILTERING_PIPELINE_NAME}'
EVAL_ID += f'-{"cv" if USE_OCV_TRANSFORMS else "torch"}'
EVAL_ID += f'-{int(DEFAULT_TARGET_FPS)}-{"_".join(DEFAULT_OI_LIST)}-{int(DEFAULT_CACHED_FRAME_RATE_MULTIPLIER*100)}'
EVAL_ID += f'-diff_{int(DEFAULT_DIFF_THRESHOLD*100)}'
EVAL_ID += f'-cls_{CLS_MODEL_ID}_{"_".join([str(int(t*100)) for t in DEFAULT_CLS_THRESHOLDS])}'
EVAL_ID += f'-obj_{OBJ_MODEL_NAME}_{int(DEFAULT_OBJ_THRESHOLD*100)}'

TMP_EXP_EVAL_DATA_JSON_PATH = os.path.join(PROJECT_ROOT, 'data', 'eval', f'{EVAL_ID}.json')
TMP_IGNORE_N_FRAMES = config('TMP_IGNORE_N_FRAMES', default=300, cast=int)

REGISTER_EVAL_DATA = config('REGISTER_EVAL_DATA', default=True, cast=bool)

def safe_int_cast(value):
    """Safely cast to int, handling empty strings"""
    if value == '' or value is None:
        return 1
    return int(value)

REDUCE_SCALE = config('REDUCE_SCALE', default=1, cast=safe_int_cast)

IGNORE_SEND_IMAGE = config('IGNORE_SEND_IMAGE', default=False, cast=bool)

LISTEN_EVENT_TYPE_EARLY_FILTERING_UPDATED = config('LISTEN_EVENT_TYPE_EARLY_FILTERING_UPDATED')
LISTEN_EVENT_TYPE_QUERY_CREATED = config('LISTEN_EVENT_TYPE_QUERY_CREATED')
LISTEN_EVENT_TYPE_QUERY_REMOVED = config('LISTEN_EVENT_TYPE_QUERY_REMOVED')

# LISTEN_EVENT_TYPE_OTHER_EVENT_TYPE = config('LISTEN_EVENT_TYPE_OTHER_EVENT_TYPE')

SERVICE_CMD_KEY_LIST = [
    LISTEN_EVENT_TYPE_EARLY_FILTERING_UPDATED,
    LISTEN_EVENT_TYPE_QUERY_CREATED,
    LISTEN_EVENT_TYPE_QUERY_REMOVED,
]

PUB_EVENT_TYPE_PUBLISHER_CREATED = config('PUB_EVENT_TYPE_PUBLISHER_CREATED')
PUB_EVENT_TYPE_PUBLISHER_REMOVED = config('PUB_EVENT_TYPE_PUBLISHER_REMOVED')
PUB_EVENT_LIST = [
    PUB_EVENT_TYPE_PUBLISHER_CREATED,
    PUB_EVENT_TYPE_PUBLISHER_REMOVED,
]

# Only for Content Extraction services
SERVICE_DETAILS = None

# Example of how to define SERVICE_DETAILS from env vars:
# SERVICE_DETAILS_SERVICE_TYPE = config('SERVICE_DETAILS_SERVICE_TYPE')
# SERVICE_DETAILS_STREAM_KEY = config('SERVICE_DETAILS_STREAM_KEY')
# SERVICE_DETAILS_QUEUE_LIMIT = config('SERVICE_DETAILS_QUEUE_LIMIT', cast=int)
# SERVICE_DETAILS_THROUGHPUT = config('SERVICE_DETAILS_THROUGHPUT', cast=float)
# SERVICE_DETAILS_ACCURACY = config('SERVICE_DETAILS_ACCURACY', cast=float)
# SERVICE_DETAILS_ENERGY_CONSUMPTION = config('SERVICE_DETAILS_ENERGY_CONSUMPTION', cast=float)
# SERVICE_DETAILS_CONTENT_TYPES = config('SERVICE_DETAILS_CONTENT_TYPES', cast=Csv())
# SERVICE_DETAILS = {
#     'service_type': SERVICE_DETAILS_SERVICE_TYPE,
#     'stream_key': SERVICE_DETAILS_STREAM_KEY,
#     'queue_limit': SERVICE_DETAILS_QUEUE_LIMIT,
#     'throughput': SERVICE_DETAILS_THROUGHPUT,
#     'accuracy': SERVICE_DETAILS_ACCURACY,
#     'energy_consumption': SERVICE_DETAILS_ENERGY_CONSUMPTION,
#     'content_types': SERVICE_DETAILS_CONTENT_TYPES
# }

LOGGING_LEVEL = config('LOGGING_LEVEL', default='DEBUG')

# Micro-batching configuration
USE_MICRO_BATCHING = config('USE_MICRO_BATCHING', default=True, cast=bool)
BATCH_SIZE = config('BATCH_SIZE', default=3, cast=int)
BATCH_TIMEOUT = config('BATCH_TIMEOUT', default=0.5, cast=float)  # seconds

# Adaptive batching configuration
USE_ADAPTIVE_BATCHING = config('USE_ADAPTIVE_BATCHING', default=False, cast=bool)
ADAPTIVE_MIN_BATCH_SIZE = config('ADAPTIVE_MIN_BATCH_SIZE', default=1, cast=int)
ADAPTIVE_MAX_BATCH_SIZE = config('ADAPTIVE_MAX_BATCH_SIZE', default=10, cast=int)
ADAPTIVE_INITIAL_BATCH_SIZE = config('ADAPTIVE_INITIAL_BATCH_SIZE', default=3, cast=int)
ADAPTIVE_TARGET_BATCH_TIME_MS = config('ADAPTIVE_TARGET_BATCH_TIME_MS', default=150.0, cast=float)

# Experiment configuration
EXPERIMENT_NUM_FRAMES = config('EXPERIMENT_NUM_FRAMES', default=0, cast=int)  # 0 = no limit
COLLECT_EXPERIMENT_METRICS = config('COLLECT_EXPERIMENT_METRICS', default=True, cast=bool)
EXPERIMENT_OUTPUT_DIR = config('EXPERIMENT_OUTPUT_DIR', default=os.path.join(PROJECT_ROOT, 'data', 'experiment_results'))

# Image compression configuration
# Options: 'none' (baseline), 'lz4', 'qoi'
IMAGE_COMPRESSION_MODE = config('IMAGE_COMPRESSION_MODE', default='none')

# LZ4 compression settings
LZ4_COMPRESSION_LEVEL = config('LZ4_COMPRESSION_LEVEL', default=0, cast=int)  # 0=fastest, higher=better compression