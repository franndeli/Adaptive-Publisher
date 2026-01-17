import json
import time
import multiprocessing
import threading
import os

from opentracing.ext import tags
from opentracing.propagation import Format
from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.event_driven import BaseEventDrivenCMDService
from event_service_utils.tracing.jaeger import init_tracer

from adaptive_publisher.event_publishers.publisher import EventPublisher
from adaptive_publisher.event_publishers.batched_publisher import MicroBatchingEventPublisher

from adaptive_publisher.conf import (
    LISTEN_EVENT_TYPE_EARLY_FILTERING_UPDATED,
    LISTEN_EVENT_TYPE_QUERY_CREATED,
    LISTEN_EVENT_TYPE_QUERY_REMOVED,
    PUB_EVENT_TYPE_PUBLISHER_CREATED,
    TMP_EXP_EVAL_DATA_JSON_PATH,
    DEFAULT_THRESHOLDS,
    DEFAULT_TARGET_FPS,
    IGNORE_SEND_IMAGE,
    USE_MICRO_BATCHING,
    BATCH_SIZE,
    BATCH_TIMEOUT,
    COLLECT_EXPERIMENT_METRICS,
    EXPERIMENT_OUTPUT_DIR,
    EXPERIMENT_NUM_FRAMES,
    PROJECT_ROOT,
)
from adaptive_publisher.event_generators import OCVEventGenerator, LocalOCVEventGenerator, MockedEventGenerator

# Import metrics collector if available
try:
    from experiments.metrics_collector import MetricsCollector, ExperimentConfig
    HAS_METRICS_COLLECTOR = True
except ImportError:
    HAS_METRICS_COLLECTOR = False
    MetricsCollector = None
    ExperimentConfig = None

class AdaptivePublisher(BaseEventDrivenCMDService):
    def __init__(self,
                 service_stream_key, service_cmd_key_list,
                 pub_event_list, service_details,
                 stream_factory,
                 file_storage_cli,
                 publisher_configs,
                 event_generator_type,
                 early_filtering_pipeline_name,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(AdaptivePublisher, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key_list=service_cmd_key_list,
            pub_event_list=pub_event_list,
            service_details=service_details,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.cmd_validation_fields = ['id']
        self.data_validation_fields = ['id']
        self.event_generator_type = event_generator_type
        self.available_event_generators = {
            'MockedEventGenerator': MockedEventGenerator,
            'OCVEventGenerator': OCVEventGenerator,
            'LocalOCVEventGenerator': LocalOCVEventGenerator,
        }
        self.early_filtering_pipeline_name = early_filtering_pipeline_name
        self.event_generator = None
        self.bufferstream_dict = {}
        self.early_filtering_rules = {
            'pipeline': self.early_filtering_pipeline_name,
            'thresholds': DEFAULT_THRESHOLDS,
            'target_fps': DEFAULT_TARGET_FPS,
        }
        self.file_storage_cli = file_storage_cli
        self.publisher_configs = publisher_configs
        self.setup_event_generator()
        self.publisher_parent_conn = None
        self.publisher_child_conn = None
        self.publisher = None
        
        # Initialize metrics collector for experiments
        self.metrics_collector = None
        self._setup_metrics_collector()
    
    def _setup_metrics_collector(self):
        """Initialize metrics collector if experiment mode is enabled."""
        if COLLECT_EXPERIMENT_METRICS and HAS_METRICS_COLLECTOR:
            config = ExperimentConfig(
                batch_size=BATCH_SIZE if USE_MICRO_BATCHING else 1,
                batch_timeout=BATCH_TIMEOUT if USE_MICRO_BATCHING else 0,
                num_frames=EXPERIMENT_NUM_FRAMES,
                fps=self.publisher_configs.get('fps', DEFAULT_TARGET_FPS),
                resolution=f"{self.publisher_configs.get('width', 1920)}x{self.publisher_configs.get('height', 1080)}",
                publisher_id=self.publisher_configs.get('id', 'unknown'),
                source=self.publisher_configs.get('input_source', 'unknown'),
                use_micro_batching=USE_MICRO_BATCHING
            )
            self.metrics_collector = MetricsCollector(config)
            self.logger.info(f'📊 Metrics collector initialized (batch_size={config.batch_size})')

    def setup_event_generator(self):
        self.event_generator = self.available_event_generators[self.event_generator_type](
            self,
            self.early_filtering_pipeline_name,
            self.publisher_configs['id'],
            self.publisher_configs['input_source'],
            self.early_filtering_rules['target_fps'],
            self.publisher_configs['width'],
            self.publisher_configs['height'],
            self.early_filtering_rules['thresholds']
        )
        self.event_generator.setup()

    def experiment_temporary_exit_data_gathering(self):
        "adding this method just to double check the results and have them saved for later"
        with open(TMP_EXP_EVAL_DATA_JSON_PATH, 'w') as f:
            json.dump(self.event_generator._get_experiment_eval_data(), f, indent=4)

    def process_data(self):
        self.logger.debug('Processing DATA..')
        buffer_stream_key_list = self.bufferstream_dict.keys()
        no_bufferstreams = len(buffer_stream_key_list) == 0
        generator_not_open = not self.event_generator.is_open()

        ignore_publishing = no_bufferstreams or generator_not_open
        if ignore_publishing:
            time.sleep(0.05)
            return
        event_data = None
        buffer_stream_key_list = None

        try:
            buffer_stream_key_list = self.bufferstream_dict.keys()
            if len(buffer_stream_key_list) > 0:
                with self.tracer.start_active_span('process_next_frame', child_of=None) as scope:
                    tracer_tags = {
                        tags.SPAN_KIND: tags.SPAN_KIND_PRODUCER,
                    }
                    for tag, value in tracer_tags.items():
                        scope.span.set_tag(tag, value)

                    init_time = time.perf_counter()

                    # --- READ FRAME (measure + tag) ---
                    t_read0 = time.perf_counter()
                    frame = self.event_generator.read_next_frame_or_drop()
                    read_ms = (time.perf_counter() - t_read0) * 1000

                    frame_idx = self.event_generator.current_frame_index
                    scope.span.set_tag("frame_index", frame_idx)
                    scope.span.set_tag("read_ms", read_ms)

                    self.logger.debug(f"[DATA] read idx={frame_idx} read_ms={read_ms:.2f} frame_none={frame is None}")

                    if frame is not None and not IGNORE_SEND_IMAGE:
                        # Record frame read timestamp for latency measurement
                        frame_read_ts = t_read0
                        if self.publisher and hasattr(self.publisher, 'record_frame_read'):
                            self.publisher.record_frame_read(frame_idx, frame_read_ts)
                        
                        # --- TRACE ID FOR THIS FRAME ---
                        tracer_headers = {}
                        self.tracer.inject(scope.span, Format.HTTP_HEADERS, tracer_headers)
                        trace_id = tracer_headers["uber-trace-id"]
                        scope.span.set_tag("trace_id", trace_id)

                        # --- DIRECT HANDOFF (simplified, no threading) ---
                        handoff_ts = time.perf_counter()
                        scope.span.set_tag("handoff_ts", handoff_ts)
                        
                        # Store frame data for publisher.run() to pick up
                        self.publisher.frame_data = (frame, frame_idx, trace_id, handoff_ts)
                        self.publisher.frame_ready = True

                        self.logger.debug(f"[DATA] handoff idx={frame_idx} trace_id={trace_id}")

                    else:
                        self.logger.info("Event filtered (frame is None) or IGNORE_SEND_IMAGE=True")

                    # --- FPS PACING (make it visible in Jaeger) ---
                    current_time = time.perf_counter()
                    elapsed_time = current_time - init_time
                    sleep_time = max(0, self.event_generator.frame_delay - elapsed_time)

                    scope.span.set_tag("frame_delay_s", self.event_generator.frame_delay)
                    scope.span.set_tag("sleep_time_s", sleep_time)

                    with self.tracer.start_active_span("frame_pacing_sleep", child_of=scope.span) as ps:
                        ps.span.set_tag("sleep_time_s", sleep_time)
                        time.sleep(sleep_time)


        except KeyboardInterrupt as ke:
            self.event_generator.close()
            raise ke
        except Exception as e:
            self.logger.error(f'Error processing event_data "{event_data}", while sending to buffer streams: "{buffer_stream_key_list}"')
            self.logger.exception(e)
        finally:
            pass
        if not self.event_generator.is_open():
            raise KeyboardInterrupt()

    def process_early_filtering_updated(self, event_data):
        # if is early filtering for this buffer streams, than do something, otherwise, ignore.
        # but, not connected yet with the adaptation engine
        pass

    def process_query_created(self, event_data):
        buffer_stream = event_data['buffer_stream']
        publisher_id = buffer_stream['publisher_id']
        buffer_stream_key = buffer_stream['buffer_stream_key']
        query_id = event_data['query_id']
        if self.publisher_configs['id'] == publisher_id:
            self.event_generator.add_query_id(query_id)
            self.bufferstream_dict[buffer_stream_key] = {
                'bufferstream': buffer_stream_key,
                'query_ids': [query_id]
            }
            # only one query for now
            # in the future we should change to run the cmd in parallel and add more query_ids to a bufferstream
            # and add more bufferstreams for different queries on this publisher.
            
            # Start metrics collection if enabled
            if self.metrics_collector:
                self.metrics_collector.start_experiment()
                self.logger.info('📊 Experiment metrics collection started')
            
            # Use micro-batching publisher if enabled
            if USE_MICRO_BATCHING:
                self.publisher = MicroBatchingEventPublisher(
                    parent_service=self,
                    publisher_details=self.event_generator.publisher_details,
                    query_ids=[query_id],
                    buffer_stream_key=buffer_stream_key,
                    batch_size=BATCH_SIZE,
                    batch_timeout=BATCH_TIMEOUT,
                    metrics_collector=self.metrics_collector
                )
                self.logger.info(f'Using micro-batching with batch_size={BATCH_SIZE}, batch_timeout={BATCH_TIMEOUT}')
            else:
                self.publisher = EventPublisher(
                    parent_service=self,
                    publisher_details=self.event_generator.publisher_details,
                    query_ids=[query_id],
                    buffer_stream_key=buffer_stream_key,
                    metrics_collector=self.metrics_collector
                )
                self.logger.info('Using standard event publisher (no batching)')

    def process_event_type(self, event_type, event_data, json_msg):
        if not super(AdaptivePublisher, self).process_event_type(event_type, event_data, json_msg):
            return False
        if event_type == LISTEN_EVENT_TYPE_EARLY_FILTERING_UPDATED:
            self.process_early_filtering_updated(event_data=event_data)
        if event_type == LISTEN_EVENT_TYPE_QUERY_CREATED:
            self.process_query_created(event_data=event_data)
        # if event_type == LISTEN_EVENT_TYPE_QUERY_REMOVED:
        #     self.process_query_removed(event_data=event_data)

    def log_state(self):
        super(AdaptivePublisher, self).log_state()
        self.logger.info(f'Service name: {self.name}')
        # function for simple logging of python dictionary
        self._log_dict('Publishing to bufferstreams:', self.bufferstream_dict)
        self._log_dict('Early filtering rules:', self.early_filtering_rules)
        self._log_dict('Processing Times:', self.event_generator.get_stats_dict())

    def publish_publisher_created(self):
        new_event_data = {
            'id': self.service_based_random_event_id(),
        }
        new_event_data.update(self.event_generator.publisher_details)
        self.publish_event_type_to_stream(event_type=PUB_EVENT_TYPE_PUBLISHER_CREATED, new_event_data=new_event_data)

    def _save_experiment_metrics(self):
        """Save experiment metrics to file."""
        self.logger.info('_save_experiment_metrics called')
        
        if not self.metrics_collector:
            self.logger.info('No metrics_collector available - skipping save')
            return
            
        self.logger.info(f'Metrics collector has {len(self.metrics_collector.frame_metrics)} frames')
        self.logger.info(f'Metrics collector has {len(self.metrics_collector.batch_metrics)} batches')
        
        self.metrics_collector.end_experiment()
        
        # Create output directory
        output_dir = EXPERIMENT_OUTPUT_DIR
        self.logger.info(f'Creating output directory: {output_dir}')
        os.makedirs(output_dir, exist_ok=True)
        
        # Save report
        try:
            report_path = self.metrics_collector.save_report()
            self.logger.info(f'📊 Experiment metrics saved to: {report_path}')
        except Exception as e:
            self.logger.error(f'Error saving report: {e}')
            import traceback
            self.logger.error(traceback.format_exc())
            return
        
        # Log summary
        latency_stats = self.metrics_collector.get_latency_stats()
        throughput_stats = self.metrics_collector.get_throughput_stats()
        network_stats = self.metrics_collector.get_network_efficiency_stats()
        
        self.logger.info('='*60)
        self.logger.info('📊 EXPERIMENT RESULTS SUMMARY')
        self.logger.info('='*60)
        self.logger.info(f'  Throughput: {throughput_stats.get("overall_throughput_fps", 0):.2f} FPS')
        self.logger.info(f'  Mean Latency: {latency_stats.get("mean_ms", 0):.2f} ms')
        self.logger.info(f'  p90 Latency: {latency_stats.get("p90_ms", 0):.2f} ms')
        self.logger.info(f'  p99 Latency: {latency_stats.get("p99_ms", 0):.2f} ms')
        self.logger.info(f'  Network Reduction: {network_stats.get("network_reduction_percent", 0):.1f}%')
        self.logger.info(f'  Total Frames: {throughput_stats.get("total_frames", 0)}')
        self.logger.info(f'  Total Batches: {throughput_stats.get("total_batches", 0)}')
        self.logger.info('='*60)

    def run(self):
        super(AdaptivePublisher, self).run()
        self.log_state()
        self.publish_publisher_created()

        try:
            while True:
                self.process_cmd()
                if len(self.bufferstream_dict) != 0:
                    break
        except KeyboardInterrupt:
            self.logger.debug('No query to publish data to, will exit')
            return

        # Simple loop: process data and publish until video ends
        video_finished = False
        frames_processed = 0
        
        try:
            while not video_finished:
                try:
                    # Process one frame
                    self.process_data()
                    frames_processed += 1
                    
                    # Run publisher for this frame
                    self.publisher.run()
                    
                except KeyboardInterrupt:
                    self.logger.info(f'Video ended after {frames_processed} frames')
                    video_finished = True
                    
        except Exception as e:
            self.logger.exception(f'Error during processing: {e}')
        finally:
            self.logger.info('='*60)
            self.logger.info('VIDEO PROCESSING COMPLETE - SAVING METRICS')
            self.logger.info('='*60)
            
            # Flush any remaining batched events
            if self.publisher and hasattr(self.publisher, 'flush'):
                try:
                    self.logger.info('Flushing remaining frames...')
                    self.publisher.flush()
                except Exception as e:
                    self.logger.error(f'Error flushing publisher: {e}')
            
            self.log_state()
            
            try:
                self.experiment_temporary_exit_data_gathering()
            except Exception as e:
                self.logger.error(f'Error saving experiment data: {e}')
            
            # Save experiment metrics
            try:
                self._save_experiment_metrics()
            except Exception as e:
                self.logger.error(f'Error saving experiment metrics: {e}')
            
            self.logger.info('='*60)
            self.logger.info('SHUTDOWN COMPLETE')
            self.logger.info('='*60)