#!/usr/bin/env python
import numpy as np
import time
from opentracing.propagation import Format
from opentracing.ext import tags
from event_service_utils.streams.redis import RedisStreamFactory
from event_service_utils.img_serialization.redis import RedisImageCache

from adaptive_publisher.event_publishers.publisher import EventPublisher
from adaptive_publisher.conf import REDIS_ADDRESS, REDIS_PORT

class MockService:
    def __init__(self):
        self.stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
        self.file_storage_cli = RedisImageCache()
        self.file_storage_cli.file_storage_cli_config = {
            'host': REDIS_ADDRESS,
            'port': REDIS_PORT,
            'db': 0,
        }
        self.file_storage_cli.expiration_time = 30
        self.file_storage_cli.initialize_file_storage_client()
        
        from event_service_utils.tracing.jaeger import init_tracer
        self.tracer = init_tracer('TestPublisher', 
                                 reporting_host='localhost', 
                                 reporting_port='6831')
        
        import logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('TestService')
    
    def write_event_with_trace(self, event_data, stream):
        with self.tracer.start_active_span('write_to_redis_stream') as scope:
            scope.span.set_tag('event_id', event_data['id'])
            scope.span.set_tag('stream_key', 'test_buffer_stream')
            try:
                stream.write_events([{'event': str(event_data)}])
                self.logger.info(f"✓ Event written: {event_data['id'][:30]}...")
            except Exception as e:
                scope.span.set_tag('error', True)
                self.logger.error(f"Error writing event: {e}")

def test_multiple_frames():
    service = MockService()
    
    publisher_details = {
        'publisher_id': 'test_publisher',
        'source': 'gnosis://test_publisher/test_source',
        'meta': {
            'color': 'True',
            'fps': '10',
            'resolution': '640x480'
        }
    }
    
    publisher = EventPublisher(
        parent_service=service,
        publisher_details=publisher_details,
        query_ids=['test_query_1'],
        buffer_stream_key='test_buffer_stream'
    )
    
    # Sending multiple frames with tracing
    for i in range(5):
        with service.tracer.start_active_span(f'process_frame_{i}') as scope:
            scope.span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_PRODUCER)
            scope.span.set_tag('frame_index', i)
            scope.span.set_tag('frame_resolution', '640x480')
            
            color_value = i * 50
            frame = np.full((480, 640, 3), color_value, dtype=np.uint8)
            scope.span.set_tag('frame_color', color_value)
            
            tracer_headers = {}
            service.tracer.inject(scope.span, Format.HTTP_HEADERS, tracer_headers)
            trace_id = tracer_headers.get('uber-trace-id', 'no-trace-id')
            
            start = time.time()
            
            with service.tracer.start_active_span('generate_event', child_of=scope.span) as gen_scope:
                event_data = publisher.generate_event_from_frame(frame, i)
                gen_scope.span.set_tag('event_id', event_data['id'])
                gen_scope.span.set_tag('image_key', event_data['image_url'])
            
            elapsed = time.time() - start
            
            scope.span.set_tag('processing_time_ms', round(elapsed * 1000, 2))
            
            print(f"\n✅ Frame {i}:")
            print(f"   - Event ID: {event_data['id'][:40]}...")
            print(f"   - Image Key: {event_data['image_url']}")
            print(f"   - Trace ID: {trace_id[:40]}...")
            print(f"   - Processing: {elapsed:.4f}s ({elapsed*1000:.2f}ms)")
        
        time.sleep(0.1)  # Simulate some delay between frames

    
    time.sleep(3)
    
    service.tracer.close()
    time.sleep(1)

if __name__ == '__main__':
    test_multiple_frames()