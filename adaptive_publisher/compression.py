"""
Lossless image compression utilities for the Adaptive Publisher.

Supports:
- LZ4: Fast compression with minimal CPU overhead
- QOI: "Quite OK Image" format - fast lossless image compression

Usage:
    Set IMAGE_COMPRESSION_MODE environment variable to:
    - 'none' or 'baseline': No compression (raw frame upload)
    - 'lz4': LZ4 frame compression
    - 'qoi': QOI image compression
"""

import time
import uuid
from typing import Tuple, Optional, Any
import numpy as np

# Try to import compression libraries
try:
    import lz4.frame as lz4
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False
    lz4 = None

try:
    import qoi
    HAS_QOI = True
except ImportError:
    HAS_QOI = False
    qoi = None


class CompressionError(Exception):
    """Raised when compression fails."""
    pass


class ImageCompressor:
    """
    Handles lossless image compression for frame data.
    
    Attributes:
        mode: Compression mode ('none', 'lz4', 'qoi')
        lz4_compression_level: LZ4 compression level (0=fastest)
    """
    
    def __init__(
        self, 
        mode: str = 'none',
        lz4_compression_level: int = 0,
        logger: Any = None
    ):
        """
        Initialize the compressor.
        
        Args:
            mode: Compression mode ('none', 'lz4', 'qoi')
            lz4_compression_level: LZ4 compression level (0=min/fastest, higher=better)
            logger: Optional logger instance
        """
        self.mode = mode.lower().strip()
        self.lz4_compression_level = lz4_compression_level
        self.logger = logger
        
        # Validate mode and availability
        if self.mode == 'lz4' and not HAS_LZ4:
            raise CompressionError("LZ4 compression requested but lz4 library not installed. Run: pip install lz4")
        if self.mode == 'qoi' and not HAS_QOI:
            raise CompressionError("QOI compression requested but qoi library not installed. Run: pip install qoi")
        
        # Statistics tracking
        self._encode_times = []
        self._compression_ratios = []
        
    def compress(self, frame: np.ndarray) -> Tuple[bytes, dict]:
        """
        Compress a frame using the configured compression mode.
        
        Args:
            frame: NumPy array representing the image (BGR format from OpenCV)
            
        Returns:
            Tuple of (compressed_bytes, metadata_dict)
            metadata_dict contains: mode, encode_time_ms, original_size, compressed_size, ratio
        """
        original_size = frame.nbytes
        
        if self.mode in ('none', 'baseline', ''):
            # No compression - return raw bytes
            t0 = time.perf_counter()
            data = frame.tobytes()
            encode_time = (time.perf_counter() - t0) * 1000
            
            return data, {
                'mode': 'none',
                'encode_time_ms': encode_time,
                'original_size': original_size,
                'compressed_size': len(data),
                'compression_ratio': 1.0,
            }
        
        elif self.mode == 'lz4':
            return self._compress_lz4(frame, original_size)
        
        elif self.mode == 'qoi':
            return self._compress_qoi(frame, original_size)
        
        else:
            raise CompressionError(f"Unknown compression mode: {self.mode}")
    
    def _compress_lz4(self, frame: np.ndarray, original_size: int) -> Tuple[bytes, dict]:
        """Compress frame using LZ4 (same config as colleagues' code)."""
        t0 = time.perf_counter()
        
        # LZ4 compress using EXACT same parameters as colleagues:
        # - block_size=lz4.BLOCKSIZE_MAX4MB (4MB blocks)
        # - compression_level=lz4.COMPRESSIONLEVEL_MIN (fastest/minimum compression)
        compressed = lz4.compress(
            frame.tobytes(),
            block_size=lz4.BLOCKSIZE_MAX4MB,
            compression_level=lz4.COMPRESSIONLEVEL_MIN,
        )
        
        encode_time = (time.perf_counter() - t0) * 1000
        compressed_size = len(compressed)
        ratio = original_size / compressed_size if compressed_size > 0 else 1.0
        
        self._encode_times.append(encode_time)
        self._compression_ratios.append(ratio)
        
        return compressed, {
            'mode': 'lz4',
            'encode_time_ms': encode_time,
            'original_size': original_size,
            'compressed_size': compressed_size,
            'compression_ratio': ratio,
        }
    
    def _compress_qoi(self, frame: np.ndarray, original_size: int) -> Tuple[bytes, dict]:
        """Compress frame using QOI format."""
        t0 = time.perf_counter()
        
        # QOI expects RGB, OpenCV uses BGR - convert
        # frame_rgb = frame[:, :, ::-1]  # BGR to RGB
        # QOI encode - it handles the conversion internally for BGR
        compressed = qoi.encode(frame)
        
        encode_time = (time.perf_counter() - t0) * 1000
        compressed_size = len(compressed)
        ratio = original_size / compressed_size if compressed_size > 0 else 1.0
        
        self._encode_times.append(encode_time)
        self._compression_ratios.append(ratio)
        
        return compressed, {
            'mode': 'qoi',
            'encode_time_ms': encode_time,
            'original_size': original_size,
            'compressed_size': compressed_size,
            'compression_ratio': ratio,
        }
    
    def get_stats(self) -> dict:
        """Get compression statistics."""
        if not self._encode_times:
            return {
                'mode': self.mode,
                'total_frames': 0,
                'avg_encode_time_ms': 0,
                'avg_compression_ratio': 1.0,
            }
        
        return {
            'mode': self.mode,
            'total_frames': len(self._encode_times),
            'avg_encode_time_ms': sum(self._encode_times) / len(self._encode_times),
            'min_encode_time_ms': min(self._encode_times),
            'max_encode_time_ms': max(self._encode_times),
            'avg_compression_ratio': sum(self._compression_ratios) / len(self._compression_ratios),
            'min_compression_ratio': min(self._compression_ratios),
            'max_compression_ratio': max(self._compression_ratios),
        }


def upload_compressed_frame(
    file_storage_cli,
    frame: np.ndarray,
    compressor: Optional[ImageCompressor] = None,
    tracer: Any = None,
    frame_index: int = 0,
) -> Tuple[str, dict]:
    """
    Compress and upload a frame to storage.
    
    Args:
        file_storage_cli: File storage client with upload capability
        frame: NumPy array of the image
        compressor: Optional ImageCompressor instance (None = no compression)
        tracer: Optional tracer for span creation
        frame_index: Frame index for tracing
        
    Returns:
        Tuple of (image_uri, compression_metadata)
    """
    span = None
    if tracer:
        span = tracer.start_span("compress_and_upload_frame")
        span.set_tag("frame.index", frame_index)
    
    try:
        if compressor is None or compressor.mode in ('none', 'baseline', ''):
            # No compression - use standard upload
            t0 = time.perf_counter()
            img_uri = file_storage_cli.upload_inmemory_to_storage(frame)
            upload_time = (time.perf_counter() - t0) * 1000
            
            metadata = {
                'mode': 'none',
                'encode_time_ms': 0,
                'upload_time_ms': upload_time,
                'original_size': frame.nbytes,
                'compressed_size': frame.nbytes,
                'compression_ratio': 1.0,
            }
            
            if span:
                span.set_tag("compression.mode", "none")
                span.set_tag("upload.ms", upload_time)
                span.set_tag("payload.bytes", frame.nbytes)
            
            return img_uri, metadata
        
        # Compress the frame
        compressed_data, compress_meta = compressor.compress(frame)
        
        if span:
            span.set_tag("compression.mode", compressor.mode)
            span.set_tag("encode.ms", compress_meta['encode_time_ms'])
            span.set_tag("compression.ratio", compress_meta['compression_ratio'])
            span.set_tag("original.bytes", compress_meta['original_size'])
            span.set_tag("compressed.bytes", compress_meta['compressed_size'])
        
        # Upload compressed data directly to Redis
        t0 = time.perf_counter()
        img_uri = str(uuid.uuid4())
        file_storage_cli.client.set(
            name=img_uri,
            value=compressed_data,
            ex=file_storage_cli.expiration_time,
        )
        upload_time = (time.perf_counter() - t0) * 1000
        
        if span:
            span.set_tag("upload.ms", upload_time)
            span.set_tag("payload.bytes", compress_meta['compressed_size'])
        
        compress_meta['upload_time_ms'] = upload_time
        
        return img_uri, compress_meta
        
    finally:
        if span:
            span.finish()


# Convenience function to create compressor from config
def create_compressor_from_config(logger=None) -> Optional[ImageCompressor]:
    """
    Create an ImageCompressor instance from environment configuration.
    
    Returns:
        ImageCompressor instance or None if compression is disabled
    """
    from adaptive_publisher.conf import IMAGE_COMPRESSION_MODE, LZ4_COMPRESSION_LEVEL
    
    mode = IMAGE_COMPRESSION_MODE
    if mode in ('none', 'baseline', ''):
        return None
    
    return ImageCompressor(
        mode=mode,
        lz4_compression_level=LZ4_COMPRESSION_LEVEL,
        logger=logger,
    )
