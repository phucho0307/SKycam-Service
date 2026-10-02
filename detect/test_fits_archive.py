"""Unit tests for lossless FITS compression. No database, no S3.

The property under test is the module's one rule: whatever is returned
decompresses to exactly the original pixels and header, and anything that would
not is returned as the original bytes instead.
"""
import io

import numpy as np
import pytest
from astropy.io import fits

from fits_archive import compress_lossless, decompress


def camera_fits(seed: int = 0, size: int = 512, sky: int = 60) -> bytes:
    """Shaped like the camera's frames: uint16 stored as BITPIX 16 + BZERO 32768,
    with the capture header the Pi writes. Night sky: offset, noise, stars."""
    rng = np.random.default_rng(seed)
    img = 800 + rng.poisson(sky, (size, size)) + rng.normal(0, 12, (size, size))
    for _ in range(size // 4):
        y, x = rng.integers(2, size - 2, 2)
        img[y - 1:y + 2, x - 1:x + 2] += rng.uniform(500, 20000)
    data = np.clip(img, 0, 65535).astype(np.uint16)
    hdu = fits.PrimaryHDU(data)
    hdu.header["EXPTIME"] = (1.0, "seconds")
    hdu.header["GAIN"] = 200
    hdu.header["CCD-TEMP"] = -4.5
    hdu.header["DATE-OBS"] = "2026-10-02T03:14:15.926"
    hdu.header["INSTRUME"] = "ZWO ASI676MC"
    buf = io.BytesIO()
    hdu.writeto(buf)
    return buf.getvalue()


def pixels_and_header(raw: bytes):
    with fits.open(io.BytesIO(raw)) as h:
        return np.array(h[0].data), h[0].header.copy()


@pytest.mark.parametrize("algorithm", ["GZIP_2", "RICE_1"])
def test_camera_frame_compresses_and_round_trips_exactly(algorithm):
    raw = camera_fits()
    out = compress_lossless(raw, algorithm)

    assert out.compression == algorithm and out.note is None
    assert len(out.data) < len(raw) / 1.5, f"expected real compression, got {len(raw)/len(out.data):.2f}x"
    original, header = pixels_and_header(raw)
    back, back_header = decompress(out.data)
    assert back.dtype == np.uint16, "BZERO-offset uint16 must come back as uint16"
    assert np.array_equal(back, original)
    for key in ("EXPTIME", "GAIN", "CCD-TEMP", "DATE-OBS", "INSTRUME"):
        assert back_header[key] == header[key], key


def test_not_a_fits_is_kept_as_is():
    junk = bytes(range(256)) * 4096  # the kind of test upload found in the dev bucket
    out = compress_lossless(junk)
    assert out.compression == "none" and out.data == junk
    assert "not a readable FITS" in out.note


def test_floating_point_data_is_kept_as_is():
    # The codecs quantise floats by default, which is lossy. Must not happen
    # silently to a frame someone chose to keep.
    hdu = fits.PrimaryHDU(np.random.default_rng(1).normal(0, 1, (64, 64)).astype(np.float32))
    buf = io.BytesIO()
    hdu.writeto(buf)
    out = compress_lossless(buf.getvalue())
    assert out.compression == "none" and out.data == buf.getvalue()
    assert out.note == "floating-point data"


def test_extra_hdus_are_not_silently_dropped():
    buf = io.BytesIO()
    fits.HDUList([fits.PrimaryHDU(np.zeros((32, 32), np.uint16)),
                  fits.ImageHDU(np.ones((8, 8), np.uint16))]).writeto(buf)
    out = compress_lossless(buf.getvalue())
    assert out.compression == "none" and out.data == buf.getvalue()


def test_incompressible_data_is_kept_as_is():
    # Pure 16-bit noise has no redundancy; storing a "compressed" file that is
    # larger than the original would be a loss.
    data = np.random.default_rng(2).integers(0, 65536, (256, 256), dtype=np.uint16)
    buf = io.BytesIO()
    fits.PrimaryHDU(data).writeto(buf)
    out = compress_lossless(buf.getvalue())
    assert out.compression == "none"
    assert len(out.data) == len(buf.getvalue())


def test_compression_can_be_disabled():
    raw = camera_fits()
    out = compress_lossless(raw, "none")
    assert out.compression == "none" and out.data == raw


def test_unknown_algorithm_is_rejected_loudly():
    with pytest.raises(ValueError):
        compress_lossless(camera_fits(), "HCOMPRESS_LOSSY_PLEASE")


def test_round_trip_guard_catches_lossy_output():
    # The guard itself, not the float pre-check in front of it: compress float
    # data the lossy way the codecs default to, and the comparison must refuse it.
    from fits_archive import _round_trip_problem

    data = np.random.default_rng(3).normal(0, 1, (128, 128)).astype(np.float32)
    header = fits.Header()
    out = io.BytesIO()
    fits.HDUList([fits.PrimaryHDU(), fits.CompImageHDU(data=data, compression_type="RICE_1")]).writeto(out)
    assert _round_trip_problem(out.getvalue(), data, header) == "pixels differ"
