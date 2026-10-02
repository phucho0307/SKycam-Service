"""Lossless FITS compression for archiving clear-sky frames.

Pure functions, no I/O: the task in `tasks_pg.py` fetches and stores, this
decides what to store.

Measured on the camera's own FITS (ZWO ASI676MC, 3552x3552 uint16, 24 MiB):

    frames not saturated     RICE_1 1.66-1.70x  ~0.4s     GZIP_2 2.07-2.25x  ~1.7-2.3s
    synthetic night sky      RICE_1 2.12-2.38x            GZIP_2 2.30-2.57x

all verified lossless pixel-for-pixel. GZIP_2 is the default: ~25% smaller than
Rice for ~2s of CPU, which is affordable because only frames being *kept* are
compressed. Rice is the better choice where CPU is scarce (on the Pi itself).

The one rule this module never breaks: **a frame is never stored in a form that
does not decompress to exactly the original pixels.** Every result is
round-tripped and compared before it is returned; anything that fails (not a
valid FITS, floating-point data that the codecs would quantise, extra HDUs that
would be dropped, a header that does not survive) is stored as the original
bytes instead. A clear-sky frame is kept either way.
"""
import io
from typing import NamedTuple

import numpy as np
from astropy.io import fits

ALGORITHMS = ("GZIP_2", "RICE_1")

# Rewritten by the compressed-image conventions, so not compared.
_STRUCTURAL = {
    "SIMPLE", "XTENSION", "BITPIX", "NAXIS", "NAXIS1", "NAXIS2", "NAXIS3",
    "EXTEND", "PCOUNT", "GCOUNT", "BZERO", "BSCALE", "CHECKSUM", "DATASUM",
    "", "COMMENT", "HISTORY",
}


class Compressed(NamedTuple):
    data: bytes
    # The algorithm used, or "none" when the original bytes are kept.
    compression: str
    # Why it was kept uncompressed, for the archive row. None when compressed.
    note: str | None


def compress_lossless(raw: bytes, algorithm: str = "GZIP_2") -> Compressed:
    if algorithm == "none":
        return Compressed(raw, "none", "compression disabled")
    if algorithm not in ALGORITHMS:
        raise ValueError(f"unsupported FITS compression {algorithm!r}")

    try:
        with fits.open(io.BytesIO(raw), memmap=False) as hdul:
            if len(hdul) != 1:
                # Compressing only the primary image would silently drop the rest.
                return Compressed(raw, "none", f"{len(hdul)} HDUs; only single-image files are compressed")
            header, data = hdul[0].header, hdul[0].data
            if data is None or data.ndim < 2:
                return Compressed(raw, "none", "no image data")
            if data.dtype.kind == "f":
                # Tile compression quantises floating-point pixels by default,
                # which is lossy. The camera writes integers; anything else is
                # kept as sent.
                return Compressed(raw, "none", "floating-point data")
            data = np.array(data)  # detach from the file before it closes
            header = header.copy()

        out = io.BytesIO()
        fits.HDUList([
            fits.PrimaryHDU(),
            fits.CompImageHDU(data=data, header=header, compression_type=algorithm),
        ]).writeto(out)
        packed = out.getvalue()
    except (OSError, ValueError, TypeError) as e:
        return Compressed(raw, "none", f"not a readable FITS: {str(e)[:120]}")

    problem = _round_trip_problem(packed, data, header)
    if problem:
        return Compressed(raw, "none", f"round trip failed: {problem}")
    if len(packed) >= len(raw):
        return Compressed(raw, "none", "compression did not reduce size")
    return Compressed(packed, algorithm, None)


def decompress(stored: bytes) -> tuple[np.ndarray, fits.Header]:
    """Pixels and header of an archived file, compressed or not."""
    with fits.open(io.BytesIO(stored), memmap=False) as hdul:
        hdu = hdul[1] if len(hdul) > 1 and isinstance(hdul[1], fits.CompImageHDU) else hdul[0]
        return np.array(hdu.data), hdu.header.copy()


def _round_trip_problem(packed: bytes, data: np.ndarray, header: fits.Header) -> str | None:
    try:
        back, back_header = decompress(packed)
    except (OSError, ValueError, TypeError) as e:
        return f"cannot reopen: {e}"
    # Kind and width, not byte order: FITS is big-endian on disk and numpy may
    # hand back either order, which changes no value.
    if (back.dtype.kind, back.dtype.itemsize) != (data.dtype.kind, data.dtype.itemsize):
        return f"dtype {back.dtype} != {data.dtype}"
    if back.shape != data.shape:
        return f"shape {back.shape} != {data.shape}"
    if not np.array_equal(back, data):
        return "pixels differ"
    for card in header.cards:
        if card.keyword in _STRUCTURAL:
            continue
        if back_header.get(card.keyword) != card.value:
            return f"header {card.keyword} changed"
    return None
