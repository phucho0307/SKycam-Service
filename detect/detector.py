"""Cloud-detection heuristics for all-sky images (no ML).

Given an RGB image (numpy array), estimate how cloudy the sky is. This is the
classical approach real all-sky/weather products use — no model, fast, explainable.

Primary signal (night): STAR COUNT — a clear sky shows many point-like stars; clouds
occlude them. Secondary features: overall BRIGHTNESS and the daytime R/B RATIO
(clear sky is blue → low R/B; cloud is gray/white → R/B ≈ 1).

`analyze()` returns a dict with `cloud_score` in [0,1] and `is_cloudy`.

NOTE: thresholds (EXPECTED_STARS, CLOUD_THRESHOLD) are placeholders to calibrate on
real night-sky data. This heuristic is night-oriented; daytime frames have ~0 stars
and would read as cloudy — add an R/B-ratio branch for day when needed.
"""
import os

import cv2
import numpy as np

EXPECTED_STARS = int(os.environ.get("DETECT_EXPECTED_STARS", "40"))
CLOUD_THRESHOLD = float(os.environ.get("DETECT_CLOUD_THRESHOLD", "0.6"))


def _count_stars(gray: np.ndarray) -> int:
    """Count small, point-like bright sources on a dark background."""
    # Estimate the local background and subtract it to isolate point sources.
    bg = cv2.medianBlur(gray, 21)
    diff = cv2.subtract(gray, bg)
    # Threshold clearly above the background noise.
    thr = max(15, int(diff.mean() + 5 * diff.std()))
    _, binary = cv2.threshold(diff, thr, 255, cv2.THRESH_BINARY)
    n, _, stats, _ = cv2.connectedComponentsWithStats(binary, connectivity=8)
    count = 0
    for i in range(1, n):  # skip label 0 (background)
        area = stats[i, cv2.CC_STAT_AREA]
        if 1 <= area <= 40:  # star-like blobs; big blobs are moon/clouds/artifacts
            count += 1
    return count


def analyze(rgb: np.ndarray) -> dict:
    gray = cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY)
    brightness = round(float(gray.mean()), 2)

    r = float(rgb[..., 0].mean())
    b = float(rgb[..., 2].mean())
    rb_ratio = round(r / max(b, 1e-6), 3)

    star_count = _count_stars(gray)
    star_frac = min(star_count / max(EXPECTED_STARS, 1), 1.0)
    cloud_score = round(1.0 - star_frac, 3)  # fewer stars => cloudier
    is_cloudy = cloud_score >= CLOUD_THRESHOLD

    return {
        "cloud_score": cloud_score,
        "is_cloudy": bool(is_cloudy),
        "star_count": int(star_count),
        "brightness": brightness,
        "rb_ratio": rb_ratio,
    }
