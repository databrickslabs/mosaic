import base64
import os

resources_path = os.path.dirname(__file__)
mosaic_logo_path = os.path.join(resources_path, "mosaic_logo.png")

with open(mosaic_logo_path, "rb") as fh:
    mosaic_logo_b64str = base64.b64encode(fh.read()).decode("utf-8")
