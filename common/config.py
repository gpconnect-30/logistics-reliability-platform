import os
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://logistics:logistics@postgres:5432/logistics")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
RATING_URL = os.getenv("RATING_URL", "http://rating:8001")
SHIPMENT_URL = os.getenv("SHIPMENT_URL", "http://shipment:8002")
TRACKING_URL = os.getenv("TRACKING_URL", "http://tracking:8003")
