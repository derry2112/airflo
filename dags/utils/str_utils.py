from utils.telemetry.tracing import tracer

class StrUtils:
    def isEmpty(self, value):
        return value is None or str(value).strip() == "" or (hasattr(value, "__len__") and len(value) == 0)