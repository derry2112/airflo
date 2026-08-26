from typing import ClassVar, Tuple

from pydantic import BaseModel, Field


SEP = "|"


class DisputeHeader(BaseModel):
    record_type: str = Field(default=" " * 2, max_length=2)

    HEADER_LAYOUT: ClassVar[Tuple[Tuple[str, int], ...]] = (
        ("record_type", 2),
    )


class DisputeRecord(BaseModel):
    record_type: str = Field(default=" " * 2, max_length=2)
    dispute_amount: str = "0"

    def to_line(self) -> str:
        return SEP.join(str(value) for value in self.model_dump().values())
