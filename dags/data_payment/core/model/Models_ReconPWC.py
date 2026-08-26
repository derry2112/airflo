from typing import ClassVar, Tuple
import logging
from pydantic import BaseModel, Field

#Separator
SEP = "|"

#DEFINE RECON QR PAYMENT SPEC (PJSP)
_QR_PAYMENT_LAYOUT: Tuple[Tuple[str, int], ...] = (
    ("recon_header", 2),
    ("terminal_id", 16),
    ("retrieval_reference_number", 12),
    ("merchant_pan", 19),
    ("transaction_date", 8),
    ("transaction_time", 6),
    ("processing_code", 6),
    ("transaction_amount", 12),
    ("convenience_fee", 9),
    ("transaction_amount_currency", 3),
    ("merchant_type", 4),
    ("merchant_criteria", 3),
    ("acquiring_bank_code", 11),
    ("issuer_bank_code", 11),
    ("forwarding_institution_id", 11),
    ("response_code", 2),
    ("customer_pan", 28),
    ("invoice_data", 20),
    ("approval_code", 6),
    ("message_type_indicator", 4),
)

_HEADER_LAYOUT: Tuple[Tuple[str, int], ...] = (
    ("record_file_header", 2),
    ("recon_file_creator", 6),
    ("recon_file_receiver", 8),
    ("recon_file_date", 8),
)


class ReconHeader(BaseModel):
    record_file_header: str = Field(default=" " * 2, max_length=2)
    recon_file_creator: str = Field(default=" " * 6, max_length=6)
    recon_file_receiver: str = Field(default=" " * 8, max_length=8)
    recon_file_date: str = Field(default=" " * 8, max_length=8)

    HEADER_LAYOUT: ClassVar[Tuple[Tuple[str, int], ...]] = _HEADER_LAYOUT

    @classmethod
    def parse_header(cls, line: str) -> "ReconHeader":
        parts = line.rstrip("\r\n").split(SEP)
        logging.info('================ PARSE DATA QR ==============')
        logging.info('RAW LINE: %s', line)
        logging.info('TOTAL PARTS: %s', len(parts))

        for i,part in enumerate(parts):
            logging.info('PART[%s] len=%s value=%r', i, len(part),part)

        if len(parts) != len(cls.HEADER_LAYOUT):
            raise ValueError(
                f"card record expected {len(cls.HEADER_LAYOUT)} fields, got {len(parts)}"
            )
        values = {name: value for (name, _), value in zip(cls.HEADER_LAYOUT, parts)}
        return cls(**values)

    def to_header_line(self) -> str:
        parts = []
        for name, lenght in self.HEADER_LAYOUT:
            value = getattr(self, name)
            parts.append(value.ljust(lenght))
        return SEP.join(parts)


class ReconRecordData(BaseModel):
    # Common to both formats
    recon_header: str = Field(default=" " * 2, max_length=2)
    terminal_id: str = Field(default=" " * 16, max_length=16)
    retrieval_reference_number: str = Field(default=" " * 12, max_length=12)
    merchant_pan: str = Field(default=" " * 19, max_length=19)
    transaction_date: str = Field(default=" " * 8, max_length=8)
    transaction_time: str = Field(default=" " * 6, max_length=6)
    processing_code: str = Field(default=" " * 6, max_length=6)
    transaction_amount: str = Field(default=" " * 12, max_length=12)
    convenience_fee: str = Field(default=" " * 9, max_length=9)
    transaction_amount_currency: str = Field(default=" " * 3, max_length=3)
    merchant_type: str = Field(default=" " * 4, max_length=4)
    merchant_criteria: str = Field(default=" " * 3, max_length=3)
    acquiring_bank_code: str = Field(default=" " * 11, max_length=11)
    issuer_bank_code: str = Field(default=" " * 11, max_length=11)
    forwarding_institution_id: str = Field(default=" " * 11, max_length=11)
    response_code: str = Field(default=" " * 2, max_length=2)
    customer_pan: str = Field(default=" " * 28, max_length=28)
    invoice_data: str = Field(default=" " * 20, max_length=20)
    approval_code: str = Field(default=" " * 6, max_length=6)
    message_type_indicator: str = Field(default=" " * 4, max_length=4)

    QR_PAYMENT_LAYOUT: ClassVar[Tuple[Tuple[str, int], ...]] = _QR_PAYMENT_LAYOUT

    @classmethod
    def parse_recon_data_line(cls, line: str) -> "ReconRecordData":
        parts = line.rstrip("\r\n").split(SEP)
        if len(parts) != len(cls.QR_PAYMENT_LAYOUT):
            raise ValueError(
                f"card record expected {len(cls.QR_PAYMENT_LAYOUT)} fields, got {len(parts)}"
            )
        values = {name: value for (name, _), value in zip(cls.QR_PAYMENT_LAYOUT, parts)}
        return cls(**values)

    def to_recon_data_line(self) -> str:
        return SEP.join(getattr(self, name) for name, _ in self.QR_PAYMENT_LAYOUT)

    def to_line(self) -> str:
        return SEP.join(getattr(self, name) for name in self.__fields__)
