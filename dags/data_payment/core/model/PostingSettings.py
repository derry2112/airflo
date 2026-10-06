"""Setting global untuk semua generator POSTFLIN.

Ubah nilai di bawah sebelum trigger DAG. Nilai field harus berupa string ASCII
satu baris dan tidak melebihi panjang field. Jumlah spasi tambahan diatur melalui MERCHANT_OUTLET_SPACES.
"""

# Karakter untuk right-padding teks dan field kosong; harus satu karakter ASCII.
SPACE = " "

# Spasi tambahan antara merchant_number (15) dan outlet_number (15) pada HS/TS.
MERCHANT_OUTLET_SPACES = 1

# Field kosong dapat diberi nilai; generator menambahkan SPACE hingga panjang field.
POSTING_FIELDS = {
    "HR": {"institution_ref": "ID7339", "tokenization_indicator": "C"},
    "HS": {"batch_type": "P"},
    "DT": {
        "service_type": "0",
        "expiry_date": "",
        "authorization_flag": "A",
        "pos_data": "100001154110",
        "pos_entry_mode": "012",
        "pos_condition_code": "00",
        "currency_exponent": "1",
        "reversal_reason_code": "",
        "replacement_amounts": "",
        "service_code": "",
        "single_message_indicator": "Y",
    },
    "OA": {
        "tip_amount": "",
        "cashback_amount": "",
        "surcharge_fee": "",
        "conversion_rate": "",
        "rate_exponent": "",
        "rate_date": "",
        "reserved_for_future_use": "",
        "dcc_indicator": "",
    },
    "TR": {"institution_identification": "ID7339", "file_sender": "ID7339"},
}


def posting_space():
    if not isinstance(SPACE, str) or len(SPACE) != 1 or not SPACE.isascii() or not SPACE.isprintable():
        raise ValueError("PostingSettings.SPACE harus satu karakter ASCII yang dapat dicetak")
    return SPACE


def posting_field(record, name, length):
    value = POSTING_FIELDS[record][name]
    if not isinstance(value, str) or not value.isascii() or (value and not value.isprintable()):
        raise ValueError(f"Setting {record}.{name} harus string ASCII satu baris")
    if len(value) > length:
        raise ValueError(f"Setting {record}.{name} melebihi panjang field {length}")
    return value.ljust(length, posting_space())


def merchant_outlet_space():
    if type(MERCHANT_OUTLET_SPACES) is not int or MERCHANT_OUTLET_SPACES < 0:
        raise ValueError("MERCHANT_OUTLET_SPACES harus integer >= 0")
    return posting_space() * MERCHANT_OUTLET_SPACES
