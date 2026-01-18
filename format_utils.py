from decimal import Decimal


def readable_duration(second: str | int):
    second = int(second)
    if second < 60:
        return "{}s".format(second)
    elif second < 3600:
        minute = int(second / 60)
        second = int(second % 60)
        return "{}m{}s".format(minute, second)
    else:
        hour = int(second / 3600)
        minute = int((second - hour * 3600) / 60)
        second = int(second - hour * 3600 - minute * 60)
        return "{}h{}m{}s".format(hour, minute, second)


# Remove any trailing zeros after decimal point, but leave zeros for integer part.
def format_price(d: Decimal, at_most: int | None = None):
    s = "{:f}".format(d)
    if '.' in s:
        int_part, frac_part = s.split('.', 1)
        frac_part = frac_part.rstrip('0')
        if at_most is not None:
            frac_part = frac_part[:at_most]
        if frac_part == '':
            return int_part
        else:
            s = int_part + '.' + frac_part
    return s


def readable_currency(amount: Decimal) -> str:
    if amount >= Decimal("1_000_000_000_000"):
        return "{:.2f} T".format((amount / Decimal("1_000_000_000_000")))
    elif amount >= Decimal("1_000_000_000"):
        return "{:.2f} B".format((amount / Decimal("1_000_000_000")))
    elif amount >= Decimal("1_000_000"):
        return "{:.2f} M".format((amount / Decimal("1_000_000")))
    elif amount >= Decimal("1_000"):
        return "{:.2f} K".format((amount / Decimal("1_000")))
    else:
        return "{:.2f}".format(amount)


def readable_currency_cn(amount: Decimal) -> str:
    if amount >= Decimal("1_000_000_000_000"):
        return "{:.2f} 万亿".format((amount / Decimal("1_000_000_000_000")))
    elif amount >= Decimal("100_000_000"):
        return "{:.2f} 亿".format((amount / Decimal("100_000_000")))
    elif amount >= Decimal("10_000"):
        return "{:.2f} 万".format((amount / Decimal("10_000")))
    else:
        return "{:.2f}".format(amount)
