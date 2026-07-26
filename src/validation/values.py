from __future__ import annotations

from collections.abc import Collection, MutableSet
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from src.validation.errors import DataValidationError


def parse_optional_date(value: object, name: str) -> date | None:
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise DataValidationError(
            f"{name} must use YYYY-MM-DD format",
        ) from exc


def validate_period(
    period_from: date | None,
    period_to: date | None,
) -> None:
    if period_from and period_to and period_from > period_to:
        raise DataValidationError(
            "period_from cannot be later than period_to",
        )


def resolve_period(
    period_from: object = None,
    period_to: object = None,
    lookback_days: object = 0,
    *,
    current_date: date | None = None,
) -> tuple[date | None, date | None]:
    start = parse_optional_date(period_from, "period_from")
    end = parse_optional_date(period_to, "period_to")
    validate_period(start, end)

    try:
        days = int(lookback_days or 0)
    except (TypeError, ValueError) as exc:
        raise DataValidationError(
            "lookback_days must be a non-negative integer",
        ) from exc
    if days < 0:
        raise DataValidationError(
            "lookback_days must be a non-negative integer",
        )
    if days == 0:
        return start, end
    if start or end:
        raise DataValidationError(
            "lookback_days cannot be combined with an explicit period",
        )

    anchor = current_date or datetime.now(UTC).date()
    return anchor - timedelta(days=days - 1), anchor


def date_in_period(
    value: date,
    period_from: date | None = None,
    period_to: date | None = None,
) -> bool:
    return not ((period_from and value < period_from) or (period_to and value > period_to))


def parse_date(value: object, column: str, line: int) -> date:
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise _value_error(
            column,
            line,
            value,
            "expected YYYY-MM-DD",
        ) from exc


def parse_int(
    value: object,
    column: str,
    line: int,
    *,
    min_value: int | None = None,
    max_value: int | None = None,
) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise _value_error(column, line, value, "expected integer") from exc

    if min_value is not None and result < min_value:
        raise _value_error(column, line, value, f"minimum is {min_value}")
    if max_value is not None and result > max_value:
        raise _value_error(column, line, value, f"maximum is {max_value}")
    return result


def parse_decimal(
    value: object,
    column: str,
    line: int,
    *,
    scale: int = 2,
    min_value: Decimal | None = None,
) -> Decimal:
    try:
        result = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise _value_error(column, line, value, "expected decimal") from exc

    if not result.is_finite():
        raise _value_error(column, line, value, "value must be finite")
    if min_value is not None and result < min_value:
        raise _value_error(column, line, value, f"minimum is {min_value}")
    if result.as_tuple().exponent < -scale:
        raise _value_error(
            column,
            line,
            value,
            f"maximum scale is {scale}",
        )
    return result.quantize(Decimal("1").scaleb(-scale))


def parse_required_string(
    value: object,
    column: str,
    line: int,
) -> str:
    result = str(value or "").strip()
    if not result:
        raise _value_error(column, line, value, "value is required")
    return result


def parse_choice(
    value: object,
    column: str,
    line: int,
    *,
    allowed_values: Collection[str],
    uppercase: bool = False,
) -> str:
    result = parse_required_string(value, column, line)
    if uppercase:
        result = result.upper()
    if result not in allowed_values:
        raise _value_error(
            column,
            line,
            value,
            f"expected one of {sorted(allowed_values)}",
        )
    return result


def ensure_unique(
    key: object,
    seen: MutableSet[object],
    column: str,
    line: int,
) -> None:
    if key in seen:
        raise _value_error(column, line, key, "Duplicate key")
    seen.add(key)


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _value_error(
    column: str,
    line: int,
    value: object,
    message: str,
) -> DataValidationError:
    return DataValidationError(
        f"line {line}, column {column}: {message}; value={value!r}",
    )
