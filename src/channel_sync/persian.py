"""Persian presentation helpers; storage and diagnostic logs remain in English."""

from datetime import date, datetime
from zoneinfo import ZoneInfo


def persian_lines(text, *, post=False):
    """Keep the first actual letter Persian, including legacy draft lines."""
    result, fenced = [], False
    for line in text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith(("```", "~~~")):
            fenced = not fenced
            result.append(line)
            continue
        if fenced or (post and stripped.startswith(("#", "🔗"))):
            result.append(line)
            continue
        first = next((letter for letter in line if letter.isalpha()), "")
        if first and not "\u0600" <= first <= "\u06ff":
            prefix = "شناسه" if stripped.startswith("🆔") else "متن"
            line = f"{prefix}: {line}"
        result.append(line)
    return "\n".join(result)


def platform_name(platform):
    return {"telegram": "تلگرام", "bale": "بله"}.get(platform, "نامشخص")


def state_name(state):
    return {
        "queued": "در صف",
        "working": "در حال پردازش",
        "quota": "بیش از سهمیه",
        "filtered": "کنار گذاشته‌شده",
        "drafted": "پیش‌نویس آماده",
        "duplicate": "تکراری",
        "researching": "در حال پژوهش و نگارش",
        "pending": "در انتظار تأیید",
        "approved": "تأییدشده",
        "rejected": "لغوشده",
        "published": "منتشرشده",
        "partial": "انتشار ناقص",
        "dry_run": "آزمایشی؛ بدون انتشار",
        "error": "ناموفق",
        "merged": "تکراری و کنار گذاشته‌شده",
        "publishing": "در حال انتشار",
        "sent": "ارسال‌شده",
        "failed": "ناموفق",
        "uncertain": "نتیجهٔ نامشخص؛ نیازمند بررسی دستی",
    }.get(state, "نامشخص")


def challenge_description(reason):
    reason = (reason or "").lower()
    if any(word in reason for word in ("rate", "too many", "429")):
        return "محدودیت تعداد درخواست‌های حساب تشخیص داده شد."
    if any(word in reason for word in ("captcha", "human", "authenticate")):
        return "حساب به حل کپچا یا تأیید انسانی نیاز دارد."
    if any(word in reason for word in ("unusual", "suspicious")):
        return "فعالیت یا ورود غیرعادی در حساب تشخیص داده شد."
    if any(word in reason for word in ("locked", "suspended")):
        return "حساب قفل یا تعلیق شده است."
    if any(word in reason for word in ("login", "authentication", "401")):
        return "ورود به حساب یا اعتبار نشست نیاز به بررسی دارد."
    return "دسترسی به حساب نیازمند بررسی و رفع محدودیت به‌صورت دستی است."


def challenge_alert(reason):
    return (
        "خواندن تایم‌لاین ایکس متوقف شد.\n"
        f"دلیل: {challenge_description(reason)}\n"
        "ابتدا با فرمان x-login مشکل حساب را دستی رفع کنید، سپس دکمهٔ ادامه را بزنید."
    )


def jalali_date(gregorian):
    """Convert using Borkowski's Nowruz leap-cycle calculation and date ordinals.

    Reference: https://www.astro.uni.torun.pl/~kb/Papers/EMP/PersianC-EMP.htm
    """
    breaks = (
        -61,
        9,
        38,
        199,
        426,
        686,
        756,
        818,
        1111,
        1181,
        1210,
        1635,
        2060,
        2097,
        2192,
        2262,
        2324,
        2394,
        2456,
        3178,
    )

    def year_start(year):
        if not breaks[0] <= year < breaks[-1]:
            raise ValueError("Date outside supported Jalali calendar range")
        start, leaps = breaks[0], -14
        for end in breaks[1:]:
            span = end - start
            if year < end:
                break
            leaps += span // 33 * 8 + (span % 33) // 4
            start = end
        elapsed = year - start
        leaps += elapsed // 33 * 8 + (elapsed % 33 + 3) // 4
        if span % 33 == 4 and span - elapsed == 4:
            leaps += 1
        gregorian_year = year + 621
        gregorian_leaps = (
            gregorian_year // 4 - ((gregorian_year // 100 + 1) * 3) // 4 - 150
        )
        return date(gregorian_year, 3, 20 + leaps - gregorian_leaps)

    year = gregorian.year - 621
    start = year_start(year)
    if gregorian < start:
        year -= 1
        start = year_start(year)
    offset = (gregorian - start).days
    if offset < 186:
        month, day = 1 + offset // 31, 1 + offset % 31
    else:
        month, day = 7 + (offset - 186) // 30, 1 + (offset - 186) % 30
    return year, month, day


def tehran_date(now=None):
    today = (
        (now or datetime.now(ZoneInfo("Asia/Tehran")))
        .astimezone(ZoneInfo("Asia/Tehran"))
        .date()
    )
    year, month, day = jalali_date(today)
    jalali = f"{year:04}/{month:02}/{day:02}".translate(
        str.maketrans("0123456789", "۰۱۲۳۴۵۶۷۸۹")
    )
    return f"{today.isoformat()} ({jalali})"
