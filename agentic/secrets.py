"""Local secret screening, used BEFORE any source text is sent to a model (skills: migration-analysis step 2) and
when checking agent-written findings. Pattern-based: it reduces accidental disclosure, it does not guarantee
there are no secrets. Never log or echo the matched values."""
import re
from typing import List, Tuple

_NAME = r"\w*(?:pass(?:word|wd|phrase)|secret|token|api[_-]?key|access[_-]?key|private[_-]?key)\w*"
_PATTERNS = [
    # name = "value" / name: 'value'   (DB_PASSWORD, client_secret_value, aws_access_key_id ...)
    (re.compile(rf"(?i)({_NAME}\s*[=:]\s*)(['\"])(?!<REDACTED>)[^'\"]{{3,}}\2"), r'\1"<REDACTED>"'),
    # unquoted shell/properties form: export DB_PASSWORD=abc123
    (re.compile(rf"(?i)(\b{_NAME}=)(?!<REDACTED>)[^\s'\"<>]{{3,}}"), r"\1<REDACTED>"),
    # sqoop / CLI flags: --password abc   --password=abc   (but not --password-file, which is a reference)
    (re.compile(r"(?i)(--password(?:=|\s+))(?!-)[^\s'\"]+"), r"\1<REDACTED>"),
    # JDBC / URL credentials
    (re.compile(r"(?i)((?:password|pwd)=)[^&'\"\s;]+"), r"\1<REDACTED>"),
    (re.compile(r"(://[\w.-]+:)[^@/'\"\s]+@"), r"\1<REDACTED>@"),
    # Oozie / Hadoop XML properties: <name>...password...</name><value>x</value>
    (re.compile(r"(?is)(<name>[^<]*(?:pass|secret|token|key)[^<]*</name>\s*<value>)(?!<REDACTED>)[^<]+(</value>)"), r"\1<REDACTED>\2"),
]


def redact_secrets(text: str) -> Tuple[str, int]:
    """-> (text with credential-looking values masked, number of values masked)"""
    total = 0
    for rx, repl in _PATTERNS:
        text, n = rx.subn(repl, text)
        total += n
    return text, total


def line_has_secret(line: str) -> bool:
    return redact_secrets(line)[1] > 0


def secret_lines(text: str) -> List[int]:
    return [i for i, l in enumerate(text.splitlines(), 1) if line_has_secret(l)]
