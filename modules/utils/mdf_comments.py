"""Parse MDF md_comment fields into tag key-value pairs."""

import ast
import json


def _md_comment_tag_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def parse_md_comment_tags(md_comment: str | None) -> list[tuple[str, str]]:
    """Expand bronze_meta ``md_comment`` into channel tag key-value pairs."""
    if md_comment is None:
        return []
    text = md_comment.strip()
    if not text:
        return []

    parsed = None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(text)
        except (ValueError, SyntaxError):
            pass

    if isinstance(parsed, dict):
        return [(str(key), _md_comment_tag_value(val)) for key, val in parsed.items()]

    return [("comment", md_comment)]
