import pytest

from utils.mdf_comments import parse_md_comment_tags


@pytest.mark.parametrize(
    "md_comment,expected",
    [
        (None, []),
        ("", []),
        ("plain text", [("comment", "plain text")]),
        ('{"vehicle_key": "v1", "route": "berlin"}', [("vehicle_key", "v1"), ("route", "berlin")]),
        ("{'vehicle_key': 'v2'}", [("vehicle_key", "v2")]),
        ('{"nested": {"a": 1}}', [("nested", '{"a": 1}')]),
    ],
)
def test_parse_md_comment_tags(md_comment, expected):
    assert parse_md_comment_tags(md_comment) == expected


def test_parse_md_comment_tags_json_list_falls_back_to_comment():
    assert parse_md_comment_tags("[1, 2, 3]") == [("comment", "[1, 2, 3]")]
