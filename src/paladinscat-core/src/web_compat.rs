#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Pagination {
    pub page: i64,
    pub per_page: i64,
    pub offset: i64,
}

pub fn paginate(page: Option<&str>, per_page: Option<&str>) -> Pagination {
    let page = parse_js_integer(page.unwrap_or("1")).unwrap_or(1).max(1);
    let per_page = parse_js_integer(per_page.unwrap_or("20"))
        .unwrap_or(20)
        .clamp(1, 100);
    Pagination {
        page,
        per_page,
        offset: (page - 1).saturating_mul(per_page),
    }
}

/// The current TypeScript routes use `parseInt(value, 10)`, which accepts a
/// valid integer prefix such as `12abc`. Preserve that behavior during the
/// migration instead of silently switching to Rust's stricter `parse()`.
pub fn parse_js_integer(value: &str) -> Option<i64> {
    let value = value.trim_start();
    let (negative, digits) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    let length = digits.bytes().take_while(u8::is_ascii_digit).count();
    if length == 0 {
        return None;
    }
    let parsed = digits[..length].parse::<i64>().ok()?;
    if negative {
        parsed.checked_neg()
    } else {
        Some(parsed)
    }
}

pub fn sorting(sort: Option<&str>, order: Option<&str>, allowed_fields: &[&str]) -> String {
    let Some(sort) = sort.filter(|sort| allowed_fields.contains(sort)) else {
        return String::new();
    };
    let direction = match order {
        Some("asc") => "ASC",
        Some("desc") => "DESC",
        _ => "DESC",
    };
    format!(" ORDER BY {sort} {direction}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_parser_matches_javascript_prefix_behavior() {
        assert_eq!(parse_js_integer("  +12tail"), Some(12));
        assert_eq!(parse_js_integer("-4.7"), Some(-4));
        assert_eq!(parse_js_integer("tail12"), None);
        assert_eq!(parse_js_integer(""), None);
    }

    #[test]
    fn pagination_matches_typescript_defaults_and_bounds() {
        assert_eq!(
            paginate(Some("0"), Some("1000")),
            Pagination {
                page: 1,
                per_page: 100,
                offset: 0,
            }
        );
        assert_eq!(
            paginate(Some("3tail"), Some("5")),
            Pagination {
                page: 3,
                per_page: 5,
                offset: 10,
            }
        );
    }

    #[test]
    fn sorting_matches_typescript_allowlist_and_direction_defaults() {
        assert_eq!(
            sorting(Some("mu"), Some("asc"), &["mu", "phi"]),
            " ORDER BY mu ASC"
        );
        assert_eq!(
            sorting(Some("phi"), Some("invalid"), &["mu", "phi"]),
            " ORDER BY phi DESC"
        );
        assert_eq!(sorting(Some("unsafe"), Some("asc"), &["mu", "phi"]), "");
        assert_eq!(sorting(None, Some("asc"), &["mu", "phi"]), "");
    }
}
