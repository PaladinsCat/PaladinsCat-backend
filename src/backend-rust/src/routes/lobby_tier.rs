use std::collections::HashMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TierBounds {
    pub(crate) minimum: Option<i16>,
    pub(crate) maximum: Option<i16>,
}

pub(crate) fn parse_tier_bounds(query: &HashMap<String, String>) -> Option<TierBounds> {
    let minimum = parse_optional_js_number(query.get("tierMin"))?;
    let maximum = parse_optional_js_number(query.get("tierMax"))?;
    if minimum.is_some_and(|value| !(1..=26).contains(&value))
        || maximum.is_some_and(|value| !(1..=26).contains(&value))
        || minimum.zip(maximum).is_some_and(|(min, max)| min > max)
    {
        return None;
    }
    Some(TierBounds { minimum, maximum })
}

fn parse_optional_js_number(raw: Option<&String>) -> Option<Option<i16>> {
    let Some(raw) = raw else {
        return Some(None);
    };
    if raw.is_empty() {
        return Some(None);
    }
    let number = raw.trim().parse::<f64>().ok()?;
    if !number.is_finite() || number.fract() != 0.0 {
        return None;
    }
    i16::try_from(number as i64).ok().map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tier_bounds_match_javascript_number_semantics() {
        assert_eq!(
            parse_tier_bounds(&HashMap::from([
                ("tierMin".to_owned(), "1".to_owned()),
                ("tierMax".to_owned(), "26".to_owned()),
            ])),
            Some(TierBounds {
                minimum: Some(1),
                maximum: Some(26),
            })
        );
        assert_eq!(
            parse_tier_bounds(&HashMap::from([("tierMin".to_owned(), "".to_owned(),)])),
            Some(TierBounds {
                minimum: None,
                maximum: None,
            })
        );
        assert_eq!(
            parse_tier_bounds(&HashMap::from([
                ("tierMin".to_owned(), "1tail".to_owned(),)
            ])),
            None
        );
        assert_eq!(
            parse_tier_bounds(&HashMap::from([
                ("tierMin".to_owned(), "3".to_owned()),
                ("tierMax".to_owned(), "2".to_owned()),
            ])),
            None
        );
    }
}
