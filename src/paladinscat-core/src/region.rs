pub fn canonical_region(value: &str) -> String {
    let trimmed = value.trim();
    match trimmed.to_ascii_lowercase().as_str() {
        "north america" | "na" => "NA".to_owned(),
        "europe" | "eu" => "EU".to_owned(),
        "brazil" | "br" => "BR".to_owned(),
        "south america" | "sa" => "SA".to_owned(),
        "southeast asia" | "sea" => "SEA".to_owned(),
        "australia" | "oceania" | "oce" => "OCE".to_owned(),
        "japan" | "jpn" => "JPN".to_owned(),
        "russia" | "rus" => "RUS".to_owned(),
        "asia" => "ASIA".to_owned(),
        "latin america north"
        | "latam north"
        | "latin america south"
        | "latam south"
        | "unknown"
        | "" => "Unknown".to_owned(),
        _ => trimmed.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalizes_provider_region_aliases() {
        for (input, expected) in [
            ("Europe", "EU"),
            ("North America", "NA"),
            ("Brazil", "BR"),
            ("Southeast Asia", "SEA"),
            ("Oceania", "OCE"),
            (" Japan ", "JPN"),
            ("", "Unknown"),
        ] {
            assert_eq!(canonical_region(input), expected);
        }
    }
}
