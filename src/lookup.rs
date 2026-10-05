/// Masks a presented key for logging; keys are client credentials.
pub(crate) fn mask_key(key: &str) -> String {
    match key.char_indices().nth(6) {
        Some((idx, _)) => format!("{}…", &key[..idx]),
        None => "…".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_key_keeps_prefix_only() {
        assert_eq!(mask_key("aaaaaaaaaaaaaaaa"), "aaaaaa…");
        assert_eq!(mask_key("short"), "…");
        assert_eq!(mask_key(""), "…");
    }
}
