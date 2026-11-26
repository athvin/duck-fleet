pub fn valid_principal(principal: &str) -> bool {
    let trimmed = principal.trim();
    !trimmed.is_empty()
        && trimmed == principal
        && principal.len() <= 64
        && principal != "bootstrap-admin"
        && principal.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | '@')
        })
}

pub fn find_relation(name: &str) -> Option<&'static str> {
    ["query_history", "query_attempts", "usage"]
        .into_iter()
        .find(|relation| *relation == name)
}
