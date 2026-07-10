/// Generate a 128-bit provenance nonce (32 hex chars) from the OS CSPRNG.
///
/// This is the boundary token wrapping each tool's output. It must be
/// unpredictable so injected tool output cannot forge a boundary tag — the
/// earlier 24-bit value mixed from PID + wall-clock was guessable. 128 random
/// bits make forgery and collision negligible.
pub fn generate() -> String {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).expect("OS CSPRNG unavailable");
    let mut s = String::with_capacity(32);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonces_are_32_hex_chars() {
        for _ in 0..10 {
            let n = generate();
            assert_eq!(n.len(), 32, "nonce should be 32 chars, got: {}", n);
            assert!(
                n.chars().all(|c| c.is_ascii_hexdigit()),
                "non-hex char in nonce: {}",
                n
            );
        }
    }

    #[test]
    fn nonces_differ_between_calls() {
        let nonces: Vec<_> = (0..50).map(|_| generate()).collect();
        let unique: std::collections::HashSet<_> = nonces.iter().collect();
        // 128-bit random: collisions across 50 draws are astronomically unlikely.
        assert_eq!(unique.len(), nonces.len(), "nonce collision: {:?}", nonces);
    }
}
