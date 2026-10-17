use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::Notify;

/// Bytes of live stderr retained per running task: 64 KiB. The buffer is a
/// ring — past this bound the oldest bytes are dropped — because the tail
/// answers "what is it doing right now", not "what has it done"; the EXIT
/// signal carries the task's real output.
pub const TAIL_BUFFER_CAP: usize = 64 * 1024;

/// Characters of tail a REMIND signal carries.
pub const REMIND_TAIL_CHARS: usize = 4096;

/// Bounded byte ring over one task's live stderr.
///
/// Stores bytes, not chars: chunks arrive from a pipe with no framing
/// guarantee and may end mid-character. Decoding is deferred to the read edge
/// (`tail_chars`), lossily — a partial UTF-8 sequence left at the ring's
/// front by a byte-granular drop renders as U+FFFD instead of poisoning the
/// whole tail.
#[derive(Debug)]
pub struct TailBuffer {
    bytes: VecDeque<u8>,
    cap: usize,
}

impl TailBuffer {
    pub fn new(cap: usize) -> Self {
        Self {
            bytes: VecDeque::new(),
            cap,
        }
    }

    pub fn push_chunk(&mut self, chunk: &[u8]) {
        if chunk.len() >= self.cap {
            self.bytes.clear();
            self.bytes.extend(&chunk[chunk.len() - self.cap..]);
            return;
        }
        let overflow = (self.bytes.len() + chunk.len()).saturating_sub(self.cap);
        if overflow > 0 {
            self.bytes.drain(..overflow);
        }
        self.bytes.extend(chunk);
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// The latest `n` characters, clamped to what the buffer holds. Counted in
    /// characters after lossy decoding, and cut on a char boundary, so a
    /// multi-byte character is never split by the tail edge.
    pub fn tail_chars(&self, n: usize) -> String {
        if n == 0 || self.bytes.is_empty() {
            return String::new();
        }
        let (a, b) = self.bytes.as_slices();
        let mut all = Vec::with_capacity(self.bytes.len());
        all.extend_from_slice(a);
        all.extend_from_slice(b);
        let s = String::from_utf8_lossy(&all);
        match s.char_indices().rev().nth(n - 1) {
            Some((i, _)) => s[i..].to_string(),
            None => s.into_owned(),
        }
    }
}

/// Shared handle to one task's live stderr ring: the dmcp stderr reader
/// appends, REMIND and `status {"tail": n}` read. The `Task` drops its handle
/// when the task settles (EXIT/KILL) — the EXIT signal already carries the
/// task's real output, so nothing legitimate reads the tail afterwards.
#[derive(Clone, Debug)]
pub struct TaskTail {
    buffer: Arc<Mutex<TailBuffer>>,
    /// Signalled once, by the first non-empty append (see `first_output`).
    first: Arc<Notify>,
}

impl TaskTail {
    pub fn new() -> Self {
        Self {
            buffer: Arc::new(Mutex::new(TailBuffer::new(TAIL_BUFFER_CAP))),
            first: Arc::new(Notify::new()),
        }
    }

    // A poisoned lock only means a panic elsewhere mid-append; the retained
    // bytes are still sound to read, so never propagate the poison.
    fn lock(&self) -> MutexGuard<'_, TailBuffer> {
        self.buffer.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn append(&self, chunk: &[u8]) {
        let was_empty = {
            let mut buffer = self.lock();
            let was_empty = buffer.is_empty();
            buffer.push_chunk(chunk);
            was_empty
        };
        if was_empty && !chunk.is_empty() {
            // notify_one keeps a permit when nobody waits yet, so a watcher
            // that starts awaiting after the first write still wakes.
            self.first.notify_one();
        }
    }

    /// Resolves once the task has written anything.
    pub async fn first_output(&self) {
        self.first.notified().await;
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    pub fn tail_chars(&self, n: usize) -> String {
        self.lock().tail_chars(n)
    }

    /// The whole retained ring, lossily decoded — the stderr fallback for a
    /// failed call's error detail. Identical to the pre-ring full read for
    /// stderr under `TAIL_BUFFER_CAP`; beyond it only the newest bytes remain,
    /// which is the end a failure message lives at.
    pub fn snapshot(&self) -> String {
        self.lock().tail_chars(usize::MAX)
    }
}

impl Default for TaskTail {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod first_output_tests {
    use super::*;

    #[tokio::test]
    async fn first_output_resolves_even_if_written_before_awaiting() {
        let tail = TaskTail::new();
        tail.append(b"");
        tail.append(b"code");
        tokio::time::timeout(std::time::Duration::from_millis(100), tail.first_output())
            .await
            .expect("a write before the await still wakes it");
    }

    #[tokio::test]
    async fn first_output_waits_for_a_write() {
        let tail = TaskTail::new();
        tail.append(b"");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), tail.first_output())
                .await
                .is_err(),
            "an empty write is not output"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_keeps_only_newest_cap_bytes() {
        let mut b = TailBuffer::new(8);
        b.push_chunk(b"0123456789abcdef");
        assert_eq!(b.tail_chars(100), "89abcdef");
        b.push_chunk(b"XY");
        assert_eq!(b.tail_chars(100), "abcdefXY");
        assert_eq!(b.len(), 8);
    }

    #[test]
    fn ring_bounds_at_the_documented_cap() {
        let mut b = TailBuffer::new(TAIL_BUFFER_CAP);
        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        for chunk in data.chunks(4096) {
            b.push_chunk(chunk);
        }
        assert_eq!(b.len(), TAIL_BUFFER_CAP);
        let (x, y) = b.bytes.as_slices();
        let mut kept = Vec::new();
        kept.extend_from_slice(x);
        kept.extend_from_slice(y);
        assert_eq!(kept, data[data.len() - TAIL_BUFFER_CAP..]);
    }

    #[test]
    fn tail_chars_counts_characters_not_bytes() {
        let mut b = TailBuffer::new(TAIL_BUFFER_CAP);
        b.push_chunk("aé漢🎉".as_bytes());
        assert_eq!(b.tail_chars(1), "🎉");
        assert_eq!(b.tail_chars(2), "漢🎉");
        assert_eq!(b.tail_chars(3), "é漢🎉");
        assert_eq!(b.tail_chars(0), "");
        assert_eq!(b.tail_chars(99), "aé漢🎉");
    }

    #[test]
    fn byte_drop_mid_character_is_lossy_not_fatal() {
        // cap 5 over two 3-byte chars drops the first char's lead byte; the
        // orphan continuation bytes must decode as U+FFFD, never split a char
        // or panic.
        let mut b = TailBuffer::new(5);
        b.push_chunk("漢漢".as_bytes());
        let s = b.tail_chars(10);
        assert!(s.ends_with('漢'), "intact char must survive, got: {s:?}");
        assert!(
            s.chars().all(|c| c == '\u{FFFD}' || c == '漢'),
            "partial bytes must become U+FFFD, got: {s:?}"
        );
    }

    #[test]
    fn wrapped_ring_reads_across_the_seam() {
        let mut b = TailBuffer::new(4);
        b.push_chunk(b"abcd");
        b.push_chunk(b"ef");
        assert_eq!(b.tail_chars(10), "cdef");
        assert_eq!(b.tail_chars(2), "ef");
    }

    #[test]
    fn shared_handle_sees_appends_from_clones() {
        let tail = TaskTail::new();
        let writer = tail.clone();
        writer.append(b"hello ");
        writer.append("wörld".as_bytes());
        assert_eq!(tail.tail_chars(5), "wörld");
        assert_eq!(tail.snapshot(), "hello wörld");
        assert!(!tail.is_empty());
    }
}
