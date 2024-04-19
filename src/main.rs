use std::collections::HashMap;
use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::ops::BitXor;
use std::os::unix::prelude::FileExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

type Offset = Arc<AtomicUsize>;

// This is basically a straight copy of the FxHasher from the rustc crate.
// Was just curious about how the hashing internals worked. Let me live.
const KEY: usize = 0x517c_c1b7_2722_0a95;

struct FastHasher {
    hash: usize,
}

impl FastHasher {
    fn compute_hash(&mut self, int: usize) {
        self.hash = self.hash.rotate_left(5).bitxor(int).wrapping_mul(KEY)
    }
}

impl Default for FastHasher {
    fn default() -> Self {
        Self { hash: 0 }
    }
}

impl Hasher for FastHasher {
    fn write(&mut self, mut bytes: &[u8]) {
        while bytes.len() >= 8 {
            let qword: [u8; 8] = bytes[0..8].try_into().unwrap();
            let qword = usize::from_ne_bytes(qword);
            self.compute_hash(qword);
            bytes = &bytes[8..];
        }

        if bytes.len() >= 4 {
            let dword: [u8; 4] = bytes[0..4].try_into().unwrap();
            let dword = u32::from_ne_bytes(dword) as usize;
            self.compute_hash(dword);
            bytes = &bytes[4..];
        }

        if bytes.len() >= 2 {
            let word: [u8; 2] = bytes[0..2].try_into().unwrap();
            let word = u16::from_ne_bytes(word) as usize;
            self.compute_hash(word);
            bytes = &bytes[2..];
        }

        if let Some(byte) = bytes.first() {
            self.compute_hash(*byte as usize);
        }
    }
    fn finish(&self) -> u64 {
        self.hash as u64
    }
}

type BuildFastHasher = BuildHasherDefault<FastHasher>;
type FastHashMap<K, V> = HashMap<K, V, BuildFastHasher>;

type InnerMap<'thread> = FastHashMap<&'thread [u8], Record>;
type OuterMap = Arc<Mutex<FastHashMap<String, Record>>>;

#[inline(always)]
fn new_offset() -> Offset {
    Arc::new(AtomicUsize::new(0))
}

#[inline(always)]
fn new_map() -> OuterMap {
    Arc::new(Mutex::new(FastHashMap::default()))
}

#[derive(Debug, Clone, Copy)]
struct Record {
    count: usize,
    min: f64,
    max: f64,
    sum: f64,
}

impl Record {
    #[inline(always)]
    fn new(measurement: f64) -> Self {
        Self {
            count: 1,
            min: measurement,
            max: measurement,
            sum: measurement,
        }
    }

    #[inline(always)]
    fn update(&mut self, measurement: f64) {
        self.count += 1;
        self.min = self.min.min(measurement);
        self.max = self.max.max(measurement);
        self.sum += measurement;
    }

    #[inline(always)]
    fn merge(&mut self, other: Record) {
        self.count += other.count;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
    }

    #[inline(always)]
    fn avg(&self) -> f64 {
        self.sum / (self.count as f64)
    }
}

#[inline(always)]
fn parse_float(x: &[u8]) -> f64 {
    // Shamelessly stolen from a better solution.
    let neg = x[0] == b'-';
    let len = x.len();

    let (d1, d2, d3) = match (neg, len) {
        (false, 3) => (0, x[0] - b'0', x[2] - b'0'),
        (false, 4) => (x[0] - b'0', x[1] - b'0', x[3] - b'0'),
        (true, 4) => (0, x[1] - b'0', x[3] - b'0'),
        (true, 5) => (x[1] - b'0', x[2] - b'0', x[4] - b'0'),
        _ => unreachable!(),
    };

    let int = ((d1 as i64) * 100) + ((d2 as i64) * 10) + (d3 as i64);
    let int = if neg { -int } else { int };
    (int / 10) as f64
}

#[inline(always)]
fn parse_row(data: &[u8]) -> (&[u8], f64) {
    let mut split = data.split(|&c| c == b';');
    // Making the assumption that the data is correctly formatted.
    // Otherwise, what's the point?
    let city = split.next().unwrap();
    let measurement = split.next().unwrap();
    let measurement = parse_float(measurement);
    (city, measurement)
}

#[inline(always)]
fn read_file_chunk(file: &File, file_size: usize, chunk_size: usize, offset: usize) -> Vec<u8> {
    let (file_i, buf_size, bytes_excess) = match offset == 0 {
        true => (offset as u64, chunk_size, 0),
        false => {
            let file_i = (offset - 64) as u64;
            let bytes_excess = 64;
            let buf_size = (chunk_size + 64).min(file_size - offset);
            (file_i, buf_size, bytes_excess)
        }
    };
    let mut buf: Vec<u8> = vec![0; buf_size];
    file.read_exact_at(&mut buf, file_i).unwrap();
    // trim head
    for i in 0..bytes_excess {
        if buf[i] == b'\n' {
            buf.drain(..=i);
            break;
        }
    }
    // trim tail
    let tail_i = buf.len() - 1;
    for i in ((tail_i - 64)..tail_i).rev() {
        if buf[i] == b'\n' {
            buf.truncate(i);
            break;
        }
    }
    buf
}

#[inline(always)]
fn parse_chunk(
    file: &File,
    file_size: usize,
    chunk_size: usize,
    offset: usize,
    outer_map: OuterMap,
) {
    let mut local_map = InnerMap::default();
    let buf = read_file_chunk(file, file_size, chunk_size, offset);
    let split = buf.split(|&b| b == b'\n');
    for line in split {
        let (city, measurement) = parse_row(line);
        local_map
            .entry(city)
            .and_modify(|r| r.update(measurement))
            .or_insert(Record::new(measurement));
    }
    let mut lock = outer_map.lock().unwrap();
    for (city, record) in local_map.into_iter() {
        let city = String::from_utf8_lossy(city).to_string(); // assuming that the data is valid again
        lock.entry(city)
            .and_modify(|r| r.merge(record))
            .or_insert(record);
    }
}

fn main() {
    let path = "./measurements.txt";
    let thread_count: usize = std::thread::available_parallelism().unwrap().into();
    let file = &File::open(path).unwrap();
    let file_size = file.metadata().unwrap().len() as usize;
    let chunk_size = file_size / thread_count;
    let offset = new_offset();
    let outer_map = new_map();

    std::thread::scope(|scope| {
        for _ in 0..thread_count {
            let offset = offset.clone();
            let outer_map = outer_map.clone();
            scope.spawn(move || {
                let offset = offset.fetch_add(chunk_size, Ordering::SeqCst);
                parse_chunk(file, file_size, chunk_size, offset, outer_map);
            });
        }
    });

    let outer_map = Arc::into_inner(outer_map).unwrap().into_inner().unwrap();
    let mut cities = outer_map.keys().collect::<Vec<&String>>();
    cities.sort_unstable();
    for city in cities {
        let r = outer_map[city];
        let min = r.min;
        let mean = r.avg();
        let max = r.max;
        println!("{}={:.1}/{:.1}/{:.1}", city, min, mean, max);
    }
}
