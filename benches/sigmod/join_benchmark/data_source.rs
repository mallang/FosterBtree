use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

// Fixed sizes for truncation to ensure consistent record sizes
const PKEY_SIZE: usize = 8; // p_partkey as i64
const JOIN_KEY_SIZE: usize = 8; // p_partkey as i64 (same as LINEITEM l_partkey)
const VALUE_SIZE: usize = 128; // Fixed value size

#[derive(Debug, Clone)]
pub struct Part {
    pub p_partkey: i64,
    pub p_name: String,
    pub p_mfgr: String,
    pub p_brand: String,
    pub p_type: String,
    pub p_size: i32,
    pub p_container: String,
    pub p_retailprice: f64,
    pub p_comment: String,
}

fn truncate_or_pad(bytes: &[u8], size: usize) -> Vec<u8> {
    if bytes.len() >= size {
        bytes[..size].to_vec()
    } else {
        let mut result = vec![b' '; size];
        result[..bytes.len()].copy_from_slice(bytes);
        result
    }
}

impl Part {
    pub fn generate_pkey(&self) -> Vec<u8> {
        truncate_or_pad(&self.p_partkey.to_le_bytes(), PKEY_SIZE)
    }

    pub fn generate_join_key(&self) -> Vec<u8> {
        // Join on P_PARTKEY (same as LINEITEM.L_PARTKEY)
        truncate_or_pad(&self.p_partkey.to_le_bytes(), JOIN_KEY_SIZE)
    }

    pub fn generate_value(&self) -> Vec<u8> {
        let mut value = Vec::with_capacity(VALUE_SIZE);

        // p_partkey (8 bytes)
        value.extend_from_slice(&truncate_or_pad(&self.p_partkey.to_le_bytes(), 8));
        // p_name (20 bytes)
        value.extend_from_slice(&truncate_or_pad(self.p_name.as_bytes(), 20));
        // p_mfgr (16 bytes)
        value.extend_from_slice(&truncate_or_pad(self.p_mfgr.as_bytes(), 16));
        // p_brand (16 bytes)
        value.extend_from_slice(&truncate_or_pad(self.p_brand.as_bytes(), 16));
        // p_type (24 bytes)
        value.extend_from_slice(&truncate_or_pad(self.p_type.as_bytes(), 24));
        // p_size (4 bytes)
        value.extend_from_slice(&self.p_size.to_le_bytes());
        // p_container (12 bytes)
        value.extend_from_slice(&truncate_or_pad(self.p_container.as_bytes(), 12));
        // p_retailprice (8 bytes)
        value.extend_from_slice(&self.p_retailprice.to_le_bytes());
        // p_comment (remaining, truncated to fit VALUE_SIZE)
        let remaining = VALUE_SIZE - value.len();
        value.extend_from_slice(&truncate_or_pad(self.p_comment.as_bytes(), remaining));

        assert_eq!(
            value.len(),
            VALUE_SIZE,
            "Value size mismatch: expected {}, got {}",
            VALUE_SIZE,
            value.len()
        );
        value
    }
}

fn parse_part(line: &str) -> Option<Part> {
    let fields: Vec<&str> = line.trim().split('|').collect();
    if fields.len() < 9 {
        return None;
    }

    Some(Part {
        p_partkey: fields[0].parse().ok()?,
        p_name: fields[1].to_string(),
        p_mfgr: fields[2].to_string(),
        p_brand: fields[3].to_string(),
        p_type: fields[4].to_string(),
        p_size: fields[5].parse().ok()?,
        p_container: fields[6].to_string(),
        p_retailprice: fields[7].parse().ok()?,
        p_comment: fields[8].to_string(),
    })
}

pub fn load_part_table(path: &Path) -> Vec<Part> {
    let file = File::open(path).expect(&format!("Failed to open file: {:?}", path));
    let reader = BufReader::new(file);
    let mut parts = Vec::new();

    for line in reader.lines() {
        let line = line.expect("Failed to read line");
        if let Some(part) = parse_part(&line) {
            parts.push(part);
        }
    }

    parts
}

pub struct PartDataSource {
    pub parts: Vec<Part>,
}

impl PartDataSource {
    pub fn new(path: &str) -> Self {
        let parts = load_part_table(Path::new(path));
        println!("  - Loaded {} rows from {}", parts.len(), path);
        Self { parts }
    }

    pub fn get_parts(&self) -> &Vec<Part> {
        &self.parts
    }
}

#[derive(Debug, Clone)]
pub struct LineItem {
    pub l_orderkey: i64,
    pub l_partkey: i64,
    pub l_suppkey: i64,
    pub l_linenumber: i32,
    pub l_quantity: f64,
    pub l_extendedprice: f64,
    pub l_discount: f64,
    pub l_tax: f64,
    pub l_returnflag: String,
    pub l_linestatus: String,
    pub l_shipdate: String,
    pub l_commitdate: String,
    pub l_receiptdate: String,
    pub l_shipinstruct: String,
    pub l_shipmode: String,
    pub l_comment: String,
}

impl LineItem {
    pub fn generate_probe_key(&self) -> Vec<u8> {
        // Probe on L_PARTKEY (same as PART.P_PARTKEY)
        truncate_or_pad(&self.l_partkey.to_le_bytes(), 8)
    }
}

fn parse_lineitem(line: &str) -> Option<LineItem> {
    let fields: Vec<&str> = line.trim().split('|').collect();
    if fields.len() < 16 {
        return None;
    }

    Some(LineItem {
        l_orderkey: fields[0].parse().ok()?,
        l_partkey: fields[1].parse().ok()?,
        l_suppkey: fields[2].parse().ok()?,
        l_linenumber: fields[3].parse().ok()?,
        l_quantity: fields[4].parse().ok()?,
        l_extendedprice: fields[5].parse().ok()?,
        l_discount: fields[6].parse().ok()?,
        l_tax: fields[7].parse().ok()?,
        l_returnflag: fields[8].to_string(),
        l_linestatus: fields[9].to_string(),
        l_shipdate: fields[10].to_string(),
        l_commitdate: fields[11].to_string(),
        l_receiptdate: fields[12].to_string(),
        l_shipinstruct: fields[13].to_string(),
        l_shipmode: fields[14].to_string(),
        l_comment: fields[15].to_string(),
    })
}

pub fn load_lineitem_table(path: &Path) -> Vec<LineItem> {
    let file = File::open(path).expect(&format!("Failed to open file: {:?}", path));
    let reader = BufReader::new(file);
    let mut items = Vec::new();

    for line in reader.lines() {
        let line = line.expect("Failed to read line");
        if let Some(item) = parse_lineitem(&line) {
            items.push(item);
        }
    }

    items
}

pub struct LineItemDataSource {
    pub items: Vec<LineItem>,
}

impl LineItemDataSource {
    pub fn new(path: &str) -> Self {
        let items = load_lineitem_table(Path::new(path));
        println!("  - Loaded {} rows from {}", items.len(), path);
        Self { items }
    }

    pub fn get_items(&self) -> &Vec<LineItem> {
        &self.items
    }
}
